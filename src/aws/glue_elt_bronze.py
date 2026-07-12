import sys
import logging
from datetime import datetime, timezone
from functools import reduce

from pyspark.sql import functions as F
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
from awsglue.utils import getResolvedOptions

# Configura logging
log = logging.getLogger()
log.setLevel(logging.INFO)
handler = logging.StreamHandler(sys.stdout)
formatter = logging.Formatter("%(asctime)s | %(levelname)-8s | %(message)s")
handler.setFormatter(formatter)
log.handlers.clear()
log.addHandler(handler)

# Parametros do job
# JOB_NAME - nome do job
# BUCKET_RAW - bucket de dados raw
# BUCKET_BRONZE - bucket de dados bronze
args = getResolvedOptions(sys.argv, ["JOB_NAME", "BUCKET_RAW", "BUCKET_BRONZE"])

# Contexto Spark e Glue
sc = SparkContext()
glue_context = GlueContext(sc)
spark = glue_context.spark_session
job = Job(glue_context)
job.init(args["JOB_NAME"], args)

BUCKET_RAW = args["BUCKET_RAW"]
BUCKET_BRONZE = args["BUCKET_BRONZE"]

# ============================================================
# BASES PARAMETRIZADAS
# ============================================================

INPUT_BASE = f"s3a://{BUCKET_RAW}/extracted"
BRONZE_BASE = f"s3a://{BUCKET_BRONZE}"
ANOS = ["2023", "2024", "2025"]

# Timestamp único por execução (determinístico para todo o batch)
INGESTION_TS = datetime.now(timezone.utc)

# Sobrescreve apenas as partições afetadas
spark.conf.set("spark.sql.sources.partitionOverwriteMode", "dynamic")
spark.sparkContext.setLogLevel("WARN")

CSV_OPTS = {
    "header": "true",
    "sep": ";",
    "encoding": "ISO-8859-1",
}

# --------------------------------------------------------------------
# Contratos de tipo (codigo=string ; medida=double ; flag=int)
# --------------------------------------------------------------------
SCHEMA_TS_ALUNO = [
    ("NU_ANO_AVALIACAO", "int"),
    ("CO_UF", "string"),
    ("SG_UF", "string"),
    ("ID_ALUNO", "string"),
    ("TP_SERIE", "string"),
    ("ID_ESCOLA", "string"),
    ("TP_DEPENDENCIA", "string"),
    ("CO_MUNICIPIO", "string"),
    ("NO_MUNICIPIO", "string"),
    ("IN_PRESENCA_LP", "int"),
    ("IN_PREENCHIMENTO_LP", "int"),
    ("CO_CADERNO_LP", "string"),
    ("VL_PESO_ALUNO_LP", "double"),
    ("VL_PROFICIENCIA_LP", "double"),
    ("IN_ALFABETIZADO", "int"),
]
SCHEMA_TS_MUNICIPIO = [
    ("NU_ANO_AVALIACAO", "int"),
    ("CO_UF", "string"),
    ("SG_UF", "string"),
    ("CO_MUNICIPIO", "string"),
    ("NO_MUNICIPIO", "string"),
    ("TP_SERIE", "string"),
    ("ID_TIPO_REDE", "string"),
    ("PC_ALUNO_ALFABETIZADO", "double"),
    ("VL_MEDIA_LP", "double"),
]
SCHEMA_TS_ESTADO = [
    ("NU_ANO_AVALIACAO", "int"),
    ("CO_UF", "string"),
    ("SG_UF", "string"),
    ("TP_SERIE", "string"),
    ("ID_TIPO_REDE", "string"),
    ("PC_ALUNO_ALFABETIZADO", "double"),
    ("VL_MEDIA_LP", "double"),
]


# ============================================================
# FUNÇÃO GENÉRICA PARA INGESTÃO BRONZE DE CSV
# ============================================================
#
# Esta função lê os arquivos ano a ano, valida se as colunas
# esperadas existem pelo nome, aplica os tipos definidos no
# contrato, adiciona metadados de rastreabilidade e grava o
# resultado em Parquet no S3.
#
# Essa abordagem é importante porque os arquivos podem mudar
# entre anos. Por exemplo, em 2025 o arquivo TS_ALUNO.csv
# passou a conter novas colunas. Como a função seleciona os
# campos pelo nome, novas colunas fora do contrato são
# ignoradas, e mudanças de posição não corrompem os dados.
#
# Se uma coluna esperada mudar de nome ou desaparecer, a
# execução falha com mensagem clara, evitando erro silencioso
# na pipeline.
#
# Entrada:
#
#   entidade  Nome da entidade no destino Bronze.
#   arquivo   Nome do CSV dentro de cada pasta anual.
#   schema    Lista com pares de nome da coluna e tipo esperado.
#
# Saída:
#
#   DataFrame Spark tipado e gravado em:
#   s3a://{BUCKET_BRONZE}/{entidade}/
#
def bronze_csv(entidade, arquivo, schema):
    # Extrai do contrato apenas os nomes das colunas esperadas.
    nomes = [n for n, _ in schema]

    # Lista usada para armazenar o DataFrame de cada ano antes da união.
    partes = []

    for ano in ANOS:
        # Lê um ano por vez para que cada arquivo use seu próprio cabeçalho.
        # Isso protege contra mudanças na posição das colunas entre os anos.
        raw = spark.read.options(**CSV_OPTS).csv(f"{INPUT_BASE}/{ano}/{arquivo}")

        # Valida se todas as colunas do contrato existem no arquivo lido.
        # Se alguma coluna esperada estiver ausente, a execução é interrompida.
        faltando = [n for n in nomes if n not in raw.columns]
        assert not faltando, f"{arquivo} ({ano}) sem colunas do contrato: {faltando}"

        # Seleciona as colunas pelo nome e aplica os tipos definidos no contrato.
        dados = [F.col(n).cast(t).alias(n) for n, t in schema]

        # Adiciona metadados técnicos para rastrear a origem e o momento da ingestão.
        partes.append(raw.select(
            *dados,
            F.input_file_name().alias("_source_file"),
            F.lit(INGESTION_TS).alias("_ingestion_timestamp"),
        ))

    # Une os DataFrames anuais pelo nome das colunas.
    df = reduce(lambda a, b: a.unionByName(b), partes)

    # Grava a entidade na Bronze em formato Parquet, particionada por ano.
    (df.write.mode("overwrite").partitionBy("NU_ANO_AVALIACAO")
        .parquet(f"{BRONZE_BASE}/{entidade}"))

    return df


# ============================================================
# BRONZE: CONFIGURAÇÃO DA INGESTÃO DAS PLANILHAS DE METAS
# ============================================================
#
# São configurados:
#   - o leitor Spark para arquivos Excel;
#   - os contratos (schemas) utilizados na leitura;
#   - uma função auxiliar para reduzir repetição na definição
#     das colunas de metas.
#
# As planilhas de metas são tratadas separadamente dos arquivos CSV,
# pois apresentam estrutura própria e diferenças de schema entre
# os anos de publicação.

# Biblioteca Spark utilizada para leitura das planilhas XLSX.
SPARK_EXCEL = "com.crealytics.spark.excel"

# O contrato possui o ano no nome porque é válido apenas para
# as planilhas publicadas em 2023. Os schemas mudam entre anos,
# portanto cada versão possui seu próprio contrato.
SCHEMA_METAS_MUN_2023 = [
    ("ANO", "int"),
    ("CO_UF", "string"), ("SG_UF", "string"),
    ("CO_MUNICIPIO", "string"), ("NO_MUNICIPIO", "string"),
    ("NO_TP_REDE", "string"),
    ("PC_ALUNO_ALFABETIZADO", "double"),
    ("META_FINAL_2024", "double"), ("META_FINAL_2025", "double"),
    ("META_FINAL_2026", "double"), ("META_FINAL_2027", "double"),
    ("META_FINAL_2028", "double"), ("META_FINAL_2029", "double"),
    ("META_FINAL_2030", "double"),
    # Valores "-" são convertidos para null durante o cast.
    # Valores de 0 a 5 representam níveis válidos.
    ("NIVEIS_ALFABETIZACAO_2023", "int"),
    ("PC_AVALIADOS_LP", "double"),
]

SCHEMA_METAS_UF_2023 = [
    ("ANO", "int"),
    ("CD_UF", "string"),                       # PRESERVADO no Bronze (Silver -> CO_UF)
    ("SIGLA_UF", "string"), ("NOME_UF", "string"),
    ("REDE", "string"),
    ("SAEB_2019", "double"), ("SAEB_2021", "double"),   # numéricos limpos
    ("PC_ALUNO_ALFABETIZADO", "double"),                # só '- ' (suprimido) -> null
    # As metas permanecem como string na Bronze porque algumas
    # planilhas utilizam valores como ">80" ou "> 80".
    #
    # A conversão para valor numérico será realizada na Silver,
    # preservando a informação original durante a ingestão.
    ("META_FINAL_2024", "string"), ("META_FINAL_2025", "string"),
    ("META_FINAL_2026", "string"), ("META_FINAL_2027", "string"),
    ("META_FINAL_2028", "string"), ("META_FINAL_2029", "string"),
    ("META_FINAL_2030", "string"),
    ("PC_AVALIADOS_LP", "double"),                      # só '- ' (suprimido) -> null
]


def metas(tipo):   # gera META_FINAL_2024..2030 do mesmo tipo
    """
    Gera automaticamente as colunas META_FINAL_2024 até
    META_FINAL_2030 utilizando o tipo informado.

    Isso evita repetição na definição dos contratos.
    """
    return [(f"META_FINAL_{a}", tipo) for a in range(2024, 2031)]


# ------------------------------------------------------------
# MUNICÍPIOS - PLANILHAS 2024
#
# As metas são numéricas e podem ser lidas diretamente como
# double. O schema já utiliza a nomenclatura presente nas
# planilhas publicadas em 2024.
# ------------------------------------------------------------
SCHEMA_METAS_MUN_2024 = [
    ("ANO", "int"), ("CO_UF", "string"), ("SG_UF", "string"),
    ("CO_MUNICIPIO", "string"), ("NO_MUNICIPIO", "string"), ("NO_TP_REDE", "string"),
    ("PC_ALUNO_ALFABETIZADO_2023", "double"), ("PC_ALUNO_ALFABETIZADO_2024", "double"),
    *metas("double"),
    ("CO_NIVEL_ALFABETIZACAO", "int"),
    ("PC_AVALIADOS_LP", "double"),
]
SCHEMA_METAS_MUN_2025 = [
    ("ANO", "int"), ("CO_UF", "string"), ("SG_UF", "string"),
    ("CO_MUNICIPIO", "string"), ("NO_MUNICIPIO", "string"), ("NO_TP_REDE", "string"),
    ("PC_ALUNO_ALFABETIZADO_2023", "double"), ("PC_ALUNO_ALFABETIZADO_2024", "double"),
    ("PC_ALUNO_ALFABETIZADO_2025", "double"),
    *metas("double"),
    ("CO_NIVEL_ALFABETIZACAO", "int"),
    ("PC_AVALIADOS_LP", "double"),
]

# ------------------------------------------------------------
# UF - PLANILHAS 2024
#
# As metas permanecem como string para preservar valores como
# ">80". A normalização será realizada posteriormente na
# camada Silver.
# ------------------------------------------------------------
SCHEMA_METAS_UF_2024 = [
    ("ANO", "int"), ("CD_UF", "string"), ("SIGLA_UF", "string"), ("NOME_UF", "string"),
    ("REDE", "string"),
    ("PC_ALUNO_ALFABETIZADO_2023", "double"), ("PC_ALUNO_ALFABETIZADO_2024", "double"),
    *metas("string"),
    ("PC_AVALIADOS_LP", "double"),
]
SCHEMA_METAS_UF_2025 = [
    ("ANO", "int"), ("CD_UF", "string"), ("SIGLA_UF", "string"), ("NOME_UF", "string"),
    ("REDE", "string"),
    ("PC_ALUNO_ALFABETIZADO_2023", "double"), ("PC_ALUNO_ALFABETIZADO_2024", "double"),
    ("PC_ALUNO_ALFABETIZADO_2025", "double"),
    *metas("string"),
    ("PC_AVALIADOS_LP", "double"),
]


# ============================================================
# FUNÇÃO DE INGESTÃO DAS PLANILHAS XLSX
# ============================================================
#
# Realiza a ingestão de uma planilha XLSX para a camada Bronze.
#
# Etapas executadas:
#   1. Lê a planilha utilizando o conector Spark Excel.
#   2. Valida se todas as colunas previstas no contrato existem.
#   3. Aplica o schema definido para a combinação
#      (entidade, ano).
#   4. Adiciona metadados de rastreabilidade.
#   5. Remove registros que não representam dados válidos.
#   6. Grava o resultado em formato Parquet, particionado por ano.
def bronze_xlsx(entidade, arquivo, sheet, schema, ano):

    # Caminho da planilha de origem no Data Lake.
    caminho = f"{INPUT_BASE}/{ano}/{arquivo}"

    # Lê a planilha preservando todos os valores como texto.
    # A conversão de tipos será realizada utilizando o contrato.
    raw = (
        spark.read.format(SPARK_EXCEL)
        .option("dataAddress", f"'{sheet}'!A2")
        .option("header", "true")
        .option("inferSchema", "false")
        .load(caminho)
    )

    # Verifica se todas as colunas esperadas pelo contrato
    # estão presentes na planilha.
    nomes = [n for n, _ in schema]
    faltando = [n for n in nomes if n not in raw.columns]
    assert not faltando, f"{arquivo}: sem colunas do contrato: {faltando}"

    # Seleciona apenas as colunas do contrato e realiza
    # a conversão para os tipos definidos.
    dados = [F.col(n).cast(t).alias(n) for n, t in schema]

    # Adiciona metadados de rastreabilidade da ingestão.
    df = (
        raw.select(
            *dados,
            F.input_file_name().alias("_source_file"),
            F.lit(INGESTION_TS).alias("_ingestion_timestamp")
        )
        .withColumnRenamed("ANO", "NU_ANO_AVALIACAO")
    )

    # Mantém apenas registros que representam dados válidos.
    # Linhas de rodapé, observações ou registros em branco
    # possuem o ano nulo após a conversão e são descartadas.
    df = df.filter(F.col("NU_ANO_AVALIACAO").isNotNull())

    # Grava os dados na camada Bronze em formato Parquet,
    # particionados pelo ano da avaliação.
    (
        df.write
        .mode("overwrite")
        .partitionBy("NU_ANO_AVALIACAO")
        .parquet(f"{BRONZE_BASE}/{entidade}")
    )

    return df

# Anos que a carga completa deve conter. Semântica de SUBCONJUNTO:
# todos os esperados presentes; anos extras (ex.: 2026) NÃO reprovam.
ANOS_ESPERADOS = {int(a) for a in ANOS}

def checar_anos(df, entidade):
    presentes = {r[0] for r in df.select("NU_ANO_AVALIACAO").distinct().collect()}
    faltando = ANOS_ESPERADOS - presentes
    if faltando:
        raise ValueError(
            f"{entidade}: anos esperados ausentes -> {sorted(faltando)} "
            f"(presentes: {sorted(presentes)})"
        )
    log.info("%s: anos OK -> %s", entidade, sorted(presentes))

def checar_aluno(df, entidade):
    # 1 leitura: proficiência nula (diagnóstico) + invariante ausente => sem nota.
    m = df.agg(
        F.sum(F.col("VL_PROFICIENCIA_LP").isNull().cast("long")).alias("prof_nula"),
        F.sum(
            ((F.col("IN_PRESENCA_LP") == 0) & F.col("VL_PROFICIENCIA_LP").isNotNull())
            .cast("long")
        ).alias("viol"),
    ).collect()[0]
    log.info("%s: proficiência nula = %s", entidade, f"{m['prof_nula']:,}")
    if m["viol"] == 0:
        log.info("%s: invariante ausente=>sem-nota OK (0 violações)", entidade)
    else:
        log.warning(
            "%s: %s aluno(s) ausente(s) COM proficiência (anomalia)",
            entidade, f"{m['viol']:,}",
        )

def main():
    log.info("Iniciando job Bronze...")
    log.info("INPUT_BASE  : %s", INPUT_BASE)
    log.info("BRONZE_BASE : %s", BRONZE_BASE)

    # ============================================================
    # EXECUÇÃO DA INGESTÃO BRONZE DOS CSVS
    # ============================================================
    #
    # Saídas no S3:
    #
    #   s3a://{BUCKET_BRONZE}/ts_aluno/
    #   s3a://{BUCKET_BRONZE}/ts_municipio/
    #   s3a://{BUCKET_BRONZE}/ts_estado/
    #
    log.info("Ingerindo CSVs para a Bronze...")
    df_aluno = bronze_csv("ts_aluno", "TS_ALUNO.csv", SCHEMA_TS_ALUNO)
    df_municipio = bronze_csv("ts_municipio", "TS_MUNICIPIO.csv", SCHEMA_TS_MUNICIPIO)
    df_estado = bronze_csv("ts_estado", "TS_ESTADO.csv", SCHEMA_TS_ESTADO)

    # ============================================================
    # VALIDAÇÃO DA CAMADA BRONZE DOS CSVS
    # ============================================================
    for entidade in ["ts_aluno", "ts_municipio", "ts_estado"]:
        dfc = spark.read.parquet(f"{BRONZE_BASE}/{entidade}")
        checar_anos(dfc, entidade)
        log.info("%s: %s linhas", entidade, f"{dfc.count():,}")    

    checar_aluno(spark.read.parquet(f"{BRONZE_BASE}/ts_aluno"), "ts_aluno")

    # ============================================================
    # EXECUÇÃO DA INGESTÃO DAS PLANILHAS DE METAS
    # ============================================================
    #
    # Saídas no S3:
    #
    #   s3a://{BUCKET_BRONZE}/metas_municipios/
    #   s3a://{BUCKET_BRONZE}/metas_ufs/
    #
    log.info("Ingerindo planilhas XLSX de metas para a Bronze...")

    # Metas 2023
    bronze_xlsx("metas_municipios", "resultados_e_metas_municipios.xlsx",
                "Divulgação Alfabet Municipio", SCHEMA_METAS_MUN_2023, "2023")
    bronze_xlsx("metas_ufs", "resultados_e_metas_ufs.xlsx",
                "Divulgação Alfabet UF e Brasil", SCHEMA_METAS_UF_2023, "2023")

    # Metas 2024
    bronze_xlsx("metas_municipios", "resultados_e_metas_municipios_2024.xlsx",
                "Divulgação Alfabet Municipio", SCHEMA_METAS_MUN_2024, "2024")
    bronze_xlsx("metas_ufs", "resultados_e_metas_ufs_2024_2.xlsx",
                "Divulgação Alfabet UF e Brasil", SCHEMA_METAS_UF_2024, "2024")

    # Metas 2025
    bronze_xlsx("metas_municipios", "resultados_e_metas_municipios_2025_v2.xlsx",
                "Divulgação Alfabet Municipio", SCHEMA_METAS_MUN_2025, "2025")
    bronze_xlsx("metas_ufs", "resultados_e_metas_ufs_2025_v1.xlsx",
                "Divulgação Alfabet UF e Brasil", SCHEMA_METAS_UF_2025, "2025")

    # ============================================================
    # VALIDAÇÃO DA INGESTÃO DAS PLANILHAS DE METAS
    # ============================================================
    for ent in ["metas_municipios", "metas_ufs"]:
        d = (
            spark.read
            .option("mergeSchema", "true")
            .parquet(f"{BRONZE_BASE}/{ent}")
        )

        checar_anos(d, ent)
        log.info("%s: %s linhas | %s colunas", ent, f"{d.count():,}", len(d.columns))

    uf = (
        spark.read
        .option("mergeSchema", "true")
        .parquet(f"{BRONZE_BASE}/metas_ufs")
    )

    log.info("Job Bronze concluído com sucesso!")
    job.commit()


main()
