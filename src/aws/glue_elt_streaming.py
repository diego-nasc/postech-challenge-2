import sys
import time
import logging
import random
import threading
from datetime import datetime, timezone

import boto3
from pyspark import StorageLevel
from pyspark.sql import functions as F
from pyspark.sql import Window
from pyspark.sql.types import (
    StructType,
    StructField,
    IntegerType,
    StringType,
    DoubleType,
)
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
from awsglue.utils import getResolvedOptions

# ============================================================
# CONTINUACAO DO PIPELINE MEDALHAO (raw -> bronze -> silver -> gold)
# ============================================================
# Este job estende o pipeline BATCH com uma etapa de INGESTAO VIA STRUCTURED
# STREAMING para o ANO CORRENTE (2026). Ele NAO substitui o batch: escreve nas
# MESMAS tabelas Silver/Gold ja produzidas por ele, tocando EXCLUSIVAMENTE a
# particao ano=2026. Com partitionOverwriteMode=dynamic, as particoes
# historicas (2023-2025) sao preservadas.
#
# Contrato (alinhado 1:1 ao batch, decisao (i)):
#   - Silver: particionada SOMENTE por 'ano' (igual a glue_elt_silver); mesmo
#     conjunto de colunas (inclui _silver_processed_at, sem colunas extras).
#   - Gold: SQL identico ao glue_elt_gold (mesmos rotulos ACENTUADOS e mesmo
#     schema, inclui _gold_processed_at).
#
# Acumulacao (decisao (B)): a cada micro-lote, le o estado atual de ano=2026,
# une com os novos eventos, DEDUPLICA por (id_municipio, serie) preferindo o
# evento mais novo, e sobrescreve SOMENTE ano=2026 com o acumulado. Assim cada
# micro-lote substitui 2026 pelo conjunto completo ate aquele momento.
#
# Reproducibilidade (decisao: limpar sempre): no inicio limpa APENAS o prefixo
# de entrada do stream e o checkpoint (nunca Bronze/Silver/Gold). O catalogo de
# municipios e ordenado antes do collect() para casar com a seed fixa.

# Configura logging (mesmo formato dos demais jobs)
log = logging.getLogger()
log.setLevel(logging.INFO)
handler = logging.StreamHandler(sys.stdout)
formatter = logging.Formatter("%(asctime)s | %(levelname)-8s | %(message)s")
handler.setFormatter(formatter)
log.handlers.clear()
log.addHandler(handler)

# ============================================================
# PARAMETROS DO JOB
# ============================================================
# JOB_NAME      - nome do job
# BUCKET_RAW    - bucket raw; recebe os eventos crus simulados do stream
# BUCKET_SILVER - bucket da camada silver (mesmo do glue_elt_silver)
# BUCKET_GOLD   - bucket da camada gold (mesmo do glue_elt_gold)
args = getResolvedOptions(
    sys.argv,
    ["JOB_NAME", "BUCKET_RAW", "BUCKET_SILVER", "BUCKET_GOLD"],
)

# Contexto Spark e Glue
sc = SparkContext()
glue_context = GlueContext(sc)
spark = glue_context.spark_session
job = Job(glue_context)
job.init(args["JOB_NAME"], args)

BUCKET_RAW = args["BUCKET_RAW"]
BUCKET_SILVER = args["BUCKET_SILVER"]
BUCKET_GOLD = args["BUCKET_GOLD"]

# Sobrescreve apenas as particoes afetadas (ano=2026), como no batch.
spark.conf.set("spark.sql.sources.partitionOverwriteMode", "dynamic")
spark.sparkContext.setLogLevel("WARN")

# ============================================================
# BASES PARAMETRIZADAS (um bucket por camada, igual ao batch)
# ============================================================
SILVER_BASE = f"s3a://{BUCKET_SILVER}"
GOLD_BASE = f"s3a://{BUCKET_GOLD}"

# Tabelas do batch nas quais o stream escreve (particao ano=2026).
MUNICIPIO_PATH = f"{SILVER_BASE}/municipio"
META_SILVER_PATH = f"{SILVER_BASE}/meta_alfabetizacao_municipio"
GOLD_IND_MUN_PATH = f"{GOLD_BASE}/indicadores_municipio"

ANO_STREAM = 2026
ANO_CATALOGO = 2025  # municipios de referencia (ultimo ano do batch)

# --- Streaming: entrada e checkpoint ---
# Entrada: prefixo do bucket raw (o produtor grava CSVs, o readStream consome).
# Checkpoint: prefixo _checkpoints/ do bucket silver (NAO e uma tabela; fica
# fora de silver/municipio e silver/meta_alfabetizacao_municipio).
# Ambos os prefixos (e SOMENTE eles) sao limpos no inicio.
STREAM_INPUT_PREFIX = f"streaming/stream_input_{ANO_STREAM}"
STREAM_INPUT = f"s3a://{BUCKET_RAW}/{STREAM_INPUT_PREFIX}"
CKPT_PREFIX = f"_checkpoints/medalhao_{ANO_STREAM}"
CKPT = f"{SILVER_BASE}/{CKPT_PREFIX}"

TRIGGER = "5 seconds"

# Parametros da simulacao de entradas
N_EVENTOS = 200
N_POR_LOTE = 30
INTERVALO_S = 4
SEED = 2026
REDES = [3]  # a Gold filtra rede=3 (Municipal)

# Timestamp unico por execucao (deterministico para todo o batch de eventos),
# no mesmo espirito de SILVER_TS/GOLD_TS dos scripts batch.
STREAM_TS = datetime.now(timezone.utc)

# ============================================================
# SCHEMA DOS EVENTOS CRUS (schema TS_MUNICIPIO)
# ============================================================
SCHEMA_EVENTO = StructType([
    StructField("NU_ANO_AVALIACAO", IntegerType()),
    StructField("CO_UF", IntegerType()),
    StructField("SG_UF", StringType()),
    StructField("CO_MUNICIPIO", StringType()),
    StructField("NO_MUNICIPIO", StringType()),
    StructField("TP_SERIE", IntegerType()),
    StructField("ID_TIPO_REDE", IntegerType()),
    StructField("PC_ALUNO_ALFABETIZADO", DoubleType()),
    StructField("VL_MEDIA_LP", DoubleType()),
])

# Ordem das colunas nos CSVs crus gravados pelo produtor.
COLS_CSV = [
    "NU_ANO_AVALIACAO",
    "CO_UF",
    "SG_UF",
    "CO_MUNICIPIO",
    "NO_MUNICIPIO",
    "TP_SERIE",
    "ID_TIPO_REDE",
    "PC_ALUNO_ALFABETIZADO",
    "VL_MEDIA_LP",
]

# Cliente boto3 para o produtor gravar os eventos crus no S3.
s3_client = boto3.client("s3")


# ============================================================
# GOLD: SQL IDENTICO AO glue_elt_gold (indicadores_municipio)
# ============================================================
# Copiado VERBATIM do batch para garantir mesmo schema e mesmos rotulos
# (ACENTUADOS: 'Proximo'->'Próximo', 'Media'->'Média'). As views
# 'silver_municipio' (o acumulado de 2026) e 'silver_meta_municipio' (metas do
# batch) sao registradas antes de cada consulta.
#
# NOTA (limitacao documentada): 'part' e a participacao vem da tabela de metas,
# cujo 'ano' e o ANO DE PUBLICACAO (2023-2025). Nao existe publicacao 2026,
# entao para ano=2026 o LEFT JOIN de participacao nao casa e
# percentual_participacao / faixa_participacao ficam NULL -- correto, pois nao
# ha participacao oficial para o ano simulado. 'meta' (e portanto
# distancia_meta / atingiu_meta / categoria_desempenho) e populada normalmente,
# pois META_FINAL_2026 existe nas publicacoes.
GOLD_SQL = """
WITH
-- (1) META -> LONG + COALESCE latest-non-null: 1 linha por (municipio, ano-alvo)
meta_long_raw AS (
    SELECT ano AS ano_pub, id_municipio,
        stack(7,
            2024, meta_alfabetizacao_2024, 2025, meta_alfabetizacao_2025,
            2026, meta_alfabetizacao_2026, 2027, meta_alfabetizacao_2027,
            2028, meta_alfabetizacao_2028, 2029, meta_alfabetizacao_2029,
            2030, meta_alfabetizacao_2030
        ) AS (ano, meta)
    FROM silver_meta_municipio
),
meta_long AS (
    SELECT id_municipio, ano, meta FROM (
        SELECT *, ROW_NUMBER() OVER (
            PARTITION BY id_municipio, ano
            ORDER BY (meta IS NOT NULL) DESC, ano_pub DESC   -- nao-nulo, depois publicacao mais nova
        ) rn FROM meta_long_raw
    ) WHERE rn = 1 AND meta IS NOT NULL
),
-- (2) RESULTADO no nivel da meta (rede=3, Municipal) -- fonte canonica da taxa
res AS (
    SELECT ano, id_municipio, nome_municipio, id_uf, sigla_uf,
        taxa_alfabetizacao, media_portugues
    FROM silver_municipio
    WHERE rede = 3
),
-- (3) PARTICIPACAO (por ano de avaliacao) -- so existe na tabela de meta
part AS (
    SELECT ano, id_municipio, percentual_participacao
    FROM silver_meta_municipio
)
SELECT
    r.ano,
    r.id_municipio,
    r.nome_municipio,
    r.id_uf,
    r.sigla_uf,
    3                                        AS rede,   -- explicito: comparacao no nivel Municipal
    r.taxa_alfabetizacao,
    r.media_portugues,
    p.percentual_participacao,
    ml.meta,
    ROUND(r.taxa_alfabetizacao - ml.meta, 2)                        AS distancia_meta,
    CASE WHEN ml.meta IS NULL THEN NULL
        ELSE (r.taxa_alfabetizacao - ml.meta) >= 0 END            AS atingiu_meta,
    CASE WHEN ml.meta IS NULL                      THEN NULL
        WHEN r.taxa_alfabetizacao - ml.meta >= 5  THEN 'Muito acima'
        WHEN r.taxa_alfabetizacao - ml.meta >= 0  THEN 'Acima'
        WHEN r.taxa_alfabetizacao - ml.meta >= -5 THEN 'Próximo'
        ELSE 'Muito abaixo' END                                   AS categoria_desempenho,
    CASE WHEN p.percentual_participacao IS NULL THEN NULL
        WHEN p.percentual_participacao >= 95   THEN 'Alta'
        WHEN p.percentual_participacao >= 80   THEN 'Média'
        ELSE 'Baixa' END                                          AS faixa_participacao
FROM res r
LEFT JOIN part      p  ON r.ano = p.ano AND r.id_municipio = p.id_municipio
LEFT JOIN meta_long ml ON r.ano = ml.ano AND r.id_municipio = ml.id_municipio
"""


def add_gold_metadata(df):
    # Mesmo carimbo do batch (glue_elt_gold), para o schema da Gold casar 1:1.
    return df.withColumn("_gold_processed_at", F.lit(STREAM_TS))


# ============================================================
# HELPER: verifica existencia de tabela no lake
# ============================================================
def existe_no_lake(path):
    jvm = spark._jvm
    hconf = spark._jsc.hadoopConfiguration()
    p = jvm.org.apache.hadoop.fs.Path(path)
    fs = p.getFileSystem(hconf)
    return fs.exists(p)


# ============================================================
# LIMPEZA DE PREFIXO (usada SO para entrada do stream e checkpoint)
# ============================================================
def limpar_prefixo(bucket, prefix):
    # Remove todos os objetos sob 'prefix'. Restrito, por contrato, a:
    #   - raw/streaming/stream_input_2026/   (entrada do stream)
    #   - silver/_checkpoints/medalhao_2026/ (checkpoint do stream)
    # NUNCA e chamado sobre os prefixos de dados (bronze/silver/gold tabelas).
    paginador = s3_client.get_paginator("list_objects_v2")
    for pagina in paginador.paginate(Bucket=bucket, Prefix=prefix):
        objetos = [{"Key": o["Key"]} for o in pagina.get("Contents", [])]
        if objetos:
            s3_client.delete_objects(Bucket=bucket, Delete={"Objects": objetos})


# ============================================================
# SIMULACAO DAS ENTRADAS CRUAS DE 2026
# ============================================================
# Baseada nos municipios reais do ultimo ano do batch (2025), lidos da
# silver/municipio. orderBy antes do collect() -> ordem estavel -> a seed fixa
# produz sempre os mesmos eventos (reproducibilidade).
def simular_eventos():
    catalogo = (
        spark.read.parquet(MUNICIPIO_PATH)
        .filter(F.col("ano") == ANO_CATALOGO)
        .select("id_uf", "sigla_uf", "id_municipio", "nome_municipio", "serie")
        .dropDuplicates(["id_municipio", "serie"])
        .orderBy("id_municipio", "serie")   # <-- reproducibilidade com a seed
        .collect()
    )
    if not catalogo:
        raise ValueError(
            f"Catalogo vazio: sem municipios em {MUNICIPIO_PATH} "
            f"para ano={ANO_CATALOGO}. Execute o pipeline batch antes."
        )

    random.seed(SEED)
    eventos = []
    for _ in range(N_EVENTOS):
        m = random.choice(catalogo)
        eventos.append({
            "NU_ANO_AVALIACAO": ANO_STREAM,
            "CO_UF": m.id_uf,
            "SG_UF": m.sigla_uf,
            "CO_MUNICIPIO": int(m.id_municipio),
            "NO_MUNICIPIO": m.nome_municipio,
            "TP_SERIE": m.serie,
            "ID_TIPO_REDE": random.choice(REDES),
            "PC_ALUNO_ALFABETIZADO": round(random.uniform(45, 95), 2),
            "VL_MEDIA_LP": round(random.uniform(730, 800), 2),
        })
    random.shuffle(eventos)  # UFs/redes intercaladas entre os lotes
    log.info("Eventos simulados: %d", len(eventos))
    return eventos


# ============================================================
# GARANTE QUE O PREFIXO DE ENTRADA DO STREAM EXISTE (S3)
# ============================================================
# O file source de Structured Streaming exige que o diretorio-base exista.
# No S3 nao ha diretorio real: sem nenhum objeto sob o prefixo, o readStream
# lanca "Path does not exist" na montagem do stream. Criamos um marcador
# "_init" (o leitor CSV ignora nomes iniciados por "_" ou ".") apenas para
# materializar o prefixo antes do readStream.
def garantir_prefixo_entrada():
    s3_client.put_object(
        Bucket=BUCKET_RAW,
        Key=f"{STREAM_INPUT_PREFIX}/_init",
        Body=b"",
    )


# ============================================================
# PRODUTOR: grava micro-lotes CSV crus no S3 (prefixo raw)
# ============================================================
def escrever_lote(rows, nome):
    linhas = [";".join(COLS_CSV)]
    for r in rows:
        linhas.append(";".join(str(r[c]) for c in COLS_CSV))
    corpo = "\n".join(linhas).encode("utf-8")
    s3_client.put_object(
        Bucket=BUCKET_RAW,
        Key=f"{STREAM_INPUT_PREFIX}/{nome}",
        Body=corpo,
    )


def produzir(eventos):
    for n, i in enumerate(range(0, len(eventos), N_POR_LOTE)):
        lote = eventos[i:i + N_POR_LOTE]
        escrever_lote(lote, f"lote_{n:03d}.csv")
        log.info("[PRODUTOR] lote_%03d.csv -> %d eventos", n, len(lote))
        time.sleep(INTERVALO_S)


# ============================================================
# TRANSFORM SILVER (row-wise, fiel ao glue_elt_silver / notebook 04)
# ============================================================
# Schema IDENTICO ao Silver batch de municipio: sem colunas extras; apenas o
# carimbo _silver_processed_at (que o batch tambem grava).
def transform_silver(df_raw):
    return (
        df_raw
        .filter(F.col("CO_MUNICIPIO").isNotNull())
        .select(
            F.col("NU_ANO_AVALIACAO").cast("int").alias("ano"),
            F.lpad(F.trim(F.col("CO_MUNICIPIO").cast("string")), 7, "0").alias("id_municipio"),
            F.trim(F.col("NO_MUNICIPIO")).alias("nome_municipio"),
            F.col("CO_UF").cast("int").alias("id_uf"),
            F.col("SG_UF").alias("sigla_uf"),
            F.col("TP_SERIE").cast("int").alias("serie"),
            F.col("ID_TIPO_REDE").cast("int").alias("rede"),
            F.col("PC_ALUNO_ALFABETIZADO").cast("double").alias("taxa_alfabetizacao"),
            F.col("VL_MEDIA_LP").cast("double").alias("media_portugues"),
        )
        .withColumn("_silver_processed_at", F.lit(STREAM_TS))
    )


# ============================================================
# PROCESSAMENTO DO MICRO-BATCH (foreachBatch) -- ACUMULA (decisao B)
# ============================================================
# A cada micro-lote:
#   1. le o estado atual da particao ano=2026 (vazio na 1a vez);
#   2. une com os eventos novos e DEDUPLICA por (id_municipio, serie),
#      preferindo o evento mais novo (_ord=1) sobre o acumulado (_ord=0);
#   3. materializa o acumulado (persist+count) para quebrar a linhagem antes de
#      sobrescrever a MESMA particao (evita ler-e-apagar o mesmo caminho);
#   4. overwrite dinamico grava SOMENTE ano=2026 na Silver;
#   5. reconstroi a Gold do ano a partir do acumulado (reusa o DF em memoria,
#      sem reler o S3) e overwrite dinamico grava SOMENTE ano=2026 na Gold.
# Overwrite dinamico garante que 2023-2025 nunca sao tocados.
def process_micro_batch(df_raw, epoch_id):
    if df_raw.rdd.isEmpty():
        log.info("[BATCH %s] vazio, ignorado.", epoch_id)
        return

    novos = transform_silver(df_raw)

    # Estado atual de 2026 (mesmo schema; 0 linhas na primeira passada).
    atual = spark.read.parquet(MUNICIPIO_PATH).filter(F.col("ano") == ANO_STREAM)

    # Dedup (id_municipio, serie): novo (_ord=1) vence o acumulado (_ord=0);
    # desempate deterministico por taxa/media para reproducibilidade.
    w = Window.partitionBy("id_municipio", "serie").orderBy(
        F.col("_ord").desc(),
        F.col("taxa_alfabetizacao").desc(),
        F.col("media_portugues").desc(),
    )
    combinado = (
        atual.withColumn("_ord", F.lit(0))
        .unionByName(novos.withColumn("_ord", F.lit(1)))
        .withColumn("_rn", F.row_number().over(w))
        .filter(F.col("_rn") == 1)
        .drop("_ord", "_rn")
    )

    # Materializa para desacoplar da fonte antes do overwrite da mesma particao.
    combinado = combinado.persist(StorageLevel.MEMORY_AND_DISK)
    n = combinado.count()

    # SILVER: overwrite dinamico -> substitui so ano=2026, preserva 2023-2025.
    (combinado.write
        .mode("overwrite")
        .partitionBy("ano")
        .parquet(MUNICIPIO_PATH))

    # GOLD: reconstroi a partir do acumulado (reusa 'combinado' em memoria).
    combinado.createOrReplaceTempView("silver_municipio")   # 'res' aplica WHERE rede=3
    df_gold = add_gold_metadata(spark.sql(GOLD_SQL))
    (df_gold.write
        .mode("overwrite")
        .partitionBy("ano")
        .parquet(GOLD_IND_MUN_PATH))

    combinado.unpersist()
    log.info("[BATCH %s] acumulado ano=%s: %s municipios (silver+gold gravados).",
             epoch_id, ANO_STREAM, f"{n:,}")


# ============================================================
# MAIN
# ============================================================
def main():
    log.info("SILVER_BASE : %s", SILVER_BASE)
    log.info("GOLD_BASE   : %s", GOLD_BASE)
    log.info("STREAM_INPUT: %s", STREAM_INPUT)
    log.info("CKPT        : %s", CKPT)

    # A meta do ano corrente ja foi gravada pelo batch (glue_elt_silver).
    # Aqui apenas validamos que a tabela existe; nao ha bootstrap de meta.
    if not existe_no_lake(META_SILVER_PATH):
        raise FileNotFoundError(
            f"Tabela Silver nao encontrada: {META_SILVER_PATH}. "
            "Execute o pipeline batch (glue_elt_silver) antes da ingestao streaming."
        )

    # Registra a META uma unica vez (cacheada) -> evita reler o S3 por micro-lote.
    meta_df = spark.read.parquet(META_SILVER_PATH).persist(StorageLevel.MEMORY_AND_DISK)
    meta_df.createOrReplaceTempView("silver_meta_municipio")
    meta_df.count()

    eventos = simular_eventos()

    # Encerra streams ativos e LIMPA apenas entrada + checkpoint (contrato).
    for q in spark.streams.active:
        q.stop()
    limpar_prefixo(BUCKET_RAW, STREAM_INPUT_PREFIX + "/")   # entrada do stream
    limpar_prefixo(BUCKET_SILVER, CKPT_PREFIX + "/")        # checkpoint (nao e tabela)

    # Materializa o prefixo de entrada ANTES do readStream (senao: Path does not exist).
    garantir_prefixo_entrada()

    df_stream = (
        spark.readStream
        .schema(SCHEMA_EVENTO)
        .option("header", True)
        .option("sep", ";")
        .csv(STREAM_INPUT)
    )

    query = (
        df_stream.writeStream
        .foreachBatch(process_micro_batch)
        .option("checkpointLocation", CKPT)
        .outputMode("append")
        .trigger(processingTime=TRIGGER)
        .start()
    )

    # Produtor em thread: grava os micro-lotes crus no S3 enquanto o stream consome.
    produtor = threading.Thread(target=produzir, args=(eventos,), daemon=False)
    produtor.start()
    produtor.join()

    # Drena o que restou e encerra o stream (job FINITO -> termina em SUCCEEDED).
    query.processAllAvailable()
    query.stop()
    for q in spark.streams.active:
        q.stop()

    # Validacao final: a Gold do ano corrente aparece junto dos anos do batch.
    gold = spark.read.parquet(GOLD_IND_MUN_PATH)
    log.info("Registros por ano na gold/indicadores_municipio:")
    gold.groupBy("ano").count().orderBy("ano").show()
    log.info("Amostra dos indicadores de %s:", ANO_STREAM)
    (gold.filter(F.col("ano") == ANO_STREAM)
        .select("ano", "id_municipio", "sigla_uf", "taxa_alfabetizacao",
                "meta", "distancia_meta", "atingiu_meta", "categoria_desempenho")
        .show(10, truncate=False))

    log.info("Ingestao streaming concluida.")
    job.commit()


main()
