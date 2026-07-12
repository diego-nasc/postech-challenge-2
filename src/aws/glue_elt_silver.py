import sys
import logging
from datetime import datetime, timezone

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
# BUCKET_BRONZE - bucket de dados bronze
# BUCKET_SILVER - bucket de dados silver
args = getResolvedOptions(sys.argv, ["JOB_NAME", "BUCKET_BRONZE", "BUCKET_SILVER"])

# Contexto Spark e Glue
sc = SparkContext()
glue_context = GlueContext(sc)
spark = glue_context.spark_session
job = Job(glue_context)
job.init(args["JOB_NAME"], args)

BUCKET_BRONZE = args["BUCKET_BRONZE"]
BUCKET_SILVER = args["BUCKET_SILVER"]
BRONZE_BASE = f"s3a://{BUCKET_BRONZE}"
SILVER_BASE = f"s3a://{BUCKET_SILVER}"

spark.conf.set("spark.sql.sources.partitionOverwriteMode", "dynamic")
spark.sparkContext.setLogLevel("WARN")

# Timestamp único da execução Silver (determinístico p/ todo o batch, via F.lit)
SILVER_TS = datetime.now(timezone.utc)

# --- Trechos de SQL reutilizados pelas 3 tabelas de META ---
def norm_pct(col):
    # Normaliza percentual das PLANILHAS DE META p/ DOUBLE.
    # '>80'/'> 80 ' -> 80.0  |  '- '/vazio/não-numérico -> NULL  |  número (ponto) -> double
    # Vírgula decimal: NÃO observada em 2023-2025 nos arquivos INEP -> ramo omitido de propósito.
    return (
        f"CASE "
        f"WHEN {col} IS NULL THEN NULL "
        f"WHEN TRIM({col}) LIKE '>%' THEN 80.0 "
        f"WHEN TRIM({col}) RLIKE '^[0-9]+([.][0-9]+)?$' THEN CAST(TRIM({col}) AS DOUBLE) "
        f"ELSE NULL END"
    )

# Diagonal: taxa do ano corrente, cada linha da SUA publicação.
DIAGONAL_TAXA = f"""
    CASE NU_ANO_AVALIACAO
        WHEN 2023 THEN {norm_pct('PC_ALUNO_ALFABETIZADO')}
        WHEN 2024 THEN {norm_pct('PC_ALUNO_ALFABETIZADO_2024')}
        WHEN 2025 THEN {norm_pct('PC_ALUNO_ALFABETIZADO_2025')}
    END AS taxa_alfabetizacao
"""

METAS_NORMALIZADAS = ",\n        ".join(
    f"{norm_pct(f'META_FINAL_{a}')} AS meta_alfabetizacao_{a}" for a in range(2024, 2031)
)

# Metadados de linhagem — adicionados de forma UNIFORME a toda tabela Silver.
# _silver_processed_at: carimbo único do batch (alinha com o modelo etl-silver.py)
def add_metadata(df):
    return df.withColumn(
        "_silver_processed_at",
        F.lit(SILVER_TS)
    )


def main():
    log.info("Iniciando job Silver...")
    log.info("BRONZE_BASE : %s", BRONZE_BASE)
    log.info("SILVER_BASE : %s", SILVER_BASE)

    # ============================================================
    # Processamento da Meta Nacional (Brasil)
    # ============================================================
    metas_uf_bronze = spark.read.option("mergeSchema", "true").parquet(f"{BRONZE_BASE}/metas_ufs")
    metas_uf_bronze.createOrReplaceTempView("bronze_metas_ufs")

    brasil_silver = add_metadata(spark.sql(f"""
        SELECT
            NU_ANO_AVALIACAO AS ano,
            INITCAP(REDE)    AS rede,
            {DIAGONAL_TAXA},
            {METAS_NORMALIZADAS},
            PC_AVALIADOS_LP  AS percentual_participacao
        FROM bronze_metas_ufs
        WHERE NOME_UF = 'Brasil'
          AND NU_ANO_AVALIACAO IN (2023, 2024, 2025)
    """))

    (brasil_silver.write.mode("overwrite").partitionBy("ano")
        .parquet(f"{SILVER_BASE}/meta_alfabetizacao_brasil"))

    log.info("meta_alfabetizacao_brasil gravada.")
    brasil_silver.orderBy(F.col("ano").desc()).show(truncate=False)

    # ============================================================
    # Processamento das Metas por Unidade da Federação (UF)
    # ============================================================

    meta_uf_silver = add_metadata(spark.sql(f"""
        SELECT
            NU_ANO_AVALIACAO AS ano,
            SIGLA_UF         AS sigla_uf,
            INITCAP(REDE)    AS rede,
            {DIAGONAL_TAXA},
            {METAS_NORMALIZADAS},
            PC_AVALIADOS_LP  AS percentual_participacao
        FROM bronze_metas_ufs
        WHERE NOME_UF IS NOT NULL
          AND NOME_UF <> 'Brasil'
          AND NU_ANO_AVALIACAO IN (2023, 2024, 2025)
    """))

    (meta_uf_silver.write.mode("overwrite").partitionBy("ano")
        .parquet(f"{SILVER_BASE}/meta_alfabetizacao_uf"))
    log.info("meta_alfabetizacao_uf gravada. Total: %s", meta_uf_silver.count())
    meta_uf_silver.orderBy("ano", "sigla_uf").show(5, truncate=False)

    # ============================================================
    # Processamento das Metas Municipais
    # ============================================================
    metas_mun_bronze = spark.read.option("mergeSchema", "true").parquet(f"{BRONZE_BASE}/metas_municipios")
    metas_mun_bronze.createOrReplaceTempView("bronze_metas_municipios")  # view temporária

    meta_mun_silver = add_metadata(spark.sql(f"""
        SELECT
            NU_ANO_AVALIACAO AS ano,
            LPAD(TRIM(CAST(CO_MUNICIPIO AS STRING)), 7, '0') AS id_municipio,
            INITCAP(NO_TP_REDE) AS rede,
            {DIAGONAL_TAXA},
            {METAS_NORMALIZADAS},
            COALESCE(NIVEIS_ALFABETIZACAO_2023, CO_NIVEL_ALFABETIZACAO) AS nivel_alfabetizacao,
            PC_AVALIADOS_LP  AS percentual_participacao
        FROM bronze_metas_municipios
        WHERE CO_MUNICIPIO IS NOT NULL
          AND NU_ANO_AVALIACAO IN (2023, 2024, 2025)
    """))

    (meta_mun_silver.write.mode("overwrite").partitionBy("ano")
        .parquet(f"{SILVER_BASE}/meta_alfabetizacao_municipio"))
    log.info("meta_alfabetizacao_municipio gravada. Total: %s", meta_mun_silver.count())
    meta_mun_silver.orderBy("ano", "id_municipio").show(5, truncate=False)

    # ============================================================
    # Processamento dos Dados Históricos Estaduais (UF)
    # ============================================================
    uf_bronze = spark.read.option("mergeSchema", "true").parquet(f"{BRONZE_BASE}/ts_estado")
    uf_bronze.createOrReplaceTempView("bronze_ts_estado")

    uf_silver = add_metadata(spark.sql("""
        SELECT
            CAST(NU_ANO_AVALIACAO AS INT)          AS ano,
            CAST(CO_UF AS INT)                     AS id_uf,
            SG_UF                                  AS sigla_uf,
            CAST(TP_SERIE AS INT)                  AS serie,
            CAST(ID_TIPO_REDE AS INT)              AS rede,
            CAST(PC_ALUNO_ALFABETIZADO AS DOUBLE)  AS taxa_alfabetizacao,
            CAST(VL_MEDIA_LP AS DOUBLE)            AS media_portugues
        FROM bronze_ts_estado
        WHERE CO_UF IS NOT NULL
          AND CAST(ID_TIPO_REDE AS INT) = 5   -- Pública (Estadual+Municipal): indicador UF
    """))

    (uf_silver.write.mode("overwrite").partitionBy("ano")
        .parquet(f"{SILVER_BASE}/uf"))
    log.info("silver/uf gravada. Total: %s", uf_silver.count())
    uf_silver.orderBy("ano", "id_uf", "rede").show(10, truncate=False)

    # ============================================================
    # Processamento dos Dados Históricos Municipais
    # ============================================================
    mun_bronze = spark.read.option("mergeSchema", "true").parquet(f"{BRONZE_BASE}/ts_municipio")
    mun_bronze.createOrReplaceTempView("bronze_ts_municipio")

    municipio_silver = add_metadata(spark.sql("""
        SELECT
            CAST(NU_ANO_AVALIACAO AS INT)                     AS ano,
            LPAD(TRIM(CAST(CO_MUNICIPIO AS STRING)), 7, '0')  AS id_municipio,
            TRIM(NO_MUNICIPIO)                                AS nome_municipio,
            CAST(CO_UF AS INT)                                AS id_uf,
            SG_UF                                             AS sigla_uf,
            CAST(TP_SERIE AS INT)                             AS serie,
            CAST(ID_TIPO_REDE AS INT)                         AS rede,
            CAST(PC_ALUNO_ALFABETIZADO AS DOUBLE)             AS taxa_alfabetizacao,
            CAST(VL_MEDIA_LP AS DOUBLE)                        AS media_portugues
        FROM bronze_ts_municipio
        WHERE CO_MUNICIPIO IS NOT NULL
          AND CAST(ID_TIPO_REDE AS INT) = 3   -- Municipal: indicador Municipal

    """))

    (municipio_silver.write.mode("overwrite").partitionBy("ano")
        .parquet(f"{SILVER_BASE}/municipio"))
    log.info("silver/municipio gravada. Total: %s", municipio_silver.count())
    municipio_silver.orderBy("ano", "id_municipio", "rede").show(10, truncate=False)

    # ============================================================
    # Processamento dos Microdados de Alunos
    # ============================================================
    alunos_bronze = spark.read.option("mergeSchema", "true").parquet(f"{BRONZE_BASE}/ts_aluno")
    alunos_bronze.createOrReplaceTempView("bronze_ts_aluno")

    alunos_silver = add_metadata(spark.sql("""
        WITH base AS (
            SELECT *, CAST(VL_PROFICIENCIA_LP AS DOUBLE) AS _prof
            FROM bronze_ts_aluno
            WHERE ID_ALUNO IS NOT NULL
              AND CO_MUNICIPIO IS NOT NULL
              AND CAST(TP_DEPENDENCIA AS INT) IN (2, 3)
        )
        SELECT
            CAST(NU_ANO_AVALIACAO AS INT)                     AS ano,
            LPAD(TRIM(CAST(CO_MUNICIPIO AS STRING)), 7, '0')  AS id_municipio,
            TRIM(CAST(ID_ESCOLA AS STRING))                   AS id_escola,
            TRIM(CAST(ID_ALUNO  AS STRING))                   AS id_aluno,
            CAST(CO_CADERNO_LP AS INT)                        AS caderno,
            CAST(TP_SERIE AS INT)                             AS serie,
            CAST(TP_DEPENDENCIA AS INT)                       AS dependencia_administrativa,
            CAST(IN_PRESENCA_LP AS INT)                       AS presenca,
            CAST(IN_PREENCHIMENTO_LP AS INT)                  AS preenchimento_caderno,
            CASE WHEN _prof IS NULL THEN NULL                 -- 769.525 não-medidos -> NULL protege o denominador
                 ELSE CAST(IN_ALFABETIZADO AS INT) END        AS alfabetizado,
            _prof                                             AS proficiencia,
            CAST(VL_PESO_ALUNO_LP AS DOUBLE)                  AS peso_aluno
        FROM base
    """))

    (alunos_silver.write.mode("overwrite").partitionBy("ano")
        .parquet(f"{SILVER_BASE}/alunos"))
    log.info("silver/alunos gravada. Total: %s", alunos_silver.count())
    alunos_silver.show(5, truncate=False)

    # ============================================================
    # VALIDAÇÃO DE QUALIDADE — CAMADA SILVER
    # ============================================================
    # Espelha etl-silver.py: catálogo declarativo (CHECKS) + severidade
    # por regra (critico) -> PASS/FAIL/WARN, Score e raise em falha crítica.
    # Tipos: min_count, not_null, unique, range, anos, regex e expr.

    def checar_qualidade(entidade, df, checks):
        log.info(f"[DQ:SILVER] {entidade} | iniciando | checks={len(checks)}")

        # SG5(b): coluna inexistente é ERRO DE CONTRATO -> falha sempre, nunca vira WARN
        referenciadas = set()
        for c in checks:
            col = c.get("coluna")
            if isinstance(col, list):
                referenciadas.update(col)
            elif col:
                referenciadas.add(col)
        ausentes = referenciadas - set(df.columns)
        if ausentes:
            raise ValueError(
                f"[DQ:SILVER] {entidade}: coluna(s) do check ausente(s) no schema -> "
                f"{sorted(ausentes)} | schema: {df.columns}"
            )

        # SG4: UMA passada (agg) -> total + nulos + fora-de-faixa + não-nulos
        # (mesma ideia do checar_aluno da Bronze: várias métricas em 1 leitura)
        exprs = [F.count(F.lit(1)).alias("_total")]
        for c in checks:
            if c["tipo"] == "not_null":
                exprs.append(F.coalesce(F.sum(F.col(c["coluna"]).isNull().cast("long")), F.lit(0)).alias(f"nulos_{c['coluna']}"))
            elif c["tipo"] == "range":
                col, (mn, mx) = c["coluna"], c["valor"]
                exprs.append(F.coalesce(F.sum(((F.col(col) < mn) | (F.col(col) > mx)).cast("long")), F.lit(0)).alias(f"fora_{col}"))
                exprs.append(F.coalesce(F.sum(F.col(col).isNotNull().cast("long")), F.lit(0)).alias(f"naonulos_{col}"))
            elif c["tipo"] == "anos":
                exprs.append(
                    F.collect_set(F.col(c["coluna"])).alias(f"anos_{c['coluna']}")
                )
            elif c["tipo"] == "regex":
                col = c["coluna"]
                exprs.append(
                    F.coalesce(
                        F.sum((~F.col(col).rlike(c["valor"])).cast("long")),
                        F.lit(0)
                    ).alias(f"regex_{col}")
                )
            elif c["tipo"] == "expr":
                exprs.append(
                    F.coalesce(
                        F.sum(F.expr(c["valor"]).cast("long")),
                        F.lit(0)
                    ).alias(f"expr_{c['nome']}")
                )
        m = df.agg(*exprs).collect()[0].asDict()
        total = m["_total"]

        passou = falhou = criticos = 0
        for check in checks:
            tipo    = check["tipo"]
            coluna  = check.get("coluna")
            valor   = check.get("valor")
            critico = check.get("critico", True)
            ok, detalhe = False, ""

            if tipo == "min_count":
                ok, detalhe = total >= valor, f"contagem={total} | minimo={valor}"
            elif tipo == "not_null":
                nulos = m[f"nulos_{coluna}"]
                ok, detalhe = nulos == 0, f"{nulos} nulos"
            elif tipo == "unique":
                cols = coluna if isinstance(coluna, list) else [coluna]
                dups = total - df.select(*cols).distinct().count()   # 1 passada extra (semântica preservada)
                ok, detalhe = dups == 0, f"{dups} duplicatas (chave={cols})"
            elif tipo == "range":
                mn, mx = valor
                fora, naonulos = m[f"fora_{coluna}"], m[f"naonulos_{coluna}"]
                if total > 0 and naonulos == 0:                       # SG5(a): coluna 100% nula NÃO passa calada
                    ok, detalhe = False, f"coluna 100% nula ({total} linhas)"
                else:
                    ok, detalhe = fora == 0, f"{fora} fora de [{mn},{mx}] | nulos={total - naonulos}"

            elif tipo == "anos":
                presentes = set(m[f"anos_{coluna}"])
                faltando = set(valor) - presentes
                ok = len(faltando) == 0
                detalhe = (
                    f"faltando={sorted(faltando)} | "
                    f"presentes={sorted(presentes)}"
                )

            elif tipo == "regex":
                invalidos = m[f"regex_{coluna}"]
                ok = invalidos == 0
                detalhe = f"{invalidos} fora do padrão '{valor}'"

            elif tipo == "expr":
                violacoes = m[f"expr_{check['nome']}"]
                ok = violacoes == 0
                detalhe = f"{violacoes} violações ({check['nome']})"

            status = "PASS" if ok else ("FAIL" if critico else "WARN")
            log.info(f"[DQ:SILVER] {status:4} | {tipo:9} | {coluna if coluna else '-'} | {detalhe}")
            if ok:
                passou += 1
            else:
                falhou += 1
                criticos += 1 if critico else 0

        score = round(passou / len(checks) * 100, 1)
        log.info(f"[DQ:SILVER] {entidade} | Score={score}% | PASS={passou} FAIL={falhou}\n")
        if criticos > 0:
            raise Exception(f"[DQ:SILVER] {entidade}: {criticos} check(s) critico(s) falharam. Pipeline interrompido.")
        return score

    # ============================================================
    # REGRAS DE QUALIDADE (uma lista por tabela — como o CHECKS do .py)
    # ============================================================
    ANOS_ESPERADOS = [2023, 2024, 2025]

    CHECKS = {
        "meta_alfabetizacao_brasil": [
            {"tipo": "min_count", "valor": 1,                                     "critico": True},
            {"tipo": "anos",      "coluna": "ano", "valor": ANOS_ESPERADOS,       "critico": True},
            {"tipo": "not_null",  "coluna": "ano",                                "critico": True},
            {"tipo": "not_null",  "coluna": "rede",                               "critico": True},
            {"tipo": "unique",    "coluna": ["ano", "rede"],                       "critico": True},
            {"tipo": "range",     "coluna": "taxa_alfabetizacao", "valor": (0, 100), "critico": False},
        ],
        "meta_alfabetizacao_uf": [
            {"tipo": "min_count", "valor": 1,                                     "critico": True},
            {"tipo": "anos",      "coluna": "ano", "valor": ANOS_ESPERADOS,          "critico": True},
            {"tipo": "not_null",  "coluna": "ano",                                "critico": True},
            {"tipo": "not_null",  "coluna": "sigla_uf",                           "critico": True},
            {"tipo": "not_null",  "coluna": "rede",                               "critico": True},
            {"tipo": "unique",    "coluna": ["ano", "sigla_uf", "rede"],           "critico": True},
            {"tipo": "range",     "coluna": "taxa_alfabetizacao", "valor": (0, 100), "critico": False},
        ],
        "meta_alfabetizacao_municipio": [
            {"tipo": "min_count", "valor": 1,                                     "critico": True},
            {"tipo": "anos",      "coluna": "ano", "valor": ANOS_ESPERADOS,          "critico": True},
            {"tipo": "not_null",  "coluna": "ano",                                "critico": True},
            {"tipo": "not_null",  "coluna": "id_municipio",                       "critico": True},
            {"tipo": "regex",     "coluna": "id_municipio", "valor": "^[0-9]{7}$",   "critico": True},            
            {"tipo": "not_null",  "coluna": "rede",                               "critico": True},
            {"tipo": "unique",    "coluna": ["ano", "id_municipio", "rede"],       "critico": True},
            {"tipo": "range",     "coluna": "taxa_alfabetizacao", "valor": (0, 100), "critico": False},
        ],
        "uf": [
            {"tipo": "min_count", "valor": 1,                                     "critico": True},
            {"tipo": "anos",      "coluna": "ano", "valor": ANOS_ESPERADOS,          "critico": True},
            {"tipo": "not_null",  "coluna": "ano",                                "critico": True},
            {"tipo": "not_null",  "coluna": "id_uf",                              "critico": True},
            {"tipo": "not_null",  "coluna": "rede",                               "critico": True},
            {"tipo": "unique",    "coluna": ["ano", "id_uf", "rede"],              "critico": True},
            {"tipo": "range",     "coluna": "taxa_alfabetizacao", "valor": (0, 100), "critico": False},
            {"tipo": "range",     "coluna": "media_portugues", "valor": (0, 1000), "critico": False},
            {"tipo": "expr", "nome": "rede_uf_invalida",
             "valor": "rede IS NULL OR rede <> 5",                                   "critico": True},        
        ],
        "municipio": [
            {"tipo": "min_count", "valor": 1,                                     "critico": True},
            {"tipo": "not_null",  "coluna": "ano",                                "critico": True},
            {"tipo": "not_null",  "coluna": "id_municipio",                       "critico": True},
            {"tipo": "regex",     "coluna": "id_municipio", "valor": "^[0-9]{7}$",   "critico": True},
            {"tipo": "not_null",  "coluna": "rede",                               "critico": True},
            {"tipo": "unique",    "coluna": ["ano", "id_municipio", "rede"],       "critico": True},
            {"tipo": "range",     "coluna": "taxa_alfabetizacao", "valor": (0, 100), "critico": False},
            {"tipo": "range",     "coluna": "media_portugues", "valor": (0, 1000), "critico": False},
            {"tipo": "expr", "nome": "rede_municipio_invalida",
             "valor": "rede IS NULL OR rede <> 3",                                   "critico": True},        
        ],
        "alunos": [
            {"tipo": "min_count", "valor": 1, "critico": True},
            {"tipo": "anos", "coluna": "ano", "valor": ANOS_ESPERADOS, "critico": True},
            {"tipo": "not_null", "coluna": "ano", "critico": True},
            {"tipo": "not_null", "coluna": "id_aluno", "critico": True},
            {"tipo": "not_null", "coluna": "id_municipio", "critico": True},
            {"tipo": "regex", "coluna": "id_municipio", "valor": "^[0-9]{7}$", "critico": True},
            {"tipo": "unique", "coluna": ["ano", "id_aluno"], "critico": True},
            {"tipo": "range", "coluna": "proficiencia", "valor": [0, 1500], "critico": False},
            {
                "tipo": "expr",
                "nome": "dependencia_administrativa_invalida",
                "valor": "dependencia_administrativa IS NULL OR dependencia_administrativa NOT IN (2, 3)",
                "critico": True
            },
        ],
    }

    # ============================================================
    # EXECUÇÃO — valida as 6 tabelas Silver
    # ============================================================
    for tabela, checks in CHECKS.items():
        df = spark.read.parquet(f"{SILVER_BASE}/{tabela}")
        checar_qualidade(tabela, df, checks)

    log.info("Camada Silver validada com sucesso.")
    log.info("Job Silver concluído com sucesso!")
    job.commit()


main()
