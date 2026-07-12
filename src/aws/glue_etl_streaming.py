import sys
import logging

from pyspark.sql import functions as F
from pyspark.sql.types import (
    DoubleType,
    IntegerType,
    StringType,
    StructField,
    StructType,
)
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
# JOB_NAME          - nome do job
# BUCKET_SILVER     - bucket da camada silver
# BUCKET_GOLD       - bucket da camada gold
# BUCKET_STREAMING  - bucket de entrada e checkpoint do streaming
# ANO_STREAM        - ano corrente (opcional, default 2026)
args = getResolvedOptions(
    sys.argv,
    ["JOB_NAME", "BUCKET_SILVER", "BUCKET_GOLD", "BUCKET_STREAMING"],
)
try:
    args.update(getResolvedOptions(sys.argv, ["ANO_STREAM"]))
except Exception:
    args["ANO_STREAM"] = "2026"

# Contexto Spark e Glue
sc = SparkContext()
glue_context = GlueContext(sc)
spark = glue_context.spark_session
job = Job(glue_context)
job.init(args["JOB_NAME"], args)

BUCKET_SILVER = args["BUCKET_SILVER"]
BUCKET_GOLD = args["BUCKET_GOLD"]
BUCKET_STREAMING = args["BUCKET_STREAMING"]
ANO_STREAM = int(args["ANO_STREAM"])

SILVER_BASE = f"s3a://{BUCKET_SILVER}"
GOLD_BASE = f"s3a://{BUCKET_GOLD}"
STREAMING_BASE = f"s3a://{BUCKET_STREAMING}"

META_SILVER_PATH = f"{SILVER_BASE}/meta_alfabetizacao_municipio"
INPUT_PATH = f"{STREAMING_BASE}/inbound/{ANO_STREAM}/"
CHECKPOINT_PATH = f"{STREAMING_BASE}/_checkpoints/medalhao_{ANO_STREAM}/"
TRIGGER_INTERVAL = "5 seconds"

SILVER_MUNICIPIO_PATH = f"{SILVER_BASE}/municipio"
GOLD_INDICADORES_PATH = f"{GOLD_BASE}/indicadores_municipio"

spark.conf.set("spark.sql.sources.partitionOverwriteMode", "dynamic")
spark.sparkContext.setLogLevel("WARN")

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


def _existe_no_lake(path):
    jvm = spark._jvm
    hconf = spark._jsc.hadoopConfiguration()
    p = jvm.org.apache.hadoop.fs.Path(path)
    fs = p.getFileSystem(hconf)
    return fs.exists(p)


def transform_silver(df_raw):
    return (
        df_raw
        .filter(F.col("CO_MUNICIPIO").isNotNull())
        .select(
            F.col("NU_ANO_AVALIACAO").cast("int").alias("ano"),
            F.lpad(
                F.trim(F.col("CO_MUNICIPIO").cast("string")),
                7,
                "0",
            ).alias("id_municipio"),
            F.trim(F.col("NO_MUNICIPIO")).alias("nome_municipio"),
            F.col("CO_UF").cast("int").alias("id_uf"),
            F.col("SG_UF").alias("sigla_uf"),
            F.col("TP_SERIE").cast("int").alias("serie"),
            F.col("ID_TIPO_REDE").cast("int").alias("rede"),
            F.col("PC_ALUNO_ALFABETIZADO")
            .cast("double")
            .alias("taxa_alfabetizacao"),
            F.col("VL_MEDIA_LP")
            .cast("double")
            .alias("media_portugues"),
        )
        .withColumn("_silver_processed_at", F.current_timestamp())
        .withColumn("processing_timestamp", F.current_timestamp())
        .withColumn("processing_layer", F.lit("streaming"))
        .withColumn("processing_year", F.lit(ANO_STREAM))
    )


def transform_gold(df_silver_2026):
    df_silver_2026.filter(F.col("rede") == 3).createOrReplaceTempView("silver_municipio")

    (
        spark.read
        .parquet(META_SILVER_PATH)
        .createOrReplaceTempView("silver_meta_municipio")
    )

    return spark.sql("""
    WITH
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
                ORDER BY (meta IS NOT NULL) DESC, ano_pub DESC
            ) rn FROM meta_long_raw
        ) WHERE rn = 1 AND meta IS NOT NULL
    ),
    res AS (
        SELECT ano, id_municipio, nome_municipio, id_uf, sigla_uf,
               taxa_alfabetizacao, media_portugues
        FROM silver_municipio
    ),
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
        3 AS rede,
        r.taxa_alfabetizacao,
        r.media_portugues,
        p.percentual_participacao,
        ml.meta,
        ROUND(r.taxa_alfabetizacao - ml.meta, 2) AS distancia_meta,
        CASE WHEN ml.meta IS NULL THEN NULL
             ELSE (r.taxa_alfabetizacao - ml.meta) >= 0 END AS atingiu_meta,
        CASE WHEN ml.meta IS NULL                      THEN NULL
             WHEN r.taxa_alfabetizacao - ml.meta >= 5  THEN 'Muito acima'
             WHEN r.taxa_alfabetizacao - ml.meta >= 0  THEN 'Acima'
             WHEN r.taxa_alfabetizacao - ml.meta >= -5 THEN 'Próximo'
             ELSE 'Muito abaixo' END AS categoria_desempenho,
        CASE WHEN p.percentual_participacao IS NULL THEN NULL
             WHEN p.percentual_participacao >= 95   THEN 'Alta'
             WHEN p.percentual_participacao >= 80   THEN 'Média'
             ELSE 'Baixa' END AS faixa_participacao
    FROM res r
    LEFT JOIN part      p  ON r.ano = p.ano AND r.id_municipio = p.id_municipio
    LEFT JOIN meta_long ml ON r.ano = ml.ano AND r.id_municipio = ml.id_municipio
    """)


def process_micro_batch(df_raw, epoch_id):
    if df_raw.rdd.isEmpty():
        return

    log.info("Processando micro-lote epoch_id=%s", epoch_id)

    df_silver = transform_silver(df_raw)

    (
        df_silver.write
        .mode("overwrite")
        .partitionBy("ano")
        .parquet(SILVER_MUNICIPIO_PATH)
    )

    df_silver_2026 = (
        spark.read
        .parquet(SILVER_MUNICIPIO_PATH)
        .filter(F.col("ano") == ANO_STREAM)
    )

    df_gold = transform_gold(df_silver_2026)

    (
        df_gold.write
        .mode("overwrite")
        .partitionBy("ano")
        .parquet(GOLD_INDICADORES_PATH)
    )

    log.info("Micro-lote epoch_id=%s concluido | silver=%s | gold=%s",
             epoch_id, df_silver.count(), df_gold.count())


def main():
    log.info("Iniciando Glue Streaming Job...")
    log.info("SILVER_BASE      : %s", SILVER_BASE)
    log.info("GOLD_BASE        : %s", GOLD_BASE)
    log.info("INPUT_PATH       : %s", INPUT_PATH)
    log.info("CHECKPOINT_PATH  : %s", CHECKPOINT_PATH)
    log.info("ANO_STREAM       : %s", ANO_STREAM)

    if not _existe_no_lake(META_SILVER_PATH):
        raise FileNotFoundError(
            f"Tabela Silver nao encontrada: {META_SILVER_PATH}. "
            "Execute o job Silver (batch) antes da ingestao streaming."
        )

    df_stream = (
        spark.readStream
        .schema(SCHEMA_EVENTO)
        .option("header", True)
        .option("sep", ";")
        .csv(INPUT_PATH)
    )

    query = (
        df_stream.writeStream
        .foreachBatch(process_micro_batch)
        .option("checkpointLocation", CHECKPOINT_PATH)
        .outputMode("append")
        .trigger(processingTime=TRIGGER_INTERVAL)
        .start()
    )

    log.info("Streaming query iniciada (trigger=%s). Aguardando eventos...", TRIGGER_INTERVAL)
    query.awaitTermination()
    log.info("Streaming query encerrada.")
    job.commit()


main()
