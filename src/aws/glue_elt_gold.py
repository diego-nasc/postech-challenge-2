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
# BUCKET_SILVER - bucket de dados silver
# BUCKET_GOLD - bucket de dados gold
args = getResolvedOptions(sys.argv, ["JOB_NAME", "BUCKET_SILVER", "BUCKET_GOLD"])

# Contexto Spark e Glue
sc = SparkContext()
glue_context = GlueContext(sc)
spark = glue_context.spark_session
job = Job(glue_context)
job.init(args["JOB_NAME"], args)

BUCKET_SILVER = args["BUCKET_SILVER"]
BUCKET_GOLD = args["BUCKET_GOLD"]
SILVER_BASE = f"s3a://{BUCKET_SILVER}"
GOLD_BASE = f"s3a://{BUCKET_GOLD}"

spark.conf.set("spark.sql.sources.partitionOverwriteMode", "dynamic")
spark.sparkContext.setLogLevel("WARN")

GOLD_TS = datetime.now(timezone.utc)   # timestamp único do batch, como SILVER_TS


def add_gold_metadata(df):
    return df.withColumn("_gold_processed_at", F.lit(GOLD_TS))


def main():
    log.info("Iniciando job Gold...")
    log.info("SILVER_BASE : %s", SILVER_BASE)
    log.info("GOLD_BASE   : %s", GOLD_BASE)


    # Consolidação da Camada Gold: Indicadores Municipais
    (spark
        .read
        .parquet(f"{SILVER_BASE}/municipio")
        .createOrReplaceTempView("silver_municipio")
    )
    (spark
        .read
        .parquet(f"{SILVER_BASE}/meta_alfabetizacao_municipio")
        .createOrReplaceTempView("silver_meta_municipio")
    )

    gold_ind_mun = add_gold_metadata(spark.sql("""
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
                ORDER BY (meta IS NOT NULL) DESC, ano_pub DESC   -- não-nulo, depois publicação mais nova
            ) rn FROM meta_long_raw
        ) WHERE rn = 1 AND meta IS NOT NULL
    ),
    -- (2) RESULTADO no nível da meta (rede=3, Municipal) — fonte canônica da taxa
    res AS (
        SELECT ano, id_municipio, nome_municipio, id_uf, sigla_uf,
            taxa_alfabetizacao, media_portugues
        FROM silver_municipio
        WHERE rede = 3
    ),
    -- (3) PARTICIPAÇÃO (por ano de avaliação) — só existe na tabela de meta
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
        3                                        AS rede,   -- explícito: comparação no nível Municipal
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
    """))

    (gold_ind_mun.write.mode("overwrite").partitionBy("ano")
        .parquet(f"{GOLD_BASE}/indicadores_municipio"))
    log.info("gold/indicadores_municipio: %s", gold_ind_mun.count())
    gold_ind_mun.orderBy("ano", "id_municipio").show(8, truncate=False)


    # Consolidação da Camada Gold: Indicadores Estaduais (UF)
    (spark
        .read
        .parquet(f"{SILVER_BASE}/uf")
        .createOrReplaceTempView("silver_uf")
    )
    (spark
        .read
        .parquet(f"{SILVER_BASE}/meta_alfabetizacao_uf")
        .createOrReplaceTempView("silver_meta_uf")
    )

    gold_ind_uf = add_gold_metadata(spark.sql("""
    WITH
    meta_long_raw AS (
        SELECT ano AS ano_pub, sigla_uf,
            stack(7,
                2024, meta_alfabetizacao_2024, 2025, meta_alfabetizacao_2025,
                2026, meta_alfabetizacao_2026, 2027, meta_alfabetizacao_2027,
                2028, meta_alfabetizacao_2028, 2029, meta_alfabetizacao_2029,
                2030, meta_alfabetizacao_2030
            ) AS (ano, meta)
        FROM silver_meta_uf
    ),
    meta_long AS (
        SELECT sigla_uf, ano, meta FROM (
            SELECT *, ROW_NUMBER() OVER (
                PARTITION BY sigla_uf, ano
                ORDER BY (meta IS NOT NULL) DESC, ano_pub DESC
            ) rn FROM meta_long_raw
        ) WHERE rn = 1 AND meta IS NOT NULL
    ),
    res AS (
        SELECT ano, id_uf, sigla_uf, taxa_alfabetizacao, media_portugues
        FROM silver_uf
        WHERE rede = 5
    ),
    part AS (
        SELECT ano, sigla_uf, percentual_participacao
        FROM silver_meta_uf
    )
    SELECT
        r.ano,
        r.id_uf,
        r.sigla_uf,
        5                                        AS rede,   -- comparação no nível Pública
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
    LEFT JOIN part      p  ON r.ano = p.ano AND r.sigla_uf = p.sigla_uf
    LEFT JOIN meta_long ml ON r.ano = ml.ano AND r.sigla_uf = ml.sigla_uf
    """))

    (gold_ind_uf.write.mode("overwrite").partitionBy("ano")
        .parquet(f"{GOLD_BASE}/indicadores_uf"))
    log.info("gold/indicadores_uf: %s", gold_ind_uf.count())
    gold_ind_uf.orderBy("ano", "sigla_uf").show(30, truncate=False)


    # Consolidação da Camada Gold: Contexto Individual do Aluno
    (spark
        .read
        .parquet(f"{SILVER_BASE}/alunos")
        .createOrReplaceTempView("silver_alunos")
    )
    gold_ind_mun.createOrReplaceTempView("gold_ind_mun")

    gold_aluno_ctx = add_gold_metadata(spark.sql("""
    SELECT
        -- identificadores (não-features)
        a.id_aluno,
        a.ano,
        a.id_municipio,
        CAST(SUBSTRING(a.id_municipio, 1, 2) AS INT) AS id_uf,   -- derivado do código IBGE do município; sigla_uf omitida
        -- atributos do aluno
        a.dependencia_administrativa,
        a.caderno,
        -- contexto do município (features de lugar, herdadas da Gold municipal)
        c.taxa_alfabetizacao        AS ctx_taxa_municipio,
        c.media_portugues           AS ctx_media_municipio,
        c.meta                      AS ctx_meta_municipio,
        c.distancia_meta            AS ctx_distancia_meta_municipio,
        c.atingiu_meta              AS ctx_atingiu_meta_municipio,
        c.percentual_participacao   AS ctx_participacao_municipio,
        c.categoria_desempenho      AS ctx_categoria_municipio,
        c.faixa_participacao        AS ctx_faixa_participacao_municipio,
        -- medição do aluno
        a.proficiencia,
        ROUND(a.proficiencia - 743, 2) AS gap_proficiencia,   -- proficiencia - corte
        -- ALVO
        a.alfabetizado              AS label_alfabetizado
    FROM silver_alunos a
    LEFT JOIN gold_ind_mun c
      ON a.ano = c.ano AND a.id_municipio = c.id_municipio
    WHERE a.proficiencia IS NOT NULL
    """))

    (gold_aluno_ctx.write.mode("overwrite").partitionBy("ano")
        .parquet(f"{GOLD_BASE}/aluno_contexto"))
    log.info("gold/aluno_contexto: %s", gold_aluno_ctx.count())   # ~5,32M
    gold_aluno_ctx.show(5, truncate=False)

    # DICIONÁRIO DE PAPÉIS — gold/aluno_contexto (consumido pelo pipeline de ML)
    PAPEIS_ALUNO_CONTEXTO = {
        "alvo":          ["label_alfabetizado"],
        "identificador": ["id_aluno", "ano", "id_municipio", "id_uf"],
        "vazamento":     ["proficiencia", "gap_proficiencia"],   # DERIVAM do alvo -> NUNCA como feature
        "constante":     [],   # presenca/preenchimento não entraram (=1 em todos os medidos)
        "features":      [
            "dependencia_administrativa", "caderno",
            "ctx_taxa_municipio", "ctx_media_municipio", "ctx_meta_municipio",
            "ctx_distancia_meta_municipio", "ctx_atingiu_meta_municipio",
            "ctx_participacao_municipio", "ctx_categoria_municipio",
            "ctx_faixa_participacao_municipio",
        ],
    }
    # No treino: X = df.select(PAPEIS_ALUNO_CONTEXTO["features"]); y = df["label_alfabetizado"]

    # ============================================================
    # VALIDAÇÃO DE QUALIDADE — CAMADA GOLD
    # ============================================================
    # Espelha etl-silver.py: catálogo declarativo (CHECKS) + severidade
    # por regra (critico) -> PASS/FAIL/WARN, Score e raise em falha crítica.
    # Tipos: min_count, not_null, unique (aceita chave composta), range, anos, regex e expr.

    def checar_qualidade(entidade, df, checks):
        log.info(f"[DQ:GOLD] {entidade} | iniciando | checks={len(checks)}")

        # Coluna inexistente é erro de contrato -> falha sempre
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
                f"[DQ:GOLD] {entidade}: coluna(s) do check ausente(s) no schema -> "
                f"{sorted(ausentes)} | schema: {df.columns}"
            )

        # Uma passada de agg:
        # total + nulos + ranges + anos + regex + expressões
        exprs = [F.count(F.lit(1)).alias("_total")]

        for c in checks:
            if c["tipo"] == "not_null":
                exprs.append(
                    F.coalesce(
                        F.sum(
                            F.col(c["coluna"]).isNull().cast("long")
                        ),
                        F.lit(0)
                    ).alias(f"nulos_{c['coluna']}")
                )

            elif c["tipo"] == "range":
                col, (mn, mx) = c["coluna"], c["valor"]

                exprs.append(
                    F.coalesce(
                        F.sum(
                            (
                                (F.col(col) < mn)
                                | (F.col(col) > mx)
                            ).cast("long")
                        ),
                        F.lit(0)
                    ).alias(f"fora_{col}")
                )

                exprs.append(
                    F.coalesce(
                        F.sum(
                            F.col(col).isNotNull().cast("long")
                        ),
                        F.lit(0)
                    ).alias(f"naonulos_{col}")
                )

            elif c["tipo"] == "anos":
                exprs.append(
                    F.collect_set(
                        F.col(c["coluna"])
                    ).alias(f"anos_{c['coluna']}")
                )

            elif c["tipo"] == "regex":
                col = c["coluna"]

                exprs.append(
                    F.coalesce(
                        F.sum(
                            (~F.col(col).rlike(c["valor"])).cast("long")
                        ),
                        F.lit(0)
                    ).alias(f"regex_{col}")
                )

            elif c["tipo"] == "expr":
                exprs.append(
                    F.coalesce(
                        F.sum(
                            F.expr(c["valor"]).cast("long")
                        ),
                        F.lit(0)
                    ).alias(f"expr_{c['nome']}")
                )

        m = df.agg(*exprs).collect()[0].asDict()
        total = m["_total"]

        passou = falhou = criticos = 0

        for check in checks:
            tipo = check["tipo"]
            coluna = check.get("coluna")
            valor = check.get("valor")
            critico = check.get("critico", True)

            ok, detalhe = False, ""

            if tipo == "min_count":
                ok = total >= valor
                detalhe = f"contagem={total} | minimo={valor}"

            elif tipo == "not_null":
                nulos = m[f"nulos_{coluna}"]

                ok = nulos == 0
                detalhe = f"{nulos} nulos"

            elif tipo == "unique":
                cols = coluna if isinstance(coluna, list) else [coluna]

                dups = (
                    total
                    - df.select(*cols).distinct().count()
                )

                ok = dups == 0
                detalhe = f"{dups} duplicatas (chave={cols})"

            elif tipo == "range":
                mn, mx = valor

                fora = m[f"fora_{coluna}"]
                naonulos = m[f"naonulos_{coluna}"]

                if total > 0 and naonulos == 0:
                    ok = False
                    detalhe = f"coluna 100% nula ({total} linhas)"
                else:
                    ok = fora == 0
                    detalhe = (
                        f"{fora} fora de [{mn},{mx}] | "
                        f"nulos={total - naonulos}"
                    )

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
                detalhe = (
                    f"{invalidos} fora do padrão '{valor}'"
                )

            elif tipo == "expr":
                violacoes = m[f"expr_{check['nome']}"]

                ok = violacoes == 0
                detalhe = (
                    f"{violacoes} violações ({check['nome']})"
                )

            status = (
                "PASS"
                if ok
                else ("FAIL" if critico else "WARN")
            )

            log.info(
                f"[DQ:GOLD] {status:4} | "
                f"{tipo:9} | "
                f"{coluna if coluna else '-'} | "
                f"{detalhe}"
            )

            if ok:
                passou += 1
            else:
                falhou += 1
                criticos += 1 if critico else 0

        score = round(
            passou / len(checks) * 100,
            1
        )

        log.info(
            f"[DQ:GOLD] {entidade} | "
            f"Score={score}% | "
            f"PASS={passou} FAIL={falhou}\n"
        )

        if criticos > 0:
            raise Exception(
                f"[DQ:GOLD] {entidade}: "
                f"{criticos} check(s) critico(s) falharam. "
                "Pipeline interrompido."
            )

        return score

    ANOS_ESPERADOS = [2023, 2024, 2025]

    CHECKS_GOLD = {
        "indicadores_municipio": [
            {"tipo": "min_count", "valor": 1,                                        "critico": True},
            {"tipo": "anos", "coluna": "ano",
             "valor": ANOS_ESPERADOS, "critico": True},
            {"tipo": "not_null",  "coluna": "ano",                                   "critico": True},
            {"tipo": "not_null",  "coluna": "id_municipio",                          "critico": True},
            {"tipo": "unique",    "coluna": ["ano", "id_municipio"],                 "critico": True},
            {"tipo": "range",     "coluna": "taxa_alfabetizacao", "valor": (0, 100),  "critico": False},
            {"tipo": "range",     "coluna": "meta", "valor": (0, 100),                "critico": False},
            {"tipo": "expr", "nome": "meta_ausente_em_ano_alvo",
            "valor": "ano IN (2024, 2025) AND meta IS NULL",
            "critico": False},
            {"tipo": "range",     "coluna": "percentual_participacao", "valor": (0, 100), "critico": False},
            {"tipo": "expr", "nome": "atingiu_meta_incoerente",
            "valor": "meta IS NOT NULL AND ((taxa_alfabetizacao - meta >= 0) <> atingiu_meta)",
            "critico": True},
        ],
        "indicadores_uf": [
            {"tipo": "min_count", "valor": 1, "critico": True},
            {"tipo": "anos", "coluna": "ano",
            "valor": ANOS_ESPERADOS, "critico": True},
            {"tipo": "not_null", "coluna": "ano", "critico": True},
            {"tipo": "not_null", "coluna": "sigla_uf", "critico": True},
            {"tipo": "unique", "coluna": ["ano", "sigla_uf"], "critico": True},
            {"tipo": "range", "coluna": "taxa_alfabetizacao",
            "valor": (0, 100), "critico": False},
            {"tipo": "range", "coluna": "meta",
            "valor": (0, 100), "critico": False},
            {"tipo": "expr", "nome": "meta_ausente_em_ano_alvo",
            "valor": "ano IN (2024, 2025) AND meta IS NULL",
            "critico": False},
            {"tipo": "range", "coluna": "percentual_participacao",
            "valor": (0, 100), "critico": False},
            {"tipo": "expr", "nome": "atingiu_meta_incoerente",
            "valor": "meta IS NOT NULL AND ((taxa_alfabetizacao - meta >= 0) <> atingiu_meta)",
            "critico": True},
        ],
                
        "aluno_contexto": [
            {"tipo": "min_count", "valor": 1,                                   "critico": True},
            {"tipo": "anos", "coluna": "ano",
            "valor": ANOS_ESPERADOS, "critico": True},
            {"tipo": "not_null",  "coluna": "id_aluno",                        "critico": True},
            {"tipo": "not_null",  "coluna": "ano",                             "critico": True},
            {"tipo": "not_null",  "coluna": "proficiencia",                    "critico": True},   # filtro garante
            {"tipo": "not_null",  "coluna": "label_alfabetizado",             "critico": True},   # sem NULL no alvo
            {"tipo": "unique",    "coluna": ["ano", "id_aluno"],               "critico": True},
            {"tipo": "range",     "coluna": "proficiencia", "valor": (0, 1500),   "critico": False},
            {"tipo": "expr", "nome": "label_incoerente_com_corte",
            "valor": "(proficiencia >= 743) <> (label_alfabetizado = 1)",
            "critico": True},
        ],
    }

    for tabela, checks in CHECKS_GOLD.items():
        df = spark.read.parquet(f"{GOLD_BASE}/{tabela}")
        checar_qualidade(tabela, df, checks)

    log.info("Camada Gold validada com sucesso.")
    log.info("Job Gold concluído com sucesso!")
    job.commit()


main()
