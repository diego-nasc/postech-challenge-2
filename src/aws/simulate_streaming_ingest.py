#!/usr/bin/env python3
"""
Simula ingestao streaming enviando micro-lotes CSV para o S3.

Substitui o produtor em thread do notebook `06_Pipeline_Streaming.ipynb`.
Roda de forma independente do Glue Streaming Job.

Uso (a partir da raiz do projeto, com credenciais AWS configuradas):

    python src/aws/simulate_streaming_ingest.py

Variaveis lidas de `config_vars.sh` (raiz do repo) e/ou `.env` / ambiente:
    BUCKET_SILVER, BUCKET_STREAMING, ANO_STREAM, AWS_REGION

Argumentos opcionais:

    python src/aws/simulate_streaming_ingest.py --n-eventos 200 --n-por-lote 30 --intervalo 4
    python src/aws/simulate_streaming_ingest.py --bucket-streaming meu-bucket-streaming

Pre-requisitos:
    - Pipeline Silver batch executado (municipio/ano=2025 no S3).
    - Glue Streaming Job em execucao (ou iniciar depois dos primeiros arquivos).
"""

from __future__ import annotations

import argparse
import logging
import os
import random
import sys
import time
from pathlib import Path

import boto3
import pandas as pd
from botocore.exceptions import BotoCoreError, ClientError
from dotenv import find_dotenv, load_dotenv

SCRIPT_DIR = Path(__file__).resolve().parent
PROJ_ROOT = SCRIPT_DIR.parent.parent
CONFIG_VARS_PATH = PROJ_ROOT / "config_vars.sh"

CSV_COLS = [
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

log = logging.getLogger(__name__)


def setup_logging() -> None:
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s | %(levelname)-8s | %(message)s",
        stream=sys.stdout,
    )


def load_config_vars(path: Path) -> dict[str, str]:
    cfg: dict[str, str] = {}
    if not path.is_file():
        return cfg
    for line in path.read_text(encoding="utf-8").splitlines():
        line = line.strip()
        if not line or line.startswith("#") or "=" not in line:
            continue
        key, value = line.split("=", 1)
        cfg[key.strip()] = value.strip().strip('"')
    return cfg


def resolve_settings(args: argparse.Namespace) -> dict[str, str | int]:
    load_dotenv(find_dotenv(usecwd=True))
    cfg = load_config_vars(CONFIG_VARS_PATH)

    def pick(cli_value, env_key: str, cfg_key: str, default: str | None = None) -> str:
        if cli_value is not None:
            return str(cli_value)
        env_val = os.getenv(env_key)
        if env_val:
            return env_val
        if cfg_key in cfg:
            return cfg[cfg_key]
        if default is not None:
            return default
        return ""

    return {
        "bucket_silver": pick(args.bucket_silver, "BUCKET_SILVER", "BUCKET_SILVER"),
        "bucket_streaming": pick(args.bucket_streaming, "BUCKET_STREAMING", "BUCKET_STREAMING"),
        "ano_stream": int(
            pick(
                str(args.ano_stream) if args.ano_stream is not None else None,
                "ANO_STREAM",
                "ANO_STREAM",
                "2026",
            )
        ),
        "aws_region": pick(args.aws_region, "AWS_REGION", "AWS_REGION", "us-east-1"),
        "n_eventos": args.n_eventos,
        "n_por_lote": args.n_por_lote,
        "intervalo": args.intervalo,
    }


def load_catalogo_municipios(bucket_silver: str, ano_catalogo: int) -> pd.DataFrame:
    silver_path = f"s3://{bucket_silver}/municipio/"
    log.info("Lendo catalogo Silver: %s (ano=%s)", silver_path, ano_catalogo)

    df = pd.read_parquet(
        silver_path,
        columns=[
            "ano",
            "id_uf",
            "sigla_uf",
            "id_municipio",
            "nome_municipio",
            "serie",
        ],
    )
    catalogo = (
        df.loc[df["ano"] == ano_catalogo, [
            "id_uf",
            "sigla_uf",
            "id_municipio",
            "nome_municipio",
            "serie",
        ]]
        .drop_duplicates(subset=["id_municipio", "serie"])
        .reset_index(drop=True)
    )
    if catalogo.empty:
        raise RuntimeError(
            f"Catalogo vazio para ano={ano_catalogo} em s3://{bucket_silver}/municipio/. "
            "Execute o job Silver (batch) antes."
        )
    log.info("Catalogo carregado: %s municipios (serie unica por linha)", len(catalogo))
    return catalogo


def gerar_eventos(
    catalogo: pd.DataFrame,
    ano_stream: int,
    n_eventos: int,
    seed: int,
) -> list[dict]:
    random.seed(seed)
    redes = [3]
    registros = catalogo.to_dict("records")
    eventos: list[dict] = []

    for _ in range(n_eventos):
        m = random.choice(registros)
        co_municipio = str(int(m["id_municipio"]))
        eventos.append({
            "NU_ANO_AVALIACAO": ano_stream,
            "CO_UF": int(m["id_uf"]),
            "SG_UF": m["sigla_uf"],
            "CO_MUNICIPIO": co_municipio,
            "NO_MUNICIPIO": m["nome_municipio"],
            "TP_SERIE": int(m["serie"]),
            "ID_TIPO_REDE": random.choice(redes),
            "PC_ALUNO_ALFABETIZADO": round(random.uniform(45, 95), 2),
            "VL_MEDIA_LP": round(random.uniform(730, 800), 2),
        })

    random.shuffle(eventos)
    return eventos


def lote_para_csv(rows: list[dict]) -> str:
    linhas = [";".join(CSV_COLS)]
    for row in rows:
        linhas.append(";".join(str(row[col]) for col in CSV_COLS))
    return "\n".join(linhas)


def enviar_lotes(
    s3_client,
    bucket_streaming: str,
    ano_stream: int,
    eventos: list[dict],
    n_por_lote: int,
    intervalo_s: float,
) -> None:
    prefix = f"inbound/{ano_stream}"
    total_lotes = (len(eventos) + n_por_lote - 1) // n_por_lote

    for n, i in enumerate(range(0, len(eventos), n_por_lote)):
        lote = eventos[i : i + n_por_lote]
        key = f"{prefix}/lote_{n:03d}.csv"
        body = lote_para_csv(lote)

        s3_client.put_object(
            Bucket=bucket_streaming,
            Key=key,
            Body=body.encode("utf-8"),
            ContentType="text/csv",
        )
        log.info(
            "[PRODUTOR] s3://%s/%s -> %s eventos (lote %s/%s)",
            bucket_streaming,
            key,
            len(lote),
            n + 1,
            total_lotes,
        )

        if i + n_por_lote < len(eventos):
            time.sleep(intervalo_s)


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Simula ingestao streaming via micro-lotes CSV no S3.",
    )
    parser.add_argument("--n-eventos", type=int, default=200, help="Total de eventos simulados")
    parser.add_argument("--n-por-lote", type=int, default=30, help="Eventos por micro-lote")
    parser.add_argument("--intervalo", type=float, default=4.0, help="Segundos entre lotes")
    parser.add_argument("--ano-stream", type=int, default=None, help="Ano dos eventos (default: config/2026)")
    parser.add_argument("--ano-catalogo", type=int, default=2025, help="Ano base do catalogo Silver")
    parser.add_argument("--bucket-silver", default=None, help="Bucket Silver (override)")
    parser.add_argument("--bucket-streaming", default=None, help="Bucket streaming (override)")
    parser.add_argument("--aws-region", default=None, help="Regiao AWS (override)")
    return parser.parse_args()


def main() -> None:
    setup_logging()
    args = parse_args()
    settings = resolve_settings(args)

    bucket_silver = str(settings["bucket_silver"])
    bucket_streaming = str(settings["bucket_streaming"])
    ano_stream = int(settings["ano_stream"])
    aws_region = str(settings["aws_region"])

    if not bucket_silver or not bucket_streaming:
        raise ValueError(
            "BUCKET_SILVER e BUCKET_STREAMING sao obrigatorios "
            "(config_vars.sh, .env ou argumentos CLI)."
        )

    log.info("Bucket Silver    : %s", bucket_silver)
    log.info("Bucket Streaming : %s", bucket_streaming)
    log.info("Ano stream       : %s", ano_stream)
    log.info("Destino inbound  : s3://%s/inbound/%s/", bucket_streaming, ano_stream)

    try:
        catalogo = load_catalogo_municipios(bucket_silver, args.ano_catalogo)
        eventos = gerar_eventos(catalogo, ano_stream, int(settings["n_eventos"]), seed=ano_stream)
        s3_client = boto3.client("s3", region_name=aws_region)
        enviar_lotes(
            s3_client,
            bucket_streaming,
            ano_stream,
            eventos,
            int(settings["n_por_lote"]),
            float(settings["intervalo"]),
        )
    except (BotoCoreError, ClientError) as exc:
        log.error("Falha ao acessar o S3: %s", exc)
        raise SystemExit(1) from exc

    log.info("Simulacao concluida: %s eventos enviados em micro-lotes.", len(eventos))


if __name__ == "__main__":
    main()
