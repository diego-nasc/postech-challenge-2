#!/usr/bin/env bash
set -euo pipefail

# Provisiona recursos AWS para o pipeline de streaming (ano corrente).
# Pre-requisito: pipeline batch (silver/gold) ja executado — meta municipal no S3.
#
# Uso:
#   bash src/aws/create_aws_resources_streaming.sh
#   AUTOCONFIRM=1 bash src/aws/create_aws_resources_streaming.sh

SCRIPT_DIR="$(dirname "${BASH_SOURCE[0]}")"
PROJ_ROOT="${SCRIPT_DIR}/../.."

echo "Script dir: ${SCRIPT_DIR}"
echo "Proj root: ${PROJ_ROOT}"

source "${PROJ_ROOT}/config_vars.sh"

JOB_STREAMING="glue-job-streaming-etl"
STREAMING_INBOUND="s3://${BUCKET_STREAMING}/inbound/${ANO_STREAM}/"

# 1 - Cria bucket de streaming

echo "Bucket de streaming a ser criado:
- ${BUCKET_STREAMING}"

if [ "${AUTOCONFIRM:-}" != "1" ]; then
    read -r -p "Deseja criar o bucket de streaming? (s/N): " resposta
    if [ "$resposta" != "s" ]; then
        echo "Bucket de streaming nao foi criado"
        exit 0
    fi
fi

aws --region "${AWS_REGION}" s3 mb "s3://${BUCKET_STREAMING}" || true

# Marcador do prefixo de entrada (file stream source)
echo -n "" | aws --region "${AWS_REGION}" s3 cp - \
    "s3://${BUCKET_STREAMING}/inbound/${ANO_STREAM}/.keep"

echo "Prefixo de entrada: ${STREAMING_INBOUND}"
aws --region "${AWS_REGION}" s3 ls "s3://${BUCKET_STREAMING}/"

# 2 - Cria Glue Streaming Job

if [ "${AUTOCONFIRM:-}" != "1" ]; then
    read -r -p "Deseja criar o Glue Streaming Job? (s/N): " resposta
    if [ "$resposta" != "s" ]; then
        echo "Glue Streaming Job nao foi criado"
        exit 0
    fi
fi

echo "Criando Glue Streaming Job..."

# 2.1 - Sobe script para o S3
aws --region "${AWS_REGION}" s3 cp "${SCRIPT_DIR}/glue_etl_streaming.py" \
    "s3://${BUCKET_SCRIPTS}/glue_etl_streaming.py"

# 2.2 - Cria job (contínuo — nao aguarda termino como jobs batch)
# Streaming ETL: Structured Streaming + awaitTermination() no script Python.
aws --region "${AWS_REGION}" glue create-job \
    --name "${JOB_STREAMING}" \
    --role "${ROLE_NAME}" \
    --glue-version "5.1" \
    --worker-type "G.1X" \
    --number-of-workers 2 \
    --command "{
        \"Name\": \"glueetl\",
        \"ScriptLocation\": \"s3://${BUCKET_SCRIPTS}/glue_etl_streaming.py\",
        \"PythonVersion\": \"3\"
    }" \
    --default-arguments "{
        \"--JOB_NAME\": \"${JOB_STREAMING}\",
        \"--BUCKET_SILVER\": \"${BUCKET_SILVER}\",
        \"--BUCKET_GOLD\": \"${BUCKET_GOLD}\",
        \"--BUCKET_STREAMING\": \"${BUCKET_STREAMING}\",
        \"--ANO_STREAM\": \"${ANO_STREAM}\",
        \"--job-bookmark-option\": \"job-bookmark-disable\",
        \"--enable-metrics\": \"\"
    }"

echo ""
echo "Glue Streaming Job '${JOB_STREAMING}' criado."
echo ""
echo "IMPORTANTE: jobs streaming sao CONTINUOS — rodam ate serem parados manualmente."
echo "  Iniciar:  aws glue start-job-run --job-name ${JOB_STREAMING} --region ${AWS_REGION}"
echo "  Parar:    aws glue stop-job-run --job-name ${JOB_STREAMING} --job-run-id <ID> --region ${AWS_REGION}"
echo "  Status:   aws glue get-job-runs --job-name ${JOB_STREAMING} --region ${AWS_REGION} --output table"
echo ""
echo "Envie CSVs simulados para: ${STREAMING_INBOUND}"
echo "Depois dispare o crawler gold para atualizar o catalogo:"
echo "  aws glue start-crawler --name crawler-gold --region ${AWS_REGION}"

# 3 - Opcional: iniciar o job agora

if [ "${AUTOCONFIRM:-}" != "1" ]; then
    read -r -p "Deseja iniciar o Glue Streaming Job agora? (s/N): " resposta
    if [ "$resposta" != "s" ]; then
        echo "Job nao iniciado. Execute manualmente quando estiver pronto."
        exit 0
    fi
fi

RUN_ID=$(aws --region "${AWS_REGION}" glue start-job-run \
    --job-name "${JOB_STREAMING}" \
    --query 'JobRunId' \
    --output text)

echo "Streaming job iniciado | JobRunId=${RUN_ID}"
aws --region "${AWS_REGION}" glue get-job-runs \
    --job-name "${JOB_STREAMING}" \
    --query 'JobRuns[0].{State:JobRunState,Id:Id,Started:StartedOn}' \
    --output table
