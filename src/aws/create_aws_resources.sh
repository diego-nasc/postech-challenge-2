#!/usr/bin/env bash
set -euo pipefail

# 0 - Define variáveis
SCRIPT_DIR="$(dirname "${BASH_SOURCE[0]}")"
PROJ_ROOT="${SCRIPT_DIR}/../.."

echo "Script dir: ${SCRIPT_DIR}"
echo "Proj root: ${PROJ_ROOT}"

source "${PROJ_ROOT}/config_vars.sh"

# 1 - Adequa as permissões necessárias
# SEM PERMISSÃO NECESSÁRIA PARA ISSO
# NESTE CASO A ROLE labRole SERÁ USADA

# 2 - Cria buckets no S3

echo "Os buckets a serem criados são:
- ${BUCKET_RAW}
- ${BUCKET_BRONZE}
- ${BUCKET_SILVER}
- ${BUCKET_GOLD}
- ${BUCKET_SCRIPTS}"

read -r -p "Deseja criar os buckets? (s/N): " resposta
if [ "$resposta" != "s" ]; then
    echo "Os buckets não foram criados"
    exit 1
fi
aws --region ${AWS_REGION} s3 mb "s3://${BUCKET_RAW}"
aws --region ${AWS_REGION} s3 mb "s3://${BUCKET_BRONZE}"
aws --region ${AWS_REGION} s3 mb "s3://${BUCKET_SILVER}"
aws --region ${AWS_REGION} s3 mb "s3://${BUCKET_GOLD}"
aws --region ${AWS_REGION} s3 mb "s3://${BUCKET_SCRIPTS}"

echo "Listando S3 buckets:"
aws --region ${AWS_REGION} s3 ls

# 3 - cria databases no Glue

echo "Os databases a serem criados são:
- ${DATABASE_BRONZE}
- ${DATABASE_SILVER}
- ${DATABASE_GOLD}"

read -r -p "Deseja criar os databases? (s/N): " resposta
if [ "$resposta" != "s" ]; then
    echo "Os databases não foram criados"
    exit 1
fi

# camada bronze
aws --region ${AWS_REGION} glue create-database \
    --database-input "{
        \"Name\": \"${DATABASE_BRONZE}\",
        \"Description\": \"Camada Bronze/Dados Brutos\"
    }"
# camada silver
aws --region ${AWS_REGION} glue create-database \
    --database-input "{
        \"Name\": \"${DATABASE_SILVER}\",
        \"Description\": \"Camada Silver/Dados Processados\"
    }"
# camada gold
aws --region ${AWS_REGION} glue create-database \
    --database-input "{
        \"Name\": \"${DATABASE_GOLD}\",
        \"Description\": \"Camada Gold/Dados Analíticos\"
    }"

# Lista databases no Glue
aws --region ${AWS_REGION} glue get-databases \
    --query "DatabaseList[].Name"

# 4 - Cria crawlers para camada bronze

read -r -p "Deseja criar os crawlers? (s/N): " resposta
if [ "$resposta" != "s" ]; then
    echo "Os crawlers não foram criados"
    exit 1
fi

# camada bronze - repetir para cada entidade
# agendamento as 9 da manhã (UTC)
aws --region ${AWS_REGION} glue create-crawler \
    --name "crawler-bronze" \
    --database-name "${DATABASE_BRONZE}" \
    --role "${ROLE_NAME}" \
    --schedule "cron(0 9 * * ? *)" \
    --targets "{
      \"S3Targets\":[
        {\"Path\":\"s3://${BUCKET_BRONZE}/\"}
      ]}"

# executa o crawler (comando para rodar por demanda)
aws --region ${AWS_REGION} glue start-crawler \
    --name "crawler-bronze"

# 5 - cria glue job

read -r -p "Deseja criar o glue job? (s/N): " resposta
if [ "$resposta" != "s" ]; then
    echo "O glue job não foi criado"
    exit 1
fi

# 5.1 - Glue jop para a RAW

# 5.1.1 - sobe script para o S3
aws --region ${AWS_REGION} s3 cp ${SCRIPT_DIR}/glue_etl_raw.py \
    s3://${BUCKET_SCRIPTS}/glue_etl_raw.py

# 5.1.2 - cria job
aws --region ${AWS_REGION} glue create-job \
    --name "glue-job-raw-etl" \
    --role "${ROLE_NAME}" \
    --command "{
        \"Name\": \"pythonshell\",
        \"ScriptLocation\": \"s3://${BUCKET_SCRIPTS}/glue_etl_raw.py\",
        \"PythonVersion\": \"3.9\"
    }" \
    --default-arguments "{
        \"--JOB_NAME\": \"glue-job-raw-etl\",
        \"--BUCKET_RAW\": \"${BUCKET_RAW}\",
        \"--MAX_TENTATIVAS\": \"3\"
    }"

# 5.1.3 - executa job
aws --region ${AWS_REGION} glue start-job-run \
    --job-name "glue-job-raw-etl"


# 5.1.4 - verifica status do job
aws --region ${AWS_REGION} glue get-job-runs \
    --job-name "glue-job-raw-etl" \
    --query 'JobRuns[0].{State:JobRunState,Error:ErrorMessage}' \
    --output table

# 5.2 - Glue job para a BRONZE

# 5.2.0 - sobe dependências para o S3
# 1. Baixa o JAR principal
curl -L -o /tmp/spark-excel_2.12-3.5.1_0.20.4.jar \
  https://repo1.maven.org/maven2/com/crealytics/spark-excel_2.12/3.5.1_0.20.4/spark-excel_2.12-3.5.1_0.20.4.jar

# 2. Sobe para o bucket de scripts
aws s3 cp /tmp/spark-excel_2.12-3.5.1_0.20.4.jar \
  s3://${BUCKET_SCRIPTS}/jars/spark-excel_2.12-3.5.1_0.20.4.jar \
  --region ${AWS_REGION}

# 5.2.1 - sobe script para o S3
aws --region ${AWS_REGION} s3 cp ${SCRIPT_DIR}/glue_etl_bronze.py \
    s3://${BUCKET_SCRIPTS}/glue_etl_bronze.py

# 5.2.2 - cria job
aws --region ${AWS_REGION} glue create-job \
    --name "glue-job-bronze-etl" \
    --role "${ROLE_NAME}" \
    --glue-version "5.1" \
    --worker-type "G.1X" \
    --number-of-workers 2 \
    --command "{
        \"Name\": \"glueetl\",
        \"ScriptLocation\": \"s3://${BUCKET_SCRIPTS}/glue_etl_bronze.py\",
        \"PythonVersion\": \"3\"
    }" \
    --default-arguments "{
        \"--JOB_NAME\": \"glue-job-bronze-etl\",
        \"--BUCKET_BRONZE\": \"${BUCKET_BRONZE}\",
        \"--BUCKET_RAW\": \"${BUCKET_RAW}\",
        \"--extra-jars\": \"s3://${BUCKET_SCRIPTS}/jars/spark-excel_2.12-3.5.1_0.20.4.jar\"
    }"

# 5.2.3 - executa job
aws --region ${AWS_REGION} glue start-job-run \
    --job-name "glue-job-bronze-etl"

# 5.2.4 - verifica status do job
aws --region ${AWS_REGION} glue get-job-runs \
    --job-name "glue-job-bronze-etl" \
    --query 'JobRuns[0].{State:JobRunState,Error:ErrorMessage}' \
    --output table

# 5.3 - Glue job para a SILVER

# 5.3.1 - sobe script para o S3
aws --region ${AWS_REGION} s3 cp ${SCRIPT_DIR}/glue_etl_silver.py \
    s3://${BUCKET_SCRIPTS}/glue_etl_silver.py

# 5.3.2 - cria job
aws --region ${AWS_REGION} glue create-job \
    --name "glue-job-silver-etl" \
    --role "${ROLE_NAME}" \
    --glue-version "5.1" \
    --worker-type "G.1X" \
    --number-of-workers 2 \
    --command "{
        \"Name\": \"glueetl\",
        \"ScriptLocation\": \"s3://${BUCKET_SCRIPTS}/glue_etl_silver.py\",
        \"PythonVersion\": \"3\"
    }" \
    --default-arguments "{
        \"--JOB_NAME\": \"glue-job-silver-etl\",
        \"--BUCKET_BRONZE\": \"${BUCKET_BRONZE}\",
        \"--BUCKET_SILVER\": \"${BUCKET_SILVER}\"
    }"

# 5.3.3 - executa job
aws --region ${AWS_REGION} glue start-job-run \
    --job-name "glue-job-silver-etl"

# 5.3.4 - verifica status do job
aws --region ${AWS_REGION} glue get-job-runs \
    --job-name "glue-job-silver-etl" \
    --query 'JobRuns[0].{State:JobRunState,Error:ErrorMessage}' \
    --output table

# 5.4 - Glue job para a GOLD

# 5.4.1 - sobe script para o S3
aws --region ${AWS_REGION} s3 cp ${SCRIPT_DIR}/glue_etl_gold.py \
    s3://${BUCKET_SCRIPTS}/glue_etl_gold.py

# 5.4.2 - cria job
aws --region ${AWS_REGION} glue create-job \
    --name "glue-job-gold-etl" \
    --role "${ROLE_NAME}" \
    --glue-version "5.1" \
    --worker-type "G.1X" \
    --number-of-workers 2 \
    --command "{
        \"Name\": \"glueetl\",
        \"ScriptLocation\": \"s3://${BUCKET_SCRIPTS}/glue_etl_gold.py\",
        \"PythonVersion\": \"3\"
    }" \
    --default-arguments "{
        \"--JOB_NAME\": \"glue-job-gold-etl\",
        \"--BUCKET_SILVER\": \"${BUCKET_SILVER}\",
        \"--BUCKET_GOLD\": \"${BUCKET_GOLD}\"
    }"

# 5.4.3 - executa job
aws --region ${AWS_REGION} glue start-job-run \
    --job-name "glue-job-gold-etl"

# 5.4.4 - verifica status do job
aws --region ${AWS_REGION} glue get-job-runs \
    --job-name "glue-job-gold-etl" \
    --query 'JobRuns[0].{State:JobRunState,Error:ErrorMessage}' \
    --output table

########################################################

# Verifica athena com uma consulta de exemplo.

# inicia o crawler para a camada bronze
aws --region ${AWS_REGION} glue start-crawler \
    --name "crawler-bronze"

# verifica status do crawler
aws --region ${AWS_REGION} glue get-crawler \
    --name "crawler-bronze" \
    --query 'Crawler.State'

QUERY_ID=$(aws athena start-query-execution \
  --region "$AWS_REGION" \
  --query-string "SELECT COUNT(*) AS n FROM ts_aluno" \
  --query-execution-context "Database=$DATABASE_BRONZE" \
  --result-configuration "OutputLocation=s3://${BUCKET_BRONZE}/athena_results/" \
  --query 'QueryExecutionId' \
  --output text)

echo "Rodando: $QUERY_ID"

# espera até SUCCEEDED ou FAILED
while true; do
  STATE=$(aws athena get-query-execution \
    --region "$AWS_REGION" \
    --query-execution-id "$QUERY_ID" \
    --query 'QueryExecution.Status.State' \
    --output text)
  echo "Status: $STATE"
  [[ "$STATE" == "SUCCEEDED" || "$STATE" == "FAILED" || "$STATE" == "CANCELLED" ]] && break
  sleep 2
done

if [[ "$STATE" == "SUCCEEDED" ]]; then
  aws athena get-query-results \
    --region "$AWS_REGION" \
    --query-execution-id "$QUERY_ID" \
    --output table
else
  aws athena get-query-execution \
    --region "$AWS_REGION" \
    --query-execution-id "$QUERY_ID" \
    --query 'QueryExecution.Status.StateChangeReason' \
    --output text
fi