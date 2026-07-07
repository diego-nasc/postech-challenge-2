#!/usr/bin/env bash
set -euo pipefail

# 0 - Define variáveis
SCRIPT_DIR="$(dirname "${BASH_SOURCE[0]}")"
source "${SCRIPT_DIR}/config_vars.sh"

# 1 - Adequa as permissões necessárias
# SEM PERMISSÃO NECESSÁRIA PARA ISSO
# NESTE CASO A ROLE labRole SERÁ USADA

# 2 - Cria buckets no S3

echo "Os buckets a serem criados são:
- ${BUCKET_BRONZE}
- ${BUCKET_SILVER}
- ${BUCKET_GOLD}
- ${BUCKET_SCRIPTS}"

read -r -p "Deseja criar os buckets? (s/N): " resposta
if [ "$resposta" != "s" ]; then
    echo "Os buckets não foram criados"
    exit 1
fi

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

exit 0

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

# 5.1 - sobe script para o S3
aws --region ${AWS_REGION} s3 cp glue_job.py s3://${BUCKET_SCRIPTS}/glue_job.py

# 5.2 - cria job
aws --region ${AWS_REGION} glue create-job \
    --name "glue-job-bronze" \
    --role "${ROLE_NAME}" \
    --command '{
        "Name": "glueetl",
        "ScriptLocation": "s3://techchallenge2-scripts/glue_job.py",
        "PythonVersion": "3"
    }' \
    --glue-version "4.0" \
    --number-of-workers 2 \
    --worker-type "G.1X"

# 5.3 - executa job
aws --region ${AWS_REGION} glue start-job-run \
    --job-name "glue-job-bronze"


# 5.4 - verifica status do job
aws --region ${AWS_REGION} glue get-job-runs \
    --job-name "glue-job-bronze" \
    --query 'JobRuns[0].{State:JobRunState,Error:ErrorMessage}' \
    --output table

########################################################

