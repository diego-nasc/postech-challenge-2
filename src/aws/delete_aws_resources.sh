#!/usr/bin/env bash
set -euo pipefail

# 0 - Define variáveis
SCRIPT_DIR="$(dirname "${BASH_SOURCE[0]}")"
source "${SCRIPT_DIR}/config_vars.sh"

# comandos para deletar os recursos criados
# Esvazia cada bucket
aws s3 rm "s3://${BUCKET_BRONZE}" --recursive --region ${AWS_REGION}
aws s3 rm "s3://${BUCKET_SILVER}" --recursive --region ${AWS_REGION}
aws s3 rm "s3://${BUCKET_GOLD}" --recursive --region ${AWS_REGION}
# Remove os buckets
aws s3 rb "s3://${BUCKET_BRONZE}" --region ${AWS_REGION}
aws s3 rb "s3://${BUCKET_SILVER}" --region ${AWS_REGION}
aws s3 rb "s3://${BUCKET_GOLD}" --region ${AWS_REGION}

# Remove o crawler
# aws --region ${AWS_REGION} glue delete-crawler --name "crawler-bronze"

# Remove o database
# aws --region ${AWS_REGION} glue delete-database --name "${DATABASE_BRONZE}"
# aws --region ${AWS_REGION} glue delete-database --name "${DATABASE_SILVER}"
# aws --region ${AWS_REGION} glue delete-database --name "${DATABASE_GOLD}"