#!/usr/bin/env bash
set -euo pipefail

# 0 - Define variáveis
SCRIPT_DIR="$(dirname "${BASH_SOURCE[0]}")"
PROJ_ROOT="${SCRIPT_DIR}/../.."

echo "Script dir: ${SCRIPT_DIR}"
echo "Proj root: ${PROJ_ROOT}"

source "${PROJ_ROOT}/config_vars.sh"

# comandos para deletar os recursos criados
# Esvazia cada bucket
aws s3 rm "s3://${BUCKET_RAW}" --recursive --region ${AWS_REGION} || true
aws s3 rm "s3://${BUCKET_BRONZE}" --recursive --region ${AWS_REGION} || true
aws s3 rm "s3://${BUCKET_SILVER}" --recursive --region ${AWS_REGION} || true
aws s3 rm "s3://${BUCKET_GOLD}" --recursive --region ${AWS_REGION} || true
aws s3 rm "s3://${BUCKET_SCRIPTS}" --recursive --region ${AWS_REGION} || true
# Remove os buckets
aws s3 rb "s3://${BUCKET_RAW}" --region ${AWS_REGION} || true
aws s3 rb "s3://${BUCKET_BRONZE}" --region ${AWS_REGION} || true
aws s3 rb "s3://${BUCKET_SILVER}" --region ${AWS_REGION} || true
aws s3 rb "s3://${BUCKET_GOLD}" --region ${AWS_REGION} || true
aws s3 rb "s3://${BUCKET_SCRIPTS}" --region ${AWS_REGION} || true

# Remove o crawler
aws --region ${AWS_REGION} glue delete-crawler --name "crawler-bronze" || true 
aws --region ${AWS_REGION} glue delete-crawler --name "crawler-silver" || true
aws --region ${AWS_REGION} glue delete-crawler --name "crawler-gold" || true

# Remove o database
aws --region ${AWS_REGION} glue delete-database --name "${DATABASE_BRONZE}" || true
aws --region ${AWS_REGION} glue delete-database --name "${DATABASE_SILVER}" || true
aws --region ${AWS_REGION} glue delete-database --name "${DATABASE_GOLD}" || true

# Remove o glue job
aws --region ${AWS_REGION} glue delete-job --job-name "glue-job-raw-etl" || true
aws --region ${AWS_REGION} glue delete-job --job-name "glue-job-bronze-etl" || true
aws --region ${AWS_REGION} glue delete-job --job-name "glue-job-silver-etl" || true