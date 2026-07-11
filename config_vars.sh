BUCKET_RAW="alfabetizacao-data-lake-diego-raw"
BUCKET_BRONZE="alfabetizacao-data-lake-diego-bronze"
BUCKET_SILVER="alfabetizacao-data-lake-diego-silver"
BUCKET_GOLD="alfabetizacao-data-lake-diego-gold"
BUCKET_SCRIPTS="alfabetizacao-data-lake-diego-scripts"
AWS_REGION="us-east-1"
DATABASE_BRONZE=${BUCKET_BRONZE}
DATABASE_SILVER=${BUCKET_SILVER}
DATABASE_GOLD=${BUCKET_GOLD}
ROLE_NAME="LabRole"

export AWS_PAGER="cat"