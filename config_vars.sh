BUCKET_RAW="alfabetizacao-data-lake-raw"
BUCKET_BRONZE="alfabetizacao-data-lake-bronze"
BUCKET_SILVER="alfabetizacao-data-lake-silver"
BUCKET_GOLD="alfabetizacao-data-lake-gold"
BUCKET_SCRIPTS="alfabetizacao-data-lake-scripts"
AWS_REGION="us-east-1"
DATABASE_BRONZE=${BUCKET_BRONZE}
DATABASE_SILVER=${BUCKET_SILVER}
DATABASE_GOLD=${BUCKET_GOLD}
ROLE_NAME="LabRole"

export AWS_PAGER="cat"
