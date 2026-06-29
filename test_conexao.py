from dotenv import load_dotenv, find_dotenv
from google.cloud import bigquery

load_dotenv(find_dotenv(usecwd=True))
client = bigquery.Client(project="postech-alfabetizacao")
client.query("SELECT 1").result()
print("Conexão OK")