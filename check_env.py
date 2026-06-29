from pathlib import Path
import os
from dotenv import load_dotenv, find_dotenv, dotenv_values

dotenv_path = find_dotenv(usecwd=True)
print(".env encontrado em:", dotenv_path or "NENHUM")
print("Chaves definidas no .env:", list(dotenv_values(dotenv_path).keys()) if dotenv_path else [])

load_dotenv(dotenv_path)
cred = os.getenv("GOOGLE_APPLICATION_CREDENTIALS")
print("Variável:", cred)
print("Arquivo existe?", Path(cred).exists() if cred else "NÃO DEFINIDA")