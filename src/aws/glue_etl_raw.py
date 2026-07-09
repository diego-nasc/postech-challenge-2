import sys
import logging
import time
from collections import defaultdict
import json
from pathlib import Path
import tempfile
import zipfile

import requests
from requests.exceptions import RequestException
import boto3
from botocore.exceptions import BotoCoreError, ClientError
from awsglue.utils import getResolvedOptions

import urllib3
# Desativa avisos gerados pelo uso de verify=False nas requisições.
urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)

# Configura logging
log = logging.getLogger()
log.setLevel(logging.INFO)
handler = logging.StreamHandler(sys.stdout)
formatter = logging.Formatter("%(asctime)s | %(levelname)-8s | %(message)s")
handler.setFormatter(formatter)
log.handlers.clear()
log.addHandler(handler)


# Parametros do job
# JOB_NAME - nome do job
# BUCKET_RAW - bucket de dados raw
# MAX_TENTATIVAS - número máximo de tentativas de transferência
args = getResolvedOptions(sys.argv, ["JOB_NAME", "BUCKET_RAW", "MAX_TENTATIVAS"])

# Contexto S3
s3_client = boto3.client("s3")

# Número máximo de tentativas de transferência
MAX_TENTATIVAS = int(args["MAX_TENTATIVAS"])
BUCKET_RAW = args["BUCKET_RAW"]

# Definição das variáveis globais

# Arquivos alvo para extração dos microdados do INEP dentro do ZIP
ARQUIVOS_ALVO = ["DADOS/TS_ALUNO.csv", "DADOS/TS_ESTADO.csv",
                 "DADOS/TS_MUNICIPIO.csv"]


# URLs dos microdados do INEP para download
MICRODADOS_INEP = {
    2023: ["https://download.inep.gov.br/dados_abertos/microdados_avaliacao_da_alfabetizacao_2023.zip"],
    2024: ["https://download.inep.gov.br/dados_abertos/microdados_avaliacao_da_alfabetizacao_2024.zip"],
    2025: ["https://download.inep.gov.br/dados_abertos/microdados_AEEB_2025.zip"],
}

# URLs das planilhas de metas do INEP para download
METAS_INEP = {
    2023: ["https://download.inep.gov.br/avaliacao_da_alfabetizacao/resultados_e_metas_municipios.xlsx",
           "https://download.inep.gov.br/avaliacao_da_alfabetizacao/resultados_e_metas_ufs.xlsx"],
    2024: ["https://download.inep.gov.br/alfabetiza_brasil/resultados_e_metas_municipios_2024.xlsx",
           "https://download.inep.gov.br/alfabetiza_brasil/resultados_e_metas_ufs_2024_2.xlsx"],
    2025: ["https://download.inep.gov.br/avaliacao_da_alfabetizacao/resultados/resultados_e_metas_municipios_2025_v2.xlsx",
           "https://download.inep.gov.br/avaliacao_da_alfabetizacao/resultados/resultados_e_metas_ufs_2025_v1.xlsx"]
}

# Headers para evitar bloqueio de requests
HEADERS = {
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36"
}

# definição das funções

def transfere_para_s3(url: str, bucket: str, key: str, espera: int = 5) -> str:
    """
    Transfere arquivo da Web para o S3.

    Parâmetros:
        url: endereço do arquivo que será transferido.
        bucket: bucket do S3 onde o arquivo será transferido.
        key: key do arquivo no S3.

    Retorno:
        URI do arquivo transferido para o S3.
    """
    log.info(f"Transferindo arquivo: {url} para {bucket}/{key}")
    for tentativa in range(1, MAX_TENTATIVAS + 1):
        try:
            with requests.get(
                url,
                headers=HEADERS,
                stream=True,
                timeout=(30, 300),
                verify=False
            ) as resposta:
                # Interrompe a execução caso a URL retorne erro HTTP.
                resposta.raise_for_status()

                s3_client.upload_fileobj(
                    resposta.raw,
                    bucket,
                    key
                )

            log.info(f"Transferência concluída para {bucket}/{key}")

            return f"s3://{bucket}/{key}"
        except (RequestException, ClientError, BotoCoreError) as erro:
            if tentativa == MAX_TENTATIVAS:
                log.error(
                    f"Falha após {MAX_TENTATIVAS} tentativas: {url} -> {bucket}/{key}"
                )
                raise
            log.warning(
                f"Tentativa {tentativa}/{MAX_TENTATIVAS} falhou ({erro}). "
                f"Retry em {espera}s..."
            )
            time.sleep(espera)


def extrai_arquivos(ano_to_url: dict[int, str],
                    prefix: str) -> dict[int, str]:
    """
    Extrai os arquivos de um dicionário de URLs para o S3.

    Parâmetros:
        ano_to_url: dicionário com o ano e a lista de URLs.
        prefix: prefixo do caminho do arquivo no S3.

    Retorno:
        Dicionário com o ano e a lista de caminhos dos arquivos no S3.
    """
    prefix = prefix.rstrip("/")
    paths = defaultdict(list)
    for ano, urls in ano_to_url.items():
        for url in urls:
            # Extrai o nome do arquivo a partir do final da URL.
            nome_arquivo = url.split("/")[-1]
            log.info(f"Ano {ano}:")
            path = f"{prefix}/{ano}/{nome_arquivo}"
            transfere_para_s3(url, BUCKET_RAW.rstrip("/"), path)
            paths[ano].append(path)
    return paths


def descompactar(ano_to_awskey: dict[int, str],
                 prefix: str,
                 arquivos_alvo: list[str] = ARQUIVOS_ALVO) -> dict[int, str]:
    """
    Extrai os CSVs necessários de um arquivo ZIP do INEP e transfere para o S3.

    Parâmetros:
        ano_to_awskeys: dicionário com o ano e a lista de caminhos dos arquivos ZIP no S3.
        arquivos_alvo: lista de caminhos dos arquivos a serem extraídos.

    Retorno:
        Dicionário com o ano e o caminho do arquivo CSV extraído no S3.
    """
    prefix = prefix.rstrip("/")
    output_paths = defaultdict(list)
    for ano, awskeys in ano_to_awskey.items():
        for awskey in awskeys:
            awskey_basename = awskey.rsplit("/", 1)[1]
            tmp_zip = Path(tempfile.gettempdir()) / f"{ano}_{awskey_basename}"
            try:
                s3_client.download_file(BUCKET_RAW, awskey, str(tmp_zip))
                with zipfile.ZipFile(tmp_zip) as arquivo_zip:
                    for alvo in arquivos_alvo:
                        basename = alvo.rsplit("/", 1)[1]
                        destino_arquivo = f"{prefix}/{ano}/{basename}"
                        with arquivo_zip.open(alvo) as fonte:
                            s3_client.upload_fileobj(fonte,
                                                     BUCKET_RAW, destino_arquivo)
                            log.info(f"{alvo} extraido (ano {ano})!")
                            output_paths[ano].append(destino_arquivo)
            finally:
                if tmp_zip.exists():
                    tmp_zip.unlink()
    return output_paths


def main():

    log.info("Iniciando job de extração dos arquivos do INEP...")
    log.info("Extraindo arquivos de microdados do INEP...")

    zip_paths = extrai_arquivos(MICRODADOS_INEP, "zip/")

    log.info("Caminhos dos arquivos zip extraídos: %s",
             json.dumps(zip_paths, indent=4))
    log.info("Extraindo arquivos microdados do INEP...")

    file_paths = descompactar(zip_paths, "extracted/", ARQUIVOS_ALVO)

    log.info("Caminhos dos arquivos extraídos: %s",
             json.dumps(file_paths, indent=4))
    log.info("Extraindo arquivos de metas do INEP...")

    metas_paths = extrai_arquivos(METAS_INEP, "extracted/")
    
    log.info("Caminhos dos arquivos metas extraídos: %s",
             json.dumps(metas_paths, indent=4))
    log.info("Extração dos arquivos do INEP concluída com sucesso!")
    log.info("Job de extração dos arquivos do INEP concluído com sucesso!")


main()