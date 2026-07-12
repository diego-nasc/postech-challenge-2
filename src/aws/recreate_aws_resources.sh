#!/usr/bin/env bash
set -euo pipefail

# ==========================================================================
# create_aws_resources.sh
#
# Provisionamento + deploy + execução do pipeline (RAW -> BRONZE -> SILVER ->
# GOLD) idempotente.
#
# Contrato de idempotência:
#   - Buckets S3 e databases Glue: SKIP-IF-EXISTS (recursos persistentes;
#     delete seria destrutivo).
#   - Crawlers e jobs Glue: DELETE + CREATE (o script é a fonte única da
#     configuração; qualquer mudança em GlueVersion/workers/argumentos/
#     ScriptLocation é refletida no próximo deploy).
#   - Re-run re-executa o pipeline completo.
#
# Guards de estado:
#   - Nunca deleta/inicia crawler em RUNNING/STOPPING (espera ficar READY).
#   - Nunca recria job com run ativo (espera o run terminar).
#
# Observação sobre 'set -e':
#   As funções de CHECAGEM (*_existe, *_estado, *_run_ativo) só podem ser
#   usadas dentro de 'if' ou de $( ), pois retornam código != 0 / string
#   quando o recurso não existe -- se chamadas "soltas", o set -e abortaria o
#   script no caso que justamente queremos tratar. As funções que EXECUTAM
#   ação (cria_*, recria_*, inicia_*, prepara_*) são chamadas diretas, para que
#   uma falha real de create/delete continue abortando o deploy.
# ==========================================================================

# --------------------------------------------------------------------------
# 0 - Variáveis e configuração
# --------------------------------------------------------------------------
SCRIPT_DIR="$(dirname "${BASH_SOURCE[0]}")"
PROJ_ROOT="${SCRIPT_DIR}/../.."

echo "Script dir: ${SCRIPT_DIR}"
echo "Proj root:  ${PROJ_ROOT}"

source "${PROJ_ROOT}/config_vars.sh"

# Default seguro: evita "unbound variable" com set -u quando o script é
# chamado como 'bash create_aws_resources.sh' sem AUTOCONFIRM definido.
AUTOCONFIRM="${AUTOCONFIRM:-0}"

# ==========================================================================
# Helpers
# ==========================================================================

confirma() {
  # confirma "mensagem" -> 0 se pode prosseguir, 1 se o usuário recusou.
  local msg="$1"
  if [ "${AUTOCONFIRM}" = "1" ]; then
    return 0
  fi
  read -r -p "${msg} (s/N): " resposta
  [ "${resposta:-}" = "s" ]
}

# ---- S3 --------------------------------------------------------------------
bucket_existe() {
  # Usa list-buckets (nível de conta) em vez de head-bucket: detecta buckets
  # que VOCÊ possui sem depender de permissão por-bucket e sem o 403 vs 404
  # ambíguo. Captura em variável (sem pipe) para não cair no gotcha
  # pipefail + SIGPIPE.
  local found
  found="$(aws --region "${AWS_REGION}" s3api list-buckets \
    --query "Buckets[?Name=='$1'].Name" --output text 2>/dev/null || true)"
  [ -n "${found}" ]
}

cria_bucket_se_novo() {
  local b="$1"
  if bucket_existe "${b}"; then
    echo "  [skip]   bucket já existe: ${b}"
  else
    echo "  [create] bucket: ${b}"
    aws --region "${AWS_REGION}" s3 mb "s3://${b}"
  fi
}

# ---- Glue Database ---------------------------------------------------------
db_existe() {
  aws --region "${AWS_REGION}" glue get-database --name "$1" >/dev/null 2>&1
}

cria_db_se_novo() {
  local name="$1" desc="$2"
  if db_existe "${name}"; then
    echo "  [skip]   database já existe: ${name}"
  else
    echo "  [create] database: ${name}"
    aws --region "${AWS_REGION}" glue create-database \
      --database-input "{\"Name\": \"${name}\", \"Description\": \"${desc}\"}"
  fi
}

# ---- Glue Crawler ----------------------------------------------------------
crawler_existe() {
  aws --region "${AWS_REGION}" glue get-crawler --name "$1" >/dev/null 2>&1
}

crawler_estado() {
  # Imprime READY / RUNNING / STOPPING (vazio se não existir).
  # '|| true' evita que o set -e derrube o script quando o crawler não existe.
  aws --region "${AWS_REGION}" glue get-crawler --name "$1" \
    --query 'Crawler.State' --output text 2>/dev/null || true
}

espera_crawler_pronto() {
  # Bloqueia enquanto RUNNING/STOPPING. Retorna quando READY (ou inexistente).
  # Guard para delete/start.
  local name="$1" estado
  while true; do
    estado="$(crawler_estado "${name}")"
    if [ -z "${estado}" ] || [ "${estado}" = "READY" ]; then
      break
    fi
    echo "  [guard]  crawler '${name}' em ${estado}; aguardando READY..."
    sleep 15
  done
}

recria_crawler() {
  # delete + create.
  local name="$1" db="$2" bucket="$3"

  if crawler_existe "${name}"; then
    espera_crawler_pronto "${name}"        # não deletar em RUNNING
    echo "  [delete] crawler: ${name}"
    aws --region "${AWS_REGION}" glue delete-crawler --name "${name}"
  fi

  echo "  [create] crawler: ${name} (db=${db}, alvo=s3://${bucket}/)"

  aws --region "${AWS_REGION}" glue create-crawler \
    --name "${name}" \
    --database-name "${db}" \
    --role "${ROLE_NAME}" \
    --schedule "cron(0 9 * * ? *)" \
    --targets "{\"S3Targets\":[{\"Path\":\"s3://${bucket}/\",\"Exclusions\":[\"_checkpoints/**\",\"athena_results/**\"]}]}"
}

inicia_crawler() {
  # Não inicia em RUNNING: espera ficar READY e então dispara.
  local name="$1"
  espera_crawler_pronto "${name}"
  echo "  [start]  crawler: ${name}"
  aws --region "${AWS_REGION}" glue start-crawler --name "${name}"
}

espera_crawler_concluir() {
  # Espera o crawl INICIAR (sair de READY) e depois VOLTAR para READY.
  # Usado antes de consultar no Athena, para garantir o catálogo populado.
  local name="$1" estado i
  for i in {1..6}; do                      # aguarda o crawl efetivamente iniciar
    estado="$(crawler_estado "${name}")"
    [ "${estado}" != "READY" ] && break
    sleep 5
  done
  espera_crawler_pronto "${name}"          # aguarda concluir
}

# ---- Glue Job --------------------------------------------------------------
job_existe() {
  aws --region "${AWS_REGION}" glue get-job --job-name "$1" >/dev/null 2>&1
}

job_run_ativo() {
  # Imprime os IDs de runs ativos (vazio se nenhum).
  aws --region "${AWS_REGION}" glue get-job-runs --job-name "$1" \
    --query "JobRuns[?JobRunState=='RUNNING' || JobRunState=='STARTING' || JobRunState=='STOPPING' || JobRunState=='WAITING'].Id" \
    --output text 2>/dev/null || true
}

espera_job_livre() {
  # Guard: não recriar job com run ativo. Espera terminar.
  # (Alternativa: cancelar o run velho com
  #   aws glue batch-stop-job-run --job-name "$name" --job-run-ids <ids>
  #  em vez de esperar.)
  local name="$1" ativos
  while true; do
    ativos="$(job_run_ativo "${name}")"
    if [ -z "${ativos}" ]; then break; fi
    echo "  [guard]  job '${name}' com run(s) ativo(s): ${ativos}; aguardando terminar..."
    sleep 30
  done
}

prepara_recriacao_job() {
  # Guard + delete. Chamar IMEDIATAMENTE antes do 'create-job' específico de
  # cada camada (os create-job diferem demais para parametrizar num único bloco).
  local name="$1"
  if job_existe "${name}"; then
    espera_job_livre "${name}"             # não deletar com run ativo
    echo "  [delete] job: ${name}"
    aws --region "${AWS_REGION}" glue delete-job --job-name "${name}"
  fi
}

espera_job_run() {
  # Espera o run corrente chegar a um estado terminal e reporta.
  local job="$1" estado
  sleep 5                                   # dá tempo do run registrar após o start
  while true; do
    estado="$(aws --region "${AWS_REGION}" glue get-job-runs --job-name "${job}" \
      --query 'JobRuns[0].JobRunState' --output text 2>/dev/null || true)"
    case "${estado}" in
      STARTING|RUNNING|STOPPING|WAITING)
        echo "  [wait]   job '${job}' em ${estado}..."
        sleep 30
        ;;
      
      FAILED|TIMEOUT|ERROR)
        echo "  [ERROR]  job '${job}' falhou com estado: ${estado}. Abortando pipeline."
        exit 1
        ;;
      SUCCEEDED)
        echo "  [done]   job '${job}' terminou com sucesso."
        break
        ;;
      *)
        echo "  [warn]   job '${job}' terminou em um estado desconhecido/incomum: ${estado:-DESCONHECIDO}"
        break
        ;;
    esac
  done
}

# ==========================================================================
# 1 - Permissões
#     Sem permissão adicional necessária; a role labRole (${ROLE_NAME}) é usada.
# ==========================================================================

# ==========================================================================
# 2 - Buckets S3  (SKIP-IF-EXISTS)
# ==========================================================================
echo "Buckets alvo:
- ${BUCKET_RAW}
- ${BUCKET_BRONZE}
- ${BUCKET_SILVER}
- ${BUCKET_GOLD}
- ${BUCKET_SCRIPTS}"

if confirma "Deseja garantir a criação dos buckets?"; then
  cria_bucket_se_novo "${BUCKET_RAW}"
  cria_bucket_se_novo "${BUCKET_BRONZE}"
  cria_bucket_se_novo "${BUCKET_SILVER}"
  cria_bucket_se_novo "${BUCKET_GOLD}"
  cria_bucket_se_novo "${BUCKET_SCRIPTS}"
  echo "Buckets no S3:"
  aws --region "${AWS_REGION}" s3 ls
else
  echo "Etapa de buckets pulada."
fi

# ==========================================================================
# 3 - Databases Glue  (SKIP-IF-EXISTS)
# ==========================================================================
echo "Databases alvo:
- ${DATABASE_BRONZE}
- ${DATABASE_SILVER}
- ${DATABASE_GOLD}"

if confirma "Deseja garantir a criação dos databases?"; then
  cria_db_se_novo "${DATABASE_BRONZE}" "Camada Bronze/Dados Brutos"
  cria_db_se_novo "${DATABASE_SILVER}" "Camada Silver/Dados Processados"
  cria_db_se_novo "${DATABASE_GOLD}"   "Camada Gold/Dados Analíticos"
  echo "Databases no Glue:"
  aws --region "${AWS_REGION}" glue get-databases --query "DatabaseList[].Name"
else
  echo "Etapa de databases pulada."
fi

# ==========================================================================
# 4 - Crawlers Glue  (DELETE + CREATE)
# ==========================================================================
if confirma "Deseja (re)criar os crawlers?"; then
  recria_crawler "crawler-bronze" "${DATABASE_BRONZE}" "${BUCKET_BRONZE}"
  recria_crawler "crawler-silver" "${DATABASE_SILVER}" "${BUCKET_SILVER}"
  recria_crawler "crawler-gold"   "${DATABASE_GOLD}"   "${BUCKET_GOLD}"

  # OBS: iniciar os crawlers aqui roda um crawl sobre buckets ainda VAZIOS
  # (antes dos jobs popularem os dados). Mantido para fidelidade ao fluxo
  # original; os guards de inicia_crawler tornam o duplo-start seguro. Se
  # preferir, remova estes 3 'inicia_crawler' e deixe o crawl só após cada
  # job (seções 5.x).
  inicia_crawler "crawler-bronze"
  inicia_crawler "crawler-silver"
  inicia_crawler "crawler-gold"
else
  echo "Etapa de crawlers pulada."
fi

# ==========================================================================
# 5 - Glue Jobs  (DELETE + CREATE) + execução
# ==========================================================================
if confirma "Deseja (re)criar e executar os glue jobs?"; then

  # ---------------- 5.1 - RAW (pythonshell) ----------------
  aws --region "${AWS_REGION}" s3 cp "${SCRIPT_DIR}/glue_elt_raw.py" \
      "s3://${BUCKET_SCRIPTS}/glue_elt_raw.py"

  prepara_recriacao_job "glue-job-raw-etl"
  aws --region "${AWS_REGION}" glue create-job \
      --name "glue-job-raw-etl" \
      --role "${ROLE_NAME}" \
      --command "{\"Name\": \"pythonshell\", \"ScriptLocation\": \"s3://${BUCKET_SCRIPTS}/glue_elt_raw.py\", \"PythonVersion\": \"3.9\"}" \
      --default-arguments "{\"--JOB_NAME\": \"glue-job-raw-etl\", \"--BUCKET_RAW\": \"${BUCKET_RAW}\", \"--MAX_TENTATIVAS\": \"3\"}"

  aws --region "${AWS_REGION}" glue start-job-run --job-name "glue-job-raw-etl"
  espera_job_run "glue-job-raw-etl"

  # ---------------- 5.2 - BRONZE (glueetl) ----------------
  # Dependência spark-excel (crealytics): baixa só se ainda não estiver em /tmp.
  JAR_LOCAL="/tmp/spark-excel_2.12-3.5.1_0.20.4.jar"
  JAR_URL="https://repo1.maven.org/maven2/com/crealytics/spark-excel_2.12/3.5.1_0.20.4/spark-excel_2.12-3.5.1_0.20.4.jar"
  if [ ! -f "${JAR_LOCAL}" ]; then
    echo "  [download] ${JAR_URL}"
    curl -L -o "${JAR_LOCAL}" "${JAR_URL}"
  else
    echo "  [skip]     JAR já baixado: ${JAR_LOCAL}"
  fi
  aws --region "${AWS_REGION}" s3 cp "${JAR_LOCAL}" \
      "s3://${BUCKET_SCRIPTS}/jars/spark-excel_2.12-3.5.1_0.20.4.jar"

  aws --region "${AWS_REGION}" s3 cp "${SCRIPT_DIR}/glue_elt_bronze.py" \
      "s3://${BUCKET_SCRIPTS}/glue_elt_bronze.py"

  prepara_recriacao_job "glue-job-bronze-etl"
  aws --region "${AWS_REGION}" glue create-job \
      --name "glue-job-bronze-etl" \
      --role "${ROLE_NAME}" \
      --glue-version "5.1" \
      --worker-type "G.1X" \
      --number-of-workers 2 \
      --command "{\"Name\": \"glueetl\", \"ScriptLocation\": \"s3://${BUCKET_SCRIPTS}/glue_elt_bronze.py\", \"PythonVersion\": \"3\"}" \
      --default-arguments "{\"--JOB_NAME\": \"glue-job-bronze-etl\", \"--BUCKET_BRONZE\": \"${BUCKET_BRONZE}\", \"--BUCKET_RAW\": \"${BUCKET_RAW}\", \"--extra-jars\": \"s3://${BUCKET_SCRIPTS}/jars/spark-excel_2.12-3.5.1_0.20.4.jar\"}"

  aws --region "${AWS_REGION}" glue start-job-run --job-name "glue-job-bronze-etl"
  espera_job_run "glue-job-bronze-etl"
  inicia_crawler "crawler-bronze"

  # ---------------- 5.3 - SILVER (glueetl) ----------------
  aws --region "${AWS_REGION}" s3 cp "${SCRIPT_DIR}/glue_elt_silver.py" \
      "s3://${BUCKET_SCRIPTS}/glue_elt_silver.py"

  prepara_recriacao_job "glue-job-silver-etl"
  aws --region "${AWS_REGION}" glue create-job \
      --name "glue-job-silver-etl" \
      --role "${ROLE_NAME}" \
      --glue-version "5.1" \
      --worker-type "G.1X" \
      --number-of-workers 2 \
      --command "{\"Name\": \"glueetl\", \"ScriptLocation\": \"s3://${BUCKET_SCRIPTS}/glue_elt_silver.py\", \"PythonVersion\": \"3\"}" \
      --default-arguments "{\"--JOB_NAME\": \"glue-job-silver-etl\", \"--BUCKET_BRONZE\": \"${BUCKET_BRONZE}\", \"--BUCKET_SILVER\": \"${BUCKET_SILVER}\"}"

  aws --region "${AWS_REGION}" glue start-job-run --job-name "glue-job-silver-etl"
  espera_job_run "glue-job-silver-etl"
  inicia_crawler "crawler-silver"

  # ---------------- 5.4 - GOLD (glueetl) ----------------
  aws --region "${AWS_REGION}" s3 cp "${SCRIPT_DIR}/glue_elt_gold.py" \
      "s3://${BUCKET_SCRIPTS}/glue_elt_gold.py"

  prepara_recriacao_job "glue-job-gold-etl"
  aws --region "${AWS_REGION}" glue create-job \
      --name "glue-job-gold-etl" \
      --role "${ROLE_NAME}" \
      --glue-version "5.1" \
      --worker-type "G.1X" \
      --number-of-workers 2 \
      --command "{\"Name\": \"glueetl\", \"ScriptLocation\": \"s3://${BUCKET_SCRIPTS}/glue_elt_gold.py\", \"PythonVersion\": \"3\"}" \
      --default-arguments "{\"--JOB_NAME\": \"glue-job-gold-etl\", \"--BUCKET_SILVER\": \"${BUCKET_SILVER}\", \"--BUCKET_GOLD\": \"${BUCKET_GOLD}\"}"

  aws --region "${AWS_REGION}" glue start-job-run --job-name "glue-job-gold-etl"
  espera_job_run "glue-job-gold-etl"
  inicia_crawler "crawler-gold"
  
  # ---------------- 5.5 - STREAMING (glueetl, job FINITO) ----------------
  # Structured Streaming com fonte de arquivos no S3. Auto-encerra
  # (produtor -> processAllAvailable -> stop) => glueetl comum + espera_job_run.
  # Escreve a particao ano=2026 em silver/municipio e gold/indicadores_municipio.
  aws --region "${AWS_REGION}" s3 cp "${SCRIPT_DIR}/glue_elt_streaming.py" \
      "s3://${BUCKET_SCRIPTS}/glue_elt_streaming.py"

  prepara_recriacao_job "glue-job-streaming-etl"
  aws --region "${AWS_REGION}" glue create-job \
      --name "glue-job-streaming-etl" \
      --role "${ROLE_NAME}" \
      --glue-version "5.1" \
      --worker-type "G.1X" \
      --number-of-workers 2 \
      --command "{\"Name\": \"glueetl\", \"ScriptLocation\": \"s3://${BUCKET_SCRIPTS}/glue_elt_streaming.py\", \"PythonVersion\": \"3\"}" \
      --default-arguments "{\"--JOB_NAME\": \"glue-job-streaming-etl\", \"--BUCKET_RAW\": \"${BUCKET_RAW}\", \"--BUCKET_SILVER\": \"${BUCKET_SILVER}\", \"--BUCKET_GOLD\": \"${BUCKET_GOLD}\"}"

  aws --region "${AWS_REGION}" glue start-job-run --job-name "glue-job-streaming-etl"
  espera_job_run "glue-job-streaming-etl"

  # O stream adicionou a particao ano=2026: recataloga silver e gold p/ o Athena.
  inicia_crawler "crawler-silver"
  inicia_crawler "crawler-gold"
  
else
  echo "Etapa de jobs pulada."
fi

# ==========================================================================
# Verificação: consulta de exemplo no Athena
# --------------------------------------------------------------------------
# Diferente do original, aqui ESPERAMOS o crawl da bronze concluir antes de
# consultar, garantindo que 'ts_aluno' já esteja no catálogo. Esta etapa é
# auto-contida: funciona mesmo se os jobs foram pulados, desde que os dados já
# existam de um deploy anterior.
# ==========================================================================
echo "Exemplo de consulta no Athena..."

inicia_crawler "crawler-bronze"
espera_crawler_concluir "crawler-bronze"

QUERY_ID=$(aws athena start-query-execution \
  --region "${AWS_REGION}" \
  --query-string "SELECT COUNT(*) AS n FROM ts_aluno" \
  --query-execution-context "Database=${DATABASE_BRONZE}" \
  --result-configuration "OutputLocation=s3://${BUCKET_BRONZE}/athena_results/" \
  --query 'QueryExecutionId' \
  --output text)

echo "Rodando query: ${QUERY_ID}"

while true; do
  STATE=$(aws athena get-query-execution \
    --region "${AWS_REGION}" \
    --query-execution-id "${QUERY_ID}" \
    --query 'QueryExecution.Status.State' \
    --output text)
  echo "Status: ${STATE}"
  [[ "${STATE}" == "SUCCEEDED" || "${STATE}" == "FAILED" || "${STATE}" == "CANCELLED" ]] && break
  sleep 2
done

if [[ "${STATE}" == "SUCCEEDED" ]]; then
  aws athena get-query-results \
    --region "${AWS_REGION}" \
    --query-execution-id "${QUERY_ID}" \
    --output table
else
  aws athena get-query-execution \
    --region "${AWS_REGION}" \
    --query-execution-id "${QUERY_ID}" \
    --query 'QueryExecution.Status.StateChangeReason' \
    --output text
fi

echo "Deploy + execução concluídos."