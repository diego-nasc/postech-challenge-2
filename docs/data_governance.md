
# Governança de Dados

> Pipeline **ELT híbrido (batch + streaming)** de indicadores de alfabetização (INEP / SAEB) sobre **arquitetura Medallion** (Raw → Bronze → Silver → Gold) na AWS.
>
> **Abordagem:** governança *leve*, implementada **como código**. Não usamos Apache Atlas, AWS Lake Formation nem catálogos de terceiros. As garantias de governança (rastreabilidade, qualidade, catálogo, acesso e evolução) nascem dos próprios artefatos versionados — os *jobs* PySpark/Glue e o script de provisionamento — e são reforçadas por serviços nativos do Glue.

---

## 1. Objetivo da Governança

O objetivo é assegurar que os dados sejam **rastreáveis, confiáveis, reproduzíveis e auditáveis** ao longo de todo o ciclo de vida — da captura na fonte primária até o consumo analítico — sem introduzir a complexidade operacional de um *framework *de governança dedicado.

### 1.1 Por que "governança leve"

O projeto roda em ambiente de laboratório (AWS Academy Learner Lab), com uma única equipe e escopo bem delimitado. Nesse contexto, o custo/benefício de ferramentas como Atlas ou Lake Formation não se justifica: elas resolvem problemas de escala organizacional (linhagem automática entre centenas de *pipelines*, segurança em nível de linha/coluna, *stewardship* distribuído) que aqui não existem.

A decisão de projeto foi **fazer do código a superfície de governança**:

- **Versionado** — todo o pipeline está no Git. O histórico do repositório fornece a trilha de rastreabilidade das mudanças de schema, regras de negócio e configuração de infraestrutura versionadas como código. O histórico do Git funciona como registro de evolução técnica do pipeline, permitindo identificar o que mudou no código e associar alterações ao fluxo de revisão do repositório.
- **Revisável** — cada garantia é um trecho de código legível (um `assert`, uma
  função de qualidade, uma configuração de `overwrite`), sujeito a *code review*.
- **Executável** — os contratos não são texto em um documento; eles **rodam** e
  **abortam o pipeline** quando violados (ver `docs/data_quality.md`).

### 1.2 Pilares

| Pilar                               | Como é implementado                                                                       | Onde vive                                         |
| ----------------------------------- | ------------------------------------------------------------------------------------------ | ------------------------------------------------- |
| Fonte única da verdade (infra)     | Provisionamento idempotente; jobs/crawlers em modo*DELETE + CREATE*                      | `create_aws_resources.sh`, `config_vars.sh` |
| Contratos de qualidade executáveis | Catálogo declarativo`CHECKS` + severidade + `raise` em falha crítica                 | `glue_elt_silver.py`, `glue_elt_gold.py`      |
| Rastreabilidade em dois planos      | Metadados de linhagem**embutidos nos dados** + **logging operacional padronizado** | Todos os jobs                                     |
| Catálogo automatizado              | Glue Crawlers → Glue Data Catalog → Athena                                               | `create_aws_resources.sh`                     |
| Idempotência / reprodutibilidade   | `partitionOverwriteMode=dynamic`, *guards* de estado, seed fixa no stream              | Todos os jobs                                     |

### 1.3 Fronteira de escopo — o que esta abordagem **não** cobre (por decisão)

Documentar os limites é parte da governança. Ficam **conscientemente fora** do
escopo desta abordagem leve, e seriam o passo natural em um cenário produtivo:

- **Segurança em nível de linha/coluna** (o papel do Lake Formation). Aqui o isolamento é físico, por *bucket* de camada.
- **Grafo de linhagem automático entre jobs** (o papel do Atlas). Aqui a linhagem é rastreada por metadados por linha + logs, e a topologia do pipeline é lida no próprio orquestrador (`create_aws_resources.sh`).
- **Data stewardship / glossário de negócio formal.** As definições de negócio vivem em comentários de código e dicionários (ex.: `PAPEIS_ALUNO_CONTEXTO`).

---

## 2. Responsabilidade das Camadas

Cada camada tem **uma responsabilidade única** e um conjunto próprio de garantias de governança. A regra de ouro é que **nenhuma camada refaz o trabalho da anterior**: a validade a montante é pré-condição para a camada a jusante rodar.

| Camada           | Responsabilidade                                          | Formato                                       | Garantias de governança                                       |
| ---------------- | --------------------------------------------------------- | --------------------------------------------- | -------------------------------------------------------------- |
| **Raw**    | Cópia fiel e imutável da fonte primária                | ZIP / CSV / XLSX (original)                   | Auditabilidade da origem; captura resiliente                   |
| **Bronze** | Tipagem e padronização estrutural                       | Parquet, particionado por`NU_ANO_AVALIACAO` | Contrato de tipo; detecção de *drift*; linhagem de origem |
| **Silver** | Conformação semântica + regras de negócio + qualidade | Parquet, particionado por`ano`              | Contratos de qualidade executáveis; carimbo de processamento  |
| **Gold**   | Modelagem analítica para consumo (BI + ML)               | Parquet, particionado por`ano`              | Contratos de negócio; documentação de papéis e vazamento   |

### 2.1 Raw — fidelidade à fonte

Responsável por capturar e preservar os dados disponibilizados pelo INEP antes de qualquer transformação analítica ou aplicação de regra de negócio (`glue_elt_raw.py`). É a âncora de rastreabilidade da origem: enquanto os dados da Raw existirem, as camadas a jusante podem ser reconstruídas.

Garantias de governança:

- **Preservação da fonte primária** — A Raw preserva os dados capturados no formato disponibilizado ou extraído da fonte (os microdados (ZIP → `TS_ALUNO`, `TS_ESTADO`, `TS_MUNICIPIO`) e as planilhas de metas (XLSX)), antes de qualquer transformação analítica.
- **Captura resiliente** — a transferência Web → S3 é feita em *streaming *(`upload_fileobj` sobre a resposta HTTP, sem carregar o arquivo inteiro em memória) e protegida por *retry* com *backoff* (`MAX_TENTATIVAS`). Falha persistente **interrompe** o job (nada de camada seguinte sobre captura parcial).
- **Extração seletiva** — do ZIP são extraídos apenas os CSVs de interesse; arquivos temporários são removidos (`finally: unlink`).

### 2.2 Bronze — contrato estrutural

Responsável por converter os formatos crus em  **Parquet tipado e particionado por `NU_ANO_AVALIACAO`** , estabelecendo o primeiro contrato de schema (`glue_elt_bronze.py`).

Garantias de governança:

- **Contrato de tipo explícito** — cada entidade tem um schema declarado (`SCHEMA_TS_ALUNO`, `SCHEMA_METAS_UF_2025`, …) com a convenção *código = string, medida = double, flag = int*.
- **Robustez a mudança de layout** — a leitura é feita **ano a ano** e as colunas são selecionadas **pelo nome**, não pela posição. Colunas novas fora do contrato (ex.: as que o `TS_ALUNO.csv` de 2025 passou a conter) são ignoradas; uma coluna esperada que suma ou mude de nome faz o job **falhar alto e claro** (`assert`), evitando erro silencioso.
- **Linhagem de origem por linha** — cada registro recebe `_source_file` (arquivo de origem) e `_ingestion_timestamp` (carimbo único e determinístico da execução, via `F.lit`).
- **Preservação de valores ambíguos** — metas com valores como `">80"` são mantidas como *string* na Bronze; a normalização é adiada para a Silver, preservando o dado original durante a ingestão.

### 2.3 Silver — conformação e qualidade

Responsável por **limpar, conformar e aplicar regras de negócio**, entregando tabelas semanticamente corretas (`glue_elt_silver.py`). É aqui que os **contratos de qualidade executáveis** entram em cena.

Garantias de governança:

- **Resolução de ambiguidade semântica** — a coluna `rede` carrega três significados incompatíveis entre as fontes. A Silver os separa por decisão documentada: a tabela `uf` filtra `ID_TIPO_REDE = 5` (Pública), `municipio` filtra `= 3` (Municipal) e `alunos` filtra `TP_DEPENDENCIA IN (2, 3)`. Isso evita a **soma sobreposta** de agregados (que inflaria contagens em 2–3×).
- **Normalização controlada** — `norm_pct` converte percentuais de metas para `double` (`">80" → 80.0`, `"-"`/vazio → `NULL`), com o ramo de vírgula decimal omitido de propósito (limitação documentada no código).
- **Chaves canônicas** — `id_municipio` é padronizado para 7 dígitos (`LPAD(..., 7, '0')`), garantindo integridade de *join*.
- **Rótulo protegido** — `alfabetizado` vira `NULL` quando a proficiência é nula, para não contaminar denominador nem alvo de ML.
- **Contratos de qualidade** — catálogo declarativo `CHECKS` por tabela, com severidade por regra e `raise` em falha crítica (detalhado em `docs/data_quality.md`).
- **Carimbo de processamento** — `_silver_processed_at` (único por execução).

### 2.4 Gold — modelagem para consumo

Responsável por produzir os **indicadores analíticos** e as tabelas prontas para BI e ML (`glue_elt_gold.py`).

Garantias de governança:

- **Reconstrução determinística das metas** — o *unpivot* das metas (`stack`) seguido de `ROW_NUMBER` ordenado por *não-nulo, publicação mais nova* implementa a estratégia diagonal (*latest-non-null coalesce*), consolidando metas dispersas entre anos de publicação.
- **Regras de negócio versionadas** — `distancia_meta`, `atingiu_meta`, `categoria_desempenho` e `faixa_participacao` são definidas em SQL explícito, com corte de proficiência `743` documentado.
- **Governança do consumo de ML** — o dicionário `PAPEIS_ALUNO_CONTEXTO` classifica cada coluna de `aluno_contexto` como `alvo`, `identificador`, `feature`, `constante` ou **`vazamento`** (`proficiencia`/`gap_proficiencia` derivam do alvo e **nunca** devem ser *features*). Esse contrato de papéis previne *data leakage* já na fronteira dos dados.
- **Carimbo de processamento** — `_gold_processed_at` (único por execução).

---

## 3. Catálogo de Dados

Os metadados **não** são mantidos em um catálogo próprio. Eles são expostos automaticamente pela combinação **Glue Crawlers + Glue Data Catalog + Parquet autodescritivo**, e consultados via **Athena**.

### 3.1 Como funciona

- **Um crawler por camada** (`crawler-bronze`, `crawler-silver`, `crawler-gold`), cada um apontando para o *bucket* da sua camada e populando o *database* Glue correspondente (`DATABASE_BRONZE`, `DATABASE_SILVER`, `DATABASE_GOLD`).
- **Schema inferido do Parquet** — como o Parquet embute o schema no rodapé do arquivo, o crawler descobre colunas e tipos sem heurística frágil. Na prática, o Glue Data Catalog combinado ao schema autodescritivo do Parquet funciona como catálogo técnico dos datasets do lake. As definições semânticas e regras de negócio permanecem documentadas no código e nos documentos de governança.
- **Descoberta de partições** — a partição `ano` é detectada automaticamente, habilitando *partition pruning* nas consultas Athena.
- **Agendamento + gatilho** — os crawlers têm agendamento diário (`cron(0 9 * * ? *)`) e também são disparados pelo orquestrador logo após cada job popular a sua camada, mantendo o catálogo sempre coerente com o dado físico.

### 3.2 Higiene do catálogo

O catálogo só deve refletir **dados**, nunca artefatos operacionais. Por isso os crawlers excluem explicitamente:

```
Exclusions: ["_checkpoints/**", "athena_results/**"]
```

- `_checkpoints/**` — estado interno do Structured Streaming (não é tabela).
- `athena_results/**` — arquivos de saída das próprias consultas Athena.

Sem essas exclusões, o Glue catalogaria *checkpoints* e resultados de query como se fossem tabelas, poluindo o *namespace* e quebrando consultas. A exclusão é uma decisão de **governança de catálogo**.

### 3.3 Catálogo e o dado de streaming

Depois que o job de streaming grava a partição `ano=2026` em `silver/municipio` e `gold/indicadores_municipio`, o orquestrador **re-executa** os crawlers de Silver e Gold. Assim o novo período aparece no Athena junto dos anos do batch, sem intervenção
manual e sem *fork* de schema (o stream grava linhas 1:1 com o batch).

---

## 4. Rastreabilidade Operacional

A rastreabilidade opera em **dois planos complementares**: um embutido **nos dados** (quem sou eu, de onde vim) e outro nos **logs** (o que aconteceu, quando).

### 4.1 Plano 1 — Linhagem embutida nos dados

Toda linha carrega sua própria proveniência, consultável **via SQL no Athena**, sem depender de sistema externo:

| Coluna                   | Camada onde é gravada      | Significado                                  |
| ------------------------ | --------------------------- | -------------------------------------------- |
| `_source_file`         | Bronze                      | Arquivo de origem (via`input_file_name()`) |
| `_ingestion_timestamp` | Bronze                      | Momento da ingestão Bronze                  |
| `_silver_processed_at` | Silver**e** Streaming | Momento do processamento Silver              |
| `_gold_processed_at`   | Gold**e** Streaming   | Momento do processamento Gold                |

Ponto de projeto importante: cada carimbo é um **valor único por execução** (`INGESTION_TS`, `SILVER_TS`, `GOLD_TS`, `STREAM_TS`, aplicados com `F.lit`). Isso torna o lote inteiro **determinístico** — todas as linhas de uma execução compartilham o mesmo instante, permitindo isolar exatamente "o que entrou na carga de tal horário".

O job de streaming reaproveita esses **mesmos** carimbos, de modo que uma linha de `ano=2026` produzida pelo stream é indistinguível, em contrato, de uma linha do batch.

### 4.2 Plano 2 — logging operacional padronizado e vocabulário de eventos

Todos os jobs compartilham a **mesma configuração de logging** (formato uniforme, *handler* único para `stdout`, que o Glue encaminha para o **CloudWatch Logs**):

```
%(asctime)s | %(levelname)-8s | %(message)s
```

Isso dá um **trilho de auditoria operacional centralizado e consultável**: como tudo vai para `stdout`, o Glue centraliza os logs no CloudWatch, onde ficam retidos e podem ser filtrados por *job run*.

Sobre a estrutura das mensagens, os logs seguem um  **padrão semântico recorrente de eventos operacionais** . Os eventos de qualidade possuem marcação explícita no código, por meio do prefixo `[DQ:CAMADA]` e dos estados `PASS`, `FAIL`, `WARN` e `Score=`. Os demais eventos são registrados de forma descritiva, mas mantêm semântica recorrente entre os jobs.

A tabela abaixo organiza essas mensagens em  **categorias semânticas de operação** , associando cada categoria à evidência atualmente emitida pelo código.

| Categoria semântica     | Evento                                            | Evidência no código (hoje)                                                                           |
| ------------------------ | ------------------------------------------------- | ------------------------------------------------------------------------------------------------------ |
| `JOB_STARTED`          | Início de um job                                 | `log.info("Iniciando job Bronze...")`                                                                |
| `INGESTION_RETRY`      | Tentativa de transferência falhou, novo*retry* | `log.warning("Tentativa X/Y falhou (...). Retry em Ns...")` (`glue_elt_raw.py`)                    |
| `DATASET_WRITTEN`      | Um dataset foi gravado no lake                    | `log.info("silver/uf gravada. Total: %s", ...)`, `log.info("gold/indicadores_municipio: %s", ...)` |
| `DQ_CHECK`             | Resultado de um*check* individual               | `[DQ:SILVER] PASS \| unique \| [ano, id_municipio, rede] \| ...`                                        |
| `DATA_QUALITY_PASSED`  | Tabela aprovada (score consolidado)               | `[DQ:SILVER] municipio \| Score=100.0% \| PASS=9 FAIL=0`                                               |
| `DATA_QUALITY_FAILED`  | *Check* crítico falhou → pipeline abortado    | `raise Exception("[DQ:SILVER] ...: N check(s) critico(s) falharam. Pipeline interrompido.")`         |
| `MICROBATCH_PROCESSED` | Micro-lote do stream acumulado e gravado          | `[BATCH %s] acumulado ano=2026: %s municipios (silver+gold gravados).`                               |
| `JOB_COMPLETED`        | Fim de um job com sucesso                         | `log.info("Job Bronze concluído com sucesso!")`                                                     |

### 4.3 O que a rastreabilidade responde

Combinando os dois planos, o operador consegue responder, sem ferramenta externa:

- *De onde veio esta linha e quando cada camada a processou?* → colunas de linhagem (`_source_file`, `_*_processed_at`) via Athena.
- *Este job gravou o que se esperava, ou abortou?* → mensagens de escrita de datasets e resultados consolidados dos checks de qualidade no CloudWatch.
- *Por que o pipeline parou?* → a exceção emitida em falhas críticas de qualidade identifica a tabela e a quantidade de *checks* críticos violados.

---

## 5. Controle de Acesso e Política de Evolução

### 5.1 Controle de Acesso

- **Papel IAM único (contexto de laboratório).** Todos os jobs e crawlers usam a role pré-provisionada do ambiente (`labRole`, referida como `${ROLE_NAME}`). É uma restrição do AWS Academy Learner Lab, **documentada como tal**. Em produção, o passo seguinte seria segregar papéis de menor privilégio por camada.
- **Isolamento físico por camada.** Cada camada vive em um *bucket* S3 próprio (`BUCKET_RAW`, `BUCKET_BRONZE`, `BUCKET_SILVER`, `BUCKET_GOLD`), e os scripts ficam em um *bucket* separado (`BUCKET_SCRIPTS`). Mesmo com um papel compartilhado, a separação física permite raciocinar sobre — e, em produção, escopar — o acesso por camada, além de separar *código* de *dado*.
- **Consumo governado pelo catálogo.** O acesso analítico se dá via Athena sobre o Glue Data Catalog; os resultados de query são isolados em `athena_results/` (fora do alcance dos crawlers).
- **Ausência de segredos no código.** Os argumentos dos jobs são não-sensíveis (nomes de *bucket*, número de tentativas); a autenticação em S3/Glue é feita pelo papel, sem credenciais embutidas.
- **Trade-off de rede documentado.** A captura no Raw usa `verify=False` (mais supressão do *warning* correspondente) para contornar a cadeia de certificado do portal do INEP. É um risco **conhecido e escopado apenas à chamada de download**,não a operações internas de dados — registrado aqui para ser revisitado.
- **Limite consciente.** Não há segurança em nível de linha/coluna (papel do Lake Formation); está fora do escopo desta governança leve, como registrado em §1.3.

### 5.2 Política de Evolução

A evolução do pipeline — de **schema**, de **regra de negócio** e de **infra** — é governada por política, não por acidente.

**Evolução de schema (dados)**

- **Contratos por ano de publicação.** A Bronze declara schemas versionados (`SCHEMA_METAS_MUN_2023`, `_2024`, `_2025`, …), reconhecendo que as planilhas do INEP mudam de estrutura entre anos.
- **Política aditiva por padrão, restritiva quando quebra.** Como a seleção é por nome: mudanças **aditivas** (colunas novas) são absorvidas silenciosamente; mudanças **quebradoras** (coluna esperada some/renomeia) **falham o job** e exigem uma atualização explícita de contrato — ou seja, uma mudança de código, rastreável em *pull request*.
- **Leitura tolerante a histórico.** `mergeSchema=true` é usado ao reler entidades cujo schema varia entre anos.
- **Semântica de subconjunto para anos.** `checar_anos` exige que os anos esperados (2023–2025) estejam presentes, mas **não reprova anos extras** — é o que permite a camada de streaming adicionar `ano=2026` sem quebrar a validação do batch.

**Evolução de configuração (infra)**

- **Fonte única da verdade.** `create_aws_resources.sh` (+ `config_vars.sh`) é a configuração autoritativa da infra. Jobs e crawlers usam *DELETE + CREATE*: qualquer mudança (`GlueVersion`, tipo/número de *workers*, argumentos, `ScriptLocation`) é aplicada no próximo *deploy*. Recursos persistentes (buckets, *databases*) usam *SKIP-IF-EXISTS*, evitando *drift* destrutivo.
- **Idempotência como garantia de evolução.** Re-executar o pipeline é seguro: o `partitionOverwriteMode=dynamic` sobrescreve apenas as partições afetadas (preservando o histórico), e os *guards* de estado impedem *races* (nunca deletar crawler em `RUNNING`, nunca recriar job com *run* ativo).
- **Compatibilidade batch ↔ streaming.** O stream grava linhas com schema **idêntico** ao do batch nas **mesmas** tabelas, impedindo bifurcação de contrato entre os dois regimes de ingestão.

**Rastreabilidade e mudanças**

Toda alteração de schema, regra ou infraestrutura corresponde a uma alteração de  **código versionado** . O histórico do Git fornece a trilha de evolução técnica do pipeline, permitindo identificar mudanças nas definições versionadas e associá-las ao fluxo de revisão do repositório.

---

### Referências de código

| Documento cita                                              | Arquivo                       |
| ----------------------------------------------------------- | ----------------------------- |
| Captura resiliente,*retry*, `verify=False`              | `glue_elt_raw.py`           |
| Contratos de tipo, linhagem, robustez a *drift*          | `glue_elt_bronze.py`        |
| Semântica de`rede`, `norm_pct`, contratos de qualidade | `glue_elt_silver.py`        |
| Metas diagonais, papéis de ML, vazamento                   | `glue_elt_gold.py`          |
| Streaming em`ano=2026`, escopo de limpeza, checkpoint     | `glue_elt_streaming.py`     |
| Idempotência,*guards*, crawlers, exclusões de catálogo | `create_aws_resources.sh` |

*Os contratos de qualidade executáveis são detalhados em `docs/data_quality.md`.*
