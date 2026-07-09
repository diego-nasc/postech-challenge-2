
# Postech Challenge 2 — Análise da Alfabetização no Brasil

Pipeline de dados (Arquitetura Medalhão: Bronze → Silver → Gold + Streaming) para análise da
**Avaliação da Alfabetização (INEP/Saeb)** no Brasil, com ingestão *batch* a partir
das fontes oficiais, processamento na **AWS** (S3 + Glue/PySpark) e entrega de dados
prontos para **Business Intelligence** e **Machine Learning**.

O pipeline reconstrói, a partir dos microdados oficiais, as tabelas do
**Compromisso Nacional Criança Alfabetizada**, permitindo: (1) comparar resultados
contra as metas por município e UF, (2) analisar a desigualdade territorial e entre
redes de ensino, e (3) treinar modelos de predição de alfabetização a partir do ponto
de corte de **743 pontos** do Saeb.

Repositório: [https://github.com/diego-nasc/postech-challenge-2](https://github.com/diego-nasc/postech-challenge-2)

---

## Sumário

- [Arquitetura](#arquitetura)
- [Modelo de dados](#modelo-de-dados)
- [Decisões-chave (com proveniência)](#decisões-chave-com-proveniência)
- [Qualidade dos dados](#qualidade-dos-dados)
- [Limitações conhecidas](#limitações-conhecidas)
- [Estrutura do projeto](#estrutura-do-projeto)
- [Pré-requisitos](#pré-requisitos)
- [Passo a passo (setup do zero)](#passo-a-passo-setup-do-zero)
- [Estado atual e próximos passos](#estado-atual-e-próximos-passos)
- [Observações importantes / Troubleshooting](#observações-importantes--troubleshooting)

---

## Arquitetura

O projeto segue a **Arquitetura Medalhão (Medallion Architecture)**:

- **Bronze:** dados oficiais ingeridos e preservados no formato original, **sem
  transformações**. Uma tabela por arquivo de origem.
- **Silver:** dados limpos, padronizados e validados. Reconstrói as **entidades de
  negócio** espelhando o modelo da Base dos Dados (uma tabela por entidade).
- **Gold:** datasets analíticos e agregados, com indicadores materializados e features
  derivadas, prontos para BI e ML.

### Ingestão dos dados

Como os resultados da Avaliação da Alfabetização são publicados **anualmente** pelo
INEP, trata-se de um cenário de atualização periódica, e não de captura contínua de
eventos. Optou-se, portanto, pela **ingestão *batch***.

Foram avaliadas três estratégias de ingestão:

1. **Download manual pela Base dos Dados (interface web)** — inadequada para grandes
   volumes: o download da tabela de alunos (~6 milhões de registros) não pôde ser
   concluído, evidenciando que o portal é mais apropriado para consultas e arquivos
   menores.
2. **Biblioteca `basedosdados` (consulta ao Google BigQuery)** — automatizável, porém
   introduzia dependência da infraestrutura de um terceiro e sujeitava os dados à
   sincronização da Base dos Dados, com possível defasagem em relação ao INEP.
3. **Fonte primária do INEP (escolhida)** — uso direto dos arquivos oficiais publicados
   anualmente (`microdados_AEEB_YYYY`, `resultados_e_metas_municipios_YYYY`,
   `resultados_e_metas_ufs_YYYY`).

A opção pela fonte primária oferece: uso da fonte oficial sem intermediários; acesso aos
microdados completos (incluindo a base de alunos); eliminação de dependência de
terceiros; preservação integral dos dados na Bronze (auditoria e reprodutibilidade); e
maior aderência ao conceito de *Data Lake*.

> **Enriquecimento externo (IBGE) — descartado.** Avaliou-se cruzar com dados do IBGE
> (nomes canônicos, hierarquia regional) para uma tabela de território. Optou-se por
> **manter o pipeline fechado nas fontes primárias do INEP**, sem dependência externa. A
> **região** geográfica, se necessária em análises futuras, é derivável do 1º dígito do
> código IBGE do município (1=Norte, 2=Nordeste, 3=Sudeste, 4=Sul, 5=Centro-Oeste), sem
> planilha adicional.

### Ingestão Streaming

Além do pipeline batch, o projeto implementa uma simulação de ingestão streaming utilizando **Spark Structured Streaming**. Como os dados oficiais do INEP são publicados anualmente, foi adotado o cenário de **novas medições de desempenho**, gerando eventos sintéticos para o ano de 2026 a partir da camada **Silver** produzida pelo notebook 04.

Os eventos são publicados em micro-lotes (arquivos CSV) e processados com `foreachBatch`, reutilizando as mesmas regras de transformação das camadas Silver e Gold do pipeline batch. Ao final do processamento, uma nova partição (`ano = 2026`) é criada nas camadas Silver e Gold, preservando as partições históricas (2023–2025).

### Computação em nuvem

Embora a origem dos dados seja o portal do INEP, todo o **processamento** roda na **AWS**:

- Arquivos brutos no **Amazon S3**, que atua como *Data Lake*.
- Transformações **Silver** e **Gold** em **PySpark** (localmente durante o
  desenvolvimento; **AWS Glue Jobs** como alvo de produção — ver *Próximos passos*).
- Resultados gravados em **Apache Parquet**, particionados por `ano`.
- Regras de qualidade (duplicidade, nulos, validação de chaves, consistência) aplicadas
  durante as transformações.
- Como validação de ponta a ponta, o indicador de alfabetização reconstruído a partir
  dos microdados dos alunos (Gold) é comparável aos resultados oficiais do INEP.
- A Gold entrega datasets prontos para **Power BI** (via **Amazon Athena**) e para
  treinamento de modelos de **ML**.

### Fluxograma da arquitetura

```
              Fonte Oficial (INEP)
        Arquivos oficiais (CSV + XLSX)
                  │
                  ▼
        Aquisição (Python: download + extração)
                  │
────────────────────── AWS ──────────────────────
                  │
                  ▼
          Amazon S3 — BRONZE
          (5 tabelas cruas, 1 por arquivo de origem)
                  │
                  ▼
        PySpark / Glue
        • Limpeza  • Padronização  • Tipagem
        • Tratamento de nulos  • Qualidade
                  │
                  ▼
          Amazon S3 — SILVER
          (6 entidades: 3 metas + 2 resultados + alunos)
                  │
                  ▼
        PySpark / Glue
        • Long-format das metas  • Crosswalk de rede
        • Indicadores derivados  • Regras de negócio
                  │
                  ▼
          Amazon S3 — GOLD
          (indicadores_municipio, indicadores_uf, aluno_contexto)
                  │
                  ▼
      Amazon Athena → Power BI  |  Machine Learning

                     ▲
                     │
        Spark Structured Streaming
      (novas medições de desempenho)

```

---

## Modelo de dados

Todas as tabelas são gravadas em Parquet, **particionadas por `ano`**.

### Bronze — 5 tabelas cruas (1 por arquivo de origem)

| Tabela                      | Origem (INEP)                          | Grão                             |
| --------------------------- | -------------------------------------- | --------------------------------- |
| `bronze/ts_aluno`         | `TS_ALUNO.csv` (microdado)           | 1 linha por aluno/ano             |
| `bronze/ts_estado`        | `TS_ESTADO.csv` (agregado)           | UF × ano × rede                 |
| `bronze/ts_municipio`     | `TS_MUNICIPIO.csv` (agregado)        | município × ano × rede         |
| `bronze/metas_ufs`        | `resultados_e_metas_ufs.xlsx`        | UF/Brasil × ano de publicação  |
| `bronze/metas_municipios` | `resultados_e_metas_municipios.xlsx` | município × ano de publicação |

> A Bronze **espelha o arquivo físico**, não a entidade de negócio: `metas_ufs` mantém
> Brasil e UF juntos (a linha Brasil é apenas `NOME_UF = 'Brasil'`); a separação em
> entidades ocorre na Silver.

### Silver — 6 entidades de negócio (espelham a Base dos Dados)

| Tabela                                  | Chave primária (provada)     | Conteúdo                                |
| --------------------------------------- | ----------------------------- | ---------------------------------------- |
| `silver/meta_alfabetizacao_brasil`    | `(ano, rede)`               | metas nacionais 2024–2030               |
| `silver/meta_alfabetizacao_uf`        | `(ano, sigla_uf, rede)`     | metas por UF (rede`Pública`)          |
| `silver/meta_alfabetizacao_municipio` | `(ano, id_municipio, rede)` | metas por município (rede`Municipal`) |
| `silver/uf`                           | `(ano, id_uf, rede)`        | resultado agregado por UF                |
| `silver/municipio`                    | `(ano, id_municipio, rede)` | resultado agregado por município        |
| `silver/alunos`                       | `(ano, id_aluno)`           | microdado (6.090.791 linhas)             |

### Gold — 3 datasets analíticos

| Tabela                         | Chave                   | Consumidor              | Conteúdo                                              |
| ------------------------------ | ----------------------- | ----------------------- | ------------------------------------------------------ |
| `gold/indicadores_municipio` | `(ano, id_municipio)` | Gestor municipal / BI   | taxa × meta, distância, categoria (nível Municipal) |
| `gold/indicadores_uf`        | `(ano, sigla_uf)`     | Gestor estadual / BI    | taxa × meta, distância, categoria (nível Pública)  |
| `gold/aluno_contexto`        | `(ano, id_aluno)`     | Cientista de Dados / ML | aluno + contexto do município (5.321.266 linhas)      |

---

## Decisões-chave (com proveniência)

Todas as decisões abaixo foram tomadas por **verificação empírica** dos arquivos, não
por suposição — princípio adotado ao longo de todo o desenvolvimento (*inspecionar
antes de assumir*).

### Bronze

- **Uma tabela por arquivo físico, sem transformação.** A Bronze não separa entidades
  nem aplica regra — apenas ingere e preserva. O split Brasil/UF e o reshape das metas
  são operações semânticas, feitas depois.
- **Resiliência a *schema drift* (seleção por nome).** Os arquivos do INEP mudam de
  esquema entre anos: `TS_ALUNO` de 2025 traz 27 colunas (vs. 15), e as planilhas de
  meta trocam `PC_ALUNO_ALFABETIZADO` por colunas com sufixo de ano
  (`_2023`/`_2024`/`_2025`). A ingestão seleciona colunas **por nome** e preenche com
  `NULL` as ausentes, mantendo cada partição consistente.
- **Idempotência via *dynamic partition overwrite*.** Configurado na criação da
  `SparkSession` (`spark.sql.sources.partitionOverwriteMode = dynamic`) — resiliente a
  reinícios de kernel, frequentes com as credenciais temporárias do Learner Lab.

### Silver

- **Ponto de corte 743 = coluna `IN_ALFABETIZADO` da fonte.** O INEP já entrega o
  indicador calculado. Um cruzamento provou que `IN_ALFABETIZADO = 1` ⇔
  `proficiencia ≥ 743` (3.287.584 linhas) e `= 0` ⇔ `< 743` (2.033.682), **sem uma única
  contradição**. Usa-se o dado primário.
- **769.525 alunos não-medidos → `alfabetizado = NULL`.** Na fonte, `IN_ALFABETIZADO = 0`
  com proficiência nula cobre ausentes/não-medidos. Contá-los como "não alfabetizado"
  afundaria o denominador da taxa. O guard `CASE WHEN proficiencia IS NULL THEN NULL ELSE IN_ALFABETIZADO END` protege o cálculo — assim `AVG(alfabetizado)` = alfabetizados ÷
  **medidos**, a taxa correta.
- **Normalização dos percentuais das metas (`norm_pct`).** `">80"` → `80.0` (a meta
  nacional de >80% até 2030); `"- "`/vazio → `NULL` (células suprimidas); número → double.
  Verificação empírica: `">80"` aparece **só nas colunas de meta de UF** (domina o
  `META_FINAL_2030`, + Ceará nas intermediárias); **nunca** em coluna de resultado (o
  único caso aparente era uma URL em linha de rodapé); município nunca usa `">80"`;
  vírgula decimal **não ocorre** (ramo omitido).
- **Chaves primárias provadas por aritmética.** Em `alunos`, o total 6.090.791 = soma por
  ano e `dup` dentro do ano = 0 → o `ID_ALUNO` só se repete **entre** anos (mesmo aluno em
  2023 e 2025), nunca dentro do ano ⇒ PK `(ano, id_aluno)`. As demais tabelas têm
  `(ano, ente, rede)` provado único.
- **Fidelidade à Base dos Dados, sem harmonização entre tabelas.** A BD representa `rede`
  em **texto** nas metas (`Pública`, `Municipal`) e em **código** nos resultados (0/2/3/5)
  — e o pipeline espelha isso. A reconciliação texto↔código é deixada para a Gold, onde o
  join acontece.

### Gold

- **Metas em *long-format* + coalesce.** As 7 colunas `meta_alfabetizacao_2024..2030` são
  despivotadas para `(ente, rede, ano, meta)`. Como as três publicações (2023/2024/2025)
  carregam metas divergentes para o mesmo ano-alvo — São Paulo arredondado em 2025, Ceará
  **em branco** em 2025 (era `">80"`) — usa-se **latest-non-null-wins**: a meta mais
  recente com valor, com *fallback* para a anterior. Isso preserva o `">80"` do Ceará,
  cobre UFs que entraram tarde, elimina a lógica de "meta da diagonal" e torna 2023 → meta
  `NULL` natural (as metas começam em 2024).
- **Crosswalk de `rede` (as metas são mono-rede).** Confirmado nos arquivos: meta de UF =
  só `PÚBLICA` (código 5); meta de município = só `MUNICIPAL` (código 3). O join de
  meta × resultado é o resultado **filtrado no código correspondente** ⋈ meta, colapsando
  o grão do indicador para `(ano, ente)` no nível da meta.
- **`aluno_contexto` — join de lugar, sem rede.** `silver/alunos` ⋈
  `gold/indicadores_municipio` em `(ano, id_municipio)` **sem `rede`**: o indicador
  municipal é uma *feature de lugar*, anexada a todo aluno do município (o aluno tem rede
  individual 1–4; o indicador é nível Municipal). **LEFT JOIN** preserva 100% dos alunos
  (1% fica sem contexto). Treino filtra `proficiencia IS NOT NULL` (remove os 769.525
  não-medidos → 5.321.266).
- **Vazamento controlado por dicionário de papéis.** `proficiencia` e `gap_proficiencia`
  **definem** o alvo (`alfabetizado = proficiencia ≥ 743`) e não podem ser features. Em vez
  de prefixar a tabela, um dicionário `PAPEIS_ALUNO_CONTEXTO` marca cada coluna como
  *alvo / identificador / feature / vazamento* — o pipeline de ML seleciona `features`, e o
  vazamento é impossível por construção. A Gold permanece uma tabela de negócio.

---

## Qualidade dos dados

A validação segue o padrão dos scripts de referência (`etl-silver.py`): um **catálogo
declarativo de regras** (`CHECKS`) com severidade por regra (`critico`) que gera status
**PASS / FAIL / WARN**, calcula um **Score de qualidade** e faz `raise` em falha crítica.

Tipos de verificação: `min_count`, `not_null`, `unique` (chave composta), `range` e
`expr` (regra de negócio arbitrária em SQL). Exemplos de regra de negócio validada:
`atingiu_meta` coerente com `(taxa − meta) ≥ 0`; `alfabetizado` coerente com o corte 743.

As **6 tabelas Silver e as 3 tabelas Gold atingiram Score 100%** — incluindo a prova de
unicidade de todas as chaves primárias.

---

## Limitações conhecidas

- **Cobertura cresce entre os anos.** As UFs presentes vão de **24 → 25 → 27** (2023–2025);
  faltam AC/DF/RR em 2023, DF/RR em 2024 — supressão amostral progressiva, não erro. Os
  municípios vão de 5.514 → 5.516 → 5.553. **Comparações temporais devem controlar a base
  de unidades** (senão a entrada de uma UF é lida como salto).
- **Drift de rede nos agregados.** O código `0` (Total) só aparece a partir de 2024. Toda
  agregação deve **fixar uma rede**; varrer todas conta em dobro (Total + específicas) em
  2024/2025.
- **Caudas de rede em `aluno_contexto`.** 510 alunos com `rede` nula (lacuna da fonte) e 24
  na rede Privada — irrelevantes em volume, mas o pipeline de ML deve tratar (imputar,
  agrupar em "outros" ou descartar).
- **1% dos alunos sem contexto municipal** (municípios sem linha de rede Municipal no
  indicador) — `ctx_*` nulo por construção do LEFT JOIN; tratado na modelagem.
- **`ID_ALUNO` não é longitudinal.** A avaliação é do 2º ano do EF, cujo público renova a
  cada ciclo; o mesmo `ID_ALUNO` em anos diferentes **não** acompanha a mesma criança.
- **Meta 2030 = `">80"` achatada para `80.0`.** A meta real é *superar* 80; comparações que
  dependam do limite devem usar `>` e não `>=`.

---

## Estrutura do projeto

```
postech-challenge-2/
├── environment.yml            <- Ambiente Conda (fonte da verdade do setup)
├── requirements.txt           <- Dependências (referência)
├── setup.py                   <- Torna o pacote `src` instalável (pip install -e .)
├── .env.example               <- Modelo das variáveis de ambiente (copie para .env)
│
├── check_env.py               <- Verificação do ambiente (Python + libs)
├── test_spark.py              <- Teste do Spark local
├── test_conexao.py            <- Teste de conexão com o Amazon S3
│
├── data_lake/                 <- Data Lake local (conteúdo ignorado pelo Git)
│   ├── external/              <- Arquivos oficiais do INEP (fonte)
│   │   ├── microdados_avaliacao_da_alfabetizacao_2023.zip
│   │   ├── microdados_avaliacao_da_alfabetizacao_2024.zip
│   │   ├── microdados_AEEB_2025.zip        <- 2025 quebra o padrão de nome
│   │   └── extraidos/{2023,2024,2025}/     <- TS_*.csv + *.xlsx de metas
│   ├── bronze/  silver/  gold/             <- (materializadas no S3)
│
├── notebooks/
│   ├── 00_aquisicao_dados.ipynb    <- Download + extração dos arquivos do INEP
│   ├── 01_exploracao.ipynb         <- Exploração das entidades (CSV/XLSX)
│   ├── 02_teste_conexao.ipynb      <- Validação da conexão com o S3
│   ├── 03_Pipeline_Bronze.ipynb    <- Ingestão Bronze no S3 (PySpark + S3A)
│   ├── 04_Pipeline_SilverSQL.ipynb <- Transformações Silver (Spark SQL) + qualidade
│   └── 05_Pipeline_Gold.ipynb      <- Datasets Gold + qualidade
│   ├── 06_Pipeline_Streaming.ipynb <- Simulação de ingestão streaming
│
├── references/                <- Materiais de apoio (demos de aula: etl-*.py)
├── src/                       <- Código-fonte (data / features / models / viz)
├── models/                    <- Modelos treinados / previsões
├── reports/figures/           <- Gráficos e figuras
└── docs/                      <- Documentação (Sphinx)
```

> **Nota sobre os arquivos XLSX de metas:** os nomes variam por ano
> (`resultados_e_metas_municipios.xlsx`, `..._2024.xlsx`, `..._2025_v2.xlsx`, etc.). Os
> arquivos do INEP são **ISO-8859-1**, separados por `;`, e os XLSX costumam ter linhas de
> título e células mescladas. **Inspecione o arquivo bruto antes de leituras
> estruturadas.**

---

## Pré-requisitos

- **Python 3.11**
- **Conda** (Miniconda ou Anaconda)
- **Git**
- **AWS Academy Learner Lab** (cada integrante usa sua própria conta e credenciais)
- **Recomendado: Linux ou WSL.** O ambiente foi desenvolvido em WSL para evitar atritos
  com Java, Spark e dependências do S3 no Windows nativo.

## Passo a passo (setup do zero)

### 1. Clonar o repositório

```bash
git clone https://github.com/diego-nasc/postech-challenge-2.git
cd postech-challenge-2
```

### 2. Criar o ambiente Conda

O ambiente (Python 3.11, PySpark 3.5, OpenJDK 17 e demais dependências) é definido no
`environment.yml`:

```bash
conda env create -f environment.yml
```

### 3. Ativar o ambiente

```bash
conda activate postech2
```

### 4. Verificar o ambiente (Python + Spark)

```bash
python check_env.py      # versões e dependências
python test_spark.py     # SparkSession sobe localmente
```

> Ao rodar os **notebooks**, selecione o kernel do ambiente **`postech2`**.

### 5. Configurar as credenciais AWS (.env)

As credenciais do **AWS Academy Learner Lab são temporárias** e expiram ao encerrar a
sessão do lab, por isso incluem um **session token** e precisam ser atualizadas a cada
nova sessão.

```bash
cp .env.example .env
```

Conteúdo esperado do `.env` (obtido em **Learner Lab → AWS Details → AWS CLI**):

```dotenv
# Credenciais temporárias do AWS Academy Learner Lab
AWS_ACCESS_KEY_ID=<sua_access_key>
AWS_SECRET_ACCESS_KEY=<sua_secret_key>
AWS_SESSION_TOKEN=<seu_session_token>
AWS_DEFAULT_REGION=us-east-1

# Bucket S3 que atua como Data Lake
S3_BUCKET=<seu_bucket_unico>
```

> O `.env` **não deve ser versionado** (já consta no `.gitignore`). Cada integrante usa
> suas próprias credenciais e seu próprio bucket.

### 6. Criar o bucket S3 (Data Lake)

O nome do bucket é **globalmente único**. Sugestão: `alfabetizacao-data-lake-<seu-nome>`.
No Learner Lab: **S3 → Create bucket → região us-east-1 → nome único → Create**. Preencha
o nome em `S3_BUCKET` no `.env`.

### 7. Baixar os dados oficiais do INEP

Os microdados são grandes e **não são versionados**. Execute o notebook de aquisição, que
baixa de `download.inep.gov.br` e extrai em `data_lake/external/`:

```
notebooks/00_aquisicao_dados.ipynb
```

> Caso o download falhe, baixe manualmente no portal do INEP. Atenção ao nome do pacote de
> 2025 (`microdados_AEEB_2025.zip`), que **não segue** o padrão dos anos anteriores.

### 8. Testar a conexão com o S3

```bash
python test_conexao.py     # ou notebooks/02_teste_conexao.ipynb
```

### 9. Executar o pipeline (Bronze → Silver → Gold)

Com ambiente, credenciais e dados no lugar, execute os notebooks **na ordem**:

```
notebooks/03_Pipeline_Bronze.ipynb      # ingestão crua → s3://<bucket>/bronze/
notebooks/04_Pipeline_Silver.ipynb      # limpeza/padronização + qualidade → silver/
notebooks/05_Pipeline_Gold.ipynb        # indicadores + dataset de ML + qualidade → gold/
notebooks/06_Pipeline_Streaming.ipynb   # simulação de novas medições (2026)
```

Ao final, o Data Lake completo estará em `s3://<S3_BUCKET>/{bronze,silver,gold}/`, e cada
notebook imprime o relatório de qualidade (Score por tabela).

---

## Estado atual e próximos passos

**Funcionando hoje (pipeline Batch + Streaming):**

- Aquisição dos arquivos oficiais do INEP (2023–2025).
- **Bronze:** 5 tabelas cruas (PySpark → Parquet no S3), resilientes a *schema drift*.
- **Silver:** 6 entidades reconstruídas, padronizadas e **validadas (Score 100%)**.
- **Gold:** 3 datasets analíticos (indicadores município/UF + `aluno_contexto` para ML),
  **validados (Score 100%)**.
- **Streaming:** simulação de novas medições de desempenho (2026) com Spark Structured Streaming, reutilizando as transformações Silver e Gold e criando a partição 2026.

**Próximos passos:**

- **Migração dos notebooks para AWS Glue Jobs** — tirar a execução do ambiente local
  (`local[*]`), que hoje opera no limite de memória com os ~6M de registros de alunos.
- **Consumo via Amazon Athena** → dashboards no **Power BI**.
- **Treinamento de modelos de ML** sobre `gold/aluno_contexto`, usando o dicionário de
  papéis para seleção de features (evitando o vazamento de `proficiencia`/`gap`).

> Os scripts em `references/` (`etl-bronze.py` e afins) são **demonstrações das aulas**
> (usam fontes de exemplo) e serviram de base — não são o pipeline final do desafio.

---

## Observações importantes / Troubleshooting

- **Credenciais temporárias (Learner Lab):** ao configurar o S3 no Spark, use
  `org.apache.hadoop.fs.s3a.TemporaryAWSCredentialsProvider`. O
  `SimpleAWSCredentialsProvider` **ignora o session token** e causa erro `403`. As
  credenciais expiram ao fim da sessão — atualize o `.env` a cada sessão.
- **`dynamic partition overwrite` no *builder*:** configure
  `spark.sql.sources.partitionOverwriteMode = dynamic` **na criação da SparkSession**, não
  numa célula separada. Após um reinício de kernel (comum no Learner Lab), esquecer a
  célula de config faria o `overwrite` de um ano apagar os demais.
- **JARs do S3A:** provisionados via `spark.jars.packages`, coordenadas Maven
  `org.apache.hadoop:hadoop-aws:3.3.4` e `com.amazonaws:aws-java-sdk-bundle:1.12.262`
  (pareamento testado para o Hadoop 3.3.4). Para os XLSX:
  `com.crealytics:spark-excel_2.12:3.5.1_0.20.4`.
- **Python do driver/worker:** defina `os.environ["PYSPARK_PYTHON"]` e
  `os.environ["PYSPARK_DRIVER_PYTHON"]` como `sys.executable` **antes** de criar a
  `SparkSession`, para evitar divergência entre interpretadores.
- **Leitura dos arquivos do INEP:** encoding **ISO-8859-1**, separador **`;`**. Nos XLSX,
  espere linhas de título e células mescladas — inspecione o bruto antes de ler.
- **Avisos `spill()` / `MemoryManager` em alunos:** esperados ao processar ~6M de linhas no
  `local[*]` (heap de ~1 GB). São inofensivos — o resultado sai íntegro — e desaparecem no
  Glue com executor adequado.

---

<p><small>Projeto baseado no template
<a target="_blank" href="https://drivendata.github.io/cookiecutter-data-science/">cookiecutter data science</a>.
#cookiecutterdatascience</small></p>
