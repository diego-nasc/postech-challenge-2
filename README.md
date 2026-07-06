
# Postech Challenge 2 — Análise da Alfabetização no Brasil

Pipeline de dados (Arquitetura Medalhão: Bronze → Silver → Gold) para análise da
**Avaliação da Alfabetização (INEP/Saeb)** no Brasil, com ingestão *batch* a partir
das fontes oficiais, processamento na **AWS** (S3 + Glue/PySpark) e entrega
de dados prontos para **Machine Learning**.

Repositório: [https://github.com/diego-nasc/postech-challenge-2](https://github.com/diego-nasc/postech-challenge-2)

---

## Sumário

- [Arquitetura](#arquitetura)
- [Estrutura do projeto](#estrutura-do-projeto)
- [Pré-requisitos](#pré-requisitos)
- [Passo a passo (setup do zero)](#passo-a-passo-setup-do-zero)
  - [1. Clonar o repositório](#1-clonar-o-repositório)
  - [2. Criar o ambiente Conda](#2-criar-o-ambiente-conda)
  - [3. Ativar o ambiente](#3-ativar-o-ambiente)
  - [4. Verificar o ambiente (Python + Spark)](#4-verificar-o-ambiente-python--spark)
  - [5. Configurar as credenciais AWS (.env)](#5-configurar-as-credenciais-aws-env)
  - [6. Criar o bucket S3 (Data Lake)](#6-criar-o-bucket-s3-data-lake)
  - [7. Baixar os dados oficiais do INEP](#7-baixar-os-dados-oficiais-do-inep)
  - [8. Testar a conexão com o S3](#8-testar-a-conexão-com-o-s3)
  - [9. Executar o pipeline (camada Bronze)](#9-executar-o-pipeline-camada-bronze)
- [Estado atual e próximos passos](#estado-atual-e-próximos-passos)
- [Observações importantes / Troubleshooting](#observações-importantes--troubleshooting)

---

## Arquitetura

O projeto segue a **Arquitetura Medalhão (Medallion Architecture)**:

- **Bronze:** dados oficiais ingeridos e preservados no formato original.
- **Silver:** dados limpos, padronizados, integrados e validados.
- **Gold:** dados consolidados e agregados, prontos para análise e consumo.

### Ingestão dos dados

Como os resultados da Avaliação da Alfabetização são publicados **anualmente** pelo
INEP, trata-se de um cenário de atualização periódica, diferentemente de sistemas
transacionais que exigem captura contínua de eventos. Dessa forma, optou-se pela
**ingestão *batch***, na qual os dados são obtidos da fonte de origem, persistidos na
camada Bronze e reutilizados nas etapas subsequentes da pipeline.

Durante o desenvolvimento foram avaliadas diferentes estratégias de ingestão:

1. **Download manual pela Base dos Dados (interface web)** — inadequada para grandes
   volumes: o download da tabela de alunos (~3,9 milhões de registros) não pôde ser
   concluído, evidenciando que o portal é mais apropriado para consultas e arquivos
   menores.
2. **Biblioteca `basedosdados` (consulta ao Google BigQuery)** — tecnicamente viável e
   automatizável, porém introduzia dependência da infraestrutura de um serviço de
   terceiros e sujeitava a disponibilidade dos dados à sincronização da Base dos Dados,
   com possível defasagem em relação aos arquivos oficiais do INEP.
3. **Fonte primária do INEP (escolhida)** — uso direto dos arquivos oficiais publicados
   anualmente (`microdados_AEEB_YYYY`, `resultados_e_metas_municipios_YYYY`,
   `resultados_e_metas_ufs_YYYY`).

A opção pela fonte primária oferece:

- utilização da fonte oficial, sem intermediários;
- acesso aos microdados completos, incluindo a base de alunos;
- eliminação da dependência de serviços de terceiros;
- preservação integral dos dados originais na Bronze, favorecendo auditoria e
  reprodutibilidade;
- maior aderência ao conceito de *Data Lake*, armazenando os arquivos exatamente como
  foram publicados pelo INEP.

A camada Bronze armazena os arquivos **sem qualquer transformação**. Limpeza,
padronização, validação, integração entre entidades e geração dos datasets analíticos
ocorrem depois, nas camadas Silver e Gold.

### Computação em nuvem

Embora a origem dos dados seja o portal oficial do INEP, todo o **processamento** é
realizado na **Amazon Web Services (AWS)**:

- Os arquivos brutos são armazenados no **Amazon S3**, que atua como *Data Lake*.
- As transformações **Silver** e **Gold** são executadas por **AWS Glue Jobs** com
  **PySpark**, permitindo processamento distribuído e escalável.
- Regras de qualidade (duplicidade, valores ausentes, validação de chaves e
  consistência entre tabelas) são aplicadas durante as transformações nos próprios
  Glue Jobs.
- Os resultados são gravados no S3 em **Apache Parquet**, reduzindo armazenamento e
  custo e melhorando o desempenho analítico.
- Como validação de ponta a ponta, o indicador de alfabetização reconstruído a partir
  dos microdados dos alunos (Gold) é comparado aos resultados oficiais do INEP por
  município.
- A camada Gold entrega datasets prontos para **Business Intelligence** e para
  treinamento de modelos de **Machine Learning**.

### Fluxograma da arquitetura

```
              Fonte Oficial (INEP)

        Arquivos oficiais do INEP
                  │
                  ▼
        Python / Pandas (Ingestão)
                  │
──────────────────── AWS ────────────────────
                  │
                  ▼
          Amazon S3 (Bronze)
                  │
                  ▼
        AWS Glue Job (PySpark)
        • Limpeza
        • Padronização
        • Qualidade dos dados
        • Integração
                  │
                  ▼
          Amazon S3 (Silver)
                  │
                  ▼
        AWS Glue Job (PySpark)
        • Agregações
        • Regras de negócio
        • Validação cruzada
                  │
                  ▼
          Amazon S3 (Gold)
                  │
                  ▼
      Machine Learning / Power BI
```

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
├── test_environment.py        <- Verificação do ambiente
├── test_spark.py              <- Teste do Spark local
├── test_conexao.py            <- Teste de conexão com o Amazon S3
│
├── data_lake/                 <- Data Lake local (conteúdo ignorado pelo Git)
│   ├── external/              <- Arquivos oficiais do INEP (fonte)
│   │   ├── microdados_avaliacao_da_alfabetizacao_2023.zip
│   │   ├── microdados_avaliacao_da_alfabetizacao_2024.zip
│   │   ├── microdados_AEEB_2025.zip        <- 2025 quebra o padrão de nome
│   │   └── extraidos/
│   │       ├── 2023/  (TS_ALUNO.csv, TS_ESTADO.csv, TS_MUNICIPIO.csv, *.xlsx)
│   │       ├── 2024/  (TS_ALUNO.csv, TS_ESTADO.csv, TS_MUNICIPIO.csv, *.xlsx)
│   │       └── 2025/  (TS_ALUNO.csv, TS_ESTADO.csv, TS_MUNICIPIO.csv, *.xlsx)
│   ├── bronze/                <- (materializada no S3)
│   ├── silver/                <- (materializada no S3)
│   └── gold/                  <- (materializada no S3)
│
├── notebooks/
│   ├── 00_aquisicao_dados.ipynb   <- Download + extração dos arquivos do INEP
│   ├── 01_exploracao.ipynb        <- Exploração das entidades (CSV/XLSX)
│   ├── 02_teste_conexao.ipynb     <- Validação da conexão com o S3
│   ├── 02_Pipeline.ipynb          <- Ingestão Bronze no S3 (PySpark + S3A)
│   └── ETL_Pipeline.ipynb
│
├── references/                <- Materiais de apoio (demos de aula)
├── src/                       <- Código-fonte (data / features / models / viz)
├── models/                    <- Modelos treinados / previsões
├── reports/figures/           <- Gráficos e figuras
└── docs/                      <- Documentação (Sphinx)
```

> **Nota sobre os arquivos XLSX de metas:** os nomes variam por ano
> (`resultados_e_metas_municipios.xlsx`, `..._2024.xlsx`, `..._2025_v2.xlsx`, etc.).
> Além disso, os arquivos do INEP são **ISO-8859-1**, separados por `;`, e os XLSX
> costumam ter linhas de título e células mescladas. Para os anos posteriores a 2025, **inspecione o arquivo bruto
> antes de leituras estruturadas**.

---

## Pré-requisitos

- **Python 3.11**
- **Conda** (Miniconda ou Anaconda)
- **Git**
- **AWS Academy Learner Lab** (cada integrante usa sua própria conta e credenciais)
- **Recomendado: Linux ou WSL.** O ambiente foi desenvolvido em WSL para
  evitar atritos com Java, Spark e dependências do S3 no Windows nativo.

## Passo a passo (setup do zero)

1. Clonar o repositório

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

Antes de seguir, confirme que a instalação está íntegra:

```bash
python check_env.py      # versões e dependências
python test_spark.py     # SparkSession sobe localmente
```

> Ao rodar os **notebooks** no VS Code / Jupyter, lembre-se de selecionar o kernel do
> ambiente **`postech2`**.

### 5. Configurar as credenciais AWS (.env)

As credenciais do **AWS Academy Learner Lab são temporárias** e expiram ao encerrar a
sessão do lab, por isso incluem um **session token** e precisam ser atualizadas a cada
nova sessão.

Copie o modelo e preencha os valores:

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

> O arquivo `.env` **não deve ser versionado** (já deve constar no `.gitignore`).
> Cada integrante usa suas próprias credenciais e seu próprio bucket.

### 6. Criar o bucket S3 (Data Lake)

O nome do bucket é **globalmente único**. Sugestão de padrão:
`alfabetizacao-data-lake-<seu-nome>`

**Learner Lab:**
S3 → *Create bucket* → região **us-east-1** → nome único → *Create*.

Depois de criado, garanta que o nome está preenchido em `S3_BUCKET` no `.env`.

### 7. Baixar os dados oficiais do INEP

Os microdados são grandes e **não são versionados**. Cada clone precisa baixá-los.
Execute o notebook de aquisição, que faz o download dos arquivos de
`download.inep.gov.br` e os extrai em `data_lake/external/`:

```
notebooks/00_aquisicao_dados.ipynb
```

Ao final você deve ter, em `data_lake/external/extraidos/{2023,2024,2025}/`, os arquivos
`TS_ALUNO.csv`, `TS_ESTADO.csv`, `TS_MUNICIPIO.csv` e os `.xlsx` de metas.

> Caso o download automático falhe, os arquivos podem ser baixados manualmente no portal
> do INEP e colocados em `data_lake/external/`. Atenção ao nome do pacote de 2025
> (`microdados_AEEB_2025.zip`), que **não segue** o padrão dos anos anteriores.

### 8. Testar a conexão com o S3

Com `.env` preenchido e bucket criado, valide o acesso ao S3 antes do pipeline:

```bash
python test_conexao.py
```

ou execute o notebook `notebooks/02_teste_conexao.ipynb`.

### 9. Executar o pipeline (medalhão)

Com o ambiente, as credenciais e os dados no lugar.
(leitura dos arquivos do INEP e escrita em Parquet no S3 via PySpark/S3A):

```
notebooks/02_Pipeline.ipynb
```

Ao concluir, os dados brutos estarão materializados no S3, em
`s3://<S3_BUCKET>/bronze/...`.

---

## Estado atual e próximos passos

**Funcionando hoje (fim a fim até a Bronze):**

- Aquisição dos arquivos oficiais do INEP (2023–2025).
- Exploração das entidades (`TS_MUNICIPIO`, `TS_ALUNO`, `TS_ESTADO`) e dos XLSX de metas.
- Conexão com o S3 validada (credenciais temporárias do Learner Lab).
- Ingestão na camada **Bronze** (PySpark → Parquet no S3).



**Em construção:**

- Ingestão na camada **Bronze** (PySpark → Parquet no S3).
- **AWS Glue Job — Silver:** limpeza, padronização, validação e integração.
- **AWS Glue Job — Gold:** agregações, regras de negócio e validação cruzada com os
  resultados oficiais do INEP.
- Consumo via **Amazon Athena** e entrega para **Power BI / Machine Learning**.

> Os scripts em `references/` (`etl-bronze.py` e afins) são **demonstrações das aulas**
> (usam fontes de exemplo) e servem de base e não são o pipeline final do desafio.

---

## Observações importantes / Troubleshooting

- **Credenciais temporárias (Learner Lab):** ao configurar o acesso S3 no Spark, use
  `org.apache.hadoop.fs.s3a.TemporaryAWSCredentialsProvider`. O
  `SimpleAWSCredentialsProvider` **ignora o session token** e causa erro `403`.
  As credenciais expiram ao fim da sessão do lab — atualize o `.env` a cada sessão.
- **JARs do S3A:** os conectores são provisionados via `spark.jars.packages`, com as
  coordenadas Maven `org.apache.hadoop:hadoop-aws:3.3.4` e
  `com.amazonaws:aws-java-sdk-bundle:1.12.262` (pareamento testado para o Hadoop 3.3.4).
- **Python do driver/worker:** defina
  `os.environ["PYSPARK_PYTHON"]` e `os.environ["PYSPARK_DRIVER_PYTHON"]` como
  `sys.executable` **antes** de criar a `SparkSession`, para evitar divergência entre
  interpretadores.
- **Leitura dos arquivos do INEP:** encoding **ISO-8859-1**, separador **`;`**. Nos XLSX,
  espere linhas de título e células mescladas — inspecione o bruto antes de ler.

---

<p><small>Projeto baseado no template
<a target="_blank" href="https://drivendata.github.io/cookiecutter-data-science/">cookiecutter data science</a>.
#cookiecutterdatascience</small></p>
