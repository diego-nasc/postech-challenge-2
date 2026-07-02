Postech Challenge 2
==============================

Tech Challenge 2 - Análise da Alfabetização no Brasil

Project Organization
------------

    ├── LICENSE
    ├── Makefile           <- Makefile with commands like `make data` or `make train`
    ├── README.md          <- The top-level README for developers using this project.
    ├── data
    │   ├── external       <- Data from third party sources.
    │   ├── bronze (raw)   <- 
    │   ├── silver         <- 
    │   └── gold           <- 
    │
    ├── docs               <- A default Sphinx project; see sphinx-doc.org for details
    │
    ├── models             <- Trained and serialized models, model predictions, or model summaries
    │
    ├── notebooks          <- Jupyter notebooks. Naming convention is a number (for ordering),
    │                         the creator's initials, and a short `-` delimited description, e.g.
    │                         `1.0-jqp-initial-data-exploration`.
    │
    ├── references         <- Data dictionaries, manuals, and all other explanatory materials.
    │
    ├── reports            <- Generated analysis as HTML, PDF, LaTeX, etc.
    │   └── figures        <- Generated graphics and figures to be used in reporting
    │
    ├── requirements.txt   <- The requirements file for reproducing the analysis environment, e.g.
    │                         generated with `pip freeze > requirements.txt`
    │
    ├── setup.py           <- makes project pip installable (pip install -e .) so src can be imported
    ├── src                <- Source code for use in this project.
    │   ├── __init__.py    <- Makes src a Python module
    │   │
    │   ├── data           <- Scripts to download or generate data
    │   │   └── make_dataset.py
    │   │
    │   ├── features       <- Scripts to turn raw data into features for modeling
    │   │   └── build_features.py
    │   │
    │   ├── models         <- Scripts to train models and then use trained models to make
    │   │   │                 predictions
    │   │   ├── predict_model.py
    │   │   └── train_model.py
    │   │
    │   └── visualization  <- Scripts to create exploratory and results oriented visualizations
    │       └── visualize.py
    │
    └── tox.ini            <- tox file with settings for running tox; see tox.readthedocs.io


--------

<p><small>Project based on the <a target="_blank" href="https://drivendata.github.io/cookiecutter-data-science/">cookiecutter data science project template</a>. #cookiecutterdatascience</small></p>

## Configuração do Ambiente

Este projeto utiliza **Conda** para gerenciamento do ambiente virtual e **pip** para instalação das dependências Python.

### Pré-requisitos

* Python 3.11
* Conda (Miniconda ou Anaconda)
* Git

### 1. Clonar o repositório

git clone https://github.com/diego-nasc/postech-challenge-2.git
cd postech-challenge-2

### 2. Criar o ambiente Conda

conda env create -f environment.yml

### 3. Ativar o ambiente

conda activate postech2

### 4. Alterar o arquivo .env

Olhar o arquivo .env.example

## Arquitetura

O projeto segue a Arquitetura Medalhão (Medallion Architecture):

Bronze: dados ingeridos e padronizados.
Silver: dados limpos, integrados e validados.
Gold: dados consolidados para análises e consumo.

### Ingestão dos dados
Como os resultados da avaliação da alfabetização possuem uma periodicidade de atualização anual, diferente de dados transacionais, onde há necessidade de captura/transmissões em tempo real, optou-se pela **ingestão batch**,** na qual os dados são obtidos da origem, persistidos na camada Bronze e reutilizados nas etapas seguintes da pipeline.
Nossa primeira tentativa de download manual a partir da fonte <https://basedosdados.org/dataset/073a39d4-89cf-4068-b1e8-34ed0d9c0b72?table=bb27c746-18df-4ba8-8f98-5110232e2162> falhou na tabela "alunos" (~3,9M linhas). Essa falha ocorreu devido o site ser projetado para download de arquivos pequenos via navegador. 
Também foi considerada a utilização da fonte primária do INEP  <https://www.gov.br/inep/pt-br/areas-de-atuacao/avaliacao-e-exames-educacionais/avaliacao-da-alfabetizacao/resultados>, por meio do download dos arquivos microdados_AEEB_2023.zip e microdados_AEEB_2024.zip. Apesar dessa alternativa permitir acessar os microdados oficiais, os dados são ZIPs em portal sem API REST pública para consulta automatizada e dado cru que exigiria refazer o tratamento que a Base dos Dados já faz (limpeza e padronização). 
Como alternativa, consideramos usar o BigQuery via pacote basedosdados. A Base dos Dados (BD) oferece acesso aos datasets de forma gratuíta, permitindo automatizar a ingestão sem depender de downloads manuais. Devido essa característica, aptamos por acessar os dados por meio do BigQuery utilizando a biblioteca basedosdados. A extração é realizada automaticamente via BigQuery apenas quando necessário, e os dados são persistidos em arquivos Parquet na camada Bronze armazenada no Amazon S3. Dessa forma, as camadas Silver e Gold reutilizam os dados já materializados, evitando consultas repetidas à origem, reduzindo o tempo de processamento e o consumo de recursos. Além disso, a arquitetura permite implementar atualizações incrementais via SQL, baixando somente o ano seguinte aos já existentes no dataset, em vez de reprocessar toda a base.

### Cloud
Optamos por utilizar o BigQuery apenas como fonte de ingestão porque o dataset já está publicado e estruturado nessa plataforma, reduzindo o esforço de preparação dos dados. Após a ingestão, os dados são persistidos no Amazon S3, onde todas as demais etapas da pipeline são executadas. Essa escolha permitiu explorar os serviços da AWS previstos na disciplina, mantendo o BigQuery apenas como origem dos dados. (PRECISAMOS MELHORAR ESSA JUSTIFICATIVA OU FAZER TUDO NO AWS OU FAZER TUDO DO GSP)

Fluxograma da arquitetura de armazenamento e processamento em cloud:

                GCP

Base dos Dados
        │
        ▼
BigQuery
        │
        ▼
Python (Ingestão)
        │

──────────────────── AWS ────────────────────

Amazon S3 (Bronze)
        │
        ▼
Glue Job
        │
        ▼
Amazon S3 (Silver)
        │
        ▼
Glue Job
        │
        ▼
Amazon S3 (Gold)
        │
        ▼
Athena (consultas e validação)
        │
        ▼
Machine learning