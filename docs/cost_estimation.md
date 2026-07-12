
# Estimativa de Custos — Pipeline ELT Batch na AWS

> Estimativa de custos da arquitetura Medallion (Raw → Bronze → Silver → Gold) implementada sobre AWS Glue, Amazon S3, Glue Crawlers e Amazon Athena.
>
> O cenário considera **uma execução anual do pipeline de ingestão batch**, compatível com a periodicidade atual de atualização da fonte de dados.
>
> A ingestão por streaming **não será implementada no escopo atual e não compõe a estimativa financeira**. Uma eventual adequação futura deverá ser precedida pela validação da necessidade de processamento em tempo quase real e, principalmente, pela avaliação do volume dos dados gerados e do tamanho dos arquivos, de forma a justificar uma estratégia de envio e processamento em lotes.
>
> **Preços:** região `us-east-1` (N. Virginia), utilizando tarifas públicas *on-demand* de referência da AWS consultadas em julho de 2026. Os valores são estimativos e devem ser reconfirmados antes de decisões orçamentárias.

---

## 0. Parâmetros de entrada

A estimativa diferencia configurações confirmadas no projeto de premissas utilizadas exclusivamente para modelagem financeira.

| Parâmetro                     | Valor usado                                                  | Origem                                                                        |
| ------------------------------ | ------------------------------------------------------------ | ----------------------------------------------------------------------------- |
| Região                        | `us-east-1`                                                | Confirmado (`config_vars.sh` / Learner Lab)                                 |
| Frequência da ingestão batch | **1× por ano**                                        | Periodicidade atual da fonte                                                  |
| Job Raw                        | `pythonshell`, 0,0625 DPU, ~2 min                          | Tipo confirmado no`.sh`; DPU padrão e duração estimada                   |
| Job Bronze                     | 2× G.1X = 2 DPU, ~3 min                                     | Workers confirmados; duração observada/informada                            |
| Job Silver                     | 2× G.1X = 2 DPU, ~3 min                                     | Workers confirmados; duração observada/informada                            |
| Job Gold                       | 2× G.1X = 2 DPU, ~1 min                                     | Workers confirmados; duração observada/informada                            |
| Crawlers                       | 3 (`bronze`, `silver`, `gold`)                         | Quantidade confirmada no projeto                                              |
| Frequência dos Crawlers       | **1 execução por Crawler após a carga batch anual** | Estratégia considerada na estimativa                                         |
| DPU dos Crawlers               | 2 DPU por execução                                         | Premissa de cálculo baseada no cenário de referência da precificação AWS |
| Bucket Raw                     | 621 MB                                                       | Volume medido                                                                 |
| Bucket Bronze                  | 70 MB                                                        | Volume medido                                                                 |
| Bucket Silver                  | 69 MB                                                        | Volume medido                                                                 |
| Bucket Gold                    | 67 MB                                                        | Volume medido                                                                 |
| Bucket Scripts                 | 29 MB                                                        | Volume medido                                                                 |
| Armazenamento S3 total         | **856 MB (~0,856 GB)**                                 | Soma dos volumes medidos                                                      |
| Dados escaneados no Athena     | ~10 GB/mês                                                  | Premissa conservadora a confirmar por métricas                               |
| Streaming                      | **Não implementado no escopo atual**                  | Fora da arquitetura e da estimativa de custos                                 |

### Escopo da estimativa

O custo-base representa exclusivamente o pipeline batch:

```text
Fonte de dados
      ↓
Ingestão Raw
      ↓
S3 Raw
      ↓
Glue Bronze
      ↓
S3 Bronze
      ↓
Glue Silver
      ↓
S3 Silver
      ↓
Glue Gold
      ↓
S3 Gold
      ↓
Glue Crawlers
      ↓
Athena
```

A periodicidade anual é determinante para a análise financeira. Embora as etapas Bronze, Silver e Gold utilizem Apache Spark e infraestrutura distribuída, os jobs permanecem ativos por poucos minutos e são executados apenas uma vez por ano.

Consequentemente, o custo de computação do pipeline é muito baixo.

---

## 1. Visão Geral dos Custos — O Caminho do Dinheiro

Os custos da arquitetura estão distribuídos em quatro famílias principais.

### 1.1 Computação AWS Glue

Os jobs Raw, Bronze, Silver e Gold executam a ingestão e o processamento do pipeline.

O AWS Glue cobra pelo consumo de DPU durante o período de execução de cada job. Como o pipeline batch roda **uma vez por ano e por poucos minutos**, o custo anual de processamento é de poucos centavos.

Nesta escala, o uso de Apache Spark não representa uma parcela financeira significativa da arquitetura.

### 1.2 Glue Crawlers

Os três Crawlers catalogam as camadas Bronze, Silver e Gold no Glue Data Catalog.

Como a fonte é atualizada anualmente, a estimativa considera a execução de cada Crawler **uma vez após a atualização da respectiva camada**:

```text
Pipeline batch anual
        ↓
Atualização Bronze
        ↓
Crawler Bronze

Atualização Silver
        ↓
Crawler Silver

Atualização Gold
        ↓
Crawler Gold
```

Assim, são consideradas **três execuções de Crawler por ano**.

Executar os Crawlers diariamente não seria coerente com a periodicidade atual dos dados, pois significaria catalogar repetidamente estruturas e partições que permanecem inalteradas durante praticamente todo o ano.

### 1.3 Amazon S3

O Amazon S3 armazena os arquivos do pipeline e os scripts utilizados pelos jobs.

Os volumes medidos são:

| Bucket          |           Volume |
| --------------- | ---------------: |
| Raw             |           621 MB |
| Bronze          |            70 MB |
| Silver          |            69 MB |
| Gold            |            67 MB |
| Scripts         |            29 MB |
| **Total** | **856 MB** |

O armazenamento total é, portanto, de aproximadamente **0,856 GB**.

O bucket Raw concentra aproximadamente 73% do volume armazenado:

```text
621 MB / 856 MB ≈ 72,5%
```

As camadas Bronze, Silver e Gold possuem volumes significativamente menores. Esse comportamento é coerente com o processo de seleção, transformação e armazenamento em formato Parquet.

Além do armazenamento, o S3 cobra por requisições como PUT, GET e LIST. O número de operações do pipeline é reduzido devido à execução anual.

Operações DELETE são gratuitas.

### 1.4 Amazon Athena

O Athena permite consultar os dados armazenados no S3 utilizando SQL.

O custo é baseado na quantidade de bytes escaneados pelas consultas.

A arquitetura utiliza:

* arquivos Parquet;
* armazenamento colunar;
* particionamento por `ano`.

Essas decisões reduzem a quantidade de dados lidos pelo Athena e, consequentemente, o custo analítico.

---

## 2. Racional de Precificação por Serviço

| Serviço / Componente                     | Unidade de medida       | Fórmula / Regra                                                                                                        |
| ----------------------------------------- | ----------------------- | ----------------------------------------------------------------------------------------------------------------------- |
| **Glue ETL — Apache Spark**        | DPU-hora                | `nº DPU × horas × US$ 0,44`. G.1X corresponde a 1 DPU por worker. Cobrança por segundo, com mínimo de 1 minuto   |
| **Glue Python Shell**               | DPU-hora                | 0,0625 DPU ou 1 DPU × tempo × US$ 0,44. Mínimo de 1 minuto                                                           |
| **Glue Crawler**                    | DPU-hora                | DPU consumida × tempo × US$ 0,44. Cobrança por segundo, com mínimo de 10 minutos por execução                     |
| **Glue Data Catalog**               | Objetos e requisições | Faixa gratuita mensal para até 1 milhão de objetos armazenados e 1 milhão de requisições                           |
| **S3 Standard — armazenamento**    | GB-mês                 | Aproximadamente US$ 0,023 por GB-mês nos primeiros 50 TB                                                               |
| **S3 — requisições**             | Por 1.000 requisições | PUT/COPY/POST/LIST e GET possuem tarifas distintas; DELETE é gratuito                                                  |
| **Athena SQL on-demand**            | TB escaneado            | Aproximadamente US$ 5,00 por TB escaneado                                                                               |
| **Transferência na mesma região** | GB                      | A comunicação entre os serviços considerados na mesma região não adiciona custo equivalente a egress para internet |

A estimativa utiliza preços públicos *on-demand*. Créditos promocionais e o ambiente AWS Academy Learner Lab podem alterar o valor efetivamente debitado da conta.

---

## 3. Decisões de Engenharia e Impacto Financeiro

### 3.1 Parquet e particionamento reduzem o custo do Athena

O Athena cobra pela quantidade de dados escaneados, e não pela quantidade de linhas retornadas pela consulta.

Duas decisões do pipeline reduzem diretamente esse custo.

#### Parquet

O Parquet é um formato de armazenamento colunar.

Em uma consulta como:

```sql
SELECT municipio, taxa_alfabetizacao
FROM indicadores_municipio;
```

o mecanismo pode ler apenas as colunas necessárias para responder à consulta.

Esse comportamento, conhecido como *column pruning*, reduz a quantidade de bytes processados.

#### Particionamento por ano

As tabelas analíticas são particionadas pela coluna `ano`.

Uma consulta como:

```sql
SELECT *
FROM indicadores_municipio
WHERE ano = 2025;
```

pode restringir a leitura à partição correspondente a `ano=2025`.

Esse comportamento é conhecido como *partition pruning*.

O efeito combinado é:

```text
Parquet
   ↓
Lê apenas colunas necessárias

Particionamento
   ↓
Lê apenas partições necessárias

Resultado
   ↓
Menos bytes escaneados
   ↓
Menor custo no Athena
```

A manutenção de Parquet e do particionamento por `ano` é, portanto, uma decisão simultaneamente técnica e financeira.

---

### 3.2 Redução de volume entre Raw e as camadas processadas

Os volumes medidos no S3 permitem observar o efeito do pipeline sobre o armazenamento:

```text
Raw       621 MB
             ↓
Bronze     70 MB
             ↓
Silver     69 MB
             ↓
Gold       67 MB
```

A diferença mais expressiva ocorre entre Raw e Bronze.

A camada Raw preserva os arquivos de origem utilizados na ingestão, enquanto a Bronze armazena os dados processados em formato Parquet.

A redução de 621 MB para 70 MB representa aproximadamente:

```text
(621 - 70) / 621 × 100
≈ 88,7%
```

de redução no volume armazenado.

Entre Bronze e Silver, o volume passa de 70 MB para 69 MB. Entre Silver e Gold, passa de 69 MB para 67 MB.

Isso indica que o principal ganho de armazenamento ocorre na conversão inicial dos dados de origem para a representação utilizada no Data Lake.

As transformações posteriores possuem maior foco em:

* padronização;
* qualidade;
* integração;
* aplicação de regras de negócio;
* geração de indicadores analíticos.

Portanto, seu objetivo principal não é necessariamente reduzir o volume físico dos dados.

---

### 3.3 `delete_prefix` e `overwrite` no S3

O pipeline realiza operações de limpeza e sobrescrita de dados.

No S3:

* PUT é uma operação cobrada;
* LIST é uma operação cobrada;
* GET é uma operação cobrada;
* DELETE é gratuito.

Consequentemente, o custo de uma sobrescrita não está na exclusão dos arquivos antigos, mas principalmente na escrita dos novos objetos.

No caso do Spark:

```text
Partição existente
       ↓
DELETE dos arquivos antigos
       ↓
Gratuito

Novos arquivos Parquet
       ↓
PUT no S3
       ↓
Operação cobrada
```

Como o pipeline executa apenas uma vez por ano e o volume total atual é inferior a 1 GB, o impacto financeiro dessas operações é muito pequeno.

O padrão deve ser reavaliado caso a frequência de atualização ou o volume de dados aumente significativamente.

---

### 3.4 Crawlers sob demanda são coerentes com a periodicidade dos dados

O schema das camadas Bronze, Silver e Gold é controlado pelo pipeline.

Além disso, a fonte batch é atualizada uma vez por ano.

A estratégia considerada na estimativa é:

```text
1 Crawler Bronze / ano
1 Crawler Silver / ano
1 Crawler Gold / ano
```

Total:

```text
3 execuções de Crawler por ano
```

Utilizando como premissa de cálculo 2 DPU por execução e o mínimo faturável de 10 minutos:

```text
3 × 2 DPU × (10 / 60 h) × US$ 0,44
```

Resultado:

```text
≈ US$ 0,44 por ano
```

Amortizado mensalmente:

```text
≈ US$ 0,037 por mês
```

A execução associada à carga anual evita processamento recorrente sem benefício operacional.

---

## 4. Avaliação da Necessidade de Adequação para Streaming

A ingestão streaming **não será implementada no escopo atual do projeto** e, portanto, não compõe a estimativa de custos.

A fonte atualmente utilizada possui periodicidade anual e é processada por uma ingestão batch.

Nesse contexto, não há requisito identificado de baixa latência que justifique manter processamento em tempo quase real.

Entretanto, uma futura mudança no perfil da fonte pode exigir a reavaliação da arquitetura.

A questão central é:

> **o volume e a frequência de geração dos novos dados seriam suficientes para justificar o agrupamento e o envio em lotes menores e mais frequentes?**

### 4.1 Volume atual e tamanho dos arquivos

O volume total das camadas processadas é pequeno:

```text
Bronze = 70 MB
Silver = 69 MB
Gold   = 67 MB
```

Mesmo a camada Raw possui apenas 621 MB.

Para o cenário atual, dividir artificialmente esses dados em grande quantidade de pequenos lotes poderia aumentar o overhead operacional sem produzir benefício proporcional.

Por exemplo:

```text
Poucos dados
     ↓
Divisão em muitos lotes
     ↓
Muitos arquivos pequenos
     ↓
Mais metadados e operações
     ↓
Maior overhead de processamento
```

Esse comportamento está associado ao problema de *small files* em Data Lakes.

O processamento distribuído tende a ser mais eficiente quando existe volume suficiente para justificar a distribuição das tarefas.

### 4.2 Validação necessária antes de uma adequação

Uma eventual adequação para streaming ou micro-lotes deve ser baseada em medições.

Devem ser avaliados:

1. **Frequência de geração dos dados:** com que periodicidade novos registros seriam disponibilizados?
2. **Volume gerado por período:** quantos MB ou GB são produzidos por hora ou por dia?
3. **Tamanho médio dos arquivos de entrada:** os arquivos possuem volume suficiente para justificar processamento separado?
4. **Quantidade de arquivos:** a nova fonte produziria grande quantidade de arquivos pequenos?
5. **Latência necessária:** os dados precisam estar disponíveis em segundos, minutos ou apenas algumas horas?
6. **Tamanho dos lotes:** qual volume acumulado justificaria o disparo de uma nova execução?
7. **Custo computacional:** o benefício da redução de latência justificaria o aumento da frequência de processamento?

Sem essas informações, a adoção de streaming seria uma decisão orientada pela tecnologia, e não pelo requisito do problema.

### 4.3 Aumento do tamanho dos lotes

Caso o perfil dos dados mude, uma alternativa deve ser avaliada antes da adoção de um stream persistente: **acumular arquivos ou eventos até atingir um volume que justifique o processamento em lote**.

Em vez de:

```text
Arquivo pequeno
      ↓
Processamento

Arquivo pequeno
      ↓
Processamento

Arquivo pequeno
      ↓
Processamento
```

pode-se utilizar:

```text
Arquivo pequeno
Arquivo pequeno
Arquivo pequeno
Arquivo pequeno
       ↓
Acumulação
       ↓
Lote com volume suficiente
       ↓
Processamento
```

O disparo do processamento pode considerar critérios como:

```text
Volume acumulado >= limite

OU

Quantidade de arquivos >= limite

OU

Tempo máximo de espera atingido
```

Os limites não devem ser definidos arbitrariamente.

É necessário observar o comportamento real da fonte e medir:

```text
Volume
   ↕
Frequência
   ↕
Latência necessária
   ↕
Custo de processamento
```

O aumento do tamanho dos arquivos ou dos lotes pode tornar o processamento incremental mais eficiente e reduzir o problema de arquivos pequenos.

### 4.4 Decisão arquitetural atual

Com os volumes atualmente medidos e a periodicidade anual da fonte, **não há justificativa técnica ou financeira identificada para implementar streaming**.

A decisão atual é manter:

```text
Fonte anual
     ↓
Ingestão batch
     ↓
Processamento anual
```

Uma adequação futura somente deve ser considerada se houver mudança mensurável no perfil da fonte.

Por exemplo:

```text
Maior frequência de geração
            +
Maior volume acumulado
            +
Requisito de menor latência
            ↓
Reavaliar estratégia de ingestão
```

Nesse cenário, o primeiro passo deve ser validar uma estratégia de agrupamento dos dados em lotes com tamanho suficiente para justificar execuções mais frequentes.

Portanto, o streaming não é tratado como evolução obrigatória da arquitetura, mas como uma alternativa condicionada a novos requisitos.

---

## 5. Matriz de Estimativa de Custos

Premissas:

* região `us-east-1`;
* pipeline batch executado 1× por ano;
* três Crawlers executados 1× por ano;
* **856 MB (~0,856 GB) armazenados no S3**;
* aproximadamente 10 GB escaneados mensalmente pelo Athena;
* streaming não implementado.

| Componente                    | Base de cálculo                                              | Fórmula                       |                            Custo anual | Custo mensal amortizado |
| ----------------------------- | ------------------------------------------------------------- | ------------------------------ | -------------------------------------: | ----------------------: |
| **Raw**                 | Python Shell, 0,0625 DPU, 2 min                               | `0,0625 × (2/60) × 0,44`   | US$ 0,0009 |              US$ 0,0001 |                         |
| **Bronze**              | 2 DPU, 3 min                                                  | `2 × (3/60) × 0,44`        | US$ 0,0440 |              US$ 0,0037 |                         |
| **Silver**              | 2 DPU, 3 min                                                  | `2 × (3/60) × 0,44`        | US$ 0,0440 |              US$ 0,0037 |                         |
| **Gold**                | 2 DPU, 1 min                                                  | `2 × (1/60) × 0,44`        | US$ 0,0147 |              US$ 0,0012 |                         |
| **Crawlers**            | 3 execuções anuais; estimativa de 2 DPU e mínimo de 10 min | `3 × 2 × (10/60) × 0,44`  |   US$ 0,44 |               US$ 0,037 |                         |
| **S3 — armazenamento** | 0,856 GB Standard                                             | `0,856 × 0,023`             |  ~US$ 0,24 |               US$ 0,020 |                         |
| **S3 — requisições** | PUT/GET/LIST                                                  | Volume e frequência reduzidos |                                  Baixo |              < US$ 0,10 |
| **Athena**              | ~10 GB escaneados/mês                                        | `(10 / 1024) × 5`           |  ~US$ 0,59 |               US$ 0,049 |                         |
| **Glue Data Catalog**   | Abaixo da faixa gratuita considerada                          | —                             |   US$ 0,00 |                US$ 0,00 |                         |
| **Streaming**           | Não implementado                                             | Fora do escopo                 |                                     — |                      — |

Desconsiderando a margem conservadora de até US$ 0,10/mês atribuída às requisições S3, o custo mensal amortizado estimado é:

```text
Jobs Glue       ≈ US$ 0,009/mês
Crawlers        ≈ US$ 0,037/mês
S3 storage      ≈ US$ 0,020/mês
Athena          ≈ US$ 0,049/mês
--------------------------------
Total           ≈ US$ 0,115/mês
```

Incluindo as requisições S3 como margem conservadora:

```text
Custo estimado < US$ 0,22/mês
```

O custo anual estimado permanece inferior a aproximadamente:

```text
US$ 2,60/ano
```

considerando a margem conservadora de requisições S3.

O custo dos quatro jobs batch é aproximadamente:

```text
US$ 0,10 por execução anual completa
```

Incluindo os três Crawlers:

```text
Jobs batch + Crawlers
≈ US$ 0,54 por ano
```

Portanto, nesta escala, o custo computacional do ELT é praticamente irrelevante.

Os custos recorrentes mais relevantes na estimativa são o consumo analítico do Athena e as operações associadas ao armazenamento e acesso aos objetos no S3.

---

## 6. Cenários de Reavaliação Arquitetural

| Cenário                                    | Característica                                           | Decisão recomendada                                |
| ------------------------------------------- | --------------------------------------------------------- | --------------------------------------------------- |
| **Atual — Batch anual**              | Fonte atualizada 1×/ano                                  | Manter arquitetura atual                            |
| **Maior volume anual**                | Arquivos maiores, mesma periodicidade                     | Reavaliar workers, particionamento e tempo dos jobs |
| **Arquivos pequenos mais frequentes** | Novos arquivos ao longo do tempo                          | Avaliar acumulação e processamento em lotes       |
| **Maior volume e menor latência**    | Dados frequentes com necessidade de atualização rápida | Reavaliar ingestão incremental ou streaming        |

A arquitetura atual está adequada à periodicidade e ao volume medido da fonte.

A mudança para streaming não deve ser considerada uma etapa obrigatória de evolução.

Ela somente deve ocorrer caso novos requisitos alterem o perfil de volume, frequência ou latência dos dados.

---

## 7. Recomendações de Otimização e Evolução

1. **Manter a execução batch anual enquanto a periodicidade da fonte permanecer anual.** A frequência atual é coerente com a natureza dos dados.
2. **Executar os Crawlers apenas após a atualização anual das respectivas camadas.** Não há justificativa para catalogação recorrente de dados estáveis.
3. **Preservar Parquet e o particionamento por `ano`.** Essas decisões reduzem o volume escaneado pelo Athena e melhoram a eficiência analítica.
4. **Monitorar o crescimento dos buckets.** O volume atual é de 856 MB e deve ser acompanhado caso novas fontes sejam adicionadas.
5. **Medir os bytes efetivamente escaneados pelo Athena.** A premissa de 10 GB/mês deve ser substituída por métricas reais de utilização.
6. **Não implementar streaming no cenário atual.** A periodicidade anual e o volume medido não demonstram necessidade técnica ou financeira.
7. **Reavaliar a estratégia de ingestão caso a frequência da fonte aumente.** A decisão deve considerar volume, número de arquivos e requisito de latência.
8. **Avaliar o agrupamento de arquivos em lotes antes de considerar streaming persistente.** O objetivo é aumentar o volume por processamento e evitar o problema de *small files*.
9. **Definir limites de lote a partir de métricas reais.** Volume acumulado, quantidade de arquivos e tempo máximo de espera são possíveis critérios de disparo.
10. **Aplicar lifecycle ao prefixo de resultados do Athena.** Resultados antigos de consultas podem ser expirados automaticamente para evitar acúmulo desnecessário no S3.

---

## 8. Conclusão

A arquitetura atual possui um perfil de custo extremamente baixo e está coerente com a periodicidade da fonte de dados.

A ingestão batch é executada uma vez por ano e os jobs Glue permanecem ativos por poucos minutos. O custo dos jobs Raw, Bronze, Silver e Gold é de aproximadamente US$ 0,10 por execução anual completa.

Com a execução dos três Crawlers após a carga anual, o custo de computação e catalogação permanece próximo de US$ 0,54 por ano.

O volume total atualmente armazenado no S3 é de **856 MB**, distribuído entre Raw (621 MB), Bronze (70 MB), Silver (69 MB), Gold (67 MB) e Scripts (29 MB). Esse volume representa um custo estimado de armazenamento de aproximadamente **US$ 0,02 por mês**.

A redução de aproximadamente 88,7% entre os volumes Raw e Bronze evidencia o impacto da conversão dos arquivos de origem para a representação Parquet utilizada no Data Lake. Nas etapas Silver e Gold, a redução de volume é menor porque o foco das transformações passa a ser qualidade, integração e enriquecimento analítico dos dados.

A ingestão streaming não será implementada no escopo atual e não compõe a estimativa financeira.

Com a periodicidade anual e os volumes atualmente medidos, não foi identificada justificativa técnica ou financeira para processamento em tempo quase real.

Caso o perfil da fonte mude, a arquitetura deverá ser reavaliada com base no volume, frequência e requisito de latência dos novos dados. Antes da adoção de streaming, deve-se validar se o aumento do tamanho dos arquivos ou o agrupamento de eventos e arquivos em lotes torna o processamento incremental tecnicamente justificável.

Assim, a principal conclusão de FinOps é:

> **a arquitetura batch anual está adequadamente dimensionada para o volume e a periodicidade atuais. A adoção de uma estratégia de streaming somente deve ser reavaliada diante de uma mudança mensurável no perfil dos dados e após a validação de uma estratégia de agrupamento que produza lotes com volume suficiente para justificar processamento mais frequente.**

---

## Fontes de preço

| Serviço               | Página oficial        |
| ---------------------- | ---------------------- |
| AWS Glue               | AWS Glue Pricing       |
| Amazon S3              | Amazon S3 Pricing      |
| Amazon Athena          | Amazon Athena Pricing  |
| Estimativa consolidada | AWS Pricing Calculator |

*Tarifas sujeitas a alteração pela AWS e variação por região. Os valores devem ser reconfirmados antes de decisões de orçamento ou implantação em produção.*teer
