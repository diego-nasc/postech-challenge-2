
# Contratos de Qualidade de Dados

> Companheiro técnico de `docs/data_governance.md`. Enquanto aquele documento dá a visão estratégica, este especifica os **contratos de qualidade executáveis** que o pipeline aplica — o que cada dataset precisa satisfazer para ser considerado válido, como isso é verificado em código e como o resultado é lido nos logs.
>
> **Fonte da verdade:** os dicionários `CHECKS` (`glue_elt_silver.py`) e `CHECKS_GOLD` (`glue_elt_gold.py`), mais as validações inline do Bronze (`glue_elt_bronze.py`). Este documento é o espelho legível desses artefatos.

---

## 1. Conceito de Contratos de Qualidade Executáveis

### 1.1 O que é um contrato executável

Um **contrato de qualidade** é uma afirmação verificável por máquina sobre um dataset que **precisa ser verdadeira** para o dado seguir adiante no pipeline. A diferença em relação a documentação em prosa é que o contrato **não descreve** a qualidade esperada — ele a **impõe**: roda em tempo de execução e tem poder de **interromper o pipeline**.

Na prática, os *sanity checks* do código deixam de ser verificações defensivas avulsas e passam a funcionar como contratos porque reúnem três propriedades:

1. **Declarados como dado** — cada tabela tem uma lista de regras (`CHECKS["<tabela>"]`), não um bloco de `if`s espalhado. A especificação e o código são o **mesmo artefato**, então não há *drift* entre "o que deveria ser válido" e "o que é de fato verificado".
2. **Avaliados por um motor genérico** — a função `checar_qualidade` interpreta qualquer regra da lista, roda a verificação e emite o veredito.
3. **Com poder de enforcement** — uma violação **crítica** dispara `raise`, o job Glue termina em `FAILED`, e o orquestrador aborta o pipeline (`espera_job_run` faz `exit 1`). O dado ruim **não chega** à Gold nem ao consumo.

É um modelo *fail-closed*: na dúvida, o pipeline para. O contrato é o **disjuntor** da arquitetura Medallion — protege as camadas a jusante de dados que violem as invariantes assumidas por elas.

### 1.2 Como o motor avalia (fluxo de controle)

O motor de `checar_qualidade` (idêntico em Silver e Gold) executa quatro passos, nesta ordem:

```python
# (1) Guarda de contrato: coluna referenciada por um check que não existe no
#     schema é ERRO DE CONTRATO -> falha sempre, antes de qualquer avaliação.
ausentes = referenciadas - set(df.columns)
if ausentes:
    raise ValueError(f"[DQ:SILVER] {entidade}: coluna(s) do check ausente(s) ...")

# (2) UMA passada de agregação constrói todas as métricas de uma vez
#     (nulos, fora-de-faixa, não-nulos, anos, regex, expressões de negócio).
m = df.agg(*exprs).collect()[0].asDict()

# (3) Avalia cada regra e classifica a severidade.
status = "PASS" if ok else ("FAIL" if critico else "WARN")

# (4) Consolida o score e ABORTA se houve violação crítica.
if criticos > 0:
    raise Exception(f"[DQ:SILVER] {entidade}: {criticos} check(s) critico(s) falharam. Pipeline interrompido.")
```

Três decisões de projeto embutidas nesse fluxo:

- **Guarda de contrato (passo 1).** Se um *check* aponta para uma coluna que não existe, isso é falha de contrato — não vira `WARN`, falha sempre. Isso protege contra a própria matriz ficar desatualizada em relação ao schema.
- **Passada única (passo 2).** Quase todas as métricas são calculadas em **uma só** agregação Spark (varredura única dos dados), em vez de uma passada por regra —decisão de performance. A única exceção é o *check* de unicidade, que faz uma contagem `distinct()` adicional.
- **Faixa em coluna 100% nula não passa calada.** Um *check* de `range` sobre uma coluna inteiramente nula **reprova** (não é tratado como "0 valores fora da faixa"), evitando um falso verde.

### 1.3 Modelo de severidade — ERROR vs WARN

Cada regra carrega uma severidade via o campo `critico` (padrão: `True`):

| Severidade      | `critico` | Efeito                                                  | Semântica                                                     |
| --------------- | ----------- | ------------------------------------------------------- | -------------------------------------------------------------- |
| **ERROR** | `True`    | Dispara`raise` → job `FAILED` → pipeline abortado | "O dado é inutilizável; não pode propagar."                 |
| **WARN**  | `False`   | Registrado no log;**não** aborta                 | "Sinal de qualidade a monitorar; o dado ainda é utilizável." |

Detalhe importante para a leitura dos logs (§3): a contagem `FAIL=n` da linha de resumo soma **todos** os *checks* não aprovados — críticos **e** não-críticos. Ou seja, uma tabela pode terminar com `Score < 100%` e `FAIL ≥ 1` por conta de `WARN`s e **ainda assim ser aprovada** (o pipeline só aborta se houver *check* **crítico** reprovado). ERROR e WARN não se distinguem pela contagem, e sim por qual delas dispara o `raise`.

### 1.4 Dois mecanismos, o mesmo princípio

O pipeline usa dois mecanismos de contrato, escolhidos pela ergonomia de cada camada — mas ambos são executáveis e ambos abortam em falha:

- **Bronze — `assert` / `raise` inline.** A validação estrutural (colunas do contrato presentes, anos esperados presentes) vive junto da ingestão, porque é simples e específica por arquivo. Uma coluna do contrato ausente dispara `assert`; um ano esperado ausente dispara `raise ValueError`.
- **Silver e Gold — catálogo declarativo (`CHECKS` / `CHECKS_GOLD`).** Aqui as regras são muitas e repetitivas entre tabelas, então compensa o catálogo declarativo + motor genérico descrito em §1.2.

A diferença é de forma, não de natureza: em ambos os casos, **a verificação roda e tem poder de veto**.

---

## 2. Matriz de Contratos de Qualidade

A matriz é apresentada **por dataset** — cada bloco corresponde 1:1 a uma entrada de `CHECKS` / `CHECKS_GOLD` (ou às validações do Bronze), o que mantém a correspondência direta com o código. As colunas são **Regra / Descrição**, **Tipo do Check** e **Severidade**; o **Dataset** é o título de cada bloco.

### 2.1 Taxonomia — tipo de check → categoria da matriz

Os tipos técnicos do motor mapeiam para quatro categorias de governança:

| Tipo (código) | Categoria (matriz)    | O que verifica                                     | Severidade típica   |
| -------------- | --------------------- | -------------------------------------------------- | -------------------- |
| `min_count`  | **Integridade** | Tabela não vazia (≥ N linhas)                    | ERROR                |
| `not_null`   | **Integridade** | Coluna obrigatória sem nulos                      | ERROR                |
| `anos`       | **Integridade** | Anos/partições esperados presentes (subconjunto) | ERROR                |
| `range`      | **Domínio**    | Valor numérico dentro de`[min, max]`            | WARN                 |
| `regex`      | **Domínio**    | Formato textual (ex.: código IBGE de 7 dígitos)  | ERROR                |
| `unique`     | **Unicidade**   | Chave (composta) sem duplicatas — define o grão  | ERROR                |
| `expr`       | **Negócio**    | Invariante de negócio em SQL (`F.expr`)         | ERROR (algumas WARN) |

> **Integridade** = o dado existe e está completo. 
> **Domínio** = os valores são plausíveis/bem-formados. 
> **Unicidade** = o grão da tabela está correto.
> **Negócio** = as regras semânticas do domínio (rede, corte de alfabetização, coerência de indicadores) se sustentam.

### 2.2 Camada Bronze

Mecanismo: `assert` / `raise` inline (não o catálogo declarativo).

| Dataset                                                                            | Regra / Descrição                                                                                             | Tipo        | Severidade |
| ---------------------------------------------------------------------------------- | --------------------------------------------------------------------------------------------------------------- | ----------- | ---------- |
| `ts_aluno`, `ts_municipio`, `ts_estado`, `metas_municipios`, `metas_ufs` | Todas as colunas do contrato presentes no arquivo (validado por ano) —`assert`                               | Integridade | ERROR      |
| `ts_aluno`, `ts_municipio`, `ts_estado`, `metas_municipios`, `metas_ufs` | Anos`{2023, 2024, 2025}` presentes (subconjunto — anos extras não reprovam) — `raise ValueError`         | Integridade | ERROR      |
| `ts_aluno`                                                                       | Aluno ausente (`IN_PRESENCA_LP = 0`) não possui proficiência registrada — diagnóstico via `log.warning` | Negócio    | WARN       |

### 2.3 Camada Silver

**Dataset: `meta_alfabetizacao_brasil`**

| Regra / Descrição                    | Tipo        | Severidade |
| -------------------------------------- | ----------- | ---------- |
| Tabela não vazia (≥ 1 linha)         | Integridade | ERROR      |
| Anos`{2023, 2024, 2025}` presentes   | Integridade | ERROR      |
| `ano` sem nulos                      | Integridade | ERROR      |
| `rede` sem nulos                     | Integridade | ERROR      |
| Chave`[ano, rede]` única (grão)    | Unicidade   | ERROR      |
| `taxa_alfabetizacao` ∈ `[0, 100]` | Domínio    | WARN       |

**Dataset: `meta_alfabetizacao_uf`**

| Regra / Descrição                           | Tipo        | Severidade |
| --------------------------------------------- | ----------- | ---------- |
| Tabela não vazia (≥ 1 linha)                | Integridade | ERROR      |
| Anos`{2023, 2024, 2025}` presentes          | Integridade | ERROR      |
| `ano` sem nulos                             | Integridade | ERROR      |
| `sigla_uf` sem nulos                        | Integridade | ERROR      |
| `rede` sem nulos                            | Integridade | ERROR      |
| Chave`[ano, sigla_uf, rede]` única (grão) | Unicidade   | ERROR      |
| `taxa_alfabetizacao` ∈ `[0, 100]`        | Domínio    | WARN       |

**Dataset: `meta_alfabetizacao_municipio`**

| Regra / Descrição                                           | Tipo        | Severidade |
| ------------------------------------------------------------- | ----------- | ---------- |
| Tabela não vazia (≥ 1 linha)                                | Integridade | ERROR      |
| Anos`{2023, 2024, 2025}` presentes                          | Integridade | ERROR      |
| `ano` sem nulos                                             | Integridade | ERROR      |
| `id_municipio` sem nulos                                    | Integridade | ERROR      |
| `id_municipio` no formato `^[0-9]{7}$` (IBGE, 7 dígitos) | Domínio    | ERROR      |
| `rede` sem nulos                                            | Integridade | ERROR      |
| Chave`[ano, id_municipio, rede]` única (grão)             | Unicidade   | ERROR      |
| `taxa_alfabetizacao` ∈ `[0, 100]`                        | Domínio    | WARN       |

**Dataset: `uf`** (indicadores estaduais históricos)

| Regra / Descrição                                                                                | Tipo        | Severidade |
| -------------------------------------------------------------------------------------------------- | ----------- | ---------- |
| Tabela não vazia (≥ 1 linha)                                                                     | Integridade | ERROR      |
| Anos`{2023, 2024, 2025}` presentes                                                               | Integridade | ERROR      |
| `ano` sem nulos                                                                                  | Integridade | ERROR      |
| `id_uf` sem nulos                                                                                | Integridade | ERROR      |
| `rede` sem nulos                                                                                 | Integridade | ERROR      |
| Chave`[ano, id_uf, rede]` única (grão)                                                         | Unicidade   | ERROR      |
| `taxa_alfabetizacao` ∈ `[0, 100]`                                                             | Domínio    | WARN       |
| `media_portugues` ∈ `[0, 1000]`                                                               | Domínio    | WARN       |
| Toda linha de UF é`rede = 5` (Pública) — `rede IS NULL OR rede <> 5` (`rede_uf_invalida`) | Negócio    | ERROR      |

**Dataset: `municipio`** (indicadores municipais históricos)

| Regra / Descrição                                                                                            | Tipo        | Severidade |
| -------------------------------------------------------------------------------------------------------------- | ----------- | ---------- |
| Tabela não vazia (≥ 1 linha)                                                                                 | Integridade | ERROR      |
| `ano` sem nulos                                                                                              | Integridade | ERROR      |
| `id_municipio` sem nulos                                                                                     | Integridade | ERROR      |
| `id_municipio` no formato `^[0-9]{7}$` (IBGE, 7 dígitos)                                                  | Domínio    | ERROR      |
| `rede` sem nulos                                                                                             | Integridade | ERROR      |
| Chave`[ano, id_municipio, rede]` única (grão)                                                              | Unicidade   | ERROR      |
| `taxa_alfabetizacao` ∈ `[0, 100]`                                                                         | Domínio    | WARN       |
| `media_portugues` ∈ `[0, 1000]`                                                                           | Domínio    | WARN       |
| Toda linha municipal é`rede = 3` (Municipal) — `rede IS NULL OR rede <> 3` (`rede_municipio_invalida`) | Negócio    | ERROR      |

> Nota: `municipio` é a **única** tabela Silver sem *check* de `anos`. É a tabela em que o streaming injeta `ano=2026`; como a semântica de `anos` é de
> subconjunto (anos extras nunca reprovam), a ausência do *check* aqui é inofensiva e deliberada.

**Dataset: `alunos`** (microdados individuais)

| Regra / Descrição                                                                                                                                                                           | Tipo        | Severidade |
| --------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- | ----------- | ---------- |
| Tabela não vazia (≥ 1 linha)                                                                                                                                                                | Integridade | ERROR      |
| Anos`{2023, 2024, 2025}` presentes                                                                                                                                                          | Integridade | ERROR      |
| `ano` sem nulos                                                                                                                                                                             | Integridade | ERROR      |
| `id_aluno` sem nulos                                                                                                                                                                        | Integridade | ERROR      |
| `id_municipio` sem nulos                                                                                                                                                                    | Integridade | ERROR      |
| `id_municipio` no formato `^[0-9]{7}$` (IBGE, 7 dígitos)                                                                                                                                 | Domínio    | ERROR      |
| Chave`[ano, id_aluno]` única (grão)                                                                                                                                                       | Unicidade   | ERROR      |
| `proficiencia` ∈ `[0, 1500]`                                                                                                                                                             | Domínio    | WARN       |
| Dependência administrativa ∈`{2, 3}` (Estadual/Municipal) — `dependencia_administrativa IS NULL OR dependencia_administrativa NOT IN (2, 3)` (`dependencia_administrativa_invalida`) | Negócio    | ERROR      |

### 2.4 Camada Gold

**Dataset: `indicadores_municipio`**

| Regra / Descrição                                                                                                                                         | Tipo        | Severidade |
| ----------------------------------------------------------------------------------------------------------------------------------------------------------- | ----------- | ---------- |
| Tabela não vazia (≥ 1 linha)                                                                                                                              | Integridade | ERROR      |
| Anos`{2023, 2024, 2025}` presentes                                                                                                                        | Integridade | ERROR      |
| `ano` sem nulos                                                                                                                                           | Integridade | ERROR      |
| `id_municipio` sem nulos                                                                                                                                  | Integridade | ERROR      |
| Chave`[ano, id_municipio]` única (grão)                                                                                                                 | Unicidade   | ERROR      |
| `taxa_alfabetizacao` ∈ `[0, 100]`                                                                                                                      | Domínio    | WARN       |
| `meta` ∈ `[0, 100]`                                                                                                                                    | Domínio    | WARN       |
| Meta presente nos anos-alvo —`ano IN (2024, 2025) AND meta IS NULL` (`meta_ausente_em_ano_alvo`)                                                       | Negócio    | WARN       |
| `percentual_participacao` ∈ `[0, 100]`                                                                                                                 | Domínio    | WARN       |
| `atingiu_meta` coerente com `taxa ≥ meta` — `meta IS NOT NULL AND ((taxa_alfabetizacao - meta >= 0) <> atingiu_meta)` (`atingiu_meta_incoerente`) | Negócio    | ERROR      |

**Dataset: `indicadores_uf`**

| Regra / Descrição                                                                                                                                         | Tipo        | Severidade |
| ----------------------------------------------------------------------------------------------------------------------------------------------------------- | ----------- | ---------- |
| Tabela não vazia (≥ 1 linha)                                                                                                                              | Integridade | ERROR      |
| Anos`{2023, 2024, 2025}` presentes                                                                                                                        | Integridade | ERROR      |
| `ano` sem nulos                                                                                                                                           | Integridade | ERROR      |
| `sigla_uf` sem nulos                                                                                                                                      | Integridade | ERROR      |
| Chave`[ano, sigla_uf]` única (grão)                                                                                                                     | Unicidade   | ERROR      |
| `taxa_alfabetizacao` ∈ `[0, 100]`                                                                                                                      | Domínio    | WARN       |
| `meta` ∈ `[0, 100]`                                                                                                                                    | Domínio    | WARN       |
| Meta presente nos anos-alvo —`ano IN (2024, 2025) AND meta IS NULL` (`meta_ausente_em_ano_alvo`)                                                       | Negócio    | WARN       |
| `percentual_participacao` ∈ `[0, 100]`                                                                                                                 | Domínio    | WARN       |
| `atingiu_meta` coerente com `taxa ≥ meta` — `meta IS NOT NULL AND ((taxa_alfabetizacao - meta >= 0) <> atingiu_meta)` (`atingiu_meta_incoerente`) | Negócio    | ERROR      |

**Dataset: `aluno_contexto`** (tabela pronta para ML)

| Regra / Descrição                                                                                                                                  | Tipo        | Severidade |
| ---------------------------------------------------------------------------------------------------------------------------------------------------- | ----------- | ---------- |
| Tabela não vazia (≥ 1 linha)                                                                                                                       | Integridade | ERROR      |
| Anos`{2023, 2024, 2025}` presentes                                                                                                                 | Integridade | ERROR      |
| `id_aluno` sem nulos                                                                                                                               | Integridade | ERROR      |
| `ano` sem nulos                                                                                                                                    | Integridade | ERROR      |
| `proficiencia` sem nulos (garantido pelo filtro da query)                                                                                          | Integridade | ERROR      |
| `label_alfabetizado` sem nulos (alvo de ML sem nulo)                                                                                               | Integridade | ERROR      |
| Chave`[ano, id_aluno]` única (grão)                                                                                                              | Unicidade   | ERROR      |
| `proficiencia` ∈ `[0, 1500]`                                                                                                                    | Domínio    | WARN       |
| `label_alfabetizado = 1` ⟺ proficiência ≥ 743 (corte) — `(proficiencia >= 743) <> (label_alfabetizado = 1)` (`label_incoerente_com_corte`) | Negócio    | ERROR      |

---

## 3. Leitura dos contratos nos logs

Os vereditos dos contratos são emitidos no mesmo *stream* de log dos jobs (`stdout` → CloudWatch Logs). Todo evento de qualidade é prefixado por `[DQ:CAMADA]`, o que torna a filtragem trivial.

### 3.1 Formato base

O formato do log é `timestamp | nível | mensagem`. Cada *check* individual emite uma linha; ao final de cada tabela, uma linha de **resumo** com o score:

```
<data> | <NÍVEL> | [DQ:<CAMADA>] <STATUS> | <tipo> | <coluna/chave> | <detalhe>
<data> | <NÍVEL> | [DQ:<CAMADA>] <tabela> | Score=<X>% | PASS=<p> FAIL=<f>
```

`STATUS` é um de `PASS`, `FAIL` (crítico reprovado) ou `WARN` (não-crítico reprovado). Os nomes semânticos abaixo (`DATA_QUALITY_PASSED`, etc.) são as **categorias de evento** definidas em `docs/data_governance.md`; as linhas reais são as mostradas.

### 3.2 `DATA_QUALITY_PASSED` — tabela aprovada

Sequência de uma tabela que passa em todos os contratos. Exemplo real de `meta_alfabetizacao_municipio` (8 *checks*, todos `PASS` → `Score=100.0%`):

```
2026-07-11 14:03:01 | INFO     | [DQ:SILVER] meta_alfabetizacao_municipio | iniciando | checks=8
2026-07-11 14:03:02 | INFO     | [DQ:SILVER] PASS | min_count | -                               | contagem=15873 | minimo=1
2026-07-11 14:03:02 | INFO     | [DQ:SILVER] PASS | anos      | ano                             | faltando=[] | presentes=[2023, 2024, 2025]
2026-07-11 14:03:02 | INFO     | [DQ:SILVER] PASS | not_null  | ano                             | 0 nulos
2026-07-11 14:03:02 | INFO     | [DQ:SILVER] PASS | not_null  | id_municipio                    | 0 nulos
2026-07-11 14:03:02 | INFO     | [DQ:SILVER] PASS | regex     | id_municipio                    | 0 fora do padrão '^[0-9]{7}$'
2026-07-11 14:03:02 | INFO     | [DQ:SILVER] PASS | not_null  | rede                            | 0 nulos
2026-07-11 14:03:02 | INFO     | [DQ:SILVER] PASS | unique    | ['ano', 'id_municipio', 'rede'] | 0 duplicatas (chave=['ano', 'id_municipio', 'rede'])
2026-07-11 14:03:02 | INFO     | [DQ:SILVER] PASS | range     | taxa_alfabetizacao              | 0 fora de [0,100] | nulos=214
2026-07-11 14:03:02 | INFO     | [DQ:SILVER] meta_alfabetizacao_municipio | Score=100.0% | PASS=8 FAIL=0
```

A linha de resumo com `Score=100.0%` e `FAIL=0` é o evento `DATA_QUALITY_PASSED`: a tabela satisfez integralmente seu contrato.

### 3.3 `WARN` — não-crítico reprovado, pipeline continua

Um *check* não-crítico pode reprovar sem abortar. Exemplo em `indicadores_municipio`: alguns municípios não têm meta publicada para um ano-alvo (`meta_ausente_em_ano_alvo`, severidade WARN):

```
2026-07-11 14:07:41 | INFO     | [DQ:GOLD] WARN | expr  | -    | 37 violações (meta_ausente_em_ano_alvo)
2026-07-11 14:07:41 | INFO     | [DQ:GOLD] indicadores_municipio | Score=90.0% | PASS=9 FAIL=1
```

`Score=90.0%` e `FAIL=1`, mas **sem** `raise`: como nenhum *check* **crítico** reprovou, o pipeline segue. O `FAIL=1` da linha de resumo contabiliza o `WARN` — é um sinal de monitoramento, não um bloqueio.

### 3.4 `DATA_QUALITY_FAILED` — crítico reprovado, pipeline abortado

Quando um *check* crítico reprova, o veredito da tabela é seguido de uma exceção que encerra o job em `FAILED`. Exemplo: a invariante de `rede` da tabela `uf` detecta linhas com `rede <> 5`:

```
2026-07-11 14:05:12 | INFO     | [DQ:SILVER] FAIL | expr  | -    | 3 violações (rede_uf_invalida)
2026-07-11 14:05:12 | INFO     | [DQ:SILVER] uf | Score=88.9% | PASS=8 FAIL=1
```

E, na sequência, a exceção que aborta (evento `DATA_QUALITY_FAILED`):

```
Exception: [DQ:SILVER] uf: 1 check(s) critico(s) falharam. Pipeline interrompido.
```

O job Glue termina em `FAILED`; no orquestrador, `espera_job_run` faz `exit 1` e **as camadas seguintes não rodam**. A mensagem nomeia a tabela e a quantidade de *checks* críticos violados — suficiente para localizar a causa sem abrir os dados.

### 3.5 Erro de contrato — coluna ausente

Distinto de uma reprovação de dado: se um *check* referencia uma coluna que não existe no schema, o motor aborta **antes** de qualquer avaliação (guarda de contrato, §1.2), independentemente de severidade:

```
ValueError: [DQ:SILVER] municipio: coluna(s) do check ausente(s) no schema -> ['rede'] | schema: ['ano', 'id_municipio', 'nome_municipio', ...]
```

Isso sinaliza que a **matriz** e o **schema** divergiram — um problema de contrato, não de dado — e força a correção do código antes de o pipeline prosseguir.

### 3.6 Filtragem operacional

Como todo evento é prefixado, a triagem no CloudWatch Logs (ou em `grep`) é direta:

| Objetivo                          | Filtro                    |
| --------------------------------- | ------------------------- |
| Todos os eventos de qualidade     | `[DQ:`                  |
| Apenas reprovações críticas    | `[DQ:` + `FAIL`       |
| Apenas sinais de monitoramento    | `[DQ:` + `WARN`       |
| Score consolidado por tabela      | `Score=`                |
| Abortos de pipeline por qualidade | `Pipeline interrompido` |

---

### Referências de código

| Este documento cita                                                    | Arquivo                       |
| ---------------------------------------------------------------------- | ----------------------------- |
| Contrato de schema (`assert`), `checar_anos`, invariante de aluno  | `glue_elt_bronze.py`        |
| Motor`checar_qualidade`, dicionário `CHECKS` (6 datasets)         | `glue_elt_silver.py`        |
| Motor`checar_qualidade`, dicionário `CHECKS_GOLD` (3 datasets)    | `glue_elt_gold.py`          |
| Aborto do pipeline em job`FAILED` (`espera_job_run` → `exit 1`) | `create_aws_resources.sh` |

*A visão estratégica de governança está em `docs/data_governance.md`.*
