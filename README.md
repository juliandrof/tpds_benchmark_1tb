# TPC-DS Benchmark 1 TB — Databricks

Benchmark do **TPC-DS em escala 1 TB** (`scale factor 1000`) executado sobre um
**SQL Warehouse Serverless** no Databricks. Roda as 103 queries do TPC-DS de duas
formas — **paralela** (todas ao mesmo tempo) e **serial** (uma por vez, em ordem)
— e registra o tempo de cada execução em uma tabela de resultados.

Workspace de referência: `https://e2-demo-field-eng.cloud.databricks.com/`

---

## 📦 Estrutura do repositório

```
tpds_benchmark_1tb/
├── 00_setup.py            # cria catálogo, schema, tabelas (~1 TB) e warehouse
├── 01_queries/            # 103 queries .sql (dialeto Databricks) — lidas pelos notebooks
├── 02_parallel_exec.py    # dispara TODAS as queries ao mesmo tempo
├── 03_serial_exec.py      # executa da q1 à q99, uma após a outra
├── 04_cleanup.py          # apaga catálogo + warehouse (NÃO faz parte de job)
├── queries_databricks/    # 103 queries no dialeto Databricks (Spark SQL)
├── queries_redshift/      # 103 queries adaptadas para Amazon Redshift
└── README.md
```

> **Sobre os dados / pasta `dados`:** o benchmark usa a base *built-in*
> `samples.tpcds_sf1000` (~1 TB), disponível em qualquer workspace Databricks.
> Por isso **não** há arquivos `.parquet` versionados no repositório — 1 TB não
> cabe no GitHub (limite de 100 MB por arquivo). O `00_setup` copia os dados
> direto da sample para as *managed tables* de `tpds1tb.benchmark`.

---

## 🎯 O que é criado

| Recurso | Nome | Detalhe |
|---|---|---|
| Catálogo | `tpds1tb` | Unity Catalog |
| Schema | `benchmark` | dentro de `tpds1tb` |
| Managed tables | 24 tabelas TPC-DS | cópia de `samples.tpcds_sf1000` (~1 TB) |
| Tabela de resultados | `tpds1tb.benchmark.bench_results` | tempos de cada execução |
| SQL Warehouse | `BenchDatabricks` | **Serverless**, tamanho `Small`, autoscale **1 → 10** clusters |

### Tabela `bench_results`

| Coluna | Tipo | Descrição |
|---|---|---|
| `tp_exec` | STRING | `parallel` ou `serial` |
| `sql_warehouse_size` | STRING | tamanho do cluster do warehouse (lido **dinamicamente** das configs) |
| `file_name` | STRING | nome do arquivo `.sql` executado |
| `start_timestamp` | TIMESTAMP | início da execução (wall-clock) |
| `end_timestamp` | TIMESTAMP | fim da execução (wall-clock) |
| `duration` | DOUBLE | segundos entre `start` e `end` |

---

## ▶️ Como executar (passo a passo)

Todos os notebooks são executados **manualmente** (não há Job). Recomenda-se
importar o repositório como uma **Git Folder** no Databricks
(*Workspace → Repos → Add Repo*) apontando para este repositório.

### 1. `00_setup`
Cria catálogo, schema, materializa as ~1 TB de tabelas e cria o warehouse.
> ⏱️ **É a etapa mais demorada e cara** — copia ~1 TB. Rode em compute
> serverless de notebook. Ajuste `WH_CLUSTER_SIZE` se quiser um warehouse maior.

### 2. `02_parallel_exec`
Dispara as **103 queries simultaneamente** contra o `BenchDatabricks`. Uma
`threading.Barrier` garante que todas partam no mesmo instante (sem lotes). O
warehouse faz autoscale até 10 clusters conforme a carga. Grava as linhas com
`tp_exec = 'parallel'`.

### 3. `03_serial_exec`
Executa as queries **em ordem (q1 → q99)**, uma de cada vez — a próxima só
começa após a anterior concluir. Grava as linhas com `tp_exec = 'serial'`.

### 4. Analisar os resultados
```sql
-- Comparação paralelo x serial
SELECT tp_exec,
       sql_warehouse_size,
       count(*)                AS n_queries,
       round(sum(duration), 1) AS tempo_total_seg,
       round(avg(duration), 2) AS media_seg,
       round(max(duration), 2) AS mais_lenta_seg
FROM tpds1tb.benchmark.bench_results
GROUP BY tp_exec, sql_warehouse_size
ORDER BY tp_exec;

-- Top 10 queries mais lentas
SELECT tp_exec, file_name, duration
FROM tpds1tb.benchmark.bench_results
ORDER BY duration DESC
LIMIT 10;
```

### 5. `04_cleanup` (opcional, **fora de qualquer job**)
Apaga o catálogo `tpds1tb` (com dados) e deleta o warehouse. Por segurança é
preciso editar a célula e definir `CONFIRMAR = True` antes de rodar.

---

## 🔍 Paralelo x Serial — o que observar

- **Serial:** mede a latência "pura" de cada query, sem concorrência. `duration`
  reflete o tempo de execução isolado.
- **Paralelo:** todas competem pelo warehouse ao mesmo tempo; o `duration` de
  cada query inclui o tempo em fila + autoscale. O tempo de relógio total tende
  a ser **muito menor** que a soma do serial, evidenciando o ganho de
  concorrência e o autoscaling do Serverless.

---

## 🗂️ `queries_databricks` x `queries_redshift`

- **`queries_databricks/`** — dialeto Spark SQL, como rodam no Databricks.
- **`queries_redshift/`** — mesmas queries adaptadas para **Amazon Redshift**:
  - identificadores com crase `` `...` `` → aspas duplas `"..."`;
  - literais de intervalo `INTERVAL 30 days` → `INTERVAL '30 days'` (Redshift
    exige o literal entre aspas).
  - `ROLLUP`, `GROUPING()`, `stddev_samp` e `LIMIT` já são compatíveis, mantidos
    sem alteração.

  > As queries Redshift **não foram testadas** (não há cluster Redshift), apenas
  > ajustadas para evitar erros de sintaxe.

---

## ⚠️ Custos

Materializar ~1 TB e rodar 103 queries (× 2 modos) sobre um warehouse serverless
com autoscale até 10 clusters consome **DBUs de forma relevante**. Rode o
`04_cleanup` ao terminar para não deixar recursos ativos (o warehouse também tem
`auto_stop` de 15 min de ociosidade).
