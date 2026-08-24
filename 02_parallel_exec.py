# Databricks notebook source
# MAGIC %md
# MAGIC # 02_parallel_exec — Execução PARALELA
# MAGIC
# MAGIC Dispara **todas as 103 queries ao mesmo tempo** contra o warehouse
# MAGIC `BenchDatabricks`. Não há lotes: uma `threading.Barrier` garante que todas
# MAGIC as threads submetam a query no mesmo instante. Cada resultado é gravado em
# MAGIC `tpds1tb.benchmark.bench_results` com `tp_exec = 'parallel'`.

# COMMAND ----------

CATALOG        = "tpds1tb"
SCHEMA         = "benchmark"
WAREHOUSE_NAME = "BenchDatabricks"
QUERIES_SUBDIR = "01_queries"
TP_EXEC        = "parallel"

# COMMAND ----------

import os, re, time, datetime, threading
from concurrent.futures import ThreadPoolExecutor
from databricks.sdk import WorkspaceClient
from databricks.sdk.service.sql import StatementState

w = WorkspaceClient()

# COMMAND ----------

# MAGIC %md ## Warehouse (id + tamanho dinâmico) e diretório das queries

# COMMAND ----------

# Localiza o warehouse pelo nome e lê o tamanho DINAMICAMENTE das configs
matches = [x for x in w.warehouses.list() if x.name == WAREHOUSE_NAME]
assert matches, f"Warehouse '{WAREHOUSE_NAME}' não encontrado. Rode 00_setup primeiro."
wh = w.warehouses.get(matches[0].id)
WAREHOUSE_ID   = wh.id
WAREHOUSE_SIZE = wh.cluster_size          # <-- pego dinamicamente das configs do cluster
print(f"Warehouse: {wh.name} (id={WAREHOUSE_ID}, size={WAREHOUSE_SIZE}, estado={wh.state})")

# Garante que o warehouse está ligado antes de disparar
w.warehouses.start_and_wait(WAREHOUSE_ID)
print("Warehouse ativo.")

# COMMAND ----------

# Descobre o diretório das queries relativo a este notebook
ctx = dbutils.notebook.entry_point.getDbutils().notebook().getContext()
nb_path = ctx.notebookPath().get()
QUERIES_DIR = "/Workspace" + os.path.dirname(nb_path) + "/" + QUERIES_SUBDIR

def natural_key(fn):
    m = re.match(r"q(\d+)([a-z]*)", fn)
    return (int(m.group(1)), m.group(2)) if m else (10**9, fn)

files = sorted([f for f in os.listdir(QUERIES_DIR) if f.endswith(".sql")], key=natural_key)
print(f"{len(files)} arquivos .sql em {QUERIES_DIR}")

# COMMAND ----------

# MAGIC %md ## Runner — dispara todas ao mesmo tempo (Barrier)

# COMMAND ----------

N = len(files)
barrier = threading.Barrier(N)

def run_query(fname):
    sql = open(os.path.join(QUERIES_DIR, fname)).read()
    # Todas as threads esperam aqui e disparam JUNTAS (sem lotes)
    barrier.wait()
    start = datetime.datetime.now()
    try:
        resp = w.statement_execution.execute_statement(
            warehouse_id=WAREHOUSE_ID, statement=sql,
            catalog=CATALOG, schema=SCHEMA, wait_timeout="0s",
        )
        sid   = resp.statement_id
        state = resp.status.state
        while state in (StatementState.PENDING, StatementState.RUNNING):
            time.sleep(1)
            state = w.statement_execution.get_statement(sid).status.state
        ok = state == StatementState.SUCCEEDED
    except Exception as e:
        state, ok = f"ERROR: {e}", False
    end = datetime.datetime.now()
    return {
        "file_name": fname, "start": start, "end": end,
        "duration": (end - start).total_seconds(),
        "state": str(state), "ok": ok,
    }

t0 = time.time()
with ThreadPoolExecutor(max_workers=N) as ex:
    results = list(ex.map(run_query, files))
wall = time.time() - t0

results.sort(key=lambda r: natural_key(r["file_name"]))
n_ok = sum(1 for r in results if r["ok"])
print(f"\nParalelo concluído em {wall:,.1f}s de relógio | sucesso: {n_ok}/{N}")
for r in results:
    flag = "OK " if r["ok"] else "ERR"
    print(f"  [{flag}] {r['file_name']:<10} {r['duration']:>8.2f}s")

# COMMAND ----------

# MAGIC %md ## Grava em `bench_results`

# COMMAND ----------

from pyspark.sql.types import StructType, StructField, StringType, TimestampType, DoubleType

schema = StructType([
    StructField("tp_exec",            StringType()),
    StructField("sql_warehouse_size", StringType()),
    StructField("file_name",          StringType()),
    StructField("start_timestamp",    TimestampType()),
    StructField("end_timestamp",      TimestampType()),
    StructField("duration",           DoubleType()),
])

rows = [(TP_EXEC, WAREHOUSE_SIZE, r["file_name"], r["start"], r["end"], r["duration"]) for r in results]
(spark.createDataFrame(rows, schema)
      .write.mode("append").saveAsTable(f"{CATALOG}.{SCHEMA}.bench_results"))

print(f"{len(rows)} linhas gravadas em {CATALOG}.{SCHEMA}.bench_results (tp_exec='{TP_EXEC}').")
display(spark.sql(f"""
    SELECT tp_exec, sql_warehouse_size, count(*) AS n_queries,
           round(sum(duration),1) AS soma_seg, round(avg(duration),2) AS media_seg,
           round(max(duration),2) AS max_seg
    FROM {CATALOG}.{SCHEMA}.bench_results
    WHERE tp_exec = '{TP_EXEC}'
    GROUP BY tp_exec, sql_warehouse_size
"""))
