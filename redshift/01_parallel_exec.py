# Databricks notebook source
# MAGIC %md
# MAGIC # redshift/01_parallel_exec — Execução PARALELA no Redshift
# MAGIC
# MAGIC Dispara as **103 queries de `queries_redshift/` ao mesmo tempo** contra o
# MAGIC Redshift. Cada thread abre sua própria conexão (`redshift_connector` não é
# MAGIC thread-safe) e uma `threading.Barrier` garante o disparo simultâneo, sem lotes.
# MAGIC
# MAGIC > ⚠️ **WLM/concorrência:** o Redshift limita queries simultâneas. Sem
# MAGIC > *Concurrency Scaling* habilitado, a maioria vai **enfileirar** — é o
# MAGIC > comportamento real e faz parte do que o benchmark mede.

# COMMAND ----------

# MAGIC %pip install redshift_connector
# MAGIC %restart_python

# COMMAND ----------

REDSHIFT_TYPE   = "serverless"
REDSHIFT_HOST   = "<workgroup>.<acct>.<region>.redshift-serverless.amazonaws.com"
REDSHIFT_PORT   = 5439
REDSHIFT_DB     = "dev"
REDSHIFT_SCHEMA = "benchmark"
REDSHIFT_USER   = "admin"
REDSHIFT_PASSWORD = dbutils.secrets.get("redshift", "password")
AWS_REGION    = "us-east-1"
RS_WORKGROUP  = "<workgroup-name>"
RS_CLUSTER_ID = "<cluster-identifier>"
QUERIES_SUBDIR = "queries_redshift"
TP_EXEC = "parallel"

# COMMAND ----------

import os, re, time, datetime, threading
from concurrent.futures import ThreadPoolExecutor
import redshift_connector, boto3

def rs_connect():
    return redshift_connector.connect(host=REDSHIFT_HOST, port=REDSHIFT_PORT,
        database=REDSHIFT_DB, user=REDSHIFT_USER, password=REDSHIFT_PASSWORD)

def redshift_size():
    if REDSHIFT_TYPE == "serverless":
        wg = boto3.client("redshift-serverless", region_name=AWS_REGION)\
                  .get_workgroup(workgroupName=RS_WORKGROUP)["workgroup"]
        return f"{wg['baseCapacity']} RPU"
    cl = boto3.client("redshift", region_name=AWS_REGION)\
              .describe_clusters(ClusterIdentifier=RS_CLUSTER_ID)["Clusters"][0]
    return f"{cl['NodeType']} x{cl['NumberOfNodes']}"

WAREHOUSE_SIZE = redshift_size()   # <-- pego dinamicamente da config do Redshift
print("Redshift size:", WAREHOUSE_SIZE)

# COMMAND ----------

# Diretório das queries (queries_redshift/ na raiz do repo; este notebook está em redshift/)
ctx = dbutils.notebook.entry_point.getDbutils().notebook().getContext()
repo_root = os.path.dirname(os.path.dirname(ctx.notebookPath().get()))
QUERIES_DIR = "/Workspace" + repo_root + "/" + QUERIES_SUBDIR

def natural_key(fn):
    m = re.match(r"q(\d+)([a-z]*)", fn); return (int(m.group(1)), m.group(2)) if m else (10**9, fn)

files = sorted([f for f in os.listdir(QUERIES_DIR) if f.endswith(".sql")], key=natural_key)
print(f"{len(files)} queries em {QUERIES_DIR}")

# COMMAND ----------

# MAGIC %md ## Runner — dispara todas juntas (Barrier), 1 conexão por thread

# COMMAND ----------

N = len(files)
barrier = threading.Barrier(N)

def run_query(fname):
    sql = open(os.path.join(QUERIES_DIR, fname)).read()
    conn = rs_connect(); cur = conn.cursor()
    barrier.wait()                       # todas as threads disparam JUNTAS
    start = datetime.datetime.now()
    try:
        cur.execute(sql); cur.fetchall(); ok = True; state = "SUCCEEDED"
    except Exception as e:
        ok, state = False, f"ERROR: {e}"
    end = datetime.datetime.now()
    cur.close(); conn.close()
    return {"file_name": fname, "start": start, "end": end,
            "duration": (end - start).total_seconds(), "state": state, "ok": ok}

t0 = time.time()
with ThreadPoolExecutor(max_workers=N) as ex:
    results = list(ex.map(run_query, files))
wall = time.time() - t0

results.sort(key=lambda r: natural_key(r["file_name"]))
print(f"\nParalelo: {sum(r['ok'] for r in results)}/{N} ok em {wall:,.1f}s de relógio")
for r in results:
    if not r["ok"]: print(f"  ERR {r['file_name']}: {r['state'][:120]}")

# COMMAND ----------

# MAGIC %md ## Grava em bench_results (Redshift)

# COMMAND ----------

conn = rs_connect(); conn.autocommit = True; cur = conn.cursor()
for i in range(0, len(results), 50):
    ch = results[i:i+50]
    vals = ",".join(
        f"('{TP_EXEC}','{WAREHOUSE_SIZE}','{r['file_name']}',"
        f"'{r['start']:%Y-%m-%d %H:%M:%S.%f}','{r['end']:%Y-%m-%d %H:%M:%S.%f}',{r['duration']})"
        for r in ch)
    cur.execute(f"INSERT INTO {REDSHIFT_SCHEMA}.bench_results VALUES {vals};")
cur.close(); conn.close()
print(f"{len(results)} linhas gravadas em {REDSHIFT_SCHEMA}.bench_results (tp_exec='{TP_EXEC}').")
