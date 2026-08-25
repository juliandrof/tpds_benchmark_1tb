# Databricks notebook source
# MAGIC %md
# MAGIC # redshift/02_serial_exec — Execução SERIAL no Redshift
# MAGIC
# MAGIC Executa as queries de `queries_redshift/` **em ordem (q1 → q99)**, uma de
# MAGIC cada vez (a próxima só começa após a anterior concluir), numa única conexão.
# MAGIC Grava em `bench_results` com `tp_exec = 'serial'`.

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
TP_EXEC = "serial"

# COMMAND ----------

import os, re, time, datetime
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

ctx = dbutils.notebook.entry_point.getDbutils().notebook().getContext()
repo_root = os.path.dirname(os.path.dirname(ctx.notebookPath().get()))
QUERIES_DIR = "/Workspace" + repo_root + "/" + QUERIES_SUBDIR

def natural_key(fn):
    m = re.match(r"q(\d+)([a-z]*)", fn); return (int(m.group(1)), m.group(2)) if m else (10**9, fn)

files = sorted([f for f in os.listdir(QUERIES_DIR) if f.endswith(".sql")], key=natural_key)
print(f"{len(files)} queries em {QUERIES_DIR} (ordem: {files[0]} → {files[-1]})")

# COMMAND ----------

# MAGIC %md ## Runner — uma query por vez, em ordem

# COMMAND ----------

conn = rs_connect(); conn.autocommit = True; cur = conn.cursor()
results = []
t0 = time.time()
for i, fname in enumerate(files, 1):
    sql = open(os.path.join(QUERIES_DIR, fname)).read()
    start = datetime.datetime.now()
    try:
        cur.execute(sql); cur.fetchall(); ok = True; state = "SUCCEEDED"
    except Exception as e:
        ok, state = False, f"ERROR: {e}"
    end = datetime.datetime.now()
    dur = (end - start).total_seconds()
    results.append({"file_name": fname, "start": start, "end": end,
                    "duration": dur, "state": state, "ok": ok})
    print(f"  [{i:>3}/{len(files)}] {'OK ' if ok else 'ERR'} {fname:<10} {dur:>8.1f}s")
print(f"\nSerial: {sum(r['ok'] for r in results)}/{len(files)} ok em {(time.time()-t0)/60:.1f} min")

# COMMAND ----------

# MAGIC %md ## Grava em bench_results (Redshift)

# COMMAND ----------

for i in range(0, len(results), 50):
    ch = results[i:i+50]
    vals = ",".join(
        f"('{TP_EXEC}','{WAREHOUSE_SIZE}','{r['file_name']}',"
        f"'{r['start']:%Y-%m-%d %H:%M:%S.%f}','{r['end']:%Y-%m-%d %H:%M:%S.%f}',{r['duration']})"
        for r in ch)
    cur.execute(f"INSERT INTO {REDSHIFT_SCHEMA}.bench_results VALUES {vals};")
cur.close(); conn.close()
print(f"{len(results)} linhas gravadas em {REDSHIFT_SCHEMA}.bench_results (tp_exec='{TP_EXEC}').")
