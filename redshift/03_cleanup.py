# Databricks notebook source
# MAGIC %md
# MAGIC # redshift/03_cleanup — Remoção dos recursos no Redshift + staging S3
# MAGIC
# MAGIC **⚠️ Destrutivo.** Dropa o schema `benchmark` no Redshift (tabelas + dados)
# MAGIC e apaga o staging Parquet no S3. Execução **manual**, fora de qualquer job.

# COMMAND ----------

# MAGIC %pip install redshift_connector
# MAGIC %restart_python

# COMMAND ----------

REDSHIFT_HOST   = "<workgroup>.<acct>.<region>.redshift-serverless.amazonaws.com"
REDSHIFT_PORT   = 5439
REDSHIFT_DB     = "dev"
REDSHIFT_SCHEMA = "benchmark"
REDSHIFT_USER   = "admin"
REDSHIFT_PASSWORD = dbutils.secrets.get("redshift", "password")
S3_STAGING      = "s3://<bucket>/tpcds_staging"

CONFIRMAR = False   # mude para True para confirmar a limpeza

# COMMAND ----------

assert CONFIRMAR, "Defina CONFIRMAR = True para executar a limpeza."

# COMMAND ----------

# 1. Drop do schema no Redshift
import redshift_connector
conn = redshift_connector.connect(host=REDSHIFT_HOST, port=REDSHIFT_PORT,
    database=REDSHIFT_DB, user=REDSHIFT_USER, password=REDSHIFT_PASSWORD)
conn.autocommit = True; cur = conn.cursor()
cur.execute(f"DROP SCHEMA IF EXISTS {REDSHIFT_SCHEMA} CASCADE;")
cur.close(); conn.close()
print(f"Schema {REDSHIFT_SCHEMA} removido no Redshift.")

# COMMAND ----------

# 2. Limpa o staging no S3
dbutils.fs.rm(S3_STAGING, recurse=True)
print(f"Staging {S3_STAGING} removido.")
