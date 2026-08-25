# Databricks notebook source
# MAGIC %md
# MAGIC # redshift/00_setup — Carga das tabelas TPC-DS no Redshift (a partir do Databricks)
# MAGIC
# MAGIC Orquestrado **de dentro do Databricks**. Fluxo:
# MAGIC 1. Lê as *managed tables* de `tpds1tb.benchmark` (Delta em S3)
# MAGIC 2. **Unload** de cada tabela para o S3 de staging em **Parquet**
# MAGIC 3. Cria schema + tabelas no Redshift (DDL gerado do schema real, com DISTKEY/SORTKEY)
# MAGIC 4. **COPY** de cada tabela do S3 → Redshift
# MAGIC 5. Cria a tabela de resultados `bench_results` no Redshift
# MAGIC
# MAGIC > ⚠️ **Não testado** (requer um endpoint Redshift). Preencha o bloco de
# MAGIC > configuração e os pré-requisitos do README antes de rodar.

# COMMAND ----------

# MAGIC %pip install redshift_connector
# MAGIC %restart_python

# COMMAND ----------

# MAGIC %md ## Configuração

# COMMAND ----------

# ----- Origem (Databricks) -----
SRC_CATALOG = "tpds1tb"
SRC_SCHEMA  = "benchmark"

# ----- Redshift -----
REDSHIFT_TYPE   = "serverless"          # "serverless" ou "provisioned"
REDSHIFT_HOST   = "<workgroup>.<acct>.<region>.redshift-serverless.amazonaws.com"
REDSHIFT_PORT   = 5439
REDSHIFT_DB     = "dev"
REDSHIFT_SCHEMA = "benchmark"
REDSHIFT_USER   = "admin"
# Senha via Databricks Secrets (recomendado): configure o scope/chave
REDSHIFT_PASSWORD = dbutils.secrets.get("redshift", "password")

# ----- S3 staging + IAM role do COPY -----
S3_STAGING    = "s3://<bucket>/tpcds_staging"           # Databricks escreve; Redshift lê
IAM_ROLE_ARN  = "arn:aws:iam::<acct>:role/<redshift-copy-role>"

# ----- Identificação p/ capturar o "size" dinamicamente -----
AWS_REGION    = "us-east-1"
RS_WORKGROUP  = "<workgroup-name>"      # se serverless
RS_CLUSTER_ID = "<cluster-identifier>"  # se provisioned

TABLES = [r.tableName for r in spark.sql(f"SHOW TABLES IN {SRC_CATALOG}.{SRC_SCHEMA}").collect()
          if r.tableName != "bench_results"]
print(f"{len(TABLES)} tabelas de origem:", ", ".join(sorted(TABLES)))

# COMMAND ----------

# MAGIC %md ## Conexão + captura do "size" (análogo de sql_warehouse_size)

# COMMAND ----------

import redshift_connector, boto3

def rs_connect():
    return redshift_connector.connect(
        host=REDSHIFT_HOST, port=REDSHIFT_PORT, database=REDSHIFT_DB,
        user=REDSHIFT_USER, password=REDSHIFT_PASSWORD)

def redshift_size():
    """Análogo de sql_warehouse_size: RPUs (serverless) ou node_type x nós (provisioned)."""
    if REDSHIFT_TYPE == "serverless":
        wg = boto3.client("redshift-serverless", region_name=AWS_REGION)\
                  .get_workgroup(workgroupName=RS_WORKGROUP)["workgroup"]
        return f"{wg['baseCapacity']} RPU"
    else:
        cl = boto3.client("redshift", region_name=AWS_REGION)\
                  .describe_clusters(ClusterIdentifier=RS_CLUSTER_ID)["Clusters"][0]
        return f"{cl['NodeType']} x{cl['NumberOfNodes']}"

print("Redshift size:", redshift_size())

# COMMAND ----------

# MAGIC %md ## Geração de DDL a partir do schema real (tipos fiéis + DISTKEY/SORTKEY)

# COMMAND ----------

from pyspark.sql import types as T

# DISTKEY/SORTKEY dos fatos e dimensões grandes (padrão TPC-DS em Redshift)
DIST_KEYS = {
    "store_sales":     ("ss_item_sk", "ss_sold_date_sk"),
    "store_returns":   ("sr_item_sk", "sr_returned_date_sk"),
    "catalog_sales":   ("cs_item_sk", "cs_sold_date_sk"),
    "catalog_returns": ("cr_item_sk", "cr_returned_date_sk"),
    "web_sales":       ("ws_item_sk", "ws_sold_date_sk"),
    "web_returns":     ("wr_item_sk", "wr_returned_date_sk"),
    "inventory":       ("inv_item_sk", "inv_date_sk"),
    "customer":              ("c_customer_sk", None),
    "customer_address":      ("ca_address_sk", None),
    "customer_demographics": ("cd_demo_sk", None),
    "item":                  ("i_item_sk", None),
}
# Dimensões pequenas: replicar em todos os nós (DISTSTYLE ALL) acelera joins
DIST_ALL = {"date_dim", "time_dim", "store", "call_center", "catalog_page", "web_page",
            "web_site", "warehouse", "ship_mode", "reason", "income_band",
            "household_demographics", "promotion"}

def spark_to_redshift(dt):
    if isinstance(dt, T.LongType):      return "BIGINT"
    if isinstance(dt, T.IntegerType):   return "INTEGER"
    if isinstance(dt, (T.ShortType, T.ByteType)): return "SMALLINT"
    if isinstance(dt, T.DecimalType):   return f"DECIMAL({dt.precision},{dt.scale})"
    if isinstance(dt, T.DoubleType):    return "DOUBLE PRECISION"
    if isinstance(dt, T.FloatType):     return "REAL"
    if isinstance(dt, T.DateType):      return "DATE"
    if isinstance(dt, T.TimestampType): return "TIMESTAMP"
    if isinstance(dt, T.BooleanType):   return "BOOLEAN"
    return "VARCHAR(65535)"   # StringType e afins — comprimento generoso (evita truncamento)

def gen_ddl(t):
    fields = spark.table(f"{SRC_CATALOG}.{SRC_SCHEMA}.{t}").schema.fields
    cols = ",\n  ".join(f"{f.name} {spark_to_redshift(f.dataType)}" for f in fields)
    clause = ""
    if t in DIST_KEYS:
        dk, sk = DIST_KEYS[t]
        clause = f"\nDISTKEY({dk})" + (f"\nSORTKEY({sk})" if sk else "")
    elif t in DIST_ALL:
        clause = "\nDISTSTYLE ALL"
    return f"CREATE TABLE {REDSHIFT_SCHEMA}.{t} (\n  {cols}\n){clause};"

print(gen_ddl("store_sales"))   # prévia

# COMMAND ----------

# MAGIC %md ## 1. Unload Databricks → S3 (Parquet)

# COMMAND ----------

import time
for t in sorted(TABLES):
    t0 = time.time()
    (spark.table(f"{SRC_CATALOG}.{SRC_SCHEMA}.{t}")
          .write.mode("overwrite").parquet(f"{S3_STAGING}/{t}/"))
    print(f"  unload {t:<24} ({time.time()-t0:,.1f}s)")
print("unload concluído.")

# COMMAND ----------

# MAGIC %md ## 2. Cria schema + tabelas e faz COPY no Redshift

# COMMAND ----------

conn = rs_connect(); conn.autocommit = True
cur = conn.cursor()
cur.execute(f"CREATE SCHEMA IF NOT EXISTS {REDSHIFT_SCHEMA};")

for t in sorted(TABLES):
    cur.execute(f"DROP TABLE IF EXISTS {REDSHIFT_SCHEMA}.{t};")
    cur.execute(gen_ddl(t))
    t0 = time.time()
    cur.execute(f"""
        COPY {REDSHIFT_SCHEMA}.{t}
        FROM '{S3_STAGING}/{t}/'
        IAM_ROLE '{IAM_ROLE_ARN}'
        FORMAT AS PARQUET;
    """)
    print(f"  COPY {t:<24} ({time.time()-t0:,.1f}s)")

print("carga no Redshift concluída.")

# COMMAND ----------

# MAGIC %md ## 3. Tabela de resultados `bench_results`

# COMMAND ----------

cur.execute(f"""
    CREATE TABLE IF NOT EXISTS {REDSHIFT_SCHEMA}.bench_results (
        tp_exec            VARCHAR(20),
        sql_warehouse_size VARCHAR(100),
        file_name          VARCHAR(50),
        start_timestamp    TIMESTAMP,
        end_timestamp      TIMESTAMP,
        duration           DOUBLE PRECISION
    );
""")
cur.close(); conn.close()
print(f"OK: {REDSHIFT_SCHEMA}.bench_results criada. Setup Redshift pronto.")
