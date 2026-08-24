# Databricks notebook source
# MAGIC %md
# MAGIC # 00_setup — Provisionamento do Benchmark TPC-DS 1 TB
# MAGIC
# MAGIC Este notebook cria toda a infraestrutura do benchmark:
# MAGIC 1. Catálogo `tpds1tb`
# MAGIC 2. Schema `benchmark`
# MAGIC 3. Managed tables (cópia de `samples.tpcds_sf1000` — ~1 TB)
# MAGIC 4. Tabela de resultados `bench_results`
# MAGIC 5. SQL Warehouse Serverless `BenchDatabricks` (autoscale 1→10)
# MAGIC
# MAGIC > **Atenção:** a materialização das tabelas copia ~1 TB. É a etapa mais
# MAGIC > demorada/custosa. Rode com compute serverless de notebook.

# COMMAND ----------

# MAGIC %md ## Configuração

# COMMAND ----------

CATALOG        = "tpds1tb"
SCHEMA         = "benchmark"
SOURCE         = "samples.tpcds_sf1000"   # base sample de 1 TB (built-in)
WAREHOUSE_NAME = "BenchDatabricks"
WH_CLUSTER_SIZE = "Small"                 # tamanho de cada cluster do warehouse
WH_MIN_CLUSTERS = 1                       # autoscale mínimo
WH_MAX_CLUSTERS = 10                      # autoscale máximo
WH_AUTO_STOP    = 15                      # minutos ociosos até parar

print(f"Catálogo........: {CATALOG}")
print(f"Schema..........: {SCHEMA}")
print(f"Origem..........: {SOURCE}")
print(f"Warehouse.......: {WAREHOUSE_NAME} ({WH_CLUSTER_SIZE}, {WH_MIN_CLUSTERS}→{WH_MAX_CLUSTERS} clusters)")

# COMMAND ----------

# MAGIC %md ## 1. Catálogo e Schema

# COMMAND ----------

spark.sql(f"CREATE CATALOG IF NOT EXISTS {CATALOG}")
spark.sql(f"CREATE SCHEMA  IF NOT EXISTS {CATALOG}.{SCHEMA}")
print(f"OK: {CATALOG}.{SCHEMA} criado/existente.")

# COMMAND ----------

# MAGIC %md ## 2. Validação da base de origem

# COMMAND ----------

# Confirma que a sample de 1 TB existe e lista suas tabelas
source_tables = [r.tableName for r in spark.sql(f"SHOW TABLES IN {SOURCE}").collect()]
assert source_tables, f"Nenhuma tabela encontrada em {SOURCE}. A sample TPC-DS 1 TB não está disponível neste workspace."
print(f"{len(source_tables)} tabelas encontradas em {SOURCE}:")
for t in sorted(source_tables):
    print(f"  - {t}")

# COMMAND ----------

# MAGIC %md ## 3. Managed tables (~1 TB) — CTAS a partir da sample
# MAGIC
# MAGIC Cada tabela é recriada como *managed table* dentro de `tpds1tb.benchmark`.
# MAGIC A etapa é sequencial e imprime a contagem de linhas de cada tabela.

# COMMAND ----------

import time

for t in sorted(source_tables):
    t0 = time.time()
    spark.sql(f"""
        CREATE OR REPLACE TABLE {CATALOG}.{SCHEMA}.{t}
        AS SELECT * FROM {SOURCE}.{t}
    """)
    cnt = spark.table(f"{CATALOG}.{SCHEMA}.{t}").count()
    print(f"  {t:<24} {cnt:>15,} linhas  ({time.time()-t0:,.1f}s)")

print("\nTodas as managed tables foram criadas.")

# COMMAND ----------

# MAGIC %md ## 4. Tabela de resultados `bench_results`

# COMMAND ----------

spark.sql(f"""
    CREATE TABLE IF NOT EXISTS {CATALOG}.{SCHEMA}.bench_results (
        tp_exec             STRING     COMMENT 'parallel ou serial',
        sql_warehouse_size  STRING     COMMENT 'tamanho do cluster do warehouse (dinâmico)',
        file_name           STRING     COMMENT 'nome do arquivo .sql executado',
        start_timestamp     TIMESTAMP  COMMENT 'início da execução (wall-clock)',
        end_timestamp       TIMESTAMP  COMMENT 'fim da execução (wall-clock)',
        duration            DOUBLE     COMMENT 'segundos entre start e end'
    )
""")
print(f"OK: {CATALOG}.{SCHEMA}.bench_results criada/existente.")

# COMMAND ----------

# MAGIC %md ## 5. SQL Warehouse Serverless `BenchDatabricks`

# COMMAND ----------

from databricks.sdk import WorkspaceClient
from databricks.sdk.service.sql import CreateWarehouseRequestWarehouseType

w = WorkspaceClient()

# Reutiliza se já existir um warehouse com esse nome
existing = [x for x in w.warehouses.list() if x.name == WAREHOUSE_NAME]

if existing:
    wh = w.warehouses.get(existing[0].id)
    print(f"Warehouse '{WAREHOUSE_NAME}' já existe (id={wh.id}, size={wh.cluster_size}). Reutilizando.")
else:
    wh = w.warehouses.create_and_wait(
        name=WAREHOUSE_NAME,
        cluster_size=WH_CLUSTER_SIZE,
        min_num_clusters=WH_MIN_CLUSTERS,
        max_num_clusters=WH_MAX_CLUSTERS,
        auto_stop_mins=WH_AUTO_STOP,
        enable_serverless_compute=True,
        warehouse_type=CreateWarehouseRequestWarehouseType.PRO,
    )
    print(f"Warehouse '{WAREHOUSE_NAME}' criado (id={wh.id}, size={wh.cluster_size}).")

print(f"\nDetalhes:")
print(f"  id.............: {wh.id}")
print(f"  cluster_size...: {wh.cluster_size}")
print(f"  min/max clusters: {wh.min_num_clusters}/{wh.max_num_clusters}")
print(f"  serverless.....: {wh.enable_serverless_compute}")
print(f"  estado.........: {wh.state}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Pronto ✅
# MAGIC Infra criada. Próximos passos (execução **manual**):
# MAGIC - `02_parallel_exec` — dispara as 103 queries simultaneamente
# MAGIC - `03_serial_exec` — executa da 1 à 99, uma após a outra
# MAGIC
# MAGIC Ao final, use `04_cleanup` para remover catálogo + warehouse.
