# Databricks notebook source
# MAGIC %md
# MAGIC # 04_cleanup — Remoção de todos os recursos
# MAGIC
# MAGIC **⚠️ Destrutivo.** Apaga o catálogo `tpds1tb` (schema + managed tables +
# MAGIC dados) e deleta o SQL Warehouse `BenchDatabricks`.
# MAGIC
# MAGIC Este notebook **NÃO** deve fazer parte de nenhum job — execução manual.

# COMMAND ----------

CATALOG        = "tpds1tb"
WAREHOUSE_NAME = "BenchDatabricks"

# Trava de segurança: mude para True para confirmar a exclusão
CONFIRMAR = False

# COMMAND ----------

assert CONFIRMAR, "Defina CONFIRMAR = True na célula acima para executar a limpeza."

# COMMAND ----------

# MAGIC %md ## 1. Drop do catálogo (cascade)

# COMMAND ----------

spark.sql(f"DROP CATALOG IF EXISTS {CATALOG} CASCADE")
print(f"Catálogo '{CATALOG}' removido (schema, tabelas e dados).")

# COMMAND ----------

# MAGIC %md ## 2. Delete do SQL Warehouse

# COMMAND ----------

from databricks.sdk import WorkspaceClient
w = WorkspaceClient()

matches = [x for x in w.warehouses.list() if x.name == WAREHOUSE_NAME]
if not matches:
    print(f"Nenhum warehouse '{WAREHOUSE_NAME}' encontrado (já removido?).")
for x in matches:
    w.warehouses.delete(x.id)
    print(f"Warehouse '{WAREHOUSE_NAME}' (id={x.id}) removido.")

# COMMAND ----------

# MAGIC %md ## Limpeza concluída ✅
