# Databricks notebook source
dbutils.widgets.text("safra", "")
dbutils.widgets.text("local", "")
dbutils.widgets.dropdown("estado", "SP", ["SP", "PR", "SC", "BR"])

# COMMAND ----------

safra_imputada = dbutils.widgets.get("safra")

local = dbutils.widgets.get("local")

estado = dbutils.widgets.get("estado")

# COMMAND ----------

print(safra_imputada)

# COMMAND ----------


