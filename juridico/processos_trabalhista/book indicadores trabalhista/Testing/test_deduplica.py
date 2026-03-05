# Databricks notebook source
# MAGIC %md
# MAGIC ### 1.1 Teste deduplicação de colunas

# COMMAND ----------

# MAGIC %run "/Workspace/Jurídico/funcao_tratamento_fechamento/common_functions"

# COMMAND ----------

# Teste deduplicação de colunas
# Example DataFrame with duplicate column names
data = [(1, 'A', 100), (2, 'B', 200)]
columns = ['id', 'name', 'id']  # Duplicate 'id' column

df = spark.createDataFrame(data, columns)

# Show initial DataFrame
print("Initial DataFrame with duplicate columns:")
df.show()

# Deduplicate columns
df_deduplicated = deduplicate_columns(df)

# Show DataFrame after deduplicating columns
print("DataFrame after deduplicating columns:")
df_deduplicated.show()
