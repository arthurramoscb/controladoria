# Databricks notebook source
# MAGIC %md
# MAGIC # Definição dos arquivos a serem utilizados no 06 - Base de Desligados RH
# MAGIC
# MAGIC Tabela será utilizada no [notebook 06](https://adb-4617509456321466.6.azuredatabricks.net/?o=4617509456321466#notebook/2415386455454703) para definição dos arquivos e filtros a serem utilizados.
# MAGIC
# MAGIC **Importante**: Manter o padrão de formato dos meses anteriores.

# COMMAND ----------

from pyspark.sql.functions import col, to_date, last_day

# COMMAND ----------

# Add MES_FECH
relacao_arquivos = [
("20230308","2000-01-01","2022-12-31",),
("20230202","2023-01-01","2023-01-31",),
("20230306","2023-02-01","2023-02-28",),
("20230404","2023-03-01","2023-03-31",),
("20230503","2023-04-01","2023-04-30",),
("20230613","2023-05-01","2023-05-31",),
("20230704","2023-06-01","2023-06-30",),
("20230802","2023-07-01","2023-07-31",),
("20230901","2023-08-01","2023-08-31",),
("20231003","2023-09-01","2023-09-30",),
("20231101","2023-10-01","2023-10-31",),
("20231205","2023-11-01","2023-11-30",),
("20240103","2023-12-01","2023-12-31",),
("20240207","2024-01-01","2024-01-31",),
("20240311","2024-02-01","2024-02-29",),
("20240401","2024-03-01","2024-03-31",),
("20240514","2024-04-01","2024-04-30",),
("20240530","2024-05-01","2024-05-31",),
("20240630","2024-06-01","2024-06-30",),
("20240730","2024-07-01","2024-07-31",),
("20240831","2024-08-01","2024-08-31",),
("20240930","2024-09-01","2024-09-30",),
("20241031","2024-10-01","2024-10-31",)

    # ("", "2024-04-01")
]

schema = ["arquivo","dt_inicio","dt_fim"]

df_arquivos = spark.createDataFrame(relacao_arquivos, schema)

df_arquivos = df_arquivos.withColumn("dt_inicio", to_date(col("dt_inicio"), 'yyyy-MM-dd'))
df_arquivos = df_arquivos.withColumn("dt_fim", to_date(col("dt_fim"), 'yyyy-MM-dd'))
df_arquivos.show()

# COMMAND ----------

# df_arquivos = df_arquivos.withColumn('dt_fim', last_day('dt_inicio'))

# COMMAND ----------

display(df_arquivos)
