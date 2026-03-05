# Databricks notebook source
from pyspark.sql.functions import dense_rank
from pyspark.sql.window import Window

# Sample data
data = [
    ('12345678900', 1, '2023-01'),
    ('12345678900', 1, '2023-01'),
    ('12345678900', 2, '2023-02'),
    ('98765432100', 1, '2023-01'),
    ('98765432100', 2, '2023-03'),
    ('98765432100', 2, '2023-03')
]

# Create DataFrame
columns = ['CPF', 'ID_PROCESSO', 'MES_DESLIGAMENTO']
df = spark.createDataFrame(data, columns)
df.show()

# Define window specification
windowSpec = Window.partitionBy('CPF', 'ID_PROCESSO', 'MES_DESLIGAMENTO').orderBy('CPF', 'ID_PROCESSO', 'MES_DESLIGAMENTO')

# Apply dense_rank
df_with_dense_rank = df.withColumn('COUNT', dense_rank().over(windowSpec))

# Show the result
df_with_dense_rank.show()

# COMMAND ----------

# ORIGINAL
data = [
    (111,"1", "2020-12-01"),
    (111,"2", "2021-01-01"),
    (111,"3", "2021-02-01"),
    (222,"1", "2021-02-01"),
    (222,"2", "2021-03-01")
]

columns = ["id", "parcela", "date"]

df = spark.createDataFrame(data, columns)
df.show()




# COMMAND ----------

# ORIGINAL
data = [
    (111,"1", "2020-12-01", "2020-12-01"),
    (111,"2", "2021-01-01", "2020-12-01"),
    (111,"3", "2021-02-01", "2020-12-01"),
    (222,"1", "2021-02-01", "2021-02-01"),
    (222,"2", "2021-03-01", "2021-02-01")
]

columns = ["id", "parcela", "date", "first_date"]

df = spark.createDataFrame(data, columns)
df.show()


# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT a,
# MAGIC          b,
# MAGIC          dense_rank() OVER(PARTITION BY a ORDER BY b) dense_rank,
# MAGIC          rank() OVER(PARTITION BY a ORDER BY b) rank,
# MAGIC          row_number() OVER(PARTITION BY a ORDER BY b) row_number
# MAGIC     FROM VALUES ('A1', 2), ('A1', 1), ('A2', 3), ('A1', 1) tab(a, b);
