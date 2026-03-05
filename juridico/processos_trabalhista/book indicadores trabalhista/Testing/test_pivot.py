# Databricks notebook source
# Sample data
data = [
    ('12345678900', 1, 1000),
    ('12345678900', 2, 1000),
    ('12345678900', 3, 1000),
    ('98765432100', 1, 1500),
    ('98765432100', 2, 2000),
    ('98765432100', 3, 3000)
]

# Create DataFrame
columns = ['ID_PROCESSO', 'PARCELA_PAGA', 'VALOR']
df = spark.createDataFrame(data, columns)
df.show()

# COMMAND ----------

pivoted_df = df.groupBy('ID_PROCESSO') \
    .pivot('PARCELA_PAGA') \
    .sum("VALOR")

pivoted_df.display()

# COMMAND ----------


