# Databricks notebook source
# MAGIC %md
# MAGIC ### Cenário

# COMMAND ----------

from pyspark.sql.functions import col, coalesce

# Create sample data for new_table
new_data = [
    (1, "Alice", "202403", 150, 1), # Atualização de valor
    (2, "Bob", "202404", 200, 1) # Não atualiza nada, tá igual na source
]

# Create sample data for source_table
source_data = [
    (1, "Alice", "202403", 100, 1),
    (1, "Alice", "202404", 100, 1),
    (2, "Bob", "202404", 200, 1),
    (3, "Charl", "202404", 300, 0)
]

# Define schema
schema = ["id", "nome", "mes", "valor", "encerrado"]

# Create DataFrames
new_df = spark.createDataFrame(new_data, schema)
source_df = spark.createDataFrame(source_data, schema)

new_df.createOrReplaceTempView('new')
source_df.createOrReplaceTempView('source')

new_df.show()
source_df.show()

# COMMAND ----------

# MAGIC %md
# MAGIC ### Função

# COMMAND ----------

def merge_dfs(source, new, join_cols):
    """
    Realiza uma junção entre dois DataFrames do Spark e aplica coalesce em todas as colunas, 
    exceto nas colunas de junção.

    Parâmetros:
    source (DataFrame): DataFrame de origem.
    novo (DataFrame): Novo DataFrame para a junção.
    colunas_uniao (List[str]): Lista de nomes das colunas pelas quais os DataFrames serão unidos.

    Retorna:
    DataFrame: Resultado da junção e aplicação do coalesce.
    """
    # Generate the join condition dynamically
    join_condition = [source[col_name] == new[col_name] for col_name in join_cols]
    
    # Perform the left join
    joined_df = source.alias("a").join(new.alias("b"), on=join_condition, how='left')
    
    # Apply coalesce to all columns except the joining columns
    coalesce_exprs = [coalesce(col("b." + col_name), col("a." + col_name)).alias(col_name) for col_name in source.columns if col_name not in join_cols]
    
    # Select the required columns
    result_df = joined_df.select(*[col("a." + col_name) for col_name in join_cols], *coalesce_exprs)
    
    return result_df

# COMMAND ----------

# MAGIC %md
# MAGIC ### Aplicação

# COMMAND ----------

join_cols = ['id', 'mes']
result_df3 = merge_dfs(source_df,new_df,['id', 'mes'])
result_df3.show()

# COMMAND ----------

# MAGIC %md
# MAGIC ### 2 Teste manual

# COMMAND ----------

result_df = spark.sql(f"""
SELECT 
a.id,
a.mes,
coalesce(b.nome, a.nome) as nome,
coalesce(b.valor, a.valor) as valor,
coalesce(b.encerrado, a.encerrado) as encerrado
FROM source a
LEFT JOIN new b ON a.id = b.id AND a.mes = b.mes
"""
)
result_df.display()

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT 
# MAGIC a.*,
# MAGIC b.*
# MAGIC FROM source a
# MAGIC LEFT JOIN new b ON a.id = b.id AND a.mes = b.mes

# COMMAND ----------

# MAGIC %md
# MAGIC ### 3 Automatiza

# COMMAND ----------

def generate_coalesce_sql(df, exclude_columns):
    coalesce_sql = [
        f"coalesce(b.{column}, a.{column}) as {column} \n"
        for column in df.columns if column not in exclude_columns
    ]
    return ", ".join(coalesce_sql)


# COMMAND ----------

coalesce_str = generate_coalesce_sql(source_df, ['id', 'mes'])
print(coalesce_str)

# COMMAND ----------

result_df2 = spark.sql(f"""
SELECT 
a.id,
a.mes,
{coalesce_str}
FROM source a
LEFT JOIN new b ON a.id = b.id AND a.mes = b.mes
"""
)
result_df2.display()

# COMMAND ----------

# MAGIC %md
# MAGIC ## Em SQL

# COMMAND ----------

new_df.write.format("delta").mode("overwrite").saveAsTable(f"databox.juridico_comum.tmp_source")
source_df.write.format("delta").mode("overwrite").saveAsTable(f"databox.juridico_comum.tmp_target")

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from databox.juridico_comum.tmp_target

# COMMAND ----------

# MAGIC %sql
# MAGIC MERGE INTO databox.juridico_comum.tmp_target target USING databox.juridico_comum.tmp_source source
# MAGIC   ON target.id = source.id and target.mes = source.mes
# MAGIC   WHEN MATCHED THEN UPDATE SET target.valor = source.valor, target.encerrado = source.encerrado
# MAGIC

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from databox.juridico_comum.tmp_target 

# COMMAND ----------

# MAGIC %sql
# MAGIC DROP TABLE databox.juridico_comum.tmp_source

# COMMAND ----------

# MAGIC %sql
# MAGIC DROP TABLE databox.juridico_comum.tmp_target
