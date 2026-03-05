# Databricks notebook source
# MAGIC %run "/Workspace/Jurídico/funcao_tratamento_fechamento/common_functions"

# COMMAND ----------

# Create sample data for source_table
source_data = [
    ("jan", "Alice", 100),
    ("jan", "Bob", 200),
    ("jan", "Charlie", 300)
]

# Create sample data for target_table
target_data = [
    ("fev", "Alice", 50),
    ("fev", "David", 400)
]

target_data2 = [
    ("mar", "Alice", 50),
    ("mar", "David", 400)
]

# Define schema
schema = ["competencia", "name", "value"]
schema2 = ["competencia", "name", "%ótra"]


# Create DataFrames
source_df = spark.createDataFrame(source_data, schema)
target_df = spark.createDataFrame(target_data, schema)
target_df2 = spark.createDataFrame(target_data2, schema2)

# Create temporary views
source_df.createOrReplaceTempView("source_table")
target_df.createOrReplaceTempView("target_table")
target_df2.createOrReplaceTempView("target_table2")

# COMMAND ----------


def combine_tables(df_list):
    if not df_list:
        raise ValueError("The list of table names cannot be empty")

    # Read the first table to use as the reference schema
    df = df_list[0]
    reference_schema = df.schema
    
    def unify_schema(df, reference_schema):
        for field in reference_schema:
            if field.name not in df.columns:
                df = df.withColumn(field.name, lit(None).cast(field.dataType))
        return df

    # Iterate through the table names, unifying schemas and collecting DataFrames
    unified_dfs = [unify_schema(table, reference_schema) for table in df_list]
    
    # Ensure all DataFrames have the same columns from all tables
    all_columns = df.columns
    
    for unified_df in unified_dfs:
        for column in unified_df.columns:
            if column not in all_columns:
                all_columns.append(column)
    
    for i in range(len(unified_dfs)):
        for column in all_columns:
            if column not in unified_dfs[i].columns:
                unified_dfs[i] = unified_dfs[i].withColumn(column, lit(None))
        unified_dfs[i] = unified_dfs[i].select(*all_columns)
    
    # Union all the DataFrames
    combined_df = unified_dfs[0]
    for unified_df in unified_dfs[1:]:
        combined_df = combined_df.union(unified_df)
    
    return combined_df

# COMMAND ----------

df_list = [source_df, target_df, target_df]

combined_df = combine_tables(df_list)
combined_df.show()

# COMMAND ----------

# Inicia o set dos nomes únicos das tabelas
all_columns = set()

# Coleta informação dos schemas das tabelas
schemas = {}
for table in table_names:
    df = spark.table(f"databox.juridico_comum.{table}")
    schemas[table] = df.schema
    for field in df.schema.fields:
        all_columns.add(field.name.upper())

# Converte o set para uma lista ordenada
all_columns = sorted(list(all_columns))

# COMMAND ----------

# Gera SELECT statement com o shcema unificado
# Se a tabela da vez não tiver uma coluna do schema unificado então atribui NULL aos valores daquela coluna
select_statements = []
for table in table_names:
    columns = schemas[table].fieldNames()
    select_clause = ", ".join([f"`{col}`" if col in columns else f"'NULL' AS `{col}`" for col in all_columns])
    select_statements.append(f"SELECT {select_clause} FROM databox.juridico_comum.{table}")

# COMMAND ----------

# Generate the UNION ALL query
union_query = "\n UNION ALL \n".join(select_statements)

# Create the final query to create the new table
final_query = f"CREATE OR REPLACE TABLE databox.juridico_comum.{nome_tabela_unida} AS {union_query} \n"
