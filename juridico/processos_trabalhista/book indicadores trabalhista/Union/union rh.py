# Databricks notebook source
# MAGIC %md
# MAGIC # União de tabelas RH
# MAGIC Monta uma única tabela com todos arquivos do RH.  
# MAGIC Inclui todas as colunas de todas as tabelas e as que não tiverem alguma coluna que outra tenha, fica com o valor NULL.

# COMMAND ----------

dbutils.widgets.dropdown("run_type", "teste", ["teste", "fechamento", "previa"])

# COMMAND ----------

# MAGIC %run "/Workspace/Jurídico/funcao_tratamento_fechamento/common_functions"

# COMMAND ----------

# MAGIC %md
# MAGIC ## 1 - Elenca arquivos

# COMMAND ----------

rh_path = '/Volumes/databox/juridico_comum/arquivos/trabalhista/bases_rh/'

arquivos = dbutils.fs.ls(rh_path)

lista_arquivos = [arquivo.name for arquivo in arquivos]

# COMMAND ----------

lista_arquivos_filtrado = []
for s in lista_arquivos:
   if s.startswith('HC por Status'):
       lista_arquivos_filtrado.append(s)

lista_arquivos_filtrado

# COMMAND ----------

# MAGIC %md
# MAGIC ## 2 - Importa como DF e faz tratamento nas colunas

# COMMAND ----------

lista_dfs_rh = []
for arquivo in lista_arquivos_filtrado:
    df = read_excel(f"{rh_path}{arquivo}")
    df = adjust_column_names(df)
    df = remove_acentos(df)
    df = df.withColumn("SOURCE", lit(arquivo))

    lista_dfs_rh.append(df)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 3 - Define funções para criar DF unido

# COMMAND ----------

def get_all_schema(dfs: list[DataFrame]) -> StructType:
    """
    Pega a lista de dfs e retorna o schema em comum. Ou seja,
    todas as colunas presentes em todas as tabelas.
    """
    # Get the schemas of all DataFrames
    schemas = [df.schema.fields for df in dfs]
    
    # Flatten the list of fields and use a set to find unique fields
    all_fields = {field for schema in schemas for field in schema}
    
    # Create a StructType with all the unique fields
    all_schema = StructType(list(all_fields))
    
    return all_schema

def create_empty_df_with_all_schema(dfs: list[DataFrame]) -> DataFrame:
    """"
    Usa as colunas em comum para criar um DF vazio.
    """
    # Get the all-inclusive schema
    all_schema = get_all_schema(dfs)
    
    # Create an empty DataFrame with the all-inclusive schema
    empty_df = spark.createDataFrame([], all_schema)
    
    return empty_df

# COMMAND ----------

# Deduplica as colunas com mesmo nome
def deduplicate_columns(df: DataFrame) -> DataFrame:
    columns = df.columns
    renamed_columns = [f"{col}@{i}" for i, col in enumerate(columns)]
    
    # Rename columns to make them unique
    df_renamed = df.toDF(*renamed_columns)
    
    # Identify and keep the first occurrence of each column
    unique_columns = []
    seen = set()
    for i, col in enumerate(columns):
        if col not in seen:
            unique_columns.append(renamed_columns[i])
            seen.add(col)
    
    # Select only the unique columns
    df_unique = df_renamed.select(*unique_columns)
    
    # Optionally, rename columns back to original names, if needed
    original_names = [col.split('@')[0] for col in unique_columns]
    df_final = df_unique.toDF(*original_names)
    
    return df_final

# COMMAND ----------

# MAGIC %md
# MAGIC ## 4 - Cast string

# COMMAND ----------

# Cast string todas as colunas
lista_dfs_rh_string = []
for df in lista_dfs_rh:
    for col in df.columns:
        df = df.withColumn(col, df[col].cast(StringType()))
    
    lista_dfs_rh_string.append(df)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 5 - Une todos os DFs

# COMMAND ----------

# Cria um DF vazio com todas as colunas string
df_rh_string = create_empty_df_with_all_schema(lista_dfs_rh_string)

for df in lista_dfs_rh_string:
    df_rh_string = df_rh_string.unionByName(df, allowMissingColumns=True)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 6 - Recast os tipos originais

# COMMAND ----------

# Cria um DF com seus tipos originais
df_ref = create_empty_df_with_all_schema(lista_dfs_rh)

# Remove colunas duplicadas da referencia
df_ref = deduplicate_columns(df_ref)

# Usa esse schema como referencia para converter novamente os tipos do DF unido
schema_ref = df_ref.schema

# COMMAND ----------

for field in schema_ref.fields:
    df_rh_string = df_rh_string.withColumn(field.name, df_rh_string[field.name].cast(field.dataType))

unified_df = df_rh_string

# COMMAND ----------

# dbutils.notebook.exit("Success")
