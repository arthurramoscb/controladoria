# Databricks notebook source
# MAGIC %md
# MAGIC # Gerencial Cosolidada

# COMMAND ----------

# MAGIC %run "/Workspace/Jurídico/funcao_tratamento_fechamento/common_functions"

# COMMAND ----------

path = '/Volumes/databox/juridico_comum/arquivos/trabalhista/bases_gerenciais/processada/'

arquivos = dbutils.fs.ls(path)

lista_arquivos = [arquivo.name for arquivo in arquivos]
lista_arquivos

# COMMAND ----------

# import re
#
# for arquivo in lista_arquivos:
#     pattern = r"(\d{6})\.xlsx$"
#     nmmes = re.search(pattern, arquivo).group(1)
#     print(f"DROP TABLE databox.juridico_comum.tb_fecham_financ_trab_{nmmes}")
#     spark.sql(f"DROP TABLE databox.juridico_comum.tb_fecham_financ_trab_{nmmes}")

# COMMAND ----------

lista_dfs = []

for arquivo in lista_arquivos:
    # Carrega
    df = read_excel(f"{path}{arquivo}")
    print(f"{arquivo} carregado.")

    # Trata os nomes de coluna
    df = adjust_column_names(df)
    df = remove_acentos(df)
    df = deduplica_cols(df)
    df = df.withColumn("SOURCE", lit(arquivo))

    # Grava a tabela
    nome_tabela = arquivo.lower()[:-5]
    df.write.format("delta").mode("overwrite").saveAsTable(f"databox.juridico_comum.{nome_tabela}")
    print(f"{nome_tabela} gravada.")

# COMMAND ----------

for arquivo in lista_arquivos:
    print(arquivo.lower()[:-5])

# COMMAND ----------

names = ['TRAB_GER_CONSOLIDA_20230322_1',
 'TRAB_GER_CONSOLIDA_20230525_1',
 'TRAB_GER_CONSOLIDA_20230626_1',
 'TRAB_GER_CONSOLIDA_20230724_1',
 'TRAB_GER_CONSOLIDA_20230823_1',
 'TRAB_GER_CONSOLIDA_20230925_1',
 'TRAB_GER_CONSOLIDA_20231023_1',
 'TRAB_GER_CONSOLIDA_20231123_1',
 'TRAB_GER_CONSOLIDA_20231218_1',
 'TRAB_GER_CONSOLIDA_20240123_1',
 'TRAB_GER_CONSOLIDA_20240226_1',
 'TRAB_GER_CONSOLIDA_20240325_1',
 'TRAB_GER_CONSOLIDA_20240423_1']

names_lower = []

for n in names:
    names_lower.append(n.lower())

# COMMAND ----------

for table_name in names_lower:
    print(table_name[:-2])

# COMMAND ----------

for table_name in names_lower:
    spark.sql(f"ALTER TABLE databox.juridico_comum.{table_name} RENAME TO databox.juridico_comum.{table_name[:-2]}")


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
lista_dfs_string = []
for df in lista_dfs:
    for col in df.columns:
        df = df.withColumn(col, df[col].cast(StringType()))
    
    lista_dfs_string.append(df)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 5 - Une todos os DFs

# COMMAND ----------

# Cria um DF vazio com todas as colunas string
df_string = create_empty_df_with_all_schema(lista_dfs_string)

for df in lista_dfs_string:
    df_string = df_string.unionByName(df, allowMissingColumns=True)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 6 - Recast os tipos originais

# COMMAND ----------

# Cria um DF com seus tipos originais
df_ref = create_empty_df_with_all_schema(lista_dfs)

# Remove colunas duplicadas da referencia
df_ref = deduplicate_columns(df_ref)

# Usa esse schema como referencia para converter novamente os tipos do DF unido
schema_ref = df_ref.schema

# COMMAND ----------

for field in schema_ref.fields:
    df_string = df_string.withColumn(field.name, df_string[field.name].cast(field.dataType))

unified_df = df_string

# COMMAND ----------

# unified_df.count()

# COMMAND ----------

# MAGIC %md
# MAGIC ## 7 - Importar 1 arquivo para delta

# COMMAND ----------

path = '/Volumes/databox/juridico_comum/arquivos/trabalhista/bases_gerenciais/processada/'
arquivo = 'TRAB_GER_CONSOLIDA_20230503_1.xlsx'

# Carrega
df = read_excel(f"{path}{arquivo}")
print(f"{arquivo} carregado.")

# Trata os nomes de coluna
df = adjust_column_names(df)
df = remove_acentos(df)
df = deduplica_cols(df)
df = df.withColumn("SOURCE", lit(arquivo))

# Grava a tabela
nome_tabela = arquivo.lower()[:-5]
print(nome_tabela)


# COMMAND ----------

df.write.format("delta").mode("overwrite").saveAsTable(f"databox.juridico_comum.{nome_tabela[:-2]}")
print(f"{nome_tabela} gravada.")

# COMMAND ----------

# MAGIC %sql
# MAGIC DROP TABLE databox.juridico_comum.trab_ger_consolida_20230503_1
