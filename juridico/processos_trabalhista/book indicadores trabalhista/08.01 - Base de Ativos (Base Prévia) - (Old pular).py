# Databricks notebook source
# MAGIC %md
# MAGIC # Base Ativos
# MAGIC
# MAGIC Agregação  da tabela do RH com as informações de prévias do mês.
# MAGIC O código deverá ser processado apenas na geração dos resultados de prévia
# MAGIC A base final deve ser apendada com a Base de Ativos com os dados de Head Count do RH

# COMMAND ----------

# MAGIC %run "/Workspace/Jurídico/funcao_tratamento_fechamento/common_functions"

# COMMAND ----------

from pyspark.sql.functions import regexp_extract, col

# COMMAND ----------

# MAGIC %md
# MAGIC ## 1 - Imports

# COMMAND ----------

# MAGIC %md
# MAGIC ### 1.1 Elenca arquivos

# COMMAND ----------

rh_path = '/Volumes/databox/juridico_comum/arquivos/trabalhista/bases_rh/previa/'

arquivos = dbutils.fs.ls(rh_path)

lista_arquivos = [arquivo.name for arquivo in arquivos]

# COMMAND ----------

lista_arquivos_filtrado = []
for s in lista_arquivos:
    if s.startswith('Previa'):
       lista_arquivos_filtrado.append(s)
    if s.startswith('PREVIA'):
       lista_arquivos_filtrado.append(s)


lista_arquivos_filtrado

# COMMAND ----------

# MAGIC %md
# MAGIC ### 1.2 Importa como DF e faz tratamento nas colunas

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
# MAGIC ### 1.3 Define funções para criar DF unido

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
# MAGIC ### 1.4 Cast string

# COMMAND ----------

# Cast string todas as colunas
lista_dfs_rh_string = []
for df in lista_dfs_rh:
    for col in df.columns:
        df = df.withColumn(col, df[col].cast(StringType()))
    
    lista_dfs_rh_string.append(df)

# COMMAND ----------

# MAGIC %md
# MAGIC ### 1.5 Une todos os DFs

# COMMAND ----------

# Cria um DF vazio com todas as colunas string
df_rh_string = create_empty_df_with_all_schema(lista_dfs_rh_string)

for df in lista_dfs_rh_string:
    df_rh_string = df_rh_string.unionByName(df, allowMissingColumns=True)

# COMMAND ----------

# MAGIC %md
# MAGIC ### 1.6 Recast os tipos originais

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

# COMMAND ----------

df_desligados_rh = df_rh_string

# COMMAND ----------

df_desligados_rh.count()

# COMMAND ----------

df_desligados_rh.printSchema()

# COMMAND ----------

# IMPORTA A BASE COM O DE PARA DO BU
path_bu = '/Volumes/databox/juridico_comum/arquivos/trabalhista/bases_auxiliares/De para Centro de Custo e BU_v2 - ago-2022.xlsx'
df_bu = read_excel(path_bu)
df_bu.createOrReplaceTempView("TB_DE_PARA_BU")


# IMPORTA A BASE COM O DE PARA DA ÁREA FUNCIONAL
path_funcional = '/Volumes/databox/juridico_comum/arquivos/trabalhista/bases_auxiliares/Centro de Custo e Área Funcional_OKENN_280623.xlsx'
df_funcional = read_excel(path_funcional, "'OKENN'!A1")
df_funcional.createOrReplaceTempView("TB_DE_PARA_AREA_FUNCIONAL")

# COMMAND ----------

df_desligados_rh.display()

# COMMAND ----------

# MAGIC %md
# MAGIC ## 2 - Transformações

# COMMAND ----------

# MAGIC %md
# MAGIC ### 2.1 Cria MES_REF baseado no nome do arquivo

# COMMAND ----------

from pyspark.sql.functions import *

# Define a regex
pattern = r"\_?(\d{8})\.xlsx"

# Extrai a data usando regexp_extract
df_desligados_rh =  df_desligados_rh.withColumn("DATA_ARQUIVO", regexp_extract(col("SOURCE"), pattern, 1))

# Converte o formato de data
df_desligados_rh = df_desligados_rh.withColumn("DATA_ARQUIVO", to_date(col("DATA_ARQUIVO"), "yyyyMMdd"))

# Cria MES_REF com dia 1 de cada mes da data do arquivo
df_desligados_rh = df_desligados_rh.withColumn("MES_REF", date_format(col("DATA_ARQUIVO"), "yyyy-MM-01"))

# df_desligados_rh.select("CPF", "SOURCE","DATA_ARQUIVO", "MES_REF").display()
df_desligados_rh.createOrReplaceTempView("ATIVOS_RH_HIST")


# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM ATIVOS_RH_HIST

# COMMAND ----------

# MAGIC %md
# MAGIC ### 2.2 Faz o agrupamento

# COMMAND ----------

df_ativos_previa = spark.sql("""
	SELECT COD_CENTRO_CUSTO_FORM AS CODCCUSTOCONTABIL --AS CodCCustoContabil
			,MES_REF
			,COD_EMPRESA AS CODEMPRESA
			,NIVEL1 AS DESCRICAON1
			,NIVEL2 AS DESCRICAON2
			,NIVEL3 AS DESCRICAON3
			,NIVEL4 AS DESCRICAON4
			,NIVEL5 AS DESCRICAON5
			,UPPER(STATUS_COLABORADOR) AS STATUS
			,COUNT(MATRICULA) AS QTD
	FROM ATIVOS_RH_HIST
	WHERE UPPER(STATUS_COLABORADOR) IN ('ATIVO','AFASTADO')
	GROUP BY ALL;
 """)

df_ativos_previa.createOrReplaceTempView("ATIVOS_RH_HIST2")

# COMMAND ----------

display(df_ativos_previa)

# COMMAND ----------

# MAGIC %md
# MAGIC ### 2.3 Da joing com BU e Área Funcional

# COMMAND ----------

df_ativos1 = spark.sql("""
SELECT A.*

    ,(CASE WHEN A.DESCRICAON1 = 'INDÚSTRIA DE MÓVEIS BARTIRA LTDA'
        THEN 'BARTIRA' ELSE B.BU END) AS BU

    ,(CASE WHEN A.DESCRICAON1 = 'INDÚSTRIA DE MÓVEIS BARTIRA LTDA' THEN 'BARTIRA'
        ELSE C.nome2 END ) AS VP

    ,(CASE WHEN A.DESCRICAON1 = 'INDÚSTRIA DE MÓVEIS BARTIRA LTDA' THEN 'BARTIRA'
        ELSE C.nome3 END ) AS DIRETORIA

    ,(CASE WHEN A.DESCRICAON1 = 'INDÚSTRIA DE MÓVEIS BARTIRA LTDA' THEN 'BARTIRA'
        ELSE UPPER(C.`nome responsavel`) END ) AS RESPONSAVEL_AREA

    ,(CASE WHEN A.DESCRICAON1 = 'INDÚSTRIA DE MÓVEIS BARTIRA LTDA' THEN 'BART'
        ELSE C.`area funcional` END ) AS AREA_FUNCIONAL

FROM ATIVOS_RH_HIST2 A 
LEFT JOIN TB_DE_PARA_BU B ON A.CODCCUSTOCONTABIL = B.`Centro de custo`
LEFT JOIN TB_DE_PARA_AREA_FUNCIONAL C ON A.CODCCUSTOCONTABIL = C.centro
"""
)

df_ativos1.createOrReplaceTempView("ATIVOS_RH_HIST3")

# COMMAND ----------

# MAGIC %md
# MAGIC ### 2.4 De paras Área Funcional

# COMMAND ----------

# FAZ O DE PARA DAS ÁREAS FUNCIONAIS
df_ativos2 = spark.sql(
  """
  SELECT *,
  CASE 
        WHEN VP = 'ADMINISTRATIVO E LOGISTICA' AND DIRETORIA IN ('ASAP LOG', 'DIR EXEC LOGISTICA E SUPPLY', 'LOGISTICA ENVVIAS RETIRA') THEN 'LOGISTICA'
        WHEN VP = 'ADMINISTRATIVO E LOGISTICA' AND DIRETORIA NOT IN ('ASAP LOG', 'DIR EXEC LOGISTICA E SUPPLY', 'LOGISTICA ENVVIAS RETIRA') THEN 'ADMINISTRATIVO'
        WHEN VP = 'VIAHUB E LOGISTICA' AND DIRETORIA IN ('CNT', 'DIR EXEC LOGISTICA E SUPPLY', 'LOGISTICA ENVVIAS RETIRA', 'ASAP LOG') THEN 'LOGISTICA'
        WHEN VP = 'VIAHUB E LOGISTICA' AND DIRETORIA NOT IN ('CNT', 'DIR EXEC LOGISTICA E SUPPLY', 'LOGISTICA ENVVIAS RETIRA', 'ASAP LOG') THEN 'VIAHUB'
        WHEN VP = 'ADMINISTRATIVO' THEN 'ADMINISTRATIVO'
        WHEN VP = 'COMERCIAL E VENDAS' THEN 'COMERCIAL E VENDAS'
        WHEN VP = 'CFO' THEN 'CFO'
        WHEN VP = 'LOGISTICA' THEN 'LOGISTICA'
        WHEN VP = 'VIAHUB' THEN 'VIAHUB'
        WHEN VP = '' THEN 'OUTROS'
        ELSE VP
  END AS DP_VP_DIRETORIA
  FROM ATIVOS_RH_HIST3
  """
)
df_ativos2.createOrReplaceTempView("ATIVOS_RH_HIST_F")

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT *
# MAGIC FROM ATIVOS_RH_HIST_F
# MAGIC -- GROUP BY STATUS
# MAGIC
# MAGIC

# COMMAND ----------

df_ativos2.count() #67475

# COMMAND ----------

# MAGIC %md
# MAGIC ## 3 - Output

# COMMAND ----------

# MAGIC %md
# MAGIC Exportar aqui

# COMMAND ----------

import pandas as pd
from shutil import copyfile

# Convert PySpark DataFrame to Pandas DataFrame
pandas_df = df_ativos2.toPandas()

# Save the Pandas DataFrame to an Excel file
local_path = f'/local_disk0/tmp/TB_ATIVOS_PREVIA_FINAL.xlsx'
pandas_df.to_excel(local_path, index=False, sheet_name='ATIVOS_RH_F', engine='xlsxwriter')

# Copy the file from the local disk to the desired volume
volume_path = f'/Volumes/databox/juridico_comum/arquivos/trabalhista/bases_indicadores_resultado/ATIVOS_PREVIA_FINAL.xlsx'

copyfile(local_path, volume_path)
