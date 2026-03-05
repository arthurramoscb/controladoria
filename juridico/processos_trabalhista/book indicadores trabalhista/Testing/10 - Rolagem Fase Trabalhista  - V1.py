# Databricks notebook source
# MAGIC %md
# MAGIC # Tratamento das Bases Gerenciais
# MAGIC
# MAGIC **Inputs**\
# MAGIC Tabela Fechamento Financeiro
# MAGIC
# MAGIC **Outputs**\
# MAGIC Relatório Rolagem

# COMMAND ----------

from pyspark.sql.types import DoubleType, DateType, StringType

# COMMAND ----------

# Configura campo para que o usuário insira parâmetros

# Parametro do formato de data do arquivo. Ex: 202404
dbutils.widgets.text("nmmes", "")

# COMMAND ----------

# MAGIC %run "/Workspace/Jurídico/funcao_tratamento_fechamento/common_functions"

# COMMAND ----------

# MAGIC %md
# MAGIC ## 1 - Importação

# COMMAND ----------

# MAGIC %md
# MAGIC ### 1.1 Tabelas fechamento financeiro trabalhista [LEGADO]

# COMMAND ----------

# nmmes = dbutils.widgets.get("nmmes")

# df_fech_financ_trab = spark.sql(f"""
# SELECT ID_PROCESSO
# 			,MES_FECH::DATE
#             ,EMPRESA
# 			,ESCRITORIO_RESPONSAVEL
# 			,PROCESSO_ESTEIRA
# 			,CLASSIFICACAO
# 			,MOTIVO_ENC_AGRP
# 			,ESTADO
# 			,PARTE_CONTRARIA_CARGO_GRUPO
# 			,MESES_AGING_ENCERR
# 			--,FX_MES_AGING_ENCERR
# 			,ANO_AGING_ENCERR
# 			,FX_ANO_AGING_ENCERR
# 			,FASE_ATUAL
# 			,FASE
# 			,NOVO_X_LEGADO
#             ,DP_FASEo
#             	WHEN ESTOQUE = 1 THEN FX_ANO_AGING_ENCERR
# 				ELSE '' END AS FX_ANO_AGING_NOVO
#     		,SUM(NOVOS) AS NOVOS
# 			,SUM(ENCERRADOS) AS ENCERRADOS
# 			,SUM(ESTOQUE) AS ESTOQUE
# 			,SUM(ACORDO) AS ACORDO
# 			,SUM(CONDENACAO) AS CONDENACAO
# 			,SUM(TOTAL_PAGAMENTOS) AS TOTAL_PAGAMENTOS
# 			,SUM(PROVISAO_TOTAL_M_1) AS PROVISAO_TOTAL_M_1
# 			,SUM(PROVISAO_TOTAL_PASSIVO_M) AS PROVISAO_TOTAL_PASSIVO_M
# 			,SUM(PROVISAO_MOV_M) AS PROVISAO_MOV_M
			
# 		FROM databox.juridico_comum.tb_fecham_financ_trab_{nmmes}
# 		WHERE MES_FECH >= '2021-01-01'
#     GROUP BY ALL
#     """
#     )

# df_fech_financ_trab.createOrReplaceTempView("TB_FECHAM_FINANC_TRAB_FF")
# df_fech_financ_trab.createOrReplaceTempView("TB_FECHAM_FINANC_TRAB_2021")

# COMMAND ----------

# df_fech_trab = spark.sql("""
# SELECT  ID_PROCESSO --###### NÃO ESTÁ PRESENTE NO SAS - VERIFICAR
#     ,MES_FECH
#     ,EMPRESA
#     ,ESCRITORIO_RESPONSAVEL
#     ,PROCESSO_ESTEIRA
#     ,CLASSIFICACAO
#     ,MOTIVO_ENC_AGRP
#     ,ESTADO
#     ,PARTE_CONTRARIA_CARGO_GRUPO
#     ,MESES_AGING_ENCERR
#     -- ,FX_MES_AGING_ENCERR
#     ,ANO_AGING_ENCERR
#     ,FX_ANO_AGING_ENCERR
#     ,FASE
#     ,DP_FASE
#     ,FASE_ATUAL
#     ,FX_ANO_AGING_NOVO
#     ,NOVO_X_LEGADO
#     ,SUM(NOVOS) AS NOVOS
#     ,SUM(ENCERRADOS) AS ENCERRADOS
#     ,SUM(ESTOQUE) AS ESTOQUE
#     ,SUM(ACORDO) AS ACORDO
#     ,SUM(CONDENACAO) AS CONDENACAO
#     ,SUM(TOTAL_PAGAMENTOS) AS TOTAL_PAGAMENTOS
#     ,SUM(PROVISAO_TOTAL_M_1) AS PROVISAO_TOTAL_M_1
#     ,SUM(PROVISAO_TOTAL_PASSIVO_M) AS PROVISAO_TOTAL_PASSIVO_M
#     ,SUM(PROVISAO_MOV_M) AS PROVISAO_MOV_M
#   FROM TB_FECHAM_FINANC_TRAB_2021
#   GROUP BY ALL
# """
# )

# df_fech_trab.createOrReplaceTempView("TB_FECHAMENTO_TRAB_F")

# COMMAND ----------

# MAGIC %md
# MAGIC ### 1.2 Tabela fechamento financeiro trabalhista

# COMMAND ----------


nmmes = dbutils.widgets.get("nmmes")

df_ff_trab = spark.sql(f"SELECT * FROM databox.juridico_comum.tb_fecham_financ_trab_{nmmes}")

df_ff_trab.createOrReplaceTempView("TB_FECHAM_FINANC_TRAB")

# COMMAND ----------

df_encer_202012 = spark.sql("""
SELECT ID_PROCESSO,
  MES_FECH,
  ENCERRADOS,
  NOVO_X_LEGADO
FROM TB_FECHAM_FINANC_TRAB
WHERE
  MES_FECH >= '2020-12-01'
  AND ENCERRADOS = 1
ORDER BY ID_PROCESSO, MES_FECH
"""
)

df_encer_202012 = df_encer_202012.dropDuplicates(["ID_PROCESSO"])
df_encer_202012.createOrReplaceTempView("TB_ENCER_A_PARTIR_202012")

# COMMAND ----------

### PREPARA A BASE INICIAL PARA IDENTIFICAR A ROLAGEM
df_fase_202012 = spark.sql(f"""
SELECT ID_PROCESSO,
  MES_FECH::DATE,
  ENCERRADOS,
  DP_FASE,
  NOVO_X_LEGADO
  FROM databox.juridico_comum.tb_fecham_financ_trab_{nmmes}
WHERE
  MES_FECH >= '2020-12-01'
  AND (ENCERRADOS IS NULL OR ENCERRADOS <> 1)
ORDER BY ID_PROCESSO, MES_FECH
"""
)

df_fase_202012 = df_fase_202012.dropDuplicates(["ID_PROCESSO"])
df_fase_202012.createOrReplaceTempView("TB_FASE_A_PARTIR_202012_1")

# COMMAND ----------

# df_fase_202012.count()

# COMMAND ----------

# MAGIC %md
# MAGIC ## 2 - União gerencial trab

# COMMAND ----------

# MAGIC %md
# MAGIC ### 2.1 Lista as tabelas

# COMMAND ----------

nome_schema = 'databox.juridico_comum'
nome_tabelas = "'trab_ger_consolida_%'"

# Lista todas as tabelas no catálogo com o nome buscado
tables = spark.sql(f"SHOW TABLES IN {nome_schema}").collect()
tables = spark.createDataFrame(tables)

tables = tables.where(f"tableName LIKE {nome_tabelas} AND isTemporary=False")

table_names = [row.tableName for row in tables.select('tableName').collect()]

table_names.sort(reverse = True)

print(table_names)

# COMMAND ----------

# MAGIC %md
# MAGIC ### 2.2 Relação nome do arquivo e MES_FECH

# COMMAND ----------

# MAGIC %run "./mes_fech"

# COMMAND ----------

# MAGIC %md
# MAGIC ### 2.3 Loop para selecionar as colunas das trab_ger_consolidado's

# COMMAND ----------

schema = StructType([
    StructField("ID_PROCESSO", DoubleType(), True),
    StructField("MES_FECH", DateType(), True),
    StructField("FASE", StringType(), True),
    StructField("DP_FASE", StringType(), True)
])


df_prepara_tb = spark.createDataFrame(data = [], schema = schema)

for nmtabela in table_names:
  df = spark.sql(f"""
  WITH cte AS (
  SELECT PROCESSO_ID AS ID_PROCESSO
      ,{nmtabela[-8:]}::STRING AS arquivo
      ,FASE
      ,(CASE WHEN TRIM(FASE) IN ('', 'N/A', 'INATIVO', 'ENCERRAMENTO', 'ADMINISTRATIVO', 'DEMAIS') THEN 'DEMAIS'
          WHEN TRIM(FASE) IS NULL THEN 'DEMAIS' -- ADDED
          WHEN TRIM(FASE) IN ('EXECUCAO',
                          'EXECUÇÃO',
                          'EXECUÇÃO - INATIVO',
                          'EXECUÇÃO - TRT',
                          'EXECUÇÃO - TST',
                          'EXECUÇÃO DEFIN',
                          'EXECUÇÃO DEFINITIVA', 
                          'EXECUÇÃO DEFINITIVA (TRT)', 
                          'EXECUÇÃO DEFINITIVA (TST)',
                          'EXECUÇÃO DEFINITIVA PROSSEGUIMENTO', 
                          'EXECUÇÃO PROVI',
                          'EXECUÇÃO PROVISÓRIA',
                          'EXECUÇÃO PROVISORIA (TRT)',
                          'EXECUÇÃO PROVISORIA (TST)',
                          'EXECUÇÃO PROVISÓRIA PROSSEGUIMENTO'
                      ) THEN 'EXECUÇÃO'
          WHEN TRIM(FASE) IN ('RECURSAL',
                          'RECURSAL - INATIVO',
                          'RECURSAL TRT',
                          'RECURSAL TRT - INATIVO'
                      ) THEN 'RECURSAL_TRT'
          WHEN TRIM(FASE) IN ('RECURSAL TST - INATIVO',
                          'RECURSAL TST'
                      ) THEN 'RECURSAL_TST'
          ELSE TRIM(FASE) END) AS DP_FASE

  FROM databox.juridico_comum.{nmtabela} A
  )
  SELECT
    ID_PROCESSO,
    B.mes_fech AS MES_FECH,
    FASE,
    DP_FASE
  FROM
    cte A
    LEFT JOIN tb_mes_fech B ON A.arquivo = B.arquivo
  """)

  df_prepara_tb = df_prepara_tb.unionAll(df)
  print(nmtabela+" CONCLUIDA")


# COMMAND ----------

df_prepara_tb.createOrReplaceTempView("TRAB_GER_CONSOLIDA1")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 3 - Prepara tabela a ser pivotada

# COMMAND ----------

# MAGIC %md
# MAGIC ### 3.1 Query para unir TB_ENCER_A_PARTIR_202012

# COMMAND ----------

df_prepara_tb2 = spark.sql("""
/* CARREGA A DATA DO ENCERRAMENTO E ATUALIZADA A FASE DO PROCESSO */
SELECT
  A.ID_PROCESSO,
  A.MES_FECH,
  A.FASE,
  CASE
    WHEN A.MES_FECH = B.MES_FECH THEN 'ENCERRADO'
    ELSE A.DP_FASE
  END AS DP_FASE,
  B.MES_FECH AS MES_ENCERRADO,
  CASE
    WHEN A.MES_FECH >= '2023-03-01' THEN NULL
    ELSE B.NOVO_X_LEGADO
  END AS NOVO_X_LEGADO
FROM
  TRAB_GER_CONSOLIDA1 A
  LEFT JOIN TB_ENCER_A_PARTIR_202012 AS B ON A.ID_PROCESSO = B.ID_PROCESSO
ORDER BY 2, 1
"""
)

df_prepara_tb2.createOrReplaceTempView("TRAB_GER_CONSOLIDA2")

# COMMAND ----------

# df_prepara_tb2 = spark.sql("""
# /* CARREGA A DATA DO ENCERRAMENTO E ATUALIZADA A FASE DO PROCESSO */
# SELECT
#   A.ID_PROCESSO,
#   A.MES_FECH,
#   B.FASE,
#   CASE
#     WHEN A.MES_FECH = B.MES_FECH THEN 'ENCERRADO'
#     ELSE B.DP_FASE
#   END AS DP_FASE,
#   B.MES_FECH AS MES_ENCERRADO,
#   CASE
#     WHEN B.MES_FECH >= '2023-03-01' THEN NULL
#     ELSE A.NOVO_X_LEGADO
#   END AS NOVO_X_LEGADO
# FROM
#   TB_ENCER_A_PARTIR_202012 A
#   LEFT JOIN TRAB_GER_CONSOLIDA1 B ON A.ID_PROCESSO = B.ID_PROCESSO
# ORDER BY 2, 1
# """
# )

# df_prepara_tb2.createOrReplaceTempView("TRAB_GER_CONSOLIDA2")

# COMMAND ----------

# df_prepara_tb2.display()

# COMMAND ----------

df_prepara_tb3 = df_prepara_tb2.select('ID_PROCESSO', 'MES_FECH', 'DP_FASE')
df_prepara_tb3.createOrReplaceTempView('TRAB_GER_CONSOLIDA3')

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from TRAB_GER_CONSOLIDA2 where ID_PROCESSO = 58626

# COMMAND ----------

df_prepara_tb4 = spark.sql("""
/* CARREGA A DATA DO ENCERRAMENTO E ATUALIZADA A FASE DO PROCESSO */
SELECT
  B.ID_PROCESSO,
  B.MES_FECH,
  B.DP_FASE
FROM
  TB_FASE_A_PARTIR_202012_1 A
  LEFT JOIN TRAB_GER_CONSOLIDA3 AS B ON A.ID_PROCESSO = B.ID_PROCESSO
ORDER BY 2, 1
"""
)

df_prepara_tb4.createOrReplaceTempView("TB_FASE")

# COMMAND ----------

# MAGIC %md
# MAGIC ### 3.2 Faz a rolagem, referencia ao mês anterior com LAG
# MAGIC Preferir esse DF caso seja posivel dar sequencia ao relatório nesse formato

# COMMAND ----------

df_rolagem = spark.sql("""
WITH cte AS (
SELECT
  *,
  LAG(DP_FASE) OVER(PARTITION BY ID_PROCESSO ORDER BY ID_PROCESSO, MES_FECH) AS FASE_DO_MES_ANTERIOR
FROM
  TB_FASE
)
SELECT ID_PROCESSO, MES_FECH, DP_FASE, FASE_DO_MES_ANTERIOR,
'MUDOU_FASE_'||date_format(MES_FECH, 'yyyyMM') AS LABEL_MUDOU_FASE,
'DP_FASE_'||date_format(MES_FECH, 'yyyyMM') AS LABEL_DP_FASE,
CASE 
  WHEN FASE_DO_MES_ANTERIOR IS NULL AND DP_FASE IS NOT NULL THEN 0
  WHEN DP_FASE IS NOT NULL AND FASE_DO_MES_ANTERIOR IS NOT NULL AND FASE_DO_MES_ANTERIOR = DP_FASE THEN 1
  WHEN DP_FASE IS NOT NULL AND FASE_DO_MES_ANTERIOR IS NOT NULL AND FASE_DO_MES_ANTERIOR <> DP_FASE AND DP_FASE <> 'ENCERRADO' THEN 2
  WHEN DP_FASE IS NOT NULL AND FASE_DO_MES_ANTERIOR = 'ENCERRADO' THEN 3
ELSE 4 END AS MUDOU_FASE,
CASE 
  WHEN DP_FASE IS NOT NULL AND FASE_DO_MES_ANTERIOR IS NOT NULL AND DP_FASE <> FASE_DO_MES_ANTERIOR
					THEN TRIM(FASE_DO_MES_ANTERIOR)||" - "||TRIM(DP_FASE) END AS DP_FASE_MES
FROM cte
"""
)
# df_rolagem.display()

df_rolagem.createOrReplaceTempView("TB_AUX_FASE")

# COMMAND ----------

df_rolagem_freq = df_rolagem.groupBy("MUDOU_FASE").count()
display(df_rolagem_freq)

# COMMAND ----------

# MAGIC %md
# MAGIC ### 3.3 Pivota tabela rolagem
# MAGIC Baseado no relatório já existente. Exportar esse caso seja necessário manter como era feito no SAS.

# COMMAND ----------

df_rolagem1 = df_rolagem.groupBy('ID_PROCESSO') \
  .pivot('LABEL_MUDOU_FASE') \
  .agg(first('MUDOU_FASE'))

df_rolagem2 = df_rolagem.groupBy('ID_PROCESSO') \
  .pivot('LABEL_DP_FASE') \
  .agg(first('DP_FASE_MES'))

# COMMAND ----------

# TABELA PIVOTADA - Colunas são os meses
df_rolagem_f = df_rolagem2.join(df_rolagem1, 'ID_PROCESSO')
# df_rolagem_f.display()

# COMMAND ----------

display(df_rolagem_f)

# COMMAND ----------

# MAGIC %md
# MAGIC ### 3.4 Unir com fechamento trab para add valor provisão

# COMMAND ----------

comps = ['202101', '202102', '202103', '202104', '202105', '202106', '202107', '202108', '202109', '202110', '202111', '202112','202201', '202202', '202203', '202204', '202205', '202206', '202207', '202208', '202209', '202210', '202211', '202212', '202301', '202302', '202303', '202304', '202305', '202306', '202307', '202308', '202309', '202310', '202311', '202312', '202401', '202402', '202403', '202404','202405','202406']

# Cria df vaziu para popular com as tabelas das comps listadas
df_fech_trab_provisao = spark.sql("""SELECT ID_PROCESSO, MES_FECH, PROVISAO_MOV_M
                                  FROM databox.juridico_comum.tb_fecham_trab_202406 
                                  LIMIT 0""")

for comp in comps:
    # print(comp)
    df = spark.sql(f"SELECT ID_PROCESSO, MES_FECH, PROVISAO_MOV_M FROM databox.juridico_comum.tb_fecham_trab_{comp}")
    
    df_fech_trab_provisao = df_fech_trab_provisao.unionByName(df)
    

df_fech_trab_provisao.createOrReplaceTempView("FECHAMENTO_TRAB_1")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 4 Tabela Auxiliar Fase Final

# COMMAND ----------

df_aux_fase_final_f = spark.sql(f"""
SELECT
DISTINCT
  A.ID_PROCESSO,
  A.MES_FECH AS MES_ROLAGEM,
  A.MUDOU_FASE AS TP_FASE,
  A.DP_FASE_MES AS DP_FASE,
  B.PROVISAO_MOV_M
FROM
  TB_AUX_FASE A
LEFT JOIN FECHAMENTO_TRAB_1 B
  ON A.ID_PROCESSO = B.ID_PROCESSO AND A.MES_FECH = B.MES_FECH
"""
)
df_aux_fase_final_f.createOrReplaceTempView("TB_AUX_FASE_FINAL_F")

# COMMAND ----------

df_aux_fase_final = spark.sql(f"""
SELECT 999999 AS ID_PROCESSO
		,MES_ROLAGEM
		,TP_FASE
		,DP_FASE
		,SUM(PROVISAO_MOV_M) AS PROVISAO_MOV_M
		,COUNT(ID_PROCESSO) AS QTDE
FROM TB_AUX_FASE_FINAL_F
GROUP BY ALL
ORDER BY MES_ROLAGEM, DP_FASE
"""
)
df_aux_fase_final.display()

# COMMAND ----------

df_freq_tp_fase = df_aux_fase_final.groupBy("TP_FASE").count()
display(df_freq_tp_fase)

# COMMAND ----------

# MAGIC %md
# MAGIC # Exportação

# COMMAND ----------

import pandas as pd
from shutil import copyfile

# Convert PySpark DataFrame to Pandas DataFrame
pandas_df = df_fechamento_trab_2.toPandas()

# Save the Pandas DataFrame to an Excel file
local_path = f'/local_disk0/tmp/Trabalhista_Automacao_F.xlsx'
pandas_df.to_excel(local_path, index=False, sheet_name='Automacao', engine='xlsxwriter')

# Copy the file from the local disk to the desired volume
volume_path = f'/Volumes/databox/juridico_comum/arquivos/modelo_provisao/output/FECH_TRAB_MODELAGEM_{dtanomesdia}_F.xlsx'

copyfile(local_path, volume_path)

# COMMAND ----------

# MAGIC %md
# MAGIC # Validação

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT
# MAGIC   ID_PROCESSO,
# MAGIC   MES_ROLAGEM,
# MAGIC   TP_FASE,
# MAGIC   DP_FASE
# MAGIC   --PROVISAO_MOV_M
# MAGIC FROM
# MAGIC   TB_AUX_FASE_FINAL_F
# MAGIC WHERE
# MAGIC   -- MES_ROLAGEM = '2023-01-01' AND
# MAGIC   -- DP_FASE = 'CONHECIMENTO - DEMAIS' AND
# MAGIC ID_PROCESSO = 58626

# COMMAND ----------

dbutils.notebook.exit("SUCCESS")

# COMMAND ----------

#formatação dos valores númericos com padrão 00.000,00

from pyspark.sql.functions import col, udf
from pyspark.sql.types import StringType

# Define the UDF to format float numbers
def format_float(number):
    if number is not None:
        formatted_number = "{:,.2f}".format(float(number)).replace(",", "@").replace(".", ",").replace("@", ".")
        return formatted_number
    return None

# Register the function as a UDF
format_float_udf = udf(format_float, StringType())

# Example list of variable names that need formatting
numbers_list = ['PROVISAO_MOV_M']

# Apply the UDF to each specified column and create a new column for each formatted value
bloco_4_base_rolagem = df_aux_fase_final
for col_name in numbers_list:
    bloco_4_base_rolagem = bloco_4_base_rolagem.withColumn(f"formatted_{col_name}", format_float_udf(col(col_name)))


# COMMAND ----------

# display(bloco_4_base_rolagem)

# COMMAND ----------

# columns_to_drop = ['PROVISAO_MOV_M']
# bloco_4_base_rolagem = bloco_4_base_rolagem.drop(*columns_to_drop)

# COMMAND ----------

# bloco_4_base_rolagem =bloco_4_base_rolagem.withColumnRenamed("formatted_PROVISAO_MOV_M", 'PROVISAO_MOV_M')

# COMMAND ----------

# display(bloco_4_base_rolagem)

# COMMAND ----------

# bloco_4_base_rolagem_f = (
#     bloco_4_base_rolagem
#     .select(
#         'ID_PROCESSO',
#         'MES_ROLAGEM',
#         'TP_FASE',
#         'DP_FASE',
#         'QTDE',
#         'PROVISAO_MOV_M',
#         'QTDE'
#     )
#     .orderBy('MES_ROLAGEM')
# )

# COMMAND ----------

# display(bloco_4_base_rolagem_f)

# COMMAND ----------

# MAGIC %md
# MAGIC # Validação 2

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT
# MAGIC   *
# MAGIC FROM
# MAGIC   TB_AUX_FASE_FINAL_F
# MAGIC WHERE
# MAGIC   DP_FASE = 'CONHECIMENTO - DEMAIS' AND
# MAGIC   MES_ROLAGEM = '2023-01-01'
# MAGIC -- AND ID_PROCESSO = 1090500
# MAGIC -- GROUP BY ALL
# MAGIC -- ORDER BY 2 DESC

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM TB_AUX_FASE_FINAL_F WHERE ID_PROCESSO = 229876 ORDER BY MES_ROLAGEM, ID_PROCESSO
