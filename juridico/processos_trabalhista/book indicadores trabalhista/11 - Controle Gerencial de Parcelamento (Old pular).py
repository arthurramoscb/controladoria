# Databricks notebook source
# MAGIC %md 
# MAGIC # Controle Gerencial de Parcelamento
# MAGIC

# COMMAND ----------

from pyspark.sql.types import *
from pyspark.sql.window import Window

# COMMAND ----------

dbutils.widgets.text("dt_semanal_acordos", "") #Esse cria o widget
 

# COMMAND ----------

# MAGIC %run "/Workspace/Jurídico/funcao_tratamento_fechamento/common_functions"

# COMMAND ----------

# Parametro do nome da tabela da competência atual. Ex: 202404
dbutils.widgets.text("nmmes", "")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 1 - Imports

# COMMAND ----------

# MAGIC %md
# MAGIC ### 1.1 Importa planilha de acordos

# COMMAND ----------

dt_semanal_acordos = dbutils.widgets.get("dt_semanal_acordos")

path_acordos = f'/Volumes/databox/juridico_comum/arquivos/trabalhista/base_semanal_acordos/{dt_semanal_acordos} - REPORTE DIARIO.xlsx'
df_acordos = read_excel(path_acordos, "'MATRIZ'!A1")
df_acordos = adjust_column_names(df_acordos)
df_acordos = remove_acentos(df_acordos)

print(path_acordos)

# COMMAND ----------

df_acordos.createOrReplaceTempView("TB_GERENCIAL_ACORDOS_1")

# COMMAND ----------

# MAGIC %md
# MAGIC ### 1.2 Importa Fechamento Trabalhista

# COMMAND ----------

comps = ['202201', '202202', '202203', '202204', '202205', '202206', '202207', '202208', '202209', '202210', '202211', '202212', '202301', '202302', '202303', '202304', '202305', '202306', '202307', '202308', '202309', '202310', '202311', '202312', '202401', '202402', '202403', '202404','202405','202406','202407','202408','202409','202410','202411','202412','202501']

# Schema do DF unido
schema = StructType([
    StructField("ID_PROCESSO", DoubleType(), True),
    StructField("PARCELAMENTO_ACORDO", StringType(), True),
    StructField("PARCELAMENTO_CONDENACAO", StringType(), True),
    StructField("SUB_TIPO", StringType(), True),
    StructField("TIPO_PGTO", StringType(), True),
    StructField("ENCERRADOS", DoubleType(), True),
    StructField("ESTOQUE", DoubleType(), True),
    StructField("MES_FECH", TimestampType(), True),
    StructField("FASE_M", StringType(), True),
    StructField("PROVISAO_M_1", DoubleType(), True),
    StructField("PROVISAO_TOTAL_M_1", DoubleType(), True)
])

# Cria DF vaziu
df_fech_trab = spark.createDataFrame([], schema)

# Lista as colunas
cols = df_fech_trab.columns

for comp in comps:
    # print(comp)
    df = spark.read.table(f"databox.juridico_comum.tb_fecham_trab_{comp}")

    df = df.select(*cols) \
           .where("PARCELAMENTO_ACORDO = 'PARCELA DE ACORDO' OR PARCELAMENTO_CONDENACAO NOT IN ('PAGAMENTO SEM PARCELAMENTO', '')")
    # print(df.count())

    df_fech_trab = df_fech_trab.unionByName(df)
    
# df_fech_trab.count()

# COMMAND ----------

df_fech_trab = df_fech_trab.sort('ID_PROCESSO', 'MES_FECH')
df_fech_trab = df_fech_trab.dropDuplicates(['ID_PROCESSO'])
df_parcelados_trab = df_fech_trab

# COMMAND ----------

df_parcelados_trab.createOrReplaceTempView("TB_FINANCEIRO_TRAB")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 2 - Parcelas Pagas

# COMMAND ----------

# MAGIC %md
# MAGIC ### 2.1 Acordo

# COMMAND ----------

df_parcelas_pagas_acordo = spark.sql("""
SELECT
DISTINCT 
/*DISTINCT está presente no SAS como:
 "PROC SORT DATA=TB_PARCEL_ACORDO NODUPKEY; BY 'ID PROCESSO'N;"
 Recomenda-se revisar potenciais situações em que o pagamento é feito duas vezes.
 Exemplo recomendado para teste: PROCESSO_ID = 721292 */
  A.PROCESSO_ID,
  A.SUB_TIPO,
  A.DATA_EFETIVA_DO_PAGAMENTO,
  A.ACORDO,
  A.PARCELAMENTO_ACORDO
FROM
  databox.juridico_comum.stg_historico_pgto A
  """
)
df_parcelas_pagas_acordo.createOrReplaceTempView("TB_PARCELAS_PAGAS_ACORDO")

# COMMAND ----------

# %sql
# SELECT * FROM TB_PARCELAS_PAGAS_ACORDO WHERE PROCESSO_ID = 721292 AND
#   PARCELAMENTO_ACORDO IN (
#     'PARCELA DE ACORDO',
#     'ÚLTIMA PARCELA DE%',
#     'ÚLTIMA PARCELA DE ACORDO'
#   )

# COMMAND ----------

df_parcelas_pagas_acordo1 = spark.sql("""
SELECT
--GERAL
  A.PROCESSO_ID,
  A.SUB_TIPO,
  'ACORDO' AS DP_SUB_TIPO,

--PARCELAS
  C.QUANTIDADE_DE_PARCELAS AS QTD_PARCELAS_ACORDO,
  ROW_NUMBER() OVER(PARTITION BY A.PROCESSO_ID ORDER BY A.DATA_EFETIVA_DO_PAGAMENTO, A.ACORDO) AS PARCELA,
  CASE
    WHEN A.DATA_EFETIVA_DO_PAGAMENTO IS NOT NULL THEN PARCELA END AS PARCELA_PAGA,

--DATAS
  DATE_FORMAT(A.DATA_EFETIVA_DO_PAGAMENTO, 'y-MM-01') AS MES_FECH,
  A.DATA_EFETIVA_DO_PAGAMENTO AS DATA_PAGTO,
  FIRST_VALUE(A.DATA_EFETIVA_DO_PAGAMENTO) OVER (
    PARTITION BY PROCESSO_ID ORDER BY A.DATA_EFETIVA_DO_PAGAMENTO
  ) AS DATA_DO_ACORDO,
  CASE
    WHEN PARCELA = 1 THEN DATA_PAGTO
    ELSE DATA_PAGTO + ((PARCELA -1) * 30)
  END AS DT_VENCIMENTO_PARCELA,

--VALORES
  C.ALCADA_1,
  C.ALCADA_2,
  C.ALCADA_3,
  A.ACORDO AS VALOR_PARCELA,
  CASE
    WHEN A.DATA_EFETIVA_DO_PAGAMENTO IS NOT NULL THEN A.ACORDO
  END AS VALOR_PARCELA_PAGA,
  C.VALOR_DO_ACORDO_FECHADO AS VALOR_DO_ACORDO

FROM
  TB_PARCELAS_PAGAS_ACORDO A
INNER JOIN TB_FINANCEIRO_TRAB B ON A.PROCESSO_ID = B.ID_PROCESSO
LEFT JOIN TB_GERENCIAL_ACORDOS_1 C ON A.PROCESSO_ID = C.ID
WHERE
  A.PARCELAMENTO_ACORDO IN (
    'PARCELA DE ACORDO',
    'ÚLTIMA PARCELA DE%',
    'ÚLTIMA PARCELA DE ACORDO'
  )
  AND A.ACORDO IS NOT NULL
ORDER BY A.PROCESSO_ID, A.DATA_EFETIVA_DO_PAGAMENTO, A.ACORDO
"""
)

# COMMAND ----------

df_parcelas_pagas_acordo1.createOrReplaceTempView("TB_PARCELAS_PAGAS_ACORDO1")

# COMMAND ----------

df_parcelas_pagas_acordo1.count() #3227

# COMMAND ----------

# MAGIC %md
# MAGIC ### 2.2 Condenação

# COMMAND ----------

df_parcelas_pagas_conden = spark.sql("""
SELECT
-- GERAL
  A.PROCESSO_ID,
  A.SUB_TIPO,
  'CONDENAÇÃO' AS DP_SUB_TIPO,
  DATE_FORMAT(A.DATA_EFETIVA_DO_PAGAMENTO, 'y-MM-01') AS MES_FECH,

--PARCELA
  DENSE_RANK() OVER(PARTITION BY A.PROCESSO_ID ORDER BY A.DATA_EFETIVA_DO_PAGAMENTO) AS PARCELA,
  CASE
    WHEN A.DATA_EFETIVA_DO_PAGAMENTO IS NOT NULL THEN PARCELA END AS PARCELA_PAGA,
  7 AS QTD_PARCELAS_ACORDO,

--DATAS
  A.DATA_EFETIVA_DO_PAGAMENTO AS DATA_PAGTO,
  FIRST_VALUE(A.DATA_EFETIVA_DO_PAGAMENTO) OVER (
    PARTITION BY PROCESSO_ID ORDER BY A.DATA_EFETIVA_DO_PAGAMENTO
    ) AS DATA_DO_ACORDO,

--VALORES
  A.`PARCELAMENTO_CONDENAÇÃO`,
  A.CONDENACAO AS VALOR_PARCELA,
  CASE
    WHEN A.DATA_EFETIVA_DO_PAGAMENTO IS NOT NULL THEN A.CONDENACAO
  END AS VALOR_PARCELA_PAGA,
  FIRST_VALUE(A.CONDENACAO) OVER (
    PARTITION BY PROCESSO_ID ORDER BY A.DATA_EFETIVA_DO_PAGAMENTO
    ) / 30 AS VALOR_DO_ACORDO

FROM
  databox.juridico_comum.stg_historico_pgto A
  LEFT JOIN TB_FINANCEIRO_TRAB B ON A.PROCESSO_ID = B.ID_PROCESSO
WHERE
  A.`PARCELAMENTO_CONDENAÇÃO` NOT IN ('PAGAMENTO SEM PARCELAMENTO', '')
  AND A.CONDENACAO IS NOT NULL
  AND A.DATA_EFETIVA_DO_PAGAMENTO >= '2022-01-01'
"""
)

# COMMAND ----------

df_parcelas_pagas_conden.createOrReplaceTempView('TB_PARCELAS_PAGAS_CONDENACAO_1')
# df_parcelas_pagas_conden.display()

# COMMAND ----------

# MAGIC %md
# MAGIC #### 2.2.1 Condenação - Após numerar parcelas

# COMMAND ----------

df_parcelas_pagas_conden1 = spark.sql("""
SELECT
-- GERAL
  PROCESSO_ID,
  SUB_TIPO,
  DP_SUB_TIPO,
  MES_FECH,

--PARCELA
  PARCELA,
  PARCELA_PAGA,
  QTD_PARCELAS_ACORDO,
  CASE WHEN 
    SUM(VALOR_PARCELA_PAGA) OVER(PARTITION BY PROCESSO_ID) >= VALOR_DO_ACORDO
  THEN 'SIM' ELSE 'NÃO' END AS QUITADO,

--DATAS
  DATA_PAGTO,
  DATA_DO_ACORDO,
  CASE
    WHEN PARCELA = 1 THEN DATA_PAGTO
    ELSE DATA_PAGTO + ((PARCELA -1) * 30)
  END AS DT_VENCIMENTO_PARCELA,

--VALORES
  `PARCELAMENTO_CONDENAÇÃO`,
  VALOR_PARCELA_PAGA,
  VALOR_DO_ACORDO,
  CASE
    WHEN PARCELA = 1 THEN VALOR_PARCELA
    ELSE ((VALOR_PARCELA / 0.30) - VALOR_PARCELA) / 6
  END AS VALOR_PARCELA

FROM
  TB_PARCELAS_PAGAS_CONDENACAO_1
"""
)
# df_parcelas_pagas_conden1.display()

# COMMAND ----------

# MAGIC %md
# MAGIC ## 3 - Unir Acordos e Condenação

# COMMAND ----------

# MAGIC %md
# MAGIC ### 3.1 União

# COMMAND ----------

df_contr_parc = df_parcelas_pagas_conden1.unionByName(df_parcelas_pagas_acordo1, allowMissingColumns=True)
df_contr_parc.createOrReplaceTempView('TB_CONTROLE_PARCELAMENTOS_1')

# COMMAND ----------

# MAGIC %md
# MAGIC ### 3.2 Add meses das datas

# COMMAND ----------

# ACRECENTAR MESES
df_contr_parc = spark.sql("""
SELECT A.*,
  DATE_FORMAT(A.DATA_PAGTO, 'y-MM-01') AS MES_DO_PAGTO,
  DATE_FORMAT(A.DATA_DO_ACORDO, 'y-MM-01') AS MES_DO_ACORDO,
  DATE_FORMAT(A.DT_VENCIMENTO_PARCELA, 'y-MM-01') AS MES_VENCIMENTO_PARCELA
  FROM TB_CONTROLE_PARCELAMENTOS_1 A
"""
)
df_contr_parc.createOrReplaceTempView('TB_CONTROLE_PARCELAMENTOS_1')

# COMMAND ----------

df_contr_parc_f = spark.sql("""
SELECT A.PROCESSO_ID,
  A.SUB_TIPO,
  A.DP_SUB_TIPO,
  A.MES_FECH,
  A.PARCELA,
  A.PARCELA_PAGA,
  A.QTD_PARCELAS_ACORDO,
  A.QUITADO,
  A.DATA_PAGTO,
  A.MES_DO_PAGTO,
  A.DATA_DO_ACORDO,
  A.MES_DO_ACORDO,
  A.MES_VENCIMENTO_PARCELA,
  A.DT_VENCIMENTO_PARCELA,
  A.`PARCELAMENTO_CONDENAÇÃO`,
  A.VALOR_PARCELA_PAGA,
  A.VALOR_DO_ACORDO,
  A.VALOR_PARCELA,
  -- A.DATA_EFETIVA_DO_PAGAMENTO,
--   A.ACORDO,
--   A.PARCELAMENTO_ACORDO,

  B.FASE_M AS FASE,
  B.PROVISAO_M_1,
  B.PROVISAO_TOTAL_M_1,

  (CASE WHEN A.DP_SUB_TIPO = 'CONDENAÇÃO' AND A.PARCELA = 1 THEN (B.PROVISAO_M_1* 0.30)
    WHEN A.DP_SUB_TIPO = 'CONDENAÇÃO' AND A.PARCELA > 1 THEN ( (B.PROVISAO_M_1 * 0.70) / 6)
    WHEN A.DP_SUB_TIPO = 'ACORDO' THEN ( B.PROVISAO_M_1 / A.QTD_PARCELAS_ACORDO) END) AS PROVISAO_M_1_PARCELA,

  (CASE WHEN A.DP_SUB_TIPO = 'CONDENAÇÃO' AND A.PARCELA = 1 THEN (B.PROVISAO_TOTAL_M_1 * 0.30)
    WHEN A.DP_SUB_TIPO = 'CONDENAÇÃO' AND A.PARCELA > 1 THEN ( (B.PROVISAO_TOTAL_M_1 * 0.70) / 6)
    WHEN A.DP_SUB_TIPO = 'ACORDO' THEN ( B.PROVISAO_TOTAL_M_1 / A.QTD_PARCELAS_ACORDO) END) AS PROVISAO_TOTAL_M_1_PARCELA
FROM TB_CONTROLE_PARCELAMENTOS_1 A
LEFT JOIN TB_FINANCEIRO_TRAB B ON A.PROCESSO_ID=B.ID_PROCESSO AND A.MES_DO_ACORDO=B.MES_FECH
ORDER BY PROCESSO_ID, MES_FECH
"""
)

# COMMAND ----------

df_contr_parc_f.createOrReplaceTempView('TB_CONTROLE_PARCELAMENTOS_F')

# COMMAND ----------

display(df_contr_parc_f)

# COMMAND ----------

# Exportar AQUI

# COMMAND ----------

# MAGIC %md
# MAGIC ## Validação

# COMMAND ----------

df_contr_parc_f.display()

# COMMAND ----------

df_contr_parc_f.count() #14294

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT PROCESSO_ID, count(*) FROM TB_CONTROLE_PARCELAMENTOS_F GROUP BY 1 ORDER BY 2 DESC

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from TB_CONTROLE_PARCELAMENTOS_F where PROCESSO_ID = 35010

# COMMAND ----------

# MAGIC %sql
# MAGIC select CAST(MES_FECH AS DATE) from TB_CONTROLE_PARCELAMENTOS_F WHERE CAST(MES_FECH AS DATE) >= '2024-04-01'
