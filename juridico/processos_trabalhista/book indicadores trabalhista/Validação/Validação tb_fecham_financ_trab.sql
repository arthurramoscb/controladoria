-- Databricks notebook source
-- MAGIC %md
-- MAGIC Para conferencia:
-- MAGIC
-- MAGIC Abril
-- MAGIC
-- MAGIC Linhas 1.239.624
-- MAGIC
-- MAGIC (Arquivo 22.04)
-- MAGIC
-- MAGIC Colunas 99

-- COMMAND ----------

-- MAGIC %run "/Workspace/Jurídico/funcao_tratamento_fechamento/common_functions"

-- COMMAND ----------

select COUNT(*) from databox.juridico_comum.geral_fechamento_trab

-- COMMAND ----------

select PROCESSO_ID, PARTE_CONTRARIA_DATA_ADMISSAO from databox.juridico_comum.trab_ger_consolidado_20240423 where PROCESSO_ID IN (34733, 232260)

-- COMMAND ----------

SELECT COUNT(*) FROM databox.juridico_comum.tb_fecham_financ_trab_202404

-- COMMAND ----------

-- MAGIC %python
-- MAGIC # IMPORTA A BASE COM O DE PARA DO BU
-- MAGIC path_bu = '/Volumes/databox/juridico_comum/arquivos/trabalhista/bases_auxiliares/De para Centro de Custo e BU_v2 - ago-2022.xlsx'
-- MAGIC df_bu = read_excel(path_bu)
-- MAGIC df_bu.createOrReplaceTempView("TB_DE_PARA_BU")
-- MAGIC
-- MAGIC
-- MAGIC # IMPORTA A BASE COM O DE PARA DA ÁREA FUNCIONAL
-- MAGIC path_funcional = '/Volumes/databox/juridico_comum/arquivos/trabalhista/bases_auxiliares/Centro de Custo e Área Funcional_OKENN_280623.xlsx'
-- MAGIC df_funcional = read_excel(path_funcional, "'OKENN'!A1")
-- MAGIC df_funcional.createOrReplaceTempView("TB_DE_PARA_AREA_FUNCIONAL")

-- COMMAND ----------

SELECT * FROM TB_DE_PARA_BU WHERE `Centro de custo` = 2300010001

-- COMMAND ----------

SELECT centro, COUNT(*) FROM TB_DE_PARA_AREA_FUNCIONAL GROUP BY 1 ORDER BY 2 DESC

-- COMMAND ----------

SELECT ID_PROCESSO
,DATA_ADMISSAO
,DATA_DISPENSA
,TEMPO_EMPRESA_MESES FROM databox.juridico_comum.tb_fecham_financ_trab_202404

-- COMMAND ----------

SELECT
  ANO_AGING_ENCERR,
  count(*)
FROM
  databox.juridico_comum.tb_fecham_financ_trab_202404
GROUP BY
  1
ORDER BY
  1

-- COMMAND ----------

SELECT ID_PROCESSO, count(*) FROM
  databox.juridico_comum.tb_fecham_financ_trab_202404
GROUP BY 1 
ORDER BY 1 
LIMIT 100

-- COMMAND ----------

df_count = spark.sql("""
    SELECT `MES_FECH`, `DP_NATUREZA`, COUNT(*) AS count
    FROM TB_FECH_FIN_TRAB_CONSOLIDADO_11
    GROUP BY `MES_FECH`, `DP_NATUREZA`
    ORDER BY MES_FECH, DP_NATUREZA
""")

display(df_count)

-- COMMAND ----------

# Executing a SQL query to get the frequency of the "Acordos" column
spark.sql("""
SELECT  MOTIVO_ENC_AGRP, COUNT(*) as Frequency
FROM TB_FECH_FIN_TRAB_CONSOLIDADO_12
ORDER BY  MOTIVO_ENC_AGRP DESC
""").createOrReplaceTempView("acordos_frequency")

# Displaying the result
display(spark.table("acordos_frequency"))
