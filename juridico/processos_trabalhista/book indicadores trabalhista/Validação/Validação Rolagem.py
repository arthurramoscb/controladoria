# Databricks notebook source
# MAGIC %md
# MAGIC
# MAGIC # Validação Rolagem

# COMMAND ----------



# COMMAND ----------

# MAGIC %run "/Workspace/Jurídico/funcao_tratamento_fechamento/common_functions"

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

df_trab_ger_consolida = spark.sql("SELECT * FROM databox.juridico_comum.trab_ger_consolida_20240423 LIMIT 0")

# COMMAND ----------

table_names = [ 'trab_ger_consolida_20230201', 'trab_ger_consolida_20221226' ]
for nmtabela in table_names:
    df = spark.sql(f""" 
    SELECT A.*
      ,{nmtabela[-8:]}::STRING AS arquivo
      ,(CASE WHEN TRIM(FASE) IN ('', 'N/A', 'INATIVO', 'ENCERRAMENTO', 'ADMINISTRATIVO', 'DEMAIS') THEN 'DEMAIS'
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
                   """)
    df_trab_ger_consolida = df_trab_ger_consolida.unionByName(df,allowMissingColumns=True)
    print(nmtabela+" CONCLUIDA")

# COMMAND ----------

df_trab_ger_consolida.createOrReplaceTempView("trab_ger_consolida")

# COMMAND ----------

# MAGIC %sql
# MAGIC select ARQUIVO,PROCESSO_ID, FASE, DP_FASE from trab_ger_consolida where PROCESSO_ID = 33985
# MAGIC --ARQUIVO,PROCESSO_ID, FASE, DP_FASE
