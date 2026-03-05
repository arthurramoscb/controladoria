# Databricks notebook source
# MAGIC %md
# MAGIC # Tabela de Provisão Trabalhista
# MAGIC
# MAGIC - **Inputs**\
# MAGIC Tabelas:
# MAGIC   - União das tb_fecham_trab_{comp}
# MAGIC   - stg_trab_pgto_garantias gerada no [02 - Pagamentos e Garantias](https://adb-4617509456321466.6.azuredatabricks.net/?o=4617509456321466#notebook/1406052724603435)
# MAGIC
# MAGIC - **Outputs**\
# MAGIC Arquivos: 
# MAGIC   - PROVISAO_TRAB;
# MAGIC   - TB_PROVISAO_TOTAL
# MAGIC   - TB_ENCERR_X_PROVISAO_TRAB;

# COMMAND ----------

# MAGIC %run "/Workspace/Jurídico/funcao_tratamento_fechamento/common_functions"

# COMMAND ----------

# Utilizado no código alternativo que coleta provisão dos meses fechamento que houve pagamento
dbutils.widgets.text("mes_fechamento", "")
dbutils.widgets.text("nmtabela_pgto", "")
dbutils.widgets.text("nmtabela_garantias", "")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 1 - Une tabelas Fechamento Trabalhista

# COMMAND ----------

# Lista das competencias para união das tb_fecham_trab
# Adiconar nova competêcia quando for executar o novo mês.
comps = ['202110', '202111', '202112','202201', '202202', '202203', '202204', '202205', '202206', '202207', '202208', '202209', '202210', '202211', '202212', '202301', '202302', '202303', '202304', '202305', '202306', '202307', '202308', '202309', '202310', '202311', '202312', '202401', '202402', '202403', '202404','202405','202406','202407','202408','202409','202410','202411','202412','202501','202502','202503','202504']

df_fech_trab_provisao = spark.read.table(f"databox.juridico_comum.tb_fecham_trab_202504").limit(0)

for comp in comps:
    # print(comp)
    df = spark.read.table(f"databox.juridico_comum.tb_fecham_trab_{comp}")
    
    df_fech_trab_provisao = df_fech_trab_provisao.unionByName(df, allowMissingColumns=True)
    


# COMMAND ----------

# Ajusta na base qualquer processo que o STATUS_M esteja ENCERRADO ou BAIXA PROVISORIA para que a coluna Encerrados fique marcada com 1

from pyspark.sql.functions import when, col

df_fech_trab_provisao = df_fech_trab_provisao.withColumn("Encerrados", when((col("STATUS_M") == "ENCERRADO") | (col("STATUS_M") == "BAIXA PROVISORIA"), 1).otherwise(col("Encerrados")))

df_fech_trab_provisao.createOrReplaceTempView('df_fech_trab_provisao')

# COMMAND ----------

from pyspark.sql.functions import *

# Trata coluna ESTRATEGIA
df_fech_trab_provisao = df_fech_trab_provisao.withColumn(
    "ESTRATEGIA",
    when(col("ESTRATEGIA").isin("ACO", "ACOR", "ACORD", "ACORDO"), "ACORDO")
    .when(col("ESTRATEGIA").isin("DEF", "DEFE", "DEFES", "DEFESA"), "DEFESA")
    .otherwise(col("ESTRATEGIA"))
)

df_fech_trab_provisao.createOrReplaceTempView('TB_FECH_FIN_TRAB_PROVISAO')

# COMMAND ----------

df_fech_trab_provisao = spark.sql('''
    SELECT *,
        CASE 
            WHEN ESTRATEGIA LIKE '%ACO%' THEN "ACORDO"
            WHEN ESTRATEGIA LIKE '%ACOR%' THEN "ACORDO"
            WHEN ESTRATEGIA LIKE '%ACORD%' THEN "ACORDO"
            WHEN ESTRATEGIA LIKE '%ACORDO%' THEN "ACORDO"
            WHEN ESTRATEGIA LIKE '%DEF%' THEN "DEFESA"
            WHEN ESTRATEGIA LIKE '%DEFE%' THEN "DEFESA"
            WHEN ESTRATEGIA LIKE '%DEFES%' THEN "DEFESA"
            WHEN ESTRATEGIA LIKE '%DEFESA%' THEN "DEFESA"
        ELSE ESTRATEGIA
    END ESTRATEGIA2
    FROM TB_FECH_FIN_TRAB_PROVISAO
''')

df_fech_trab_provisao = df_fech_trab_provisao.withColumn("ESTRATEGIA", col("ESTRATEGIA2"))

df_fech_trab_provisao = df_fech_trab_provisao.drop("ESTRATEGIA2")

# COMMAND ----------

# CRIA BASE COM OS ÚLTIMOS ENCERRAMENTOS
df_fech_trab_provisao66 = df_fech_trab_provisao.drop('DT_ULT_PGTO', 'ACORDOS', 'CONDENACAO', 'PENHORA', 
                                                     'OUTROS_PAGAMENTOS', 'IMPOSTO', 'GARANTIA', 'TOTAL_PAGAMENTOS') \
                                                     .sort(asc('ID_PROCESSO'), desc('MES_FECH')) \
                                                     .where("ENCERRADOS = 1") \
                                                     .dropDuplicates(['ID_PROCESSO'])
df_fech_trab_provisao66.createOrReplaceTempView('TB_FECH_FIN_TRAB_PROVISAO_66')

# COMMAND ----------

# CARRREGA OS VALORES DA BASE DE PAGAMENTOS
df_fech_trab_provisao77 = spark.sql("""
SELECT A.*
    ,B.DT_ULT_PGTO
    ,B.ACORDO AS ACORDOS
    ,B.CONDENACAO
    ,B.PENHORA
    ,B.OUTROS_PAGAMENTOS
    ,B.IMPOSTO
    ,B.GARANTIA
FROM TB_FECH_FIN_TRAB_PROVISAO_66 AS A
LEFT JOIN databox.juridico_comum.tb_trab_pgto_garantias_ff B ON A.ID_PROCESSO = B.PROCESSO_ID
"""
)
df_fech_trab_provisao77.createOrReplaceTempView('TB_FECH_FIN_TRAB_PROVISAO_77')

# COMMAND ----------

# CRIA O CAMPO COM A SOMA DE TODOS OS PAGAMENTOS
df_fech_trab_provisao88 = spark.sql("""
SELECT A.*,
    ACORDOS + CONDENACAO + PENHORA + OUTROS_PAGAMENTOS + IMPOSTO + GARANTIA  AS TOTAL_PAGAMENTOS
FROM TB_FECH_FIN_TRAB_PROVISAO_77 A
"""
)
df_fech_trab_provisao88.createOrReplaceTempView('TB_FECH_FIN_TRAB_PROVISAO_88')

# COMMAND ----------

df_fech_trab_provisao99 = merge_dfs(df_fech_trab_provisao, df_fech_trab_provisao88,['ID_PROCESSO', 'MES_FECH', 'LINHA'])
df_fech_trab_provisao99.createOrReplaceTempView('TB_FECH_FIN_TRAB_PROVISAO_99')

# COMMAND ----------

df_fech_trab_provisao_e = spark.sql("""
    SELECT *,
    CASE WHEN ACORDOS > 0 THEN 'ACORDO'
    WHEN CONDENACAO + PENHORA + OUTROS_PAGAMENTOS + IMPOSTO + GARANTIA > 0 THEN 'CONDENACAO'
    ELSE 'SEM ONUS'
    END MOTIVO_ENC_AGRP
    FROM 
    TB_FECH_FIN_TRAB_PROVISAO_99 A
"""
)

df_fech_trab_provisao_e.createOrReplaceTempView('TB_FECH_FIN_TRAB_PROVISAO_E')

# COMMAND ----------

# display para validar tabela realizada anteriormente
# display(df_fech_trab_provisao_e)

# COMMAND ----------

df_fech_trab_provisao_2 = df_fech_trab_provisao_e.drop('DP_FASE')
df_fech_trab_provisao_2.createOrReplaceTempView('TB_FECH_FIN_TRAB_PROVISAO_2')
df_fech_trab_provisao_2 = spark.sql("""
    SELECT *
    ,(CASE WHEN FASE_M IN ('', 'N/A', 'INATIVO', 'ENCERRAMENTO', 'ADMINISTRATIVO', 'DEMAIS') THEN 'DEMAIS'
        WHEN FASE_M IN ('EXECUCAO',
                        'EXECUÇÃO' ,
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

        WHEN FASE_M IN ('RECURSAL',
                        'RECURSAL - INATIVO',
                        'RECURSAL TRT',
                        'RECURSAL TRT - INATIVO'
                    ) THEN 'RECURSAL_TRT'

        WHEN FASE_M IN ('RECURSAL TST - INATIVO',
                        'RECURSAL TST'
                    ) THEN 'RECURSAL_TST'

        ELSE FASE_M END) AS DP_FASE

    FROM TB_FECH_FIN_TRAB_PROVISAO_2
"""
)
df_fech_trab_provisao_2.createOrReplaceTempView("TB_FECH_FIN_TRAB_PROVISAO_2")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 2 - Provisão

# COMMAND ----------

# MAGIC %md
# MAGIC ### 2.1 Preparação

# COMMAND ----------

# MAGIC %run "/Workspace/Jurídico/processos trabalhista/book indicadores trabalhista/mes_fech"

# COMMAND ----------

# dates = df_arquivos.select('mes_fech').where("mes_fech >= '2021-07-01'").collect()

df_dates = df_arquivos.select('mes_fech').where("mes_fech >= '2021-09-01'")
dates = [row['mes_fech'] for row in df_dates.collect()]
list_mes_fech = [d.strftime("%Y-%m-%d") for d in dates]
print(list_mes_fech)

# COMMAND ----------

# Define the schema
schema_f = StructType([
    StructField("ID_PROCESSO", DoubleType(), True),
    StructField("NATUREZA_OPERACIONAL_M", StringType(), True),
    StructField("DP_FASE", StringType(), True),
    StructField("MOTIVO_ENC_AGRP", StringType(), True),
    StructField("TOTAL_PAGAMENTOS", FloatType(), True),
    StructField("MES_FECH", DateType(), True),
    StructField("VALOR_DE_PROVISAO", FloatType(), True)
])

# Create an empty DataFrame with the specified schema
df_saldo_tot_provisao_f = spark.createDataFrame([], schema_f)

df_saldo_tot_provisao_f_alt = spark.createDataFrame([], schema_f)

# COMMAND ----------

df_saldo_tot_provisao_f.write.format("delta").mode("overwrite").saveAsTable(f"databox.juridico_comum.temp_provisao_trab_f")

df_saldo_tot_provisao_f_alt.write.format("delta").mode("overwrite").saveAsTable(f"databox.juridico_comum.temp_provisao_trab_f_alt")

# COMMAND ----------

# MAGIC %md
# MAGIC ### 2.2 Loop

# COMMAND ----------

for mes_fech in list_mes_fech:

  MESFECH = "'"+mes_fech+"'"
  
  # CRIA BASE COM OS VALORES NEGATIVOS DE PROVISÃO
  df_saldo_tot_prov = spark.sql(f"""
  SELECT
    LINHA,
    ID_PROCESSO,
    NATUREZA_OPERACIONAL_M,
    MES_FECH::DATE AS MES_PROV,
    PROVISAO_MOV_TOTAL_M AS VALOR_DE_PROVISAO,
    MOTIVO_ENC_AGRP,
    DP_FASE,
    ESTRATEGIA
  FROM
    TB_FECH_FIN_TRAB_PROVISAO_2
  WHERE
    MES_FECH > '2021-07-01'
  AND MES_FECH <= {MESFECH}
  AND PROVISAO_MOV_TOTAL_M < 0
  ORDER BY
    ID_PROCESSO, MES_FECH
  """
  )

  df_saldo_tot_prov.createOrReplaceTempView("TB_SALDO_TOT_PROVISAO")
  # print(df_saldo_tot_prov.count())

  # Filtra a estratégia mais recente
  df_estrategia = spark.sql(f"""
  WITH ordena_estr AS (
  SELECT
    LINHA,
    ID_PROCESSO,
    MES_FECH::DATE,
    ESTRATEGIA,
    ROW_NUMBER() OVER (PARTITION BY ID_PROCESSO ORDER BY MES_FECH DESC) AS row_num
  FROM
    TB_FECH_FIN_TRAB_PROVISAO_2
  WHERE
    MES_FECH > '2021-07-01'
    AND MES_FECH <= {MESFECH}
    AND NATUREZA_OPERACIONAL_M != 'TERCEIRO INSOLVENTE'
    AND ESTRATEGIA = 'DEFESA'
  ORDER BY
    ID_PROCESSO ASC, MES_FECH DESC
  )

  SELECT
    LINHA,
    ID_PROCESSO,
    MES_FECH::DATE AS MES_ULT_ESTRAT,
    ESTRATEGIA AS ULT_ESTRAT
  FROM
    ordena_estr
  WHERE
    row_num = 1
  """
  )

  df_estrategia.createOrReplaceTempView("TB_ESTRATEGIA")
  # print(df_estrategia.count())


  # CARREGA OS DADOS DA ÚLTIMA ESTRATÉGIA
  df_saldo_tot_prov_1 = spark.sql(f"""
  SELECT
    A.LINHA,
    A.ID_PROCESSO,
    A.NATUREZA_OPERACIONAL_M,
    A.MES_PROV,
    A.VALOR_DE_PROVISAO,
    A.MOTIVO_ENC_AGRP,
    A.DP_FASE,
    A.ESTRATEGIA,
    B.MES_ULT_ESTRAT,
    B.ULT_ESTRAT
  FROM
    TB_SALDO_TOT_PROVISAO A
  LEFT JOIN
    TB_ESTRATEGIA B ON A.ID_PROCESSO = B.ID_PROCESSO
  """
  )

  df_saldo_tot_prov_1.createOrReplaceTempView("TB_SALDO_TOT_PROVISAO_1")
  # print(df_saldo_tot_prov_1.count())


  # CONSIDERA APENAS OS VALORES POSTERIORES A ESTRATÉGIA DEFESA
  df_saldo_tot_prov_2 = spark.sql(f"""
  SELECT 
    LINHA,
    ID_PROCESSO,
    NATUREZA_OPERACIONAL_M,
    MES_PROV,
    MOTIVO_ENC_AGRP,
    DP_FASE,
    ESTRATEGIA,
    MES_ULT_ESTRAT,
    ULT_ESTRAT,
  CASE
      WHEN ULT_ESTRAT = 'DEFESA' AND MES_PROV <= MES_ULT_ESTRAT THEN NULL
      ELSE VALOR_DE_PROVISAO 
  END AS VALOR_DE_PROVISAO
  FROM TB_SALDO_TOT_PROVISAO_1
  WHERE LINHA <> 'linha01'
  """
  )

  df_saldo_tot_prov_2.createOrReplaceTempView("TB_SALDO_TOT_PROVISAO_2")

  # if mes_fech == '':
  #   df_saldo_tot_prov_2.createOrReplaceTempView("TB_SEM_SUMARIZAR")
  #   df_analisar = spark.sql('''
  #     SELECT * FROM TB_SEM_SUMARIZAR
  #   ''')


  # SOMA OS VALORES DE PROVISÃO
  df_saldo_tot_prov_3 = spark.sql(f"""
  SELECT
    ID_PROCESSO,
    SUM(VALOR_DE_PROVISAO) * -1 AS VALOR_DE_PROVISAO,
    MAX(MES_PROV) AS MES_FECH
  FROM
    TB_SALDO_TOT_PROVISAO_2
    GROUP BY 1
  """
  )

  # DF SEM SOMAR PARA CAMINHO ALTERNATIVO
  #df_saldo_tot_prov_3_alt = spark.sql(f"""
  #SELECT
    #ID_PROCESSO,
    #VALOR_DE_PROVISAO,
    #MES_PROV AS MES_FECH
    #--SUM(VALOR_DE_PROVISAO) * -1 AS VALOR_DE_PROVISAO,
    #--MAX(MES_PROV) AS MES_FECH
  #FROM
    #TB_SALDO_TOT_PROVISAO_2
   #--GROUP BY 1
  #"""
  #)

  df_saldo_tot_prov_3.createOrReplaceTempView("TB_SALDO_TOT_PROVISAO_3")
  #df_saldo_tot_prov_3_alt.createOrReplaceTempView("TB_SALDO_TOT_PROVISAO_3_ALT")
  # print(df_saldo_tot_prov_3.count())


  # CRIA BASE COM OS PROCESSOS ENCERRADOS
  df_encerrados = spark.sql(f"""
  SELECT
    ID_PROCESSO,
    NATUREZA_OPERACIONAL_M,
    DP_FASE,
    MOTIVO_ENC_AGRP,
    MES_FECH,
    TOTAL_PAGAMENTOS
  FROM
    TB_FECH_FIN_TRAB_PROVISAO_2
  WHERE
    ENCERRADOS = 1
    AND MES_FECH = {MESFECH}
  ORDER BY
    ID_PROCESSO
  """
  )

  df_encerrados.createOrReplaceTempView("TB_ENCERRADOS")
  # print(df_encerrados.count())

  df_encerrados.write.format("delta").mode("append").option("mergeSchema", "true").saveAsTable(f"databox.juridico_comum.temp_encerrados_f")


  # BUSCA O VALOR DE PROVISÃO DO MÊS
  df_saldo_tot_prov_4 = spark.sql(f"""
  SELECT
    A.ID_PROCESSO,
    A.NATUREZA_OPERACIONAL_M,
    A.DP_FASE,
    A.MOTIVO_ENC_AGRP,
    A.TOTAL_PAGAMENTOS::FLOAT,
    A.MES_FECH::DATE,
    B.VALOR_DE_PROVISAO::FLOAT
  FROM
    TB_ENCERRADOS AS A
    LEFT JOIN TB_SALDO_TOT_PROVISAO_3 AS B ON A.ID_PROCESSO = B.ID_PROCESSO
  """
  )

  # BUSCA O VALOR DE PROVISÃO DO MÊS SEM SOMAR - ALTERNATIVA
  #df_saldo_tot_prov_4_alt = spark.sql(f"""
  #SELECT
    #A.ID_PROCESSO,
    #A.NATUREZA_OPERACIONAL_M,
   #A.DP_FASE,
    #A.MOTIVO_ENC_AGRP,
    #A.TOTAL_PAGAMENTOS::FLOAT,
    #A.MES_FECH::DATE,
   #B.MES_FECH::DATE AS MES_PROV, -- UTILIZADO PARA SABER DE QUAL MES A PROVISAO FOI PUXADA
    #B.VALOR_DE_PROVISAO::FLOAT
  #FROM
    #TB_ENCERRADOS AS A
    #LEFT JOIN TB_SALDO_TOT_PROVISAO_3_ALT AS B ON A.ID_PROCESSO = B.ID_PROCESSO
  #"""
  #)

  df_saldo_tot_prov_4.createOrReplaceTempView("TB_SALDO_TOT_PROVISAO_4")
  #df_saldo_tot_prov_4_alt.createOrReplaceTempView("TB_SALDO_TOT_PROVISAO_4_ALT")

  df_saldo_tot_prov_4.write.format("delta").mode("append").option("mergeSchema", "true").saveAsTable(f"databox.juridico_comum.temp_provisao_trab_f")

  #df_saldo_tot_prov_4_alt.write.format("delta").mode("append").option("mergeSchema", "true").saveAsTable(f"databox.juridico_comum.temp_provisao_trab_f_alt")

  qtd_inc = df_saldo_tot_prov_4.count()

  print(MESFECH + " concluida." + str(qtd_inc))
  # print(df_saldo_tot_provisao_f.count())

# COMMAND ----------

# MAGIC %md
# MAGIC #### Rotina alternativa abaixo

# COMMAND ----------

# MAGIC %md
# MAGIC A rotina abaixo utiliza as bases de fechamento já filtradas por encerrado, regra de estrategia e linha <> de 01 adicionando uma lógica pelos pagamentos dos processos. 
# MAGIC Se houve pagamentos no mes_fechamento iremos incluir o valor de provisão na soma final.

# COMMAND ----------

# MAGIC %md
# MAGIC ##### Cria nova tabela historica de `pagamentos`

# COMMAND ----------

nmtabela_pgto = dbutils.widgets.get("nmtabela_pgto")

path_pagamentos = f'/Volumes/databox/juridico_comum/arquivos/bases_pagamentos/HISTORICO_DE-PAGAMENTOS_{nmtabela_pgto}.xlsx'

df_pagamentos_filtro = read_excel(path_pagamentos, "A6")

df_pagamentos_filtro = adjust_column_names(df_pagamentos_filtro)

pgto_data_cols = find_columns_with_word(df_pagamentos_filtro, 'DATA')

df_pagamentos_filtro_ = convert_to_date_format(df_pagamentos_filtro, pgto_data_cols)

df_pagamentos_filtro_.createOrReplaceTempView('df_pagamentos_filtro')

df_pagamentos_filtro_2 = spark.sql('''
    SELECT PROCESSO_ID
    ,ID_DO_PAGAMENTO
    ,DATA_EFETIVA_DO_PAGAMENTO
    ,CAST(date_trunc('MONTH', DATA_EFETIVA_DO_PAGAMENTO) AS DATE) AS MES_PGTO
    ,STATUS_DO_PAGAMENTO
    ,TIPO
    ,SUB_TIPO
    ,VALOR
    ,CASE 
            WHEN `SUB_TIPO` IN ('ACORDO - CÍVEL', 'ACORDO - CÍVEL ESTRATÉGICO', 'ACORDO - CÍVEL MASSA', 'ACORDO - TRABALHISTA', 'PAGAMENTO DE ACORDO - TRABALHISTA (FK)', 'RNO - ACORDO - TRABALHISTA', 'ACORDO - CÍVEL MASSA - CARTÕES', 'ACORDO - CÍVEL MASSA - FORNECEDOR', 'ACORDO - CÍVEL MASSA - MKTPLACE', 'ACORDO - CÍVEL MASSA - SEGURO', 'ACORDO - IMOBILIÁRIO', 'ACORDO - MEDIAÇÃO', 'ACORDO - REGULATÓRIO - PROCON COM AUDIÊNCIA', 'ACORDO/ PROGRAMAS DE PARCELAMENTO - REGULATÓRIO')
            THEN VALOR
        END AS ACORDO,
        CASE 
            WHEN `SUB_TIPO` IN ('CONDENAÇÃO - CÍVEL', 'CONDENAÇÃO - CÍVEL ESTRATÉGICO', 'CONDENAÇÃO - CÍVEL MASSA', 'CONDENAÇÃO - CÍVEL MASSA (INCONTROVERSO)', 'CONDENAÇÃO - REGULATÓRIO', 'CONDENAÇÃO - TRABALHISTA', 'CONDENAÇÃO - TRABALHISTA (INCONTROVERSO)', 'CONDENAÇÃO - CÍVEL MASSA (INCONTROVERSO) - CARTÕES', 'CONDENAÇÃO - CÍVEL MASSA (INCONTROVERSO) - FORNECEDOR', 'CONDENAÇÃO - CÍVEL MASSA (INCONTROVERSO) - MKTPLACE', 'CONDENAÇÃO - CÍVEL MASSA (INCONTROVERSO) - SEGURO', 'CONDENAÇÃO - CÍVEL MASSA - CARTÕES', 'CONDENAÇÃO - CÍVEL MASSA - FORNECEDOR', 'CONDENAÇÃO - CÍVEL MASSA - MKTPLACE', 'CONDENAÇÃO - CÍVEL MASSA - SEGURO', 'CONDENAÇÃO - IMOBILIÁRIO', 'CONDENAÇÃO - IMOBILIÁRIO (INCONTROVERSO)', 'CONDENAÇÃO - REGULATÓRIO', 'MULTA - REGULATÓRIO', 'MULTA PROCESSUAL - CÍVEL', 'MULTA PROCESSUAL - TRABALHISTA', 'MULTA PROCESSUAL - CÍVEL ESTRATÉGICO', 'MULTA PROCESSUAL - IMOBILIÁRIO', 'RNO - CONDENAÇÃO - TRABALHISTA', 'RNO - CONDENAÇÃO - TRABALHISTA (INCONTROVERSO)', 'RNO - MULTA PROCESSUAL - TRABALHISTA')
            THEN VALOR
        END AS CONDENACAO,
        CASE 
            WHEN `SUB_TIPO` IN ('HONORÁRIOS CONCILIADOR - CIVEL MASSA', 'HONORÁRIOS PERICIAIS - CÍVEL ESTRATÉGICO', 'HONORÁRIOS PERICIAIS - CÍVEL MASSA', 'HONORÁRIOS PERICIAIS - IMOBILIÁRIO', 'HONORÁRIOS PERICIAIS - TRABALHISTA', 'HONORÁRIOS CONCILIADOR - CIVEL MASSA - CARTÕES', 'HONORÁRIOS CONCILIADOR - CIVEL MASSA - FORNECEDOR', 'HONORÁRIOS CONCILIADOR - CIVEL MASSA - MKTPLACE', 'HONORÁRIOS CONCILIADOR - CIVEL MASSA - SEGURO', 'HONORÁRIOS PERICIAIS - CÍVEL MASSA - CARTÕES', 'HONORÁRIOS PERICIAIS - CÍVEL MASSA - FORNECEDOR', 'HONORÁRIOS PERICIAIS - CÍVEL MASSA - MKTPLACE', 'HONORÁRIOS PERICIAIS - CÍVEL MASSA - SEGURO', 'HONORÁRIOS SUCUMBENCIAIS - REGULATÓRIO', 'PAGAMENTO DE HONORÁRIOS PERICIAIS - TRABALHISTA (FK)', 'RNO - HONORÁRIOS PERICIAIS -TRABALHISTA', 'CUSTAS FK - TRABALHISTA', 'CUSTAS PERITOS - CÍVEL', 'CUSTAS PERITOS - IMOBILIÁRIO', 'CUSTAS PROCESSUAIS - CÍVEL', 'CUSTAS PROCESSUAIS - CÍVEL ESTRATÉGICO', 'CUSTAS PROCESSUAIS - CÍVEL MASSA', 'CUSTAS PROCESSUAIS - IMOBILIÁRIO', 'CUSTAS PROCESSUAIS - REGULATÓRIO', 'CUSTAS PROCESSUAIS - TRABALHISTA', 'CUSTAS PROCESSUAIS - CÍVEL MASSA - CARTÕES', 'CUSTAS PROCESSUAIS - CÍVEL MASSA - FORNECEDOR', 'CUSTAS PROCESSUAIS - CÍVEL MASSA - MKTPLACE', 'CUSTAS PROCESSUAIS - CÍVEL MASSA - SEGURO', 'PAGAMENTO DE CUSTAS - TRIBUTÁRIO', 'PERITOS - REGULATÓRIO', 'RNO - CUSTAS PROCESSUAIS - TRABALHISTA')
            THEN VALOR
        END AS CUSTAS,
        CASE 
            WHEN `SUB_TIPO` IN ('INSS - TRABALHISTA', 'IR - TRABALHISTA', 'PAGAMENTO DE INSS - INDENIZAÇÃO TRABALHISTA (FK)', 'PAGAMENTO DE IR - INDENIZAÇÃO TRABALHISTA (FK)', 'RNO - INSS - TRABALHISTA', 'RNO - IR - TRABALHISTA ', 'RNO - FGTS', 'FGTS', 'INSS/IR - DARF E-SOCIAL')
            THEN VALOR
        END AS IMPOSTO,
        CASE 
            WHEN `SUB_TIPO` IN ('LIBERAÇÃO DE PENHORA - MASSA', 'LIBERAÇÃO DE PENHORA MASSA', 'LIBERAÇÃO DE PENHORA - REGULATÓRIO')
            THEN VALOR
        END AS PENHORA,
        CASE 
            WHEN `SUB_TIPO` IN ('PENSÃO - CÍVEL', 'PENSÃO - CÍVEL ESTRATÉGICO', 'PENSÃO - CÍVEL MASSA')
            THEN VALOR
        END AS PENSAO,
        CASE 
            WHEN `SUB_TIPO` IN ('ACERTO CONTÁBIL: BLOQUEIO COM TRANSFERÊNCIA BANCÁRIA - DEP. JUDICIAL  (PAG)', 'ACERTO CONTÁBIL: BLOQUEIO COM TRANSFERÊNCIA BANCÁRIA - PAGAMENTO', 'ACERTO CONTÁBIL: PAGAMENTO DE ACORDO COM BAIXA DE DEP. JUDICIAL', 'LIMINAR (CÍV. MASSA)', 'PAGAMENTO', 'PAGAMENTO AUTUAÇÃO MINISTÉRIO DO TRABALHO - TRABALHISTA', 'PAGAMENTO DE EXECUÇÃO - TRABALHISTA', 'PAGAMENTOS - ALL - VV', 'PAGAMENTOS FK')
            THEN VALOR
        END AS OUTROS_PAGAMENTOS
    FROM df_pagamentos_filtro
    WHERE `SUB_TIPO` IN ('ACORDO - CÍVEL', 'ACORDO - CÍVEL ESTRATÉGICO', 'ACORDO - CÍVEL MASSA', 'ACORDO - TRABALHISTA', 'PAGAMENTO DE ACORDO - TRABALHISTA (FK)', 'RNO - ACORDO - TRABALHISTA', 'ACORDO - CÍVEL MASSA - CARTÕES', 'ACORDO - CÍVEL MASSA - FORNECEDOR', 'ACORDO - CÍVEL MASSA - MKTPLACE', 'ACORDO - CÍVEL MASSA - SEGURO', 'ACORDO - IMOBILIÁRIO', 'ACORDO - MEDIAÇÃO', 'ACORDO - REGULATÓRIO - PROCON COM AUDIÊNCIA', 'ACORDO/ PROGRAMAS DE PARCELAMENTO - REGULATÓRIO', 'CONDENAÇÃO - CÍVEL', 'CONDENAÇÃO - CÍVEL ESTRATÉGICO', 'CONDENAÇÃO - CÍVEL MASSA', 'CONDENAÇÃO - CÍVEL MASSA (INCONTROVERSO)', 'CONDENAÇÃO - REGULATÓRIO', 'CONDENAÇÃO - TRABALHISTA', 'CONDENAÇÃO - TRABALHISTA (INCONTROVERSO)', 'CONDENAÇÃO - CÍVEL MASSA (INCONTROVERSO) - CARTÕES', 'CONDENAÇÃO - CÍVEL MASSA (INCONTROVERSO) - FORNECEDOR', 'CONDENAÇÃO - CÍVEL MASSA (INCONTROVERSO) - MKTPLACE', 'CONDENAÇÃO - CÍVEL MASSA (INCONTROVERSO) - SEGURO', 'CONDENAÇÃO - CÍVEL MASSA - CARTÕES', 'CONDENAÇÃO - CÍVEL MASSA - FORNECEDOR', 'CONDENAÇÃO - CÍVEL MASSA - MKTPLACE', 'CONDENAÇÃO - CÍVEL MASSA - SEGURO', 'CONDENAÇÃO - IMOBILIÁRIO', 'CONDENAÇÃO - IMOBILIÁRIO (INCONTROVERSO)', 'CONDENAÇÃO - REGULATÓRIO', 'MULTA - REGULATÓRIO', 'MULTA PROCESSUAL - CÍVEL', 'MULTA PROCESSUAL - TRABALHISTA', 'MULTA PROCESSUAL - CÍVEL ESTRATÉGICO', 'MULTA PROCESSUAL - IMOBILIÁRIO', 'RNO - CONDENAÇÃO - TRABALHISTA', 'RNO - CONDENAÇÃO - TRABALHISTA (INCONTROVERSO)', 'RNO - MULTA PROCESSUAL - TRABALHISTA', 'HONORÁRIOS CONCILIADOR - CIVEL MASSA', 'HONORÁRIOS PERICIAIS - CÍVEL ESTRATÉGICO', 'HONORÁRIOS PERICIAIS - CÍVEL MASSA', 'HONORÁRIOS PERICIAIS - IMOBILIÁRIO', 'HONORÁRIOS PERICIAIS - TRABALHISTA', 'HONORÁRIOS CONCILIADOR - CIVEL MASSA - CARTÕES', 'HONORÁRIOS CONCILIADOR - CIVEL MASSA - FORNECEDOR', 'HONORÁRIOS CONCILIADOR - CIVEL MASSA - MKTPLACE', 'HONORÁRIOS CONCILIADOR - CIVEL MASSA - SEGURO', 'HONORÁRIOS PERICIAIS - CÍVEL MASSA - CARTÕES', 'HONORÁRIOS PERICIAIS - CÍVEL MASSA - FORNECEDOR', 'HONORÁRIOS PERICIAIS - CÍVEL MASSA - MKTPLACE', 'HONORÁRIOS PERICIAIS - CÍVEL MASSA - SEGURO', 'HONORÁRIOS SUCUMBENCIAIS - REGULATÓRIO', 'PAGAMENTO DE HONORÁRIOS PERICIAIS - TRABALHISTA (FK)', 'RNO - HONORÁRIOS PERICIAIS -TRABALHISTA', 'CUSTAS FK - TRABALHISTA', 'CUSTAS PERITOS - CÍVEL', 'CUSTAS PERITOS - IMOBILIÁRIO', 'CUSTAS PROCESSUAIS - CÍVEL', 'CUSTAS PROCESSUAIS - CÍVEL ESTRATÉGICO', 'CUSTAS PROCESSUAIS - CÍVEL MASSA', 'CUSTAS PROCESSUAIS - IMOBILIÁRIO', 'CUSTAS PROCESSUAIS - REGULATÓRIO', 'CUSTAS PROCESSUAIS - TRABALHISTA', 'CUSTAS PROCESSUAIS - CÍVEL MASSA - CARTÕES', 'CUSTAS PROCESSUAIS - CÍVEL MASSA - FORNECEDOR', 'CUSTAS PROCESSUAIS - CÍVEL MASSA - MKTPLACE', 'CUSTAS PROCESSUAIS - CÍVEL MASSA - SEGURO', 'PAGAMENTO DE CUSTAS - TRIBUTÁRIO', 'PERITOS - REGULATÓRIO', 'RNO - CUSTAS PROCESSUAIS - TRABALHISTA', 'INSS - TRABALHISTA', 'IR - TRABALHISTA', 'PAGAMENTO DE INSS - INDENIZAÇÃO TRABALHISTA (FK)', 'PAGAMENTO DE IR - INDENIZAÇÃO TRABALHISTA (FK)', 'RNO - INSS - TRABALHISTA', 'RNO - IR - TRABALHISTA ', 'RNO - FGTS', 'FGTS', 'LIBERAÇÃO DE PENHORA - MASSA', 'LIBERAÇÃO DE PENHORA MASSA', 'LIBERAÇÃO DE PENHORA - REGULATÓRIO', 'PENSÃO - CÍVEL', 'PENSÃO - CÍVEL ESTRATÉGICO', 'PENSÃO - CÍVEL MASSA', 'ACERTO CONTÁBIL: BLOQUEIO COM TRANSFERÊNCIA BANCÁRIA - DEP. JUDICIAL  (PAG)', 'ACERTO CONTÁBIL: BLOQUEIO COM TRANSFERÊNCIA BANCÁRIA - PAGAMENTO', 'ACERTO CONTÁBIL: PAGAMENTO DE ACORDO COM BAIXA DE DEP. JUDICIAL', 'LIMINAR (CÍV. MASSA)', 'PAGAMENTO', 'PAGAMENTO AUTUAÇÃO MINISTÉRIO DO TRABALHO - TRABALHISTA', 'PAGAMENTO DE EXECUÇÃO - TRABALHISTA', 'PAGAMENTOS - ALL - VV', 'PAGAMENTOS FK')
        AND UPPER(`STATUS_DO_PAGAMENTO`) NOT IN ('REMOVIDO', 'CANCELADO', 'REJEITADO', 'EM CORREÇÃO', 'PENDENTE')
        AND UPPER(`STATUS_DO_PAGAMENTO`) NOT LIKE ('%PAGAMENTO DEVOLVIDO%')
    AND DATA_EFETIVA_DO_PAGAMENTO >= '2021-09-01'
''')

df_pagamentos_filtro_2.createOrReplaceTempView('df_pagamentos_filtro')

# COMMAND ----------

df_pagamentos_filtro_3 = spark.sql('''
    select * 
    ,CASE 
        WHEN ACORDO >= 1 THEN 'ACORDO'
        WHEN CONDENACAO >=1 THEN 'CONDENACAO'
        WHEN CUSTAS >= 1 THEN 'CUSTAS'
        WHEN IMPOSTO >= 1 THEN 'IMPOSTO'
        WHEN PENHORA >= 1 THEN 'PENHORA'
        WHEN PENSAO >= 1 THEN 'PENSAO'
        WHEN OUTROS_PAGAMENTOS >= 1 THEN 'OUTROS_PAGAMENTOS'
        ELSE 'SEM_TIPO'
    END AS SUBTIPO_AGR
    from df_pagamentos_filtro
''')

df_pagamentos_filtro_3.createOrReplaceTempView('df_pagamentos_filtro_3')

# COMMAND ----------

df_pagamentos_filtrado = spark.sql('''
select *
 from df_pagamentos_filtro_3
    WHERE SUBTIPO_AGR IN ('ACORDO','CONDENACAO','PENHORA','PENSAO','OUTROS_PAGAMENTOS','IMPOSTO','GARANTIA')
''')

df_pagamentos_filtrado.createOrReplaceTempView('df_pagamentos_filtrado')

# COMMAND ----------

# MAGIC %md
# MAGIC ##### Cria nova tabela historica de garantias

# COMMAND ----------

nmtabela_garantias = dbutils.widgets.get("nmtabela_garantias")

path_garantias = f'/Volumes/databox/juridico_comum/arquivos/bases_garantias/GARANTIAS_{nmtabela_garantias}.xlsx'

df_garantias = read_excel(path_garantias, "A6")

garantias_data_cols = find_columns_with_word(df_garantias, 'DATA ')
df_garantias = convert_to_date_format(df_garantias, garantias_data_cols)
df_garantias = adjust_column_names(df_garantias)

df_garantias.createOrReplaceTempView("TB_GARANTIAS_F")
df_garantias.createOrReplaceTempView("TB_GARANTIAS_MES_A")

# COMMAND ----------

df_garantias = spark.sql("""
SELECT PROCESSO_ID
        ,`DATA_DE_LEVANTAMENTO_PARTE_CONTRÁRIA`
		,`VALOR_LEVANTADO_PARTE_CONTRÁRIA`
FROM TB_GARANTIAS_MES_A
WHERE STATUS_DA_GARANTIA <> 'CANCELADO'
		AND STATUS_DA_GARANTIA NOT LIKE ('%PAGAMENTO DEVOLVIDO%')
		AND STATUS <> 'REMOVIDO'
		AND TIPO_GARANTIA IN ('ACERTO CONTÁBIL: BLOQUEIO COM TRANSFERÊNCIA BANCÁRIA - DEP. JUDICIAL',
						'DEPÓSITO',
						'DEPÓSITO GARANTIA DE JUIZO - CÍVEL',
						'DEPÓSITO JUDICIAL',
						'DEPÓSITO JUDICIAL - CÍVEL',
						'DEPÓSITO JUDICIAL - CÍVEL ESTRATÉGICO',
						'DEPÓSITO JUDICIAL - CÍVEL MASSA',
						'DEPÓSITO JUDICIAL - REGULATÓRIO',
						'DEPÓSITO JUDICAL REGULATÓRIO',
						'DEPÓSITO JUDICIAL - TRABALHISTA',
						'DEPOSITO JUDICIAL TRABALHISTA',
						'DEPÓSITO JUDICIAL TRABALHISTA - BLOQUEIO',
						'DEPÓSITO JUDICIAL TRABALHISTA BLOQUEIO - TRABALHISTA',
						'DEPÓSITO JUDICIAL TRIBUTÁRIO',
						'DEPÓSITO RECURSAL - AIRO - TRABALHISTA',
						'DEPÓSITO RECURSAL - AIRR',
						'DEPÓSITO RECURSAL - AIRR - TRABALHISTA',
						'DEPÓSITO RECURSAL - CÍVEL MASSA',
						'DEPÓSITO RECURSAL - EMBARGOS TST',
						'DEPÓSITO RECURSAL - RO - TRABALHISTA',
						'DEPÓSITO RECURSAL - RR - TRABALHISTA',
						'DEPÓSITO RECURSAL AIRR',
						'DEPÓSITO RECURSAL RO',
						'PENHORA - GARANTIA',
						'PENHORA - REGULATÓRIO' )
      AND `DATA_DE_LEVANTAMENTO_PARTE_CONTRÁRIA` >= '2021-09-01'
""")

# COMMAND ----------

df_garantias.createOrReplaceTempView("TB_GARANTIAS_F")

df_garantias = spark.sql("""
	SELECT PROCESSO_ID
        ,`DATA_DE_LEVANTAMENTO_PARTE_CONTRÁRIA` AS DATA_EFETIVA_DO_PAGAMENTO
        ,SUM(`VALOR_LEVANTADO_PARTE_CONTRÁRIA`) AS GARANTIA
	FROM TB_GARANTIAS_F
  WHERE `DATA_DE_LEVANTAMENTO_PARTE_CONTRÁRIA` IS NOT NULL
	GROUP BY 1, 2
	ORDER BY 1, 2
 """)

df_garantias.createOrReplaceTempView('df_garantias_filtrado')

# COMMAND ----------

# Faz o Join das duas tabelas
df_pgto_garantias = spark.sql("""
    SELECT 
    coalesce(A.PROCESSO_ID, B.PROCESSO_ID) AS PROCESSO_ID
    ,coalesce(A.DATA_EFETIVA_DO_PAGAMENTO, B.DATA_EFETIVA_DO_PAGAMENTO) AS DATA_EFETIVA_DO_PAGAMENTO
    ,SUM(coalesce(ACORDO,0)) AS ACORDO
    ,SUM(coalesce(CONDENACAO, 0)) AS CONDENACAO
    ,SUM(coalesce(PENHORA, 0)) AS PENHORA
    ,SUM(coalesce(PENSAO, 0)) AS PENSAO
    ,SUM(coalesce(OUTROS_PAGAMENTOS, 0)) AS OUTROS_PAGAMENTOS
    ,SUM(coalesce(IMPOSTO, 0)) AS IMPOSTO
    ,SUM(coalesce(GARANTIA, 0)) AS GARANTIA
    FROM df_pagamentos_filtrado A
    FULL OUTER JOIN df_garantias_filtrado B
    ON A.PROCESSO_ID = B.PROCESSO_ID AND A.DATA_EFETIVA_DO_PAGAMENTO = B.DATA_EFETIVA_DO_PAGAMENTO
    GROUP BY 1, 2
""")

df_pgto_garantias.createOrReplaceTempView('df_pgto_garantias')

# COMMAND ----------

df_pgto_garantias_somado = spark.sql('''
    select PROCESSO_ID
        ,DATA_EFETIVA_DO_PAGAMENTO
        ,CAST(date_trunc('MONTH', DATA_EFETIVA_DO_PAGAMENTO) AS DATE) AS MES_PGTO
        ,ACORDO + CONDENACAO + PENHORA + PENSAO + OUTROS_PAGAMENTOS + IMPOSTO + GARANTIA AS TOTAL_PAGAMENTOS
    from df_pgto_garantias
''')

df_pgto_garantias_somado.createOrReplaceTempView('df_pgto_garantias_somado')

# COMMAND ----------

df_pgto_garantias_somado_f = spark.sql('''
    SELECT PROCESSO_ID
    ,MES_PGTO
    ,SUM(TOTAL_PAGAMENTOS) AS TOTAL_PAGAMENTOS
    FROM df_pgto_garantias_somado
    GROUP BY 1, 2
''')

df_pgto_garantias_somado_f.createOrReplaceTempView('df_pgto_garantias_somado_f')

# COMMAND ----------

# MAGIC %md
# MAGIC ##### Cria uma tabela de encerrados alternativa

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM databox.juridico_comum.temp_provisao_trab_f_alt A
# MAGIC LEFT JOIN 
# MAGIC (SELECT ID_PROCESSO, COUNT(ID_PROCESSO) AS QTD FROM databox.juridico_comum.temp_provisao_trab_f_alt GROUP BY ID_PROCESSO) AS B
# MAGIC ON A.ID_PROCESSO = B.ID_PROCESSO AND B.QTD >= 2

# COMMAND ----------

df_encerrados_alt = spark.sql(f"""
    SELECT
    ID_PROCESSO,
    NATUREZA_OPERACIONAL_M,
    DP_FASE,
    MOTIVO_ENC_AGRP,
    MES_FECH,
    TOTAL_PAGAMENTOS
    FROM
    TB_FECH_FIN_TRAB_PROVISAO_2
    WHERE
    ENCERRADOS = 1
    AND MES_FECH >= '2021-01-01'
    ORDER BY
    ID_PROCESSO
""")

df_encerrados_alt.createOrReplaceTempView('df_encerrados_alt')

# COMMAND ----------

# df_temp_provisao_trab_com_pagamentos_1 = spark.sql('''
#     SELECT A.*
#     ,B.QTD_PAGAMENTOS
#     FROM databox.juridico_comum.temp_provisao_trab_f_alt A
#     LEFT JOIN (
#     SELECT 
#         PROCESSO_ID,
#         MES_PGTO,
#         COUNT(ID_DO_PAGAMENTO) AS QTD_PAGAMENTOS
#         FROM df_pagamentos_filtro
#         GROUP BY PROCESSO_ID, MES_PGTO) as B
#     ON A.ID_PROCESSO = B.PROCESSO_ID AND A.MES_PROV = B.MES_PGTO
# ''')

# df_temp_provisao_trab_com_pagamentos_1.createOrReplaceTempView('df_temp_provisao_trab_com_pagamentos_1')


# COMMAND ----------

# MAGIC %md
# MAGIC #### Rotina alternativa Acima

# COMMAND ----------

############
# Utilize esse SELECT caso já tenha rodado o Loop acima.
###########

df_saldo_tot_provisao_f = spark.sql("SELECT * FROM databox.juridico_comum.temp_provisao_trab_f")

# COMMAND ----------

# display(df_saldo_tot_provisao_f)

# COMMAND ----------

# Exportada como SHEET=PROVISAO_TRAB
df_saldo_tot_provisao_f.createOrReplaceTempView("TB_SALDO_TOT_PROVISAO_F")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 3 - Outras tabelas

# COMMAND ----------

# MAGIC %md
# MAGIC ### 3.1 TB_PROVISAO_TOTAL

# COMMAND ----------

# CRIA BASE ANALÍTICA COM OS PROCESSOS ENCERRADOS
df_provisao_total = spark.sql("""
SELECT DISTINCT
  A.ID_PROCESSO,
  A.NATUREZA_OPERACIONAL_M,
  A.DP_FASE,
  A.ESTRATEGIA,
  A.PROVISAO_MOV_M,
  A.PROVISAO_MOV_TOTAL_M,
  A.PROVISAO_TOTAL_M,
  A.PROVISAO_TOTAL_PASSIVO_M,
  A.TOTAL_PAGAMENTOS,
  A.MES_FECH
FROM
  TB_FECH_FIN_TRAB_PROVISAO_2 A
WHERE
  ENCERRADOS = 1
"""
)

# COMMAND ----------

# Exportada como SHEET=TB_PROVISAO_TOTAL
df_saldo_tot_provisao_f.createOrReplaceTempView("TB_PROVISAO_TOTAL")

# COMMAND ----------

# MAGIC %md
# MAGIC ### 3.2 TB_SALDO_TOT_PROVISAO_FF

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT DISTINCT
# MAGIC   A.ID_PROCESSO,
# MAGIC   A.NATUREZA_OPERACIONAL_M,
# MAGIC   A.DP_FASE,
# MAGIC   A.ESTRATEGIA,
# MAGIC   A.PROVISAO_MOV_M,
# MAGIC   A.PROVISAO_MOV_TOTAL_M,
# MAGIC   A.PROVISAO_TOTAL_M,
# MAGIC   A.PROVISAO_TOTAL_PASSIVO_M,
# MAGIC   A.TOTAL_PAGAMENTOS,
# MAGIC   A.MES_FECH
# MAGIC FROM
# MAGIC   TB_FECH_FIN_TRAB_PROVISAO_2 A
# MAGIC WHERE
# MAGIC   ENCERRADOS = 1

# COMMAND ----------

df_fech_trab_provisao_3 = spark.sql("""
    SELECT
        ID_PROCESSO,
        MES_FECH,
        AREA_DO_DIREITO,
        SUB_AREA_DO_DIREITO,
        FASE_M,
        ESTRATEGIA,
        INDICACAO_PROCESSO_ESTRATEGICO,
        PARCELAMENTO_CONDENACAO,
        PARCELAMENTO_ACORDO,
        ENCERRADOS
    FROM TB_FECH_FIN_TRAB_PROVISAO_2
    WHERE ENCERRADOS = 1
"""
)
df_fech_trab_provisao_3.createOrReplaceTempView("TB_FECH_FIN_TRAB_PROVISAO_3")

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM TB_FECH_FIN_TRAB_PROVISAO_3

# COMMAND ----------

df_fech_trab_provisao_ff = spark.sql("""
	SELECT A.*
			,B.AREA_DO_DIREITO
			,B.SUB_AREA_DO_DIREITO
			,B.FASE_M
			,B.ESTRATEGIA
			,B.INDICACAO_PROCESSO_ESTRATEGICO
			,B.PARCELAMENTO_CONDENACAO
			,B.PARCELAMENTO_ACORDO
			,cast(B.ENCERRADOS as int) as ENCERRADOS
	FROM TB_SALDO_TOT_PROVISAO_F AS A
	LEFT JOIN TB_FECH_FIN_TRAB_PROVISAO_3 AS B 
	ON A.ID_PROCESSO = B.ID_PROCESSO AND A.MES_FECH=B.MES_FECH
"""
)


# COMMAND ----------

distinct_mes_fech = df_fech_trab_provisao_ff.select("MES_FECH").distinct()
display(distinct_mes_fech)

# COMMAND ----------

display(df_fech_trab_provisao_ff)

# COMMAND ----------

# Exportada como SHEET=TB_ENCERR_X_PROVISAO_TRAB
df_fech_trab_provisao_ff.createOrReplaceTempView("TB_SALDO_TOT_PROVISAO_FF")

# COMMAND ----------

display(df_fech_trab_provisao_ff.limit(10))

# COMMAND ----------

# MAGIC %md
# MAGIC ### 3.3 TB_ENCERR_X_PROVISAO_TRAB

# COMMAND ----------

# TB_ENCERR_X_PROVISAO_TRAB_
df_encerr_x_provisao_trab = spark.sql("""
SELECT
  ID_PROCESSO,
  NATUREZA_OPERACIONAL_M,
  DP_FASE,
  MOTIVO_ENC_AGRP,
  VALOR_DE_PROVISAO,
  TOTAL_PAGAMENTOS,
  MES_FECH,
  AREA_DO_DIREITO,
  SUB_AREA_DO_DIREITO,
  FASE_M,
  ESTRATEGIA,
  INDICACAO_PROCESSO_ESTRATEGICO,
  PARCELAMENTO_CONDENACAO,
  PARCELAMENTO_ACORDO,
  ENCERRADOS
FROM
  TB_SALDO_TOT_PROVISAO_FF
"""
)



# COMMAND ----------

df_encerr_x_provisao_trab.createOrReplaceTempView("TB_ENCERR_X_PROVISAO_TRAB_")

# COMMAND ----------

# MAGIC %md
# MAGIC # VALIDAÇÃO

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT
# MAGIC   MES_FECH,
# MAGIC   DP_FASE,
# MAGIC   count(ID_PROCESSO) AS QTD_PROCESSOS,
# MAGIC   sum(TOTAL_PAGAMENTOS) AS TOTAL_PAGAMENTOS
# MAGIC FROM
# MAGIC   databox.juridico_comum.tmp_provisao_f
# MAGIC GROUP BY
# MAGIC   ALL
# MAGIC ORDER BY 1, 2

# COMMAND ----------

# Exporta para excel 
import pandas as pd
import re
from shutil import copyfile

mes_fechamento = dbutils.widgets.get("mes_fechamento")

# Converter PySpark DataFrame para Pandas DataFrame
pandas_df = df_encerr_x_provisao_trab.toPandas()

# Salvar o DataFrame com os nomes corrigidos em um arquivo Excel no disco local primeiro
local_path = f'/local_disk0/tmp/df_encerr_x_provisao_trab_{mes_fechamento}_2.xlsx'
pandas_df.to_excel(local_path, index=False, sheet_name='TB_FECHAM_FINANC_TRAB')

# Copiar o arquivo do disco local para o volume desejado
volume_path = f'/Volumes/databox/juridico_comum/arquivos/trabalhista/bases_fechamento_financeiro/trabalhista_base_financeiro/df_encerr_x_provisao_trab_{mes_fechamento}_2.xlsx'

copyfile(local_path, volume_path)
