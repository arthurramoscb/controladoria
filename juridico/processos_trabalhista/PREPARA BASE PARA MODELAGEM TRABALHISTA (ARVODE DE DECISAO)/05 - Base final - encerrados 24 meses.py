# Databricks notebook source
# MAGIC %md
# MAGIC ####Carrega os filtros para a entrada dos parâmetros de datas 

# COMMAND ----------

# Parametro de data da tabela MESFECH. Ex: 01/04/2024
dbutils.widgets.text("mes_fechamento", "")

# Parametro do formato de data do arquivo. Ex: 20240423
dbutils.widgets.text("nmtabger", "")

# Parametro do formato do nome da tabela gerada ao final. Ex: 202404
dbutils.widgets.text("nmmes", "")

# COMMAND ----------

# MAGIC %md
# MAGIC ####Carga e tratamentos iniciais

# COMMAND ----------

# MAGIC %run "/Workspace/Jurídico/funcao_tratamento_fechamento/common_functions"

# COMMAND ----------

# MAGIC %run "./fech_elaw"

# COMMAND ----------

# MAGIC %md
# MAGIC ####Carrega o histórico com os últimos 24 meses fechamentos financeiros

# COMMAND ----------

nmmes = dbutils.widgets.get("nmmes")

# Carrega histórico dos fechamentos financeiros
df_pagamentos_hist_24_meses = spark.sql(f"select * from databox.juridico_comum.tb_fecham_financ_trab_{nmmes} where MES_FECH >= '2023-12-01' and ENCERRADOS = 1" )

# COMMAND ----------

df_pagamentos_hist_24_meses.count()

# COMMAND ----------

# MAGIC %md
# MAGIC ####Verifica se todas as bases dos últimos 24 meses foram carregadas no Dataframe

# COMMAND ----------

from pyspark.sql import functions as F

df_pagamentos_hist_24_meses_grouped = df_pagamentos_hist_24_meses.groupBy("MES_FECH").agg(
    F.count("*").alias("QTD_MES")
)

df_pagamentos_hist_24_meses_grouped_ordered = df_pagamentos_hist_24_meses_grouped.orderBy("MES_FECH")

display(df_pagamentos_hist_24_meses_grouped_ordered)

# COMMAND ----------

# MAGIC %md
# MAGIC ####Carrega a data do Fechamento do eLaw

# COMMAND ----------

df_pagamentos_hist_24_meses.createOrReplaceTempView("PAGAMENTOS_HIST_24_MESES")
df_fech_elaw.createOrReplaceTempView("TB_FECH_ELAW")

df_pagamentos_hist_24_meses = spark.sql("""
    SELECT 
    A.*
    ,B.fech_elaw AS FECH_ELAW
FROM
    PAGAMENTOS_HIST_24_MESES A
LEFT JOIN
    TB_FECH_ELAW B
ON
    A.MES_FECH = B.MES_FECH
""")

# Apaga a coluna "Distribuição" e busca na base gerencial
df_pagamentos_hist_24_meses = df_pagamentos_hist_24_meses.drop("DISTRIBUICAO")

# COMMAND ----------

# MAGIC %md
# MAGIC ####Carrega a base gerencial

# COMMAND ----------

# Cria o dataframe com a base gerencial do mês de referência
nmtabela = dbutils.widgets.get("nmtabger")
df_consolidado = spark.table(f"databox.juridico_comum.trab_ger_consolida_{nmtabela}")

# Tira a duplicidade da base pela coluna do ID do processo
df_consolidado = df_consolidado.dropDuplicates(["PROCESSO_ID"])


# COMMAND ----------

df_consolidado.createOrReplaceTempView("TB_GER_CONSOLIDADA")
df_pagamentos_hist_24_meses.createOrReplaceTempView("PAGAMENTOS_HIST_24_MESES")

df_pagamentos_hist_24_meses = spark.sql("""
    SELECT A.*
            ,(CASE WHEN B.DISTRIBUICAO IS NULL THEN A.DATACADASTRO
                ELSE B.DISTRIBUICAO END) AS DISTRIBUICAO
   
FROM PAGAMENTOS_HIST_24_MESES AS A
LEFT JOIN TB_GER_CONSOLIDADA AS B ON A.ID_PROCESSO = B.PROCESSO_ID
""")

# COMMAND ----------

# MAGIC %md
# MAGIC ####Trata o campo Cargo

# COMMAND ----------

df_pagamentos_hist_24_meses.createOrReplaceTempView("PAGAMENTOS_HIST_24_MESES")

# COMMAND ----------

# MAGIC %md
# MAGIC #####Parte 1

# COMMAND ----------

from pyspark.sql import functions as F

df_pagamentos_hist_24_meses_1 = spark.sql("""
SELECT
    *
    ,(CASE WHEN (PARTE_CONTRARIA_CARGO_GRUPO_1 = 'ASSESSOR DE VENDAS II') THEN 'VENDEDOR II'
	    WHEN (PARTE_CONTRARIA_CARGO_GRUPO_1 = 'VENDEDOR INTERNO II') THEN 'VENDEDOR II'

	    WHEN (PARTE_CONTRARIA_CARGO_GRUPO_1 = 'ASSESSOR TECNICO') THEN 'ASSESSOR PROD TECNOLOGIA'
	    WHEN (PARTE_CONTRARIA_CARGO_GRUPO_1 = 'ASSESSOR PROD TECNOLOGIA') THEN 'ASSESSOR PROD TECNOLOGIA'

	    WHEN (PARTE_CONTRARIA_CARGO_GRUPO_1 = 'ATENDENTE LOJA') THEN 'ASSESSOR DE ATENDIMENTO'
	    WHEN (PARTE_CONTRARIA_CARGO_GRUPO_1 = 'ASSISTENTE DE LOJAS MOBILE') THEN 'ASSESSOR DE ATENDIMENTO'
	    WHEN (PARTE_CONTRARIA_CARGO_GRUPO_1 = 'AUX ESTOQUE') THEN 'ASSESSOR DE ATENDIMENTO'
	    WHEN (PARTE_CONTRARIA_CARGO_GRUPO_1 = 'CAIXA') THEN 'ASSESSOR DE ATENDIMENTO'
	    WHEN (PARTE_CONTRARIA_CARGO_GRUPO_1 = 'ASSESSOR DE ATENDIMENTO') THEN 'ASSESSOR DE ATENDIMENTO'

	    WHEN (PARTE_CONTRARIA_CARGO_GRUPO_1 = 'COORD ADM LOJA') THEN 'CONS ADM LOJA'
	    WHEN (PARTE_CONTRARIA_CARGO_GRUPO_1 = 'COORD ADMINISTRATIVO LOJA') THEN 'CONS ADM LOJA'
	    WHEN (PARTE_CONTRARIA_CARGO_GRUPO_1 = 'COORDENADOR ADM LOJA I') THEN 'CONS ADM LOJA'
	    WHEN (PARTE_CONTRARIA_CARGO_GRUPO_1 = 'COORDENADOR ADM LOJA II') THEN 'CONS ADM LOJA'

	    WHEN (PARTE_CONTRARIA_CARGO_GRUPO_1 = 'VENDEDOR MOBILE') THEN 'VENDEDOR'
	    WHEN (PARTE_CONTRARIA_CARGO_GRUPO_1 = 'VENDEDOR MOVEIS PLANEJADOS') THEN 'VENDEDOR'
	    WHEN (PARTE_CONTRARIA_CARGO_GRUPO_1 = 'VENDEDOR INTERNO') THEN 'VENDEDOR'
	    WHEN (PARTE_CONTRARIA_CARGO_GRUPO_1 = 'CONS VENDAS') THEN 'VENDEDOR'
	    WHEN (PARTE_CONTRARIA_CARGO_GRUPO_1 = 'ASSESSOR DE VENDAS') THEN 'VENDEDOR'

	    WHEN (PARTE_CONTRARIA_CARGO_GRUPO_1 = 'AUX ESTOQUE 130') THEN 'BALCÃO 130'
	    WHEN (PARTE_CONTRARIA_CARGO_GRUPO_1 = 'CAIXA 130') THEN 'BALCÃO 130'
	    WHEN (PARTE_CONTRARIA_CARGO_GRUPO_1 = 'ASSISTENTE DE LOJAS MOBILE130') THEN 'BALCÃO 130'

	    ELSE PARTE_CONTRARIA_CARGO_GRUPO_1 END) AS PARTE_CONTRARIA_CARGO_GRUPO_
FROM PAGAMENTOS_HIST_24_MESES
"""
)

# Cria a coluna 'DISTRIBUICAO (ajustada)'
df_pagamentos_hist_24_meses_1 = df_pagamentos_hist_24_meses_1.withColumn("DISTRIBUICAO (ajustada)", 
                   F.expr("make_date(year(DISTRIBUICAO), month(DISTRIBUICAO), 1)"))

df_pagamentos_hist_24_meses_1.createOrReplaceTempView("PAGAMENTOS_HIST_24_MESES_1")

# COMMAND ----------

# MAGIC %md
# MAGIC #####Parte 2

# COMMAND ----------

df_pagamentos_hist_24_meses_2 = spark.sql("""
SELECT
    *
    ,(CASE WHEN NATUREZA_OPERACIONAL = 'TERCEIRO INSOLVENTE' THEN 'TERCEIRO INSOLVENTE'

	    WHEN NATUREZA_OPERACIONAL <> 'TERCEIRO INSOLVENTE' AND 
            PARTE_CONTRARIA_CARGO_GRUPO_1 IN ('AJUDANTE'
                                            ,'AJUDANTE EXTERNO'
                                            ,'ANALISTA'
                                            ,'AUXILIAR'
                                            ,'ASSESSOR DE ATENDIMENTO'
                                            ,'GERENTE'
                                            ,'MONTADOR'
                                            ,'MOTORISTA'
                                            ,'OPERADOR'
                                            ,'VENDEDOR') 
            THEN PARTE_CONTRARIA_CARGO_GRUPO_1
	            ELSE 'OUTROS' END) AS CARGO_AJUSTADO
FROM PAGAMENTOS_HIST_24_MESES_1
"""
)

df_pagamentos_hist_24_meses_2 = df_pagamentos_hist_24_meses_2.drop("PARTE_CONTRARIA_CARGO_GRUPO") \
    .withColumnRenamed("PARTE_CONTRARIA_CARGO_GRUPO_", "PARTE_CONTRARIA_CARGO_GRUPO")

df_pagamentos_hist_24_meses_2.createOrReplaceTempView("PAGAMENTOS_HIST_24_MESES_2")

display(df_pagamentos_hist_24_meses_2)

# COMMAND ----------

# MAGIC %md
# MAGIC #####Faz o "de para" do campo fase

# COMMAND ----------

from pyspark.sql import SparkSession

# Executar a transformação usando SQL
df_pagamentos_hist_24_meses_3 = spark.sql("""
    SELECT
        *,
        CASE
            WHEN PARTE_CONTRARIA_NOME LIKE '%MINISTERIO%' OR PARTE_CONTRARIA_NOME LIKE '%MINISTÉRIO%' THEN 'N/A'
            ELSE FASE
        END AS FASE_
    FROM PAGAMENTOS_HIST_24_MESES_2
""")

df_pagamentos_hist_24_meses_3 = df_pagamentos_hist_24_meses_3.drop("FASE") \
    .withColumnRenamed("FASE_", "FASE")
    
df_pagamentos_hist_24_meses_3.createOrReplaceTempView("PAGAMENTOS_HIST_24_MESES_3")

display(df_pagamentos_hist_24_meses_3)

# COMMAND ----------

# MAGIC %md
# MAGIC #####Cria o Cluster Aging Tempo de Empresa

# COMMAND ----------

df_pagamentos_hist_24_meses_4 = spark.sql("""
SELECT *
        ,(CASE WHEN FX_TEMPO_EMPRESA IS NULL THEN 'Sem info'
                ELSE FX_TEMPO_EMPRESA END) AS `Cluster Aging Tempo de Empresa` 
        
        ,(CASE WHEN CLUSTER_AGING IS NULL THEN 'Sem info'
                ELSE CLUSTER_AGING END) AS `Cluster Aging`

        ,(CASE WHEN SAFRA_RECLAMACAO IS NULL THEN 'Sem info'
                ELSE SAFRA_RECLAMACAO END) AS `Safra de Reclamação`

FROM PAGAMENTOS_HIST_24_MESES_3
"""
)

display(df_pagamentos_hist_24_meses_4)

# COMMAND ----------

# MAGIC %md
# MAGIC ####Converte as colunas para datas

# COMMAND ----------

from pyspark.sql import SparkSession
from pyspark.sql.functions import to_date

# Criar uma sessão Spark
spark = SparkSession.builder.appName("Exemplo").config("spark.sql.legacy.timeParserPolicy", "LEGACY").getOrCreate()

# Converter as colunas para data
df_pagamentos_hist_24_meses_5 = df_pagamentos_hist_24_meses_4.withColumn("MES_CADASTRO", to_date(df_pagamentos_hist_24_meses_4["MES_CADASTRO"], "yyyy-MM-dd")) \
                 .withColumn("MES_DATA_ADMISSAO", to_date(df_pagamentos_hist_24_meses_4["MES_DATA_ADMISSAO"], "yyyy-MM-dd")) \
                 .withColumn("MES_DATA_DISPENSA", to_date(df_pagamentos_hist_24_meses_4["MES_DATA_DISPENSA"], "yyyy-MM-dd")) \
                 .withColumn("DT_ULT_PGTO", to_date(df_pagamentos_hist_24_meses_4["DT_ULT_PGTO"], "yyyy-MM-dd")) 

# Mostrar o DataFrame resultante
display(df_pagamentos_hist_24_meses_5)

df_pagamentos_hist_24_meses_5.createOrReplaceTempView("PAGAMENTOS_HIST_24_MESES_5")

# COMMAND ----------

# MAGIC %md
# MAGIC #####Prepara a tabela final para exportar

# COMMAND ----------

df_pagamentos_hist_24_meses_6 = spark.sql("""
SELECT
    ID_PROCESSO 
    ,DATACADASTRO AS CADASTRO 
    ,MES_CADASTRO 
    ,AREA_DO_DIREITO 
    ,SUB_AREA_DO_DIREITO 
    ,VALOR_DA_CAUSA 
    ,ESCRITORIO_RESPONSAVEL 
    ,PARTE_CONTRARIA_CPF 
    ,PARTE_CONTRARIA_NOME 
    ,MATRICULA 
    ,PROCESSO_ESTEIRA AS CARTEIRA
    ,ESTADO
    ,COMARCA 
    ,CLASSIFICACAO 
    ,NATUREZA_OPERACIONAL 
    ,DATA_ADMISSAO 
    ,MES_DATA_ADMISSAO 
    ,DATA_DISPENSA 
    ,MES_DATA_DISPENSA
    ,DISTRIBUICAO 
    ,`DISTRIBUICAO (ajustada)`
    ,PARTE_CONTRARIA_CARGO_GRUPO 
    ,FASE 
    ,FILIAL 
    ,BANDEIRA AS BAND
    ,NOME_DA_LOJA
    ,'N/A' AS TIPO_BAND 
    ,ENCERRADOS 
    ,MOTIVO_ENCERRAMENTO 
    ,ACORDO 
    ,CONDENACAO 
    ,PENHORA 
    ,OUTROS_PAGAMENTOS 
    ,IMPOSTO 
    ,GARANTIA
    ,TOTAL_PAGAMENTOS 
    ,MES_FECH 
    ,FECH_ELAW 
    ,MOTIVO_ENC_AGRP 
    ,CARGO_AJUSTADO 
    ,TEMPO_EMPRESA_MESES 
    ,`Cluster Aging Tempo de Empresa`
    ,MESES_AGING_ENCERR 
    ,`Cluster Aging` 
    ,`Safra de Reclamação`
    ,TERCEIRO_AJUSTADO 
    ,ET 
    ,`DISTRIBUICAO (ajustada)` AS `Distribuição`
    ,(CASE WHEN DT_ULT_PGTO IS NULL OR DT_ULT_PGTO = '1900-01-01' THEN MES_FECH ELSE DT_ULT_PGTO END) AS ULT_PGTO
FROM PAGAMENTOS_HIST_24_MESES_5
"""
)

# Ajusta a coluna DT_ULT_PGTO
df_pagamentos_hist_24_meses_7 = df_pagamentos_hist_24_meses_6.withColumn("DT_ULT_PGTO", 
                   F.expr("make_date(year(ULT_PGTO), month(ULT_PGTO), 1)"))

# Apaga a coluna ULT_PGTO
df_pagamentos_hist_24_meses_7 = df_pagamentos_hist_24_meses_7.drop("ULT_PGTO")

# COMMAND ----------

# MAGIC %md
# MAGIC #####Salva o dataframe em excel e exporta

# COMMAND ----------

import pandas as pd
import re
from shutil import copyfile

# Converter PySpark DataFrame para Pandas DataFrame
pandas_df = df_pagamentos_hist_24_meses_7.toPandas()

# Salvar o DataFrame com os nomes corrigidos em um arquivo Excel no disco local primeiro
local_path = f'/local_disk0/tmp/NOVO_HISTORICO_24_11_25_TOT_PGTO.xlsx'
pandas_df.to_excel(local_path, index=False, sheet_name='OUT23_NOV25')

# Copiar o arquivo do disco local para o volume desejado
volume_path = f'/Volumes/databox/juridico_comum/arquivos/modelo_provisao/output/Novo Histórico 24_11_25 Tot Pgto.xlsx'

copyfile(local_path, volume_path)
