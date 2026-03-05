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

# MAGIC %md
# MAGIC Comando original:
# MAGIC
# MAGIC **%run "./fech_elaw"**
# MAGIC
# MAGIC Testando abaixo com mes_fech
# MAGIC
# MAGIC é para ser uma regra: 
# MAGIC '''
# MAGIC '01JAN2024'D = '23/01/2024'
# MAGIC '01FEB2024'D = '22/02/2024'
# MAGIC '01MAR2024'D = '22/03/2024'''

# COMMAND ----------

# MAGIC %run "/Workspace/Jurídico/processos trabalhista/modelo de provisão trabalhista/fech_elaw" 

# COMMAND ----------

# MAGIC %md
# MAGIC ####Carrega o histórico com os últimos 24 meses fechamentos financeiros

# COMMAND ----------

from datetime import datetime
from dateutil.relativedelta import relativedelta

nmmes = dbutils.widgets.get("nmmes")
nmmesdia = nmmes + '01'
dt_nmmes = datetime.strptime(nmmesdia, '%Y%m%d')
dt_corte_inicial_ = dt_nmmes - relativedelta(months=23)
dt_corte_inicial = "'" + dt_corte_inicial_.strftime('%Y-%m-%d') + "'"

dt_corte_inicial = "'2021-01-01'"

print(f"Cortando a partir de: {dt_corte_inicial}")

# Carrega histórico dos fechamentos financeiros
df_pagamentos_hist_24_meses = spark.sql(f"select * from databox.juridico_comum.tb_fecham_financ_trab_{nmmes} where MES_FECH >= {dt_corte_inicial} and ENCERRADOS = 1" )

# COMMAND ----------

df_pagamentos_hist_24_meses.count() # 18459

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

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC SELECT * FROM PAGAMENTOS_HIST_24_MESES

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

# MAGIC %sql
# MAGIC SELECT * FROM TB_FECH_ELAW

# COMMAND ----------

# MAGIC %md
# MAGIC ####Carrega a base gerencial

# COMMAND ----------

# Cria o dataframe com a base gerencial do mês de referência
nmtabela = dbutils.widgets.get("nmtabger")
print(nmtabela)
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

df_teste = spark.sql('''
                     SELECT * FROM PAGAMENTOS_HIST_24_MESES
                     ''')

c = df_teste.columns

print(c)

# COMMAND ----------

# MAGIC %md
# MAGIC #####Parte 1

# COMMAND ----------

from pyspark.sql import functions as F

df_pagamentos_hist_24_meses_1 = spark.sql("""
SELECT
    *
    ,(CASE WHEN (PARTE_CONTRARIA_CARGO_GRUPO = 'ASSESSOR DE VENDAS II') THEN 'VENDEDOR II'
	    WHEN (PARTE_CONTRARIA_CARGO_GRUPO = 'VENDEDOR INTERNO II') THEN 'VENDEDOR II'

	    WHEN (PARTE_CONTRARIA_CARGO_GRUPO = 'ASSESSOR TECNICO') THEN 'ASSESSOR PROD TECNOLOGIA'
	    WHEN (PARTE_CONTRARIA_CARGO_GRUPO = 'ASSESSOR PROD TECNOLOGIA') THEN 'ASSESSOR PROD TECNOLOGIA'

	    WHEN (PARTE_CONTRARIA_CARGO_GRUPO = 'ATENDENTE LOJA') THEN 'ASSESSOR DE ATENDIMENTO'
	    WHEN (PARTE_CONTRARIA_CARGO_GRUPO = 'ASSISTENTE DE LOJAS MOBILE') THEN 'ASSESSOR DE ATENDIMENTO'
	    WHEN (PARTE_CONTRARIA_CARGO_GRUPO = 'AUX ESTOQUE') THEN 'ASSESSOR DE ATENDIMENTO'
	    WHEN (PARTE_CONTRARIA_CARGO_GRUPO = 'CAIXA') THEN 'ASSESSOR DE ATENDIMENTO'
	    WHEN (PARTE_CONTRARIA_CARGO_GRUPO = 'ASSESSOR DE ATENDIMENTO') THEN 'ASSESSOR DE ATENDIMENTO'

	    WHEN (PARTE_CONTRARIA_CARGO_GRUPO = 'COORD ADM LOJA') THEN 'CONS ADM LOJA'
	    WHEN (PARTE_CONTRARIA_CARGO_GRUPO = 'COORD ADMINISTRATIVO LOJA') THEN 'CONS ADM LOJA'
	    WHEN (PARTE_CONTRARIA_CARGO_GRUPO = 'COORDENADOR ADM LOJA I') THEN 'CONS ADM LOJA'
	    WHEN (PARTE_CONTRARIA_CARGO_GRUPO = 'COORDENADOR ADM LOJA II') THEN 'CONS ADM LOJA'

	    WHEN (PARTE_CONTRARIA_CARGO_GRUPO = 'VENDEDOR MOBILE') THEN 'VENDEDOR'
	    WHEN (PARTE_CONTRARIA_CARGO_GRUPO = 'VENDEDOR MOVEIS PLANEJADOS') THEN 'VENDEDOR'
	    WHEN (PARTE_CONTRARIA_CARGO_GRUPO = 'VENDEDOR INTERNO') THEN 'VENDEDOR'
	    WHEN (PARTE_CONTRARIA_CARGO_GRUPO = 'CONS VENDAS') THEN 'VENDEDOR'
	    WHEN (PARTE_CONTRARIA_CARGO_GRUPO = 'ASSESSOR DE VENDAS') THEN 'VENDEDOR'

	    WHEN (PARTE_CONTRARIA_CARGO_GRUPO = 'AUX ESTOQUE 130') THEN 'BALCÃO 130'
	    WHEN (PARTE_CONTRARIA_CARGO_GRUPO = 'CAIXA 130') THEN 'BALCÃO 130'
	    WHEN (PARTE_CONTRARIA_CARGO_GRUPO = 'ASSISTENTE DE LOJAS MOBILE130') THEN 'BALCÃO 130'

	    ELSE PARTE_CONTRARIA_CARGO_GRUPO END) AS PARTE_CONTRARIA_CARGO_GRUPO_
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
            PARTE_CONTRARIA_CARGO_GRUPO_ IN ('AJUDANTE',
										'AJUDANTE EXTERNO',
										'ANALISTA',
										'AUXILIAR',
										'ASSESSOR DE ATENDIMENTO',
										'GERENTE',
										'MONTADOR',
										'MOTORISTA',
										'OPERADOR',
										'VENDEDOR')
            THEN PARTE_CONTRARIA_CARGO_GRUPO_
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

df_pagamentos_hist_24_meses_7.createOrReplaceTempView("PAGAMENTOS_HIST_24_MESES_7")

# COMMAND ----------

# MAGIC %md
# MAGIC ### Atualiza com pagamentos mais recente

# COMMAND ----------

df_stg_pagamentos = spark.sql('''
   select * from databox.juridico_comum.stg_trab_pgto_garantias                           
''')

# COMMAND ----------

import time
from datetime import datetime
from pyspark.sql.functions import min
import calendar

nmmes = dbutils.widgets.get("nmmes")
nmmesdia = nmmes  + '01'

dt_corte_time = datetime.strptime(nmmesdia, '%Y%m%d')
dt_corte_number_last = calendar.monthrange(dt_corte_time.year, dt_corte_time.month)[1]
dt_corte_time_last = dt_corte_time.replace(day=dt_corte_number_last)
dt_corte = "'" + dt_corte_time_last.strftime('%Y-%m-%d') + "'"

df_stg_trab_pgto_garantias_agg = spark.sql(f'''
SELECT PROCESSO_ID
        ,MAX(DATA_EFETIVA_DO_PAGAMENTO) AS DT_ULT_PGTO
        ,SUM(coalesce(ACORDO, 0)) AS ACORDO
        ,SUM(coalesce(CONDENACAO, 0)) AS CONDENACAO
        ,SUM(coalesce(PENHORA, 0)) AS PENHORA
        ,SUM(coalesce(PENSAO, 0)) AS PENSAO
        ,SUM(coalesce(OUTROS_PAGAMENTOS, 0)) AS OUTROS_PAGAMENTOS
        ,SUM(coalesce(IMPOSTO, 0)) AS IMPOSTO
        ,SUM(coalesce(GARANTIA, 0)) AS GARANTIA
        ,SUM(coalesce(TOTAL_PAGAMENTOS, 0)) AS TOTAL_PAGAMENTOS
    FROM databox.juridico_comum.stg_trab_pgto_garantias WHERE DATA_EFETIVA_DO_PAGAMENTO <= {dt_corte}
    GROUP BY 1 
    ORDER BY 1
''')

menor_data = '1900-01-01'
menor_data = df_stg_trab_pgto_garantias_agg.select(min("DT_ULT_PGTO")).first()[0]

print(f'Pagamentos obtidos de: {menor_data} até: {dt_corte}')

df_stg_trab_pgto_garantias_agg.createOrReplaceTempView('df_stg_trab_pgto_garantias_agg')


# COMMAND ----------

df_pagamentos_hist_24_meses_8 = spark.sql('''
    SELECT A.* 
    ,B.ACORDO as ACORDO_ALT	
    ,B.CONDENACAO AS CONDENACAO_ALT
    ,B.PENHORA AS PENHORA_ALT
    ,B.OUTROS_PAGAMENTOS AS OUTROS_PAGAMENTOS_ALT
    ,B.IMPOSTO AS IMPOSTO_ALT
    ,B.GARANTIA AS GARANTIA_ALT
    ,B.DT_ULT_PGTO AS DT_ULT_PGTO_ALT
    ,B.TOTAL_PAGAMENTOS AS TOTAL_PAGAMENTOS_ALT
    ,CASE 
        WHEN B.ACORDO > 1 THEN 'ACORDO'
        WHEN (B.CONDENACAO + B.PENHORA + B.OUTROS_PAGAMENTOS + B.IMPOSTO + B.GARANTIA) > 1 THEN 'CONDENACAO'
        WHEN (B.ACORDO + B.CONDENACAO + B.PENHORA + B.OUTROS_PAGAMENTOS + B.IMPOSTO + B.GARANTIA) <= 1 THEN 'SEM ONUS'
    END AS MOTIVO_ENC_AGRP_ALT
    ,ROW_NUMBER() OVER (PARTITION BY A.ID_PROCESSO ORDER BY A.MES_FECH DESC) AS row_number
    FROM PAGAMENTOS_HIST_24_MESES_7 A
    LEFT JOIN df_stg_trab_pgto_garantias_agg B 
    ON A.ID_PROCESSO = B.PROCESSO_ID
''')

df_pagamentos_hist_24_meses_8.createOrReplaceTempView('df_pagamentos_hist_24_meses_8')

# COMMAND ----------

df_pagamentos_hist_24_meses_9 = spark.sql('''
    SELECT
        ID_PROCESSO 
        ,CADASTRO
        ,MES_CADASTRO 
        ,AREA_DO_DIREITO 
        ,SUB_AREA_DO_DIREITO 
        ,VALOR_DA_CAUSA 
        ,ESCRITORIO_RESPONSAVEL 
        ,PARTE_CONTRARIA_CPF 
        ,PARTE_CONTRARIA_NOME 
        ,MATRICULA 
        ,CARTEIRA
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
        ,BAND
        ,NOME_DA_LOJA
        ,TIPO_BAND
        ,ENCERRADOS 
        ,MOTIVO_ENCERRAMENTO 
        ,coalesce(ACORDO_ALT, 0) as ACORDO 
        ,coalesce(CONDENACAO_ALT, 0) AS CONDENACAO
        ,coalesce(PENHORA_ALT, 0) AS PENHORA
        ,coalesce(OUTROS_PAGAMENTOS_ALT, 0) AS OUTROS_PAGAMENTOS
        ,coalesce(IMPOSTO_ALT, 0) AS IMPOSTO
        ,coalesce(GARANTIA_ALT, 0) AS GARANTIA
        ,coalesce(TOTAL_PAGAMENTOS_ALT, 0) AS TOTAL_PAGAMENTOS
        ,MES_FECH 
        ,FECH_ELAW 
        ,(CASE
            WHEN MOTIVO_ENC_AGRP_ALT IS NOT NULL THEN MOTIVO_ENC_AGRP_ALT
            WHEN MOTIVO_ENC_AGRP_ALT IS NULL AND MOTIVO_ENC_AGRP IS NOT NULL THEN MOTIVO_ENC_AGRP
            ELSE ''        
         END) AS MOTIVO_ENC_AGRP
        ,CARGO_AJUSTADO 
        ,TEMPO_EMPRESA_MESES 
        ,`Cluster Aging Tempo de Empresa`
        ,MESES_AGING_ENCERR 
        ,`Cluster Aging` 
        ,`Safra de Reclamação`
        ,TERCEIRO_AJUSTADO 
        ,ET 
        ,`DISTRIBUICAO (ajustada)` AS `Distribuição`
        ,(CASE 
            WHEN DT_ULT_PGTO_ALT IS NOT NULL AND DT_ULT_PGTO_ALT > '1900-01-01' THEN DT_ULT_PGTO_ALT 
            WHEN DT_ULT_PGTO_ALT IS NULL AND DT_ULT_PGTO IS NOT NULL THEN DT_ULT_PGTO
            WHEN DT_ULT_PGTO_ALT <= '1900-01-01' AND DT_ULT_PGTO IS NULL THEN MES_FECH
        END) AS ULT_PGTO

    FROM df_pagamentos_hist_24_meses_8
    where row_number = 1
''')

df_pagamentos_hist_24_meses_9.createOrReplaceTempView('df_pagamentos_hist_24_meses_9')

# Ajusta o data do ultimo pagamento com padrao ANO-MES-01
df_pagamentos_hist_24_meses_10 = df_pagamentos_hist_24_meses_9.withColumn("DT_ULT_PGTO", 
                   F.expr("make_date(year(ULT_PGTO), month(ULT_PGTO), 1)"))

# Apaga a coluna ULT_PGTO
df_pagamentos_hist_24_meses_10 = df_pagamentos_hist_24_meses_10.drop("ULT_PGTO")

# COMMAND ----------

# MAGIC %md
# MAGIC #####Salva o dataframe em excel e exporta

# COMMAND ----------

import pandas as pd
import re
from shutil import copyfile
import os

# Obtem o nome da gerencial utilizada para o nome do arquivo final
nmtabela = dbutils.widgets.get("nmtabger")

# Converter PySpark DataFrame para Pandas DataFrame
try:
    pandas_df = df_pagamentos_hist_24_meses_10.toPandas()
except:
    pass

# Salvar o DataFrame com os nomes corrigidos em um arquivo Excel no disco local primeiro
local_path = '/local_disk0/tmp/Tot_pgto.xlsx'
pandas_df.to_excel(local_path, index=False, sheet_name='Dez22_Nov24')

# Definir o nome do arquivo
qtd = 0
path = '/Volumes/databox/juridico_comum/arquivos/modelo_provisao/output/'
list_files = dbutils.fs.ls(path)
nome_arquivo = f'Novo Histórico {nmtabela} Tot Pgto'

# Essa rotina verifica quantos arquivos com o nome escolhido existem na pasta e faz um controle de versao com base na quantidade
for file in list_files:
    name = file.name
    if nome_arquivo in name:
        qtd += 1
        print(f'{name} | {qtd} \n')

if qtd == 0:
    print(f'Ainda não foi criado nenhum arquivo com o nome: {nome_arquivo}')
if qtd >= 1:
    new_qtd = qtd +1
    nome_arquivo = f'Novo Histórico {nmtabela} Tot Pgto_{new_qtd}'


# Copiar o arquivo do disco local para o volume desejado
volume_path = f'/Volumes/databox/juridico_comum/arquivos/modelo_provisao/output/{nome_arquivo}.xlsx'
copyfile(local_path, volume_path)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Tabelão

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC ----------------------- ENTRADAS ------------------------
# MAGIC
# MAGIC SELECT 'ENTRADAS' AS INDICADOR, MES_FECH, 'TOTAL' AS SUB_INDICADOR, 
# MAGIC     COUNT(NOVOS) AS VALOR, 
# MAGIC     SUM(CASE WHEN NOVOS = 1 THEN coalesce(PROVISAO_TOTAL_PASSIVO_M, 0) ELSE 0 END) AS PROVISAO_TOTAL_PASSIVO_M
# MAGIC     ,SUM(CASE WHEN NOVOS = 1 THEN COALESCE(TOTAL_PAGAMENTOS, 0) ELSE 0 END) AS TOTAL_PAGAMENTOS
# MAGIC     ,SUM(CASE WHEN NOVOS = 1 THEN COALESCE(TOTAL_PAGAMENTOS, 0) ELSE 0 END) / COUNT(NOVOS) AS TKM_MEDIO
# MAGIC FROM df_23e24
# MAGIC GROUP BY MES_FECH
# MAGIC
# MAGIC UNION ALL
# MAGIC
# MAGIC SELECT 'ENTRADAS' AS INDICADOR, MES_FECH, 'NOVO' AS SUB_INDICADOR, 
# MAGIC        COUNT(CASE WHEN NOVO_X_LEGADO = 'NOVO' AND NOVOS = 1 THEN 1 END) AS VALOR
# MAGIC        ,SUM(CASE WHEN NOVO_X_LEGADO = 'NOVO' AND NOVOS = 1 THEN coalesce(PROVISAO_TOTAL_PASSIVO_M, 0) ELSE 0 END) AS PROVISAO_TOTAL_PASSIVO_M
# MAGIC        ,SUM(CASE WHEN NOVOS = 1 AND NOVO_X_LEGADO = 'NOVO' THEN COALESCE(TOTAL_PAGAMENTOS, 0) ELSE 0 END) AS TOTAL_PAGAMENTOS
# MAGIC       ,SUM(CASE WHEN NOVOS = 1 AND NOVO_X_LEGADO = 'NOVO' THEN COALESCE(TOTAL_PAGAMENTOS, 0) ELSE 0 END) / COUNT(NOVOS) AS TKM_MEDIO
# MAGIC FROM df_23e24
# MAGIC GROUP BY MES_FECH
# MAGIC
# MAGIC UNION ALL
# MAGIC
# MAGIC SELECT 'ENTRADAS' AS INDICADOR, MES_FECH, 'LEGADO' AS SUB_INDICADOR, 
# MAGIC        COUNT(CASE WHEN NOVO_X_LEGADO = 'LEGADO' AND NOVOS = 1 THEN 1 END) AS VALOR
# MAGIC        ,SUM(CASE WHEN NOVO_X_LEGADO = 'LEGADO' AND NOVOS = 1 THEN coalesce(PROVISAO_TOTAL_PASSIVO_M, 0) ELSE 0 END) AS PROVISAO_TOTAL_PASSIVO_M
# MAGIC        ,SUM(CASE WHEN NOVOS = 1 AND NOVO_X_LEGADO = 'LEGADO' THEN COALESCE(TOTAL_PAGAMENTOS, 0) ELSE 0 END) AS TOTAL_PAGAMENTOS
# MAGIC       ,SUM(CASE WHEN NOVOS = 1 AND NOVO_X_LEGADO = 'LEGADO' THEN COALESCE(TOTAL_PAGAMENTOS, 0) ELSE 0 END) / COUNT(NOVOS) AS TKM_MEDIO
# MAGIC FROM df_23e24
# MAGIC GROUP BY MES_FECH
# MAGIC
# MAGIC UNION ALL
# MAGIC
# MAGIC ----------------------- ENCERRADOS ------------------------
# MAGIC
# MAGIC SELECT 'ENCERRADOS' AS INDICADOR, MES_FECH, 'TOTAL' AS SUB_INDICADOR, 
# MAGIC     COUNT(ENCERRADOS) AS VALOR, 
# MAGIC     SUM(CASE WHEN ENCERRADOS = 1 THEN coalesce(PROVISAO_TOTAL_PASSIVO_M, 0) ELSE 0 END) AS PROVISAO_TOTAL_PASSIVO_M
# MAGIC     ,SUM(CASE WHEN ENCERRADOS = 1 THEN COALESCE(TOTAL_PAGAMENTOS, 0) ELSE 0 END) AS TOTAL_PAGAMENTOS
# MAGIC     ,SUM(CASE WHEN ENCERRADOS = 1 THEN COALESCE(TOTAL_PAGAMENTOS, 0) ELSE 0 END) / COUNT(ENCERRADOS) AS TKM_MEDIO
# MAGIC FROM df_23e24
# MAGIC GROUP BY MES_FECH
# MAGIC
# MAGIC UNION ALL
# MAGIC
# MAGIC SELECT 'ENCERRADOS' AS INDICADOR, MES_FECH, 'NOVO' AS SUB_INDICADOR, 
# MAGIC        COUNT(CASE WHEN NOVO_X_LEGADO = 'NOVO' AND ENCERRADOS = 1 THEN 1 END) AS VALOR
# MAGIC        ,SUM(CASE WHEN NOVO_X_LEGADO = 'NOVO' AND ENCERRADOS = 1 THEN coalesce(PROVISAO_TOTAL_PASSIVO_M, 0) ELSE 0 END) AS PROVISAO_TOTAL_PASSIVO_M
# MAGIC        ,SUM(CASE WHEN ENCERRADOS = 1 AND NOVO_X_LEGADO = 'NOVO' THEN COALESCE(TOTAL_PAGAMENTOS, 0) ELSE 0 END) AS TOTAL_PAGAMENTOS
# MAGIC     ,SUM(CASE WHEN ENCERRADOS = 1 AND NOVO_X_LEGADO = 'NOVO' THEN COALESCE(TOTAL_PAGAMENTOS, 0) ELSE 0 END) / COUNT(ENCERRADOS) AS TKM_MEDIO
# MAGIC FROM df_23e24
# MAGIC GROUP BY MES_FECH
# MAGIC
# MAGIC UNION ALL
# MAGIC
# MAGIC SELECT 'ENCERRADOS' AS INDICADOR, MES_FECH, 'LEGADO' AS SUB_INDICADOR, 
# MAGIC        COUNT(CASE WHEN NOVO_X_LEGADO = 'LEGADO' AND ENCERRADOS = 1 THEN 1 END) AS VALOR
# MAGIC        ,SUM(CASE WHEN NOVO_X_LEGADO = 'LEGADO' AND ENCERRADOS = 1 THEN coalesce(PROVISAO_TOTAL_PASSIVO_M, 0) ELSE 0 END) AS PROVISAO_TOTAL_PASSIVO_M
# MAGIC        ,SUM(CASE WHEN ENCERRADOS = 1 AND NOVO_X_LEGADO = 'LEGADO' THEN COALESCE(TOTAL_PAGAMENTOS, 0) ELSE 0 END) AS TOTAL_PAGAMENTOS
# MAGIC       ,SUM(CASE WHEN ENCERRADOS = 1 AND NOVO_X_LEGADO = 'LEGADO' THEN COALESCE(TOTAL_PAGAMENTOS, 0) ELSE 0 END) / COUNT(ENCERRADOS) AS TKM_MEDIO
# MAGIC FROM df_23e24
# MAGIC GROUP BY MES_FECH
# MAGIC
# MAGIC UNION ALL
# MAGIC
# MAGIC ----------------------- ESTOQUE ------------------------
# MAGIC
# MAGIC SELECT 'ESTOQUE' AS INDICADOR, MES_FECH, 'TOTAL' AS SUB_INDICADOR, 
# MAGIC     COUNT(ESTOQUE) AS VALOR, 
# MAGIC     SUM(CASE WHEN ESTOQUE = 1 THEN coalesce(PROVISAO_TOTAL_PASSIVO_M, 0) ELSE 0 END) AS PROVISAO_TOTAL_PASSIVO_M
# MAGIC     ,SUM(CASE WHEN ESTOQUE = 1 THEN COALESCE(TOTAL_PAGAMENTOS, 0) ELSE 0 END) AS TOTAL_PAGAMENTOS
# MAGIC     ,SUM(CASE WHEN ESTOQUE = 1 THEN COALESCE(TOTAL_PAGAMENTOS, 0) ELSE 0 END) / COUNT(ESTOQUE) AS TKM_MEDIO
# MAGIC FROM df_23e24
# MAGIC GROUP BY MES_FECH
# MAGIC
# MAGIC UNION ALL
# MAGIC
# MAGIC SELECT 'ESTOQUE' AS INDICADOR, MES_FECH, 'NOVO' AS SUB_INDICADOR, 
# MAGIC        COUNT(CASE WHEN NOVO_X_LEGADO = 'NOVO' AND ESTOQUE = 1 THEN 1 END) AS VALOR
# MAGIC        ,SUM(CASE WHEN NOVO_X_LEGADO = 'NOVO' AND ESTOQUE = 1 THEN coalesce(PROVISAO_TOTAL_PASSIVO_M, 0) ELSE 0 END) AS PROVISAO_TOTAL_PASSIVO_M
# MAGIC       ,SUM(CASE WHEN ESTOQUE = 1 AND NOVO_X_LEGADO = 'NOVO' THEN COALESCE(TOTAL_PAGAMENTOS, 0) ELSE 0 END) AS TOTAL_PAGAMENTOS
# MAGIC       ,SUM(CASE WHEN ESTOQUE = 1 AND NOVO_X_LEGADO = 'NOVO' THEN COALESCE(TOTAL_PAGAMENTOS, 0) ELSE 0 END) / COUNT(ESTOQUE) AS TKM_MEDIO
# MAGIC FROM df_23e24
# MAGIC GROUP BY MES_FECH
# MAGIC
# MAGIC UNION ALL
# MAGIC
# MAGIC SELECT 'ESTOQUE' AS INDICADOR, MES_FECH, 'LEGADO' AS SUB_INDICADOR, 
# MAGIC        COUNT(CASE WHEN NOVO_X_LEGADO = 'LEGADO' AND ESTOQUE = 1 THEN 1 END) AS VALOR
# MAGIC        ,SUM(CASE WHEN NOVO_X_LEGADO = 'LEGADO' AND ESTOQUE = 1 THEN coalesce(PROVISAO_TOTAL_PASSIVO_M, 0) ELSE 0 END) AS PROVISAO_TOTAL_PASSIVO_M
# MAGIC        ,SUM(CASE WHEN ESTOQUE = 1 AND NOVO_X_LEGADO = 'LEGADO' THEN COALESCE(TOTAL_PAGAMENTOS, 0) ELSE 0 END) AS TOTAL_PAGAMENTOS
# MAGIC       ,SUM(CASE WHEN ESTOQUE = 1 AND NOVO_X_LEGADO = 'LEGADO' THEN COALESCE(TOTAL_PAGAMENTOS, 0) ELSE 0 END) / COUNT(ESTOQUE) AS TKM_MEDIO
# MAGIC FROM df_23e24
# MAGIC GROUP BY MES_FECH
# MAGIC
# MAGIC UNION ALL
# MAGIC ----------------------- ESTOQUE SALDO PROVISÃO ------------------------
# MAGIC
# MAGIC SELECT 'ESTOQUE SALDO PROVISÃO' AS INDICADOR, MES_FECH, 'TOTAL' AS SUB_INDICADOR, 
# MAGIC     COUNT(CASE WHEN ESTOQUE = 1 AND coalesce(PROVISAO_TOTAL_PASSIVO_M, 0) <> 0 THEN 1 END) AS VALOR, 
# MAGIC     SUM(CASE WHEN ESTOQUE = 1 THEN coalesce(PROVISAO_TOTAL_PASSIVO_M, 0) ELSE 0 END) AS PROVISAO_TOTAL_PASSIVO_M
# MAGIC     ,SUM(CASE WHEN ESTOQUE = 1 AND coalesce(PROVISAO_TOTAL_PASSIVO_M, 0) <> 0 THEN COALESCE(TOTAL_PAGAMENTOS, 0) ELSE 0 END) AS TOTAL_PAGAMENTOS
# MAGIC     ,SUM(CASE WHEN ESTOQUE = 1 THEN COALESCE(PROVISAO_TOTAL_PASSIVO_M, 0) ELSE 0 END) / COUNT(ESTOQUE) AS TKM_MEDIO
# MAGIC FROM df_23e24
# MAGIC GROUP BY MES_FECH
# MAGIC
# MAGIC UNION ALL
# MAGIC
# MAGIC SELECT 'ESTOQUE SALDO PROVISÃO' AS INDICADOR, MES_FECH, 'NOVO' AS SUB_INDICADOR, 
# MAGIC        COUNT(CASE WHEN NOVO_X_LEGADO = 'NOVO' AND ESTOQUE = 1 AND coalesce(PROVISAO_TOTAL_PASSIVO_M, 0) <> 0 THEN 1 END) AS VALOR
# MAGIC       ,SUM(CASE WHEN NOVO_X_LEGADO = 'NOVO' AND ESTOQUE = 1 THEN coalesce(PROVISAO_TOTAL_PASSIVO_M, 0) ELSE 0 END) AS PROVISAO_TOTAL_PASSIVO_M
# MAGIC       ,SUM(CASE WHEN ESTOQUE = 1 AND coalesce(PROVISAO_TOTAL_PASSIVO_M, 0) <> 0 AND NOVO_X_LEGADO = 'NOVO' THEN COALESCE(TOTAL_PAGAMENTOS, 0) ELSE 0 END) AS TOTAL_PAGAMENTOS
# MAGIC       ,SUM(CASE WHEN ESTOQUE = 1 AND coalesce(PROVISAO_TOTAL_PASSIVO_M, 0) <> 0 AND NOVO_X_LEGADO = 'NOVO' THEN COALESCE(PROVISAO_TOTAL_PASSIVO_M, 0) ELSE 0 END) / COUNT(ESTOQUE) AS TKM_MEDIO
# MAGIC FROM df_23e24
# MAGIC GROUP BY MES_FECH
# MAGIC
# MAGIC UNION ALL
# MAGIC
# MAGIC SELECT 'ESTOQUE SALDO PROVISÃO' AS INDICADOR, MES_FECH, 'LEGADO' AS SUB_INDICADOR, 
# MAGIC        COUNT(CASE WHEN NOVO_X_LEGADO = 'LEGADO' AND ESTOQUE = 1 AND coalesce(PROVISAO_TOTAL_PASSIVO_M, 0) <> 0 THEN 1 END) AS VALOR
# MAGIC        ,SUM(CASE WHEN NOVO_X_LEGADO = 'LEGADO' AND ESTOQUE = 1 THEN coalesce(PROVISAO_TOTAL_PASSIVO_M, 0) ELSE 0 END) AS PROVISAO_TOTAL_PASSIVO_M
# MAGIC        ,SUM(CASE WHEN ESTOQUE = 1 AND coalesce(PROVISAO_TOTAL_PASSIVO_M, 0) <> 0 AND NOVO_X_LEGADO = 'LEGADO' THEN COALESCE(TOTAL_PAGAMENTOS, 0) ELSE 0 END) AS TOTAL_PAGAMENTOS
# MAGIC       ,SUM(CASE WHEN ESTOQUE = 1 AND coalesce(PROVISAO_TOTAL_PASSIVO_M, 0) <> 0 AND NOVO_X_LEGADO = 'LEGADO' THEN COALESCE(PROVISAO_TOTAL_PASSIVO_M, 0) ELSE 0 END) / COUNT(ESTOQUE) AS 
# MAGIC FROM df_23e24
# MAGIC GROUP BY MES_FECH
# MAGIC
# MAGIC UNION ALL
# MAGIC
# MAGIC ----------------------- TICKET MÉDIO PAGAMENTOS ------------------------
# MAGIC SELECT 'TICKET MÉDIO' AS INDICADOR, MES_FECH, 'TOTAL' AS SUB_INDICADOR, 
# MAGIC     COUNT(ENCERRADOS) AS VALOR, 
# MAGIC     SUM(CASE WHEN ENCERRADOS = 1 THEN coalesce(PROVISAO_TOTAL_PASSIVO_M, 0) ELSE 0 END) AS PROVISAO_TOTAL_PASSIVO_M
# MAGIC     ,SUM(CASE WHEN ENCERRADOS = 1 THEN COALESCE(TOTAL_PAGAMENTOS, 0) ELSE 0 END) AS TOTAL_PAGAMENTOS
# MAGIC     ,SUM(CASE WHEN ENCERRADOS = 1 THEN COALESCE(TOTAL_PAGAMENTOS, 0) ELSE 0 END) / COUNT(ENCERRADOS) AS TKM_MEDIO
# MAGIC FROM df_23e24
# MAGIC GROUP BY MES_FECH
# MAGIC
# MAGIC UNION ALL
# MAGIC
# MAGIC SELECT 'TICKET MÉDIO' AS INDICADOR, MES_FECH, 'NOVO' AS SUB_INDICADOR, 
# MAGIC     COUNT(CASE WHEN NOVO_X_LEGADO = 'NOVO' AND ENCERRADOS = 1 THEN 1 END) AS VALOR
# MAGIC     ,SUM(CASE WHEN ENCERRADOS = 1 AND NOVO_X_LEGADO = 'NOVO' THEN coalesce(PROVISAO_TOTAL_PASSIVO_M, 0) ELSE 0 END) AS PROVISAO_TOTAL_PASSIVO_M
# MAGIC     ,SUM(CASE WHEN ENCERRADOS = 1 AND NOVO_X_LEGADO = 'NOVO' THEN COALESCE(TOTAL_PAGAMENTOS, 0) ELSE 0 END) AS TOTAL_PAGAMENTOS
# MAGIC     ,SUM(CASE WHEN ENCERRADOS = 1 AND NOVO_X_LEGADO = 'NOVO' THEN COALESCE(TOTAL_PAGAMENTOS, 0) ELSE 0 END) / COUNT(CASE WHEN NOVO_X_LEGADO = 'NOVO' AND ENCERRADOS = 1 THEN 1 END) AS TKM_MEDIO
# MAGIC FROM df_23e24
# MAGIC GROUP BY MES_FECH
# MAGIC
# MAGIC UNION ALL
# MAGIC
# MAGIC SELECT 'TICKET MÉDIO' AS INDICADOR, MES_FECH, 'LEGADO' AS SUB_INDICADOR, 
# MAGIC     COUNT(CASE WHEN NOVO_X_LEGADO = 'LEGADO' AND ENCERRADOS = 1 THEN 1 END) AS VALOR
# MAGIC     ,SUM(CASE WHEN ENCERRADOS = 1 AND NOVO_X_LEGADO = 'LEGADO' THEN coalesce(PROVISAO_TOTAL_PASSIVO_M, 0) ELSE 0 END) AS PROVISAO_TOTAL_PASSIVO_M
# MAGIC     ,SUM(CASE WHEN ENCERRADOS = 1 AND NOVO_X_LEGADO = 'LEGADO' THEN COALESCE(TOTAL_PAGAMENTOS, 0) ELSE 0 END) AS TOTAL_PAGAMENTOS
# MAGIC     ,SUM(CASE WHEN ENCERRADOS = 1 AND NOVO_X_LEGADO = 'LEGADO' THEN COALESCE(TOTAL_PAGAMENTOS, 0) ELSE 0 END) / COUNT(CASE WHEN NOVO_X_LEGADO = 'LEGADO' AND ENCERRADOS = 1 THEN 1 END) AS TKM_MEDIO
# MAGIC FROM df_23e24
# MAGIC GROUP BY MES_FECH
# MAGIC
# MAGIC ----------------------- CAIXA (PAGAMENTOS + CRÉDITOS) ------------------------
# MAGIC
# MAGIC UNION ALL
# MAGIC
# MAGIC SELECT 'CAIXA' AS INDICADOR, A.MES_FECH, 'TOTAL' AS SUB_INDICADOR
# MAGIC     ,-(SUM(COALESCE(A.TOTAL_PAGAMENTOS, 0)) - MAX(coalesce(B.TOTAL_CREDITOS, 0)) ) AS VALOR, 
# MAGIC     0.0 AS PROVISAO_TOTAL_PASSIVO_M
# MAGIC     ,-SUM(COALESCE(A.TOTAL_PAGAMENTOS, 0)) AS TOTAL_PAGAMENTOS
# MAGIC     ,-(SUM(COALESCE(A.TOTAL_PAGAMENTOS, 0)) - MAX(coalesce(B.TOTAL_CREDITOS, 0)) )  / COUNT(ENCERRADOS) AS TKM_MEDIO
# MAGIC FROM df_23e24 A
# MAGIC LEFT JOIN df_creditos B
# MAGIC ON A.MES_FECH = B.MES_FECH
# MAGIC GROUP BY A.MES_FECH 
# MAGIC
# MAGIC UNION ALL
# MAGIC
# MAGIC SELECT 'CAIXA' AS INDICADOR, A.MES_FECH, 'NOVO' AS SUB_INDICADOR
# MAGIC     ,-SUM(CASE WHEN NOVO_X_LEGADO = 'NOVO' THEN COALESCE(A.TOTAL_PAGAMENTOS, 0) ELSE 0 END) + MAX(CASE WHEN NOVO_X_LEGADO = 'NOVO' THEN coalesce(B.TOTAL_CREDITOS, 0) ELSE 0 END) AS VALOR, 
# MAGIC     0.0 AS PROVISAO_TOTAL_PASSIVO_M
# MAGIC     ,-SUM(CASE WHEN NOVO_X_LEGADO = 'NOVO' THEN COALESCE(A.TOTAL_PAGAMENTOS, 0) ELSE 0 END) AS TOTAL_PAGAMENTOS
# MAGIC     ,-(SUM(CASE WHEN NOVO_X_LEGADO = 'NOVO' THEN COALESCE(A.TOTAL_PAGAMENTOS, 0) ELSE 0 END) - MAX(CASE WHEN NOVO_X_LEGADO = 'NOVO' THEN coalesce(B.TOTAL_CREDITOS, 0) ELSE 0 END) )  / COUNT(CASE WHEN NOVO_X_LEGADO = 'NOVO' THEN ENCERRADOS ELSE 0 END) AS TKM_MEDIO
# MAGIC FROM df_23e24 A
# MAGIC LEFT JOIN df_creditos B
# MAGIC ON A.MES_FECH = B.MES_FECH
# MAGIC GROUP BY A.MES_FECH 
# MAGIC
# MAGIC UNION ALL
# MAGIC
# MAGIC SELECT 'CAIXA' AS INDICADOR, A.MES_FECH, 'LEGADO' AS SUB_INDICADOR
# MAGIC     ,-SUM(CASE WHEN NOVO_X_LEGADO = 'LEGADO' THEN COALESCE(A.TOTAL_PAGAMENTOS, 0) ELSE 0 END) + MAX(CASE WHEN NOVO_X_LEGADO = 'LEGADO' THEN coalesce(B.TOTAL_CREDITOS, 0) ELSE 0 END) AS VALOR, 
# MAGIC     0.0 AS PROVISAO_TOTAL_PASSIVO_M
# MAGIC     ,-SUM(CASE WHEN NOVO_X_LEGADO = 'LEGADO' THEN COALESCE(A.TOTAL_PAGAMENTOS, 0) ELSE 0 END) AS TOTAL_PAGAMENTOS
# MAGIC     ,-(SUM(CASE WHEN NOVO_X_LEGADO = 'LEGADO' THEN COALESCE(A.TOTAL_PAGAMENTOS, 0) ELSE 0 END) - MAX(CASE WHEN NOVO_X_LEGADO = 'LEGADO' THEN coalesce(B.TOTAL_CREDITOS, 0) ELSE 0 END) )  / COUNT(CASE WHEN NOVO_X_LEGADO = 'LEGADO' THEN ENCERRADOS ELSE 0 END) AS TKM_MEDIO
# MAGIC FROM df_23e24 A
# MAGIC LEFT JOIN df_creditos B
# MAGIC ON A.MES_FECH = B.MES_FECH
# MAGIC GROUP BY A.MES_FECH 
# MAGIC
# MAGIC ----------------------- RESULTADO ------------------------
# MAGIC
# MAGIC UNION ALL
# MAGIC
# MAGIC SELECT 'RESULTADO' AS INDICADOR, MES_FECH, 'TOTAL' AS SUB_INDICADOR, 
# MAGIC     SUM(COALESCE(PROVISAO_MOV_M, 0)) AS VALOR, 
# MAGIC     SUM(CASE WHEN NOVOS = 1 THEN coalesce(PROVISAO_TOTAL_PASSIVO_M, 0) ELSE 0 END) AS PROVISAO_TOTAL_PASSIVO_M
# MAGIC     ,SUM(CASE WHEN NOVOS = 1 THEN COALESCE(TOTAL_PAGAMENTOS, 0) ELSE 0 END) AS TOTAL_PAGAMENTOS
# MAGIC     ,SUM(CASE WHEN NOVOS = 1 THEN COALESCE(TOTAL_PAGAMENTOS, 0) ELSE 0 END) / COUNT(NOVOS) AS TKM_MEDIO
# MAGIC FROM df_23e24
# MAGIC GROUP BY MES_FECH
# MAGIC
# MAGIC

# COMMAND ----------

# MAGIC %sql
# MAGIC select MES_FECH, COUNT(NOVOS) AS NOVOS, COUNT(ENCERRADOS) AS ENCERRADOS, COUNT(ESTOQUE) AS ESTOQUE
# MAGIC   FROM databox.juridico_comum.tb_fecham_financ_trab_202412 GROUP BY MES_FECH
