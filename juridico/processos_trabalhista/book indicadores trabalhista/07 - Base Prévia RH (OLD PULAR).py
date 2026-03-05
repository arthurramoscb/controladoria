# Databricks notebook source
# MAGIC %md
# MAGIC #Base Prévia RH
# MAGIC #####Inputs
# MAGIC Tabela: Previa RH EX formatação: (Previa_20240815) pelo time de People Analytics - Recursos Humanos
# MAGIC
# MAGIC #####Planilhas:
# MAGIC Previa RH
# MAGIC
# MAGIC /Volumes/databox/juridico_comum/arquivos/trabalhista/bases_rh/previa/
# MAGIC
# MAGIC #####Outputs
# MAGIC Arquivos Excel

# COMMAND ----------

# Parametro do nome da tabela da competência atual. Ex: 202404
dbutils.widgets.text("nmmes", "")

# Parametro nome da tabela gerencial. Ex: 20240423
dbutils.widgets.text("dtgerencial", "")

dbutils.widgets.text("nmprevia", "")

# COMMAND ----------

# MAGIC %run "/Workspace/Jurídico/funcao_tratamento_fechamento/common_functions"

# COMMAND ----------

# MAGIC %md
# MAGIC ## 1 - Imports

# COMMAND ----------

# MAGIC %md
# MAGIC ### 1.1 Elenca arquivos

# COMMAND ----------

rh_path = '/Volumes/databox/juridico_comum/arquivos/trabalhista/bases_rh/'

arquivos = dbutils.fs.ls(rh_path+'previa/')

lista_arquivos = [arquivo.name for arquivo in arquivos]

# COMMAND ----------

# Lista arquivos fechamento
lista_arquivos_filtrado = []

for s in lista_arquivos:
   if s.startswith('Previa'):
       lista_arquivos_filtrado.append("previa/"+s)

lista_arquivos_filtrado

# COMMAND ----------

arquivos = dbutils.fs.ls(rh_path+'previa/')
lista_arquivos_previa = [arquivo.name for arquivo in arquivos]
for arquivo in lista_arquivos_previa:
    lista_arquivos_filtrado.append('previa/' + arquivo)

# COMMAND ----------

# MAGIC %md
# MAGIC ### 1.2 Importa a Base Prévia do RH atualizada

# COMMAND ----------

#/Volumes/databox/juridico_comum/arquivos/trabalhista/bases_rh/previa/Previa_20240824.xlsxarquivo = lista_arquivos_filtrado[0]  # Acessa o único arquivo da lista

nmprevia = dbutils.widgets.get("nmprevia")

df = read_excel(f"/Volumes/databox/juridico_comum/arquivos/trabalhista/bases_rh/previa/PREVIA_SEMANAL_RH_{nmprevia}.xlsx")  # Lê o arquivo
df = adjust_column_names(df)  # Ajusta os nomes das colunas
df = remove_acentos(df)  # Remove acentos das colunas
df = df.withColumn("SOURCE", lit(arquivo))  # Adiciona a coluna 'SOURCE'


df.createOrReplaceTempView("df_previa")

# COMMAND ----------

# MAGIC %sql 
# MAGIC
# MAGIC select * from df_previa
# MAGIC order by DATA_DESLIGAMENTO desc

# COMMAND ----------

#AJUSTE DO CAMPO DESLIGAMENTO PARA FORMATO DATA
from pyspark.sql.functions import col, to_timestamp

df = df.withColumn("DATA_DESLIGAMENTO", to_timestamp(col("DATA_DESLIGAMENTO"), "yyyy-MM-dd"))

# COMMAND ----------

df2 = spark.sql('''
    SELECT * FROM df_previa
    WHERE MATRICULA IS NOT NULL AND DATA_DESLIGAMENTO BETWEEN '2025-04-01' AND '2025-04-30'
                
''')

df_desligados_rh = df2.distinct()

# COMMAND ----------

spark.conf.set("spark.sql.legacy.timeParserPolicy", "LEGACY")

# COMMAND ----------

display(df_desligados_rh)

# COMMAND ----------

# MAGIC %md
# MAGIC ### 1.3 Importa as bases auxiliares para coleta do Centro de Custo e Área Funcional

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

# MAGIC %md
# MAGIC ## 2 - Transformações

# COMMAND ----------

# MAGIC %md
# MAGIC ### 2.1 De paras

# COMMAND ----------

df_desligados_rh.createOrReplaceTempView("DESLIGADOS_RH_FECH")

df_desligados_rh_1 = spark.sql("""
SELECT A.*
    ,C.`descrição` AS DESCRICAO_CENTRO_CUSTO

    ,(CASE WHEN NIVEL1 = 'INDÚSTRIA DE MÓVEIS BARTIRA LTDA'
        THEN 'BARTIRA' ELSE B.BU END) AS BU

    ,(CASE WHEN NIVEL2 = 'INDÚSTRIA DE MÓVEIS BARTIRA LTDA' THEN 'BARTIRA'
        ELSE C.nome2 END ) AS VP

    ,(CASE WHEN NIVEL3 = 'INDÚSTRIA DE MÓVEIS BARTIRA LTDA' THEN 'BARTIRA'
        ELSE C.nome3 END ) AS DIRETORIA 

    ,(CASE WHEN NIVEL4 = 'INDÚSTRIA DE MÓVEIS BARTIRA LTDA' THEN 'BARTIRA'
        ELSE UPPER(C.`nome responsavel`) END ) AS RESPONSAVEL_AREA 

    ,(CASE WHEN NIVEL5 = 'INDÚSTRIA DE MÓVEIS BARTIRA LTDA' THEN 'BART'
        ELSE C.`area funcional` END ) AS AREA_FUNCIONAL

FROM DESLIGADOS_RH_FECH A
LEFT JOIN TB_DE_PARA_BU B ON A.COD_CENTRO_CUSTO_FORM= B.`Centro de custo`
LEFT JOIN TB_DE_PARA_AREA_FUNCIONAL C ON A.COD_CENTRO_CUSTO_FORM = C.centro
""")

#df_desligados_rh_1.createOrReplaceTempView("DESLIGADOS_RH_FECH_1")

# COMMAND ----------

# MAGIC %md
# MAGIC ###2.1.1 Ajuste nos Campos Base de Prévias (Adaptação com Histórico HC RH)

# COMMAND ----------

df_desligados_rh_1 = df_desligados_rh_1.withColumnRenamed("NIVEL1", "DESCRICAON1")
df_desligados_rh_1 = df_desligados_rh_1.withColumnRenamed("NIVEL2", "DESCRICAON2")
df_desligados_rh_1 = df_desligados_rh_1.withColumnRenamed("COD_EMPRESA", "CODEMPRESA")
df_desligados_rh_1 = df_desligados_rh_1.withColumnRenamed("STATUS_COLABORADOR", "STATUS")
df_desligados_rh_1 = df_desligados_rh_1.withColumnRenamed("DESC_FUNCAO_ATUAL", "FUNCAO")
df_desligados_rh_1 = df_desligados_rh_1.withColumnRenamed("TIPO_DESLIGAMENTO", "TIPODESLIGAMENTO")
df_desligados_rh_1 = df_desligados_rh_1.withColumnRenamed("DATA_ADMISSAO", "ADMISSAO")
df_desligados_rh_1 = df_desligados_rh_1.withColumnRenamed("DATA_DESLIGAMENTO", "DESLIGAMENTO")

# COMMAND ----------

from pyspark.sql import functions as F

# Função para extrair os quatro primeiros caracteres para ANOREF e os dois últimos para MESREF
def create_anoref_mesref_columns(df):
    # Obter o valor do widget 'nmmes'
    nmmes = dbutils.widgets.get("nmmes")
    
    # Extrair os quatro primeiros caracteres para o ANOREF (ex: '2024')
    anoref_value = nmmes[:4]
    
    # Extrair os dois últimos caracteres para o MESREF (ex: '08')
    mesref_value = nmmes[-2:]
    
    # Adicionar as colunas ANOREF e MESREF ao DataFrame
    df_with_anoref_mesref = df.withColumn("ANOREF", F.lit(anoref_value)) \
                              .withColumn("MESREF", F.lit(mesref_value))
    
    return df_with_anoref_mesref

# Aplicar a função ao DataFrame df_desligados_rh_1
df_desligados_rh_1 = create_anoref_mesref_columns(df_desligados_rh_1)

# COMMAND ----------

df_desligados_rh_1 = df_desligados_rh_1.withColumn("CPF", expr("substring(MATRICULA, 1, 15)").cast("double"))
df_desligados_rh_1.createOrReplaceTempView("DESLIGADOS_RH_FECH_1")

# COMMAND ----------

display(df_desligados_rh_1)

# COMMAND ----------

# MAGIC %md
# MAGIC ### 2.2 Prepara a base de processos

# COMMAND ----------

nmmes = dbutils.widgets.get("nmmes")
df_entradas_trab = spark.sql(f"SELECT * FROM databox.juridico_comum.tb_fecham_financ_trab_{nmmes} WHERE NOVOS = 1")
df_entradas_trab = df_entradas_trab.drop("MATRICULA")
df_entradas_trab = df_entradas_trab.drop("DP_FASE") # DP_FASE vai ser refeita adiante
df_entradas_trab.sort(asc("ID_PROCESSO")).sort(desc("MES_FECH"))
df_entradas_trab.createOrReplaceTempView("TB_ENTR_TRAB_F")

# COMMAND ----------

# MAGIC %md
# MAGIC ### 2.3 Curva de acionamento
# MAGIC DESLIGADOS X JUDICIALIZAÇÃO

# COMMAND ----------

# Carrega base Gerencial
dtgerencial = dbutils.widgets.get("dtgerencial")
df_trab_ger_consolidada = spark.sql(f"SELECT * FROM databox.juridico_comum.trab_ger_consolida_{dtgerencial}")

df_trab_ger_consolidada.createOrReplaceTempView("TRAB_GER_CONSOLIDA")

# COMMAND ----------

# MAGIC %md
# MAGIC ### 2.4 Inclui número de mátricula e ajusta campo FASE

# COMMAND ----------

df_entradas_trab2 = spark.sql("""
    	SELECT A.*
			,REPLACE(B.MATRICULA, ' ', '') AS MATRICULA --######### VALIDAR ##########
			,(CASE WHEN A.FASE IN ('', 'N/A', 'INATIVO', 'ENCERRAMENTO', 'ADMINISTRATIVO', 'DEMAIS') THEN 'DEMAIS'
					WHEN A.FASE IN ('EXECUCAO',
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

					WHEN A.FASE IN ('RECURSAL',
									'RECURSAL - INATIVO',
									'RECURSAL TRT',
									'RECURSAL TRT - INATIVO'
								) THEN 'RECURSAL_TRT'

					WHEN A.FASE IN ('RECURSAL TST - INATIVO',
								  'RECURSAL TST'
								) THEN 'RECURSAL_TST'
 
					ELSE A.FASE END) AS DP_FASE
			--,B.EMPRESA -- ###### DUPLICADA
    		,B.CENTRO_DE_CUSTO_AREA_DEMANDANTE_CODIGO AS CENTRO_CUSTO
      
	FROM TB_ENTR_TRAB_F A
	LEFT JOIN TRAB_GER_CONSOLIDA B ON A.ID_PROCESSO = B.PROCESSO_ID
	"""
)
df_entradas_trab2.createOrReplaceTempView("TB_ENTR_TRAB_2F")

# COMMAND ----------

df_entradas_trab4 = spark.sql("""
SELECT A.*
    ,REPLACE(REPLACE(REPLACE(PARTE_CONTRARIA_CPF, '.', ''), '-', ''), '/', '') AS CPF
    --,PARTE_CONTRARIA_CPF as CPF
    ,C.`descrição` AS DESCRICAO_CENTRO_CUSTO

    ,(CASE WHEN A.EMPRESA = 'INDÚSTRIA DE MÓVEIS BARTIRA LTDA'
        THEN 'BARTIRA' ELSE B.BU END) AS BU

    ,(CASE WHEN A.EMPRESA = 'INDÚSTRIA DE MÓVEIS BARTIRA LTDA' THEN 'BARTIRA'
        ELSE C.nome2 END ) AS VP

    ,(CASE WHEN A.EMPRESA = 'INDÚSTRIA DE MÓVEIS BARTIRA LTDA' THEN 'BARTIRA'
        ELSE C.nome3 END ) AS DIRETORIA

    ,(CASE WHEN A.EMPRESA = 'INDÚSTRIA DE MÓVEIS BARTIRA LTDA' THEN 'BARTIRA'
        ELSE UPPER(C.`nome responsavel`) END ) AS RESPONSAVEL_AREA

    ,(CASE WHEN A.EMPRESA = 'INDÚSTRIA DE MÓVEIS BARTIRA LTDA' THEN 'BART'
        ELSE C.`area funcional` END ) AS AREA_FUNCIONAL

FROM TB_ENTR_TRAB_2F A 
LEFT JOIN TB_DE_PARA_BU B ON A.CENTRO_CUSTO = B.`Centro de custo`
LEFT JOIN TB_DE_PARA_AREA_FUNCIONAL C ON A.CENTRO_CUSTO = C.centro
"""
)

df_entradas_trab4.createOrReplaceTempView("TB_ENTR_TRAB_4F")

# COMMAND ----------

# MAGIC %md
# MAGIC ### 2.5 Join com BU e Área funcional e Ajuste CPF 

# COMMAND ----------


df_entradas_trab4 = spark.sql("""
SELECT A.*
    ,REPLACE(REPLACE(REPLACE(PARTE_CONTRARIA_CPF, '.', ''), '-', ''), '/', '') AS CPF
    --,PARTE_CONTRARIA_CPF as CPF
    ,C.`descrição` AS DESCRICAO_CENTRO_CUSTO

    ,(CASE WHEN A.EMPRESA = 'INDÚSTRIA DE MÓVEIS BARTIRA LTDA'
        THEN 'BARTIRA' ELSE B.BU END) AS BU

    ,(CASE WHEN A.EMPRESA = 'INDÚSTRIA DE MÓVEIS BARTIRA LTDA' THEN 'BARTIRA'
        ELSE C.nome2 END ) AS VP

    ,(CASE WHEN A.EMPRESA = 'INDÚSTRIA DE MÓVEIS BARTIRA LTDA' THEN 'BARTIRA'
        ELSE C.nome3 END ) AS DIRETORIA

    ,(CASE WHEN A.EMPRESA = 'INDÚSTRIA DE MÓVEIS BARTIRA LTDA' THEN 'BARTIRA'
        ELSE UPPER(C.`nome responsavel`) END ) AS RESPONSAVEL_AREA

    ,(CASE WHEN A.EMPRESA = 'INDÚSTRIA DE MÓVEIS BARTIRA LTDA' THEN 'BART'
        ELSE C.`area funcional` END ) AS AREA_FUNCIONAL

FROM TB_ENTR_TRAB_2F A 
LEFT JOIN TB_DE_PARA_BU B ON A.CENTRO_CUSTO = B.`Centro de custo`
LEFT JOIN TB_DE_PARA_AREA_FUNCIONAL C ON A.CENTRO_CUSTO = C.centro
"""
)

df_entradas_trab4.createOrReplaceTempView("TB_ENTR_TRAB_4F")

# COMMAND ----------

df_entradas_trab5 = df_entradas_trab4.sort(asc("CPF")).sort(desc("MES_FECH")).drop_duplicates(["CPF"])
df_entradas_trab5 = df_entradas_trab5.withColumn("MATRICULA", col("MATRICULA").cast("int"))

df_entradas_trab5.createOrReplaceTempView("TB_ENTR_TRAB_5F")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 3 - Desligados RH Fechamento (PRÉVIA)

# COMMAND ----------

# MAGIC %md
# MAGIC ### 3.1 Select das colunas e ajuste de datas

# COMMAND ----------

#seleção dos campos utilizados na base prévia do RH e tratamento dos campos de ADMISSAO e DESLIGAMENTO

df_desligados_rh_fech = spark.sql("""
SELECT
    ANOREF,
    MESREF,
    CODEMPRESA,
    CPF,
    MATRICULA,
    NOME,
    DESCRICAON1,
    DESCRICAON2,
    BU,
    VP,
    DIRETORIA,
    RESPONSAVEL_AREA,
    AREA_FUNCIONAL,
    STATUS,
    TIPODESLIGAMENTO,
    FUNCAO,
    ADMISSAO::DATE,
    DESLIGAMENTO::DATE,
    DATE_FORMAT(ADMISSAO, 'y-MM-01')::DATE AS MES_ADMISSAO,
    DATE_FORMAT(DESLIGAMENTO, 'y-MM-01')::DATE AS MES_DESLIGAMENTO
FROM DESLIGADOS_RH_FECH_1
"""
)

df_desligados_rh_fech.createOrReplaceTempView("DESLIGADOS_RH_FECH_2")

# COMMAND ----------

# FAZ O DE PARA DAS ÁREAS FUNCIONAIS
df_desligados_rh_fech3 = spark.sql(
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
  FROM DESLIGADOS_RH_FECH_2
  """
)
df_desligados_rh_fech3.createOrReplaceTempView("DESLIGADOS_RH_FECH_3")

# COMMAND ----------

# MAGIC %md
# MAGIC ### 3.3 Tempo de empresa e Faixa

# COMMAND ----------

# AgingTempoEmpresa

data = [
    (-99999, 360, 'até 1 ano'),
    (360, 1080, '1 - 3 anos'),
    (1080, 2160, '3 - 5 anos'),
    (2160, 9999999, '+5 anos'),
]

# Define the schema for the DataFrame
schema = StructType([
    StructField("start_range", IntegerType(), False),
    StructField("end_range", IntegerType(), False),
    StructField("label", StringType(), False)
])

df_AgingTempoEmpresa = spark.createDataFrame(data, schema)
df_AgingTempoEmpresa.createOrReplaceTempView("AgingTempoEmpresa")
df_AgingTempoEmpresa.show()

# COMMAND ----------

# MAGIC %md
# MAGIC ### 3.4 Ajuste do Cargo e Faixa Aging

# COMMAND ----------

df_desligados_rh_fech4 = spark.sql("""
SELECT A.*
    ,A.CPF AS CPF_NUM -- Já está como double. Não precisou de tratamento
    ,CASE WHEN DESLIGAMENTO = '1900-01-01' 
        THEN NULL
        ELSE DATEDIFF(day, A.ADMISSAO, A.DESLIGAMENTO)  
    END AS TEMPO_EMPRESA
    ,B.label AS FX_TMP_EMPRESA
    FROM DESLIGADOS_RH_FECH_3 A
LEFT JOIN AgingTempoEmpresa B 
    ON DATEDIFF(day, A.ADMISSAO, A.DESLIGAMENTO) > B.start_range 
    AND DATEDIFF(day, A.ADMISSAO, A.DESLIGAMENTO) <= B.end_range;
"""
)
df_desligados_rh_fech4.createOrReplaceTempView("DESLIGADOS_RH_FECH_4")

# COMMAND ----------

df_desligados_rh_fech5 = spark.sql("""
SELECT
  A.*
  ,CASE
    WHEN upper(FUNCAO) LIKE '%APRENDIZ%' THEN 'Aprendiz'
    ELSE 'Demais Cargos'
  END AS TIPO_CARGO,
  CASE
    WHEN TEMPO_EMPRESA <= 90 THEN 'Contrato Experiência'
    ELSE 'Contrato Normal'
  END AS TIPO_DESLIGAMENTO
FROM
  DESLIGADOS_RH_FECH_4 A
  """
)

df_desligados_rh_fech5.createOrReplaceTempView("DESLIGADOS_RH_FECH_5")

# COMMAND ----------

# MAGIC %md
# MAGIC ### 3.5 Prepara a Base Final com a Junção da Base de Entradas para obter os processos Novos

# COMMAND ----------

df_desligados_rh_fech6 = spark.sql("""
	SELECT 	ANOREF
			,A.CPF
			,A.MATRICULA
			,A.CODEMPRESA
			,A.DescricaoN1
			,A.DescricaoN2
			,A.BU
			,A.VP
			,A.DIRETORIA
			,A.RESPONSAVEL_AREA
			,A.AREA_FUNCIONAL
			,A.DP_VP_DIRETORIA
			,A.MES_DESLIGAMENTO
			,A.TIPODESLIGAMENTO AS TIPO_DESLIGAMENTO
			,A.TIPODESLIGAMENTO
			,A.FX_TMP_EMPRESA
            ,A.FUNCAO
			,B.DATA_DISPENSA AS DATA_DISPENSA_ELAW
			,B.MES_DATA_DISPENSA AS MES_DATA_DISPENSA_ELAW
			,B.ID_PROCESSO
			,B.FASE
			,B.DP_FASE
			,coalesce(B.NOVOS, 0) AS NOVOS
			,B.MES_FECH
			,(CASE WHEN B.ID_PROCESSO IS NOT NULL AND B.DATA_DISPENSA = A.DESLIGAMENTO THEN 1
				  WHEN B.ID_PROCESSO IS NOT NULL AND B.DATA_DISPENSA <> A.DESLIGAMENTO THEN 0 END) AS FLAG_DT_DISPENSA 
			,1 AS QTDE

	FROM DESLIGADOS_RH_FECH_5 AS A
	LEFT JOIN TB_ENTR_TRAB_5F AS B 
		ON A.MATRICULA = B.MATRICULA
"""
)

df_desligados_rh_fech6.createOrReplaceTempView("DESLIGADOS_RH_FECH_6")

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT B.MES_FECH, A.MATRICULA, B.MATRICULA FROM DESLIGADOS_RH_FECH_5 A 
# MAGIC LEFT JOIN TB_ENTR_TRAB_5F B
# MAGIC ON A.MATRICULA = B.MATRICULA 

# COMMAND ----------

# MAGIC %md
# MAGIC ## 4 - Ajustes 

# COMMAND ----------

# MAGIC %md
# MAGIC ### 4.1 Ordenamento e deduplicação da Base Final

# COMMAND ----------

df_desligados_rh_fech7 = df_desligados_rh_fech6.sort(asc("CPF")) \
                                                .sort(asc("MATRICULA")) \
                                                .sort(asc("MES_DESLIGAMENTO")) \
                                                .sort(desc("MES_FECH")) \
                                                .sort(asc("ID_PROCESSO")) 



# COMMAND ----------

df_desligados_rh_fech7_previa = df_desligados_rh_fech7.dropDuplicates()
df_desligados_rh_fech7_previa.createOrReplaceTempView("DESLIGADOS_RH_FECH_7")

# COMMAND ----------

display(df_desligados_rh_fech7_previa)

# COMMAND ----------

df_desligados_rh_fech7_previa.count() #| 124 | 642

# COMMAND ----------

from pyspark.sql.functions import col

df_desligados_rh_fech7_previa = df_desligados_rh_fech7_previa.withColumn("NOVOS", col("NOVOS").cast("string"))

df_desligados_rh_fech7_previa = df_desligados_rh_fech7_previa.withColumn("MES_DATA_DISPENSA_ELAW", col("MES_DATA_DISPENSA_ELAW").cast("string"))


df_desligados_rh_fech7_previa = df_desligados_rh_fech7_previa.withColumn("MES_FECH", col("MES_FECH").cast("date"))


# Acesse a tabela Delta através do Spark
df1 = spark.table("databox.juridico_comum.tb_desligados_rh_previa")

# Imprimir o esquema da tabela
df1.printSchema()

# COMMAND ----------

# MAGIC %md
# MAGIC ## 5 - Outputs

# COMMAND ----------

# MAGIC %md
# MAGIC ### 5.1 Gravação e atualização dos Dados de Prévia

# COMMAND ----------

try:
    df_desligados_rh_fech7.write.format("delta").mode("append").saveAsTable(f"databox.juridico_comum.tb_desligados_rh_previa")
except:
    pass

# COMMAND ----------

import pandas as pd
from shutil import copyfile

# Convert PySpark DataFrame to Pandas DataFrame
pandas_df = df_desligados_rh_fech7_previa.toPandas()

# Save the Pandas DataFrame to an Excel file
local_path = f'/local_disk0/tmp/TB_DESLIGADOS_FINAL_PREVIA.xlsx'
pandas_df.to_excel(local_path, index=False, sheet_name='DESLIGADOS_RH_F', engine='xlsxwriter')

# Copy the file from the local disk to the desired volume
volume_path = f'/Volumes/databox/juridico_comum/arquivos/trabalhista/bases_indicadores_resultado/TB_DESLIGADOS_FECHAMENTO_PREVIA.xlsx'

copyfile(local_path, volume_path)
