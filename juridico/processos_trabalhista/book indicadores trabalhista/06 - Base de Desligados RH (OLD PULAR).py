# Databricks notebook source
# MAGIC %md
# MAGIC # Base de Desligados RH
# MAGIC
# MAGIC **Inputs**\
# MAGIC Tabela: Head Count fornecido pelo time de People Analytics - Recursos Humanos
# MAGIC
# MAGIC Planilhas: \
# MAGIC HC por Status - Consolidação da Base de Fechamento Mensal 
# MAGIC
# MAGIC /Volumes/databox/juridico_comum/arquivos/trabalhista/bases_rh/fechamento/
# MAGIC
# MAGIC **Outputs**\
# MAGIC Arquivos Excel - Historico Consolidado RH (TB_DESLIGADOS_RH_HISTORICA_F)
# MAGIC

# COMMAND ----------

# Parametro do nome da tabela da competência atual. Ex: 202404
dbutils.widgets.text("nmmes", "")

# Parametro nome da tabela gerencial. Ex: 20240423
dbutils.widgets.text("dtgerencial", "")

# COMMAND ----------

# MAGIC %md
# MAGIC ### run_type
# MAGIC #### Parâmetro para escolher entre fechamento, prévia e teste. 
# MAGIC - **fechamento**
# MAGIC   - Lê todos os arquivos na pasta de 'fechamento'.
# MAGIC   - Atualiza a tabela TB_DESLIGADOS_RH_HISTORICA_F.
# MAGIC   
# MAGIC - **previa**
# MAGIC   - Lê todos os arquivos de 'fechamento' mais os arquivos da pasta 'previa'.
# MAGIC   - Não atualiza a TB_DESLIGADOS_RH_HISTORICA_F.
# MAGIC   - Faz exportação no passo 5.3 e move todos os arquivos de prévia para a pasta 'previa_arquivo' \
# MAGIC   **ATENÇÃO:** Manter na pasta de prévia somente os arquivos correspondentes ao mês requerido. Arquivos de prévia de outros meses devem ficar na pasta 'previa_arquivo'.
# MAGIC
# MAGIC - **teste**
# MAGIC   - Lê todos os arquivos na pasta de 'fechamento'.
# MAGIC   - Não realiza nenhum salvamento ou exportação.
# MAGIC   - Utilizar caso deseje fazer uma validação ou para editar essa tranformação.

# COMMAND ----------

# Parametro para escolher entre prévia e fechamento.
dbutils.widgets.dropdown("run_type", "teste", ["teste", "fechamento", "previa"])

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

arquivos = dbutils.fs.ls(rh_path+'fechamento/')

lista_arquivos = [arquivo.name for arquivo in arquivos]

# COMMAND ----------

# Lista arquivos fechamento
lista_arquivos_filtrado = []

for s in lista_arquivos:
   if s.startswith('HC por Status'):
       lista_arquivos_filtrado.append("fechamento/"+s)

lista_arquivos_filtrado

# COMMAND ----------

# Add arquivo prévia
##run_type = dbutils.widgets.get("run_type")


##if run_type == "previa":
#arquivos = dbutils.fs.ls(rh_path+'previa/')
#lista_arquivos_previa = [arquivo.name for arquivo in arquivos]
#for arquivo in lista_arquivos_previa:
#lista_arquivos_filtrado.append('previa/'+arquivo)

#lista_arquivos_filtrado

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
# MAGIC ### 1.4 Filtra DF's pela data do arquivo

# COMMAND ----------

# MAGIC %run "/Workspace/Jurídico/processos trabalhista/book indicadores trabalhista/mes_rh"

# COMMAND ----------

lista_dfs_rh_filtro = []
for df in lista_dfs_rh:
    df.printSchema()

    df = df.withColumn('DATA_AQUIVO', substring(col('SOURCE'),-13,8))
    df = df.join(df_arquivos,df['DATA_AQUIVO'] == df_arquivos['arquivo'])

    df = df.where("CPF IS NOT NULL AND DESLIGAMENTO::DATE >= dt_inicio AND DESLIGAMENTO::DATE <= dt_fim")
    df = df.distinct()

    lista_dfs_rh_filtro.append(df)

# COMMAND ----------

from pyspark.sql.functions import col, substring

lista_dfs_rh_filtro = []
for df in lista_dfs_rh:
    df.printSchema()

    # Corrected the usage of substring and col functions
    df = df.withColumn('DATA_AQUIVO', substring(col('SOURCE'), -13, 8))
    df = df.join(df_arquivos, df['DATA_AQUIVO'] == df_arquivos['arquivo'])

    # Assuming DESLIGAMENTO is a string that needs to be cast to date, corrected the casting syntax
    df = df.where("CPF IS NOT NULL AND to_date(DESLIGAMENTO) >= dt_inicio AND to_date(DESLIGAMENTO) <= dt_fim")
    df = df.distinct()

    lista_dfs_rh_filtro.append(df)

# COMMAND ----------

# MAGIC %md
# MAGIC ### 1.5 Cast string

# COMMAND ----------

# Cast string todas as colunas
lista_dfs_rh_string = []
for df in lista_dfs_rh_filtro:
    for col in df.columns:
        df = df.withColumn(col, df[col].cast(StringType()))
    
    lista_dfs_rh_string.append(df)

# COMMAND ----------

# MAGIC %md
# MAGIC ### 1.6 Une todos os DFs

# COMMAND ----------

# Cria um DF vazio com todas as colunas string
df_rh_string = create_empty_df_with_all_schema(lista_dfs_rh_string)

for df in lista_dfs_rh_string:
    df_rh_string = df_rh_string.unionByName(df, allowMissingColumns=True)

# COMMAND ----------

# MAGIC %md
# MAGIC ### 1.7 Recast os tipos originais

# COMMAND ----------

# Cria um DF com seus tipos originais
df_ref = create_empty_df_with_all_schema(lista_dfs_rh)

# Remove colunas duplicadas da referencia
df_ref = df_ref.dropDuplicates()

# Usa esse schema como referencia para converter novamente os tipos do DF unido
schema_ref = df_ref.schema

# COMMAND ----------

for field in schema_ref.fields:
    df_rh_string = df_rh_string.withColumn(field.name, df_rh_string[field.name].cast(field.dataType))

df_desligados_rh = df_rh_string

# COMMAND ----------

df_desligados_rh.createOrReplaceTempView("df_desligados_rh")

# COMMAND ----------

 df_desligados_rh.count()

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

    ,(CASE WHEN DESCRICAON1 = 'INDÚSTRIA DE MÓVEIS BARTIRA LTDA'
        THEN 'BARTIRA' ELSE B.BU END) AS BU

    ,(CASE WHEN DESCRICAON1 = 'INDÚSTRIA DE MÓVEIS BARTIRA LTDA' THEN 'BARTIRA'
        ELSE C.nome2 END ) AS VP

    ,(CASE WHEN DESCRICAON1 = 'INDÚSTRIA DE MÓVEIS BARTIRA LTDA' THEN 'BARTIRA'
        ELSE C.nome3 END ) AS DIRETORIA 

    ,(CASE WHEN DESCRICAON1 = 'INDÚSTRIA DE MÓVEIS BARTIRA LTDA' THEN 'BARTIRA'
        ELSE UPPER(C.`nome responsavel`) END ) AS RESPONSAVEL_AREA 

    ,(CASE WHEN DESCRICAON1 = 'INDÚSTRIA DE MÓVEIS BARTIRA LTDA' THEN 'BART'
        ELSE C.`area funcional` END ) AS AREA_FUNCIONAL

FROM DESLIGADOS_RH_FECH A
LEFT JOIN TB_DE_PARA_BU B ON A.CODCCUSTOCONTABIL = B.`Centro de custo`
LEFT JOIN TB_DE_PARA_AREA_FUNCIONAL C ON A.CODCCUSTOCONTABIL = C.centro
""")

df_desligados_rh_1.createOrReplaceTempView("DESLIGADOS_RH_FECH_1")

# COMMAND ----------

df_desligados_rh_1 = df_desligados_rh_1.withColumn("CPF", df_desligados_rh_1["CPF"].cast("bigint"))


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

# MAGIC %sql
# MAGIC -- SELECT ID_PROCESSO, MATRICULA FROM TB_ENTR_TRAB_2F

# COMMAND ----------

# MAGIC %sql
# MAGIC -- SELECT PARTE_CONTRARIA_CPF, count(*) FROM TB_ENTR_TRAB_2F GROUP BY 1 ORDER BY 2 DESC

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

from pyspark.sql.functions import col
from pyspark.sql.types import LongType

df_entradas_trab4 = df_entradas_trab4.withColumn("CPF", col("CPF").cast(LongType()))
df_entradas_trab4.createOrReplaceTempView("TB_ENTR_TRAB_4F")

# COMMAND ----------

df_entradas_trab5 = df_entradas_trab4.sort(asc("CPF")).sort(desc("MES_FECH")).drop_duplicates(["CPF"])

df_entradas_trab5.createOrReplaceTempView("TB_ENTR_TRAB_5F")

# COMMAND ----------

# %sql

# SELECT * FROM TB_ENTR_TRAB_5F

# COMMAND ----------

# MAGIC %md
# MAGIC ## 3 - Desligados RH Fechamento

# COMMAND ----------

# MAGIC %md
# MAGIC ### 3.1 Select das colunas e ajuste de datas

# COMMAND ----------

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

# MAGIC %md
# MAGIC ### 3.2 De Para Áreas Funcionais

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

# %sql
# SELECT DISTINCT ADMISSAO, DESLIGAMENTO, TEMPO_EMPRESA, FX_TMP_EMPRESA FROM DESLIGADOS_RH_FECH_4

# COMMAND ----------

# MAGIC %md
# MAGIC ### 3.4 De para função do colaborador
# MAGIC

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
		ON A.CPF = B.CPF
"""
)

df_desligados_rh_fech6.createOrReplaceTempView("DESLIGADOS_RH_FECH_6")

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

df_desligados_rh_fech7 = df_desligados_rh_fech7.dropDuplicates()
df_desligados_rh_fech7.createOrReplaceTempView("DESLIGADOS_RH_FECH_7")

# COMMAND ----------

df_desligados_rh_fech7.createOrReplaceTempView("TB_RH_FECHAMENTO")

# COMMAND ----------

df_desligados_rh_fech7.count() #72749 | 73822 | 

# COMMAND ----------

# Mantem só os desse mês
df_desligados_rh_fech8 = df_desligados_rh_fech7.where("MES_DESLIGAMENTO > MES_FECH")
df_desligados_rh_fech8.createOrReplaceTempView("DESLIGADOS_RH_FECH_8")

# COMMAND ----------

 df_desligados_rh_fech8.count() #

# COMMAND ----------

# %sql

# SELECT MES_DESLIGAMENTO, COUNT(MATRICULA) AS QTD
# FROM TB_RH_FECHAMENTO
# GROUP BY MES_DESLIGAMENTO
# ORDER BY 1

# COMMAND ----------

# MAGIC %md
# MAGIC ## 5 - Outputs

# COMMAND ----------

run_type = dbutils.widgets.get("run_type")

# COMMAND ----------

# MAGIC %md
# MAGIC ### 5.1 Não materializa se for teste

# COMMAND ----------

if run_type == 'teste':
    dbutils.notebook.exit(run_type)

# COMMAND ----------

# MAGIC %md
# MAGIC ### 5.2 Atualiza base histórica se for fechamento

# COMMAND ----------

df_desligados_rh_historica = spark.table("databox.juridico_comum.tb_desligados_rh_historica")
display(df_desligados_rh_historica)

# COMMAND ----------

# %sql

# select * from databox.juridico_comum.tb_desligados_rh_historica_final

# COMMAND ----------

# Carregar a tabela Delta
df = spark.read.format("delta").table("juridico_comum.tb_desligados_rh_historica_final")

# Exibir o schema
df.printSchema()


# COMMAND ----------

# #if run_type == 'fechamento':
#   df_desligados_rh_historica = spark.sql(f"SELECT * FROM databox.juridico_comumtb_desligados_rh_historica")
#   df_desligados_rh_historica = df_desligados_rh_historica.unionByName(df_desligados_rh_fech8)

from pyspark.sql.functions import col  

df_desligados_rh_fech7 = df_desligados_rh_fech7.withColumn("NOVOS", col("NOVOS").cast("string"))
df_desligados_rh_fech7 = df_desligados_rh_fech7.withColumn("MES_FECH", col("MES_FECH").cast("date"))

df_desligados_rh_fech7.write.format("delta").mode("append").saveAsTable(f"databox.juridico_comum.tb_desligados_rh_historica_final")



# COMMAND ----------

# MAGIC %md
# MAGIC #### 5.2.1 Exportação da Base de Desligados do RH 

# COMMAND ----------

import pandas as pd
from shutil import copyfile

# Convert PySpark DataFrame to Pandas DataFrame
pandas_df = df_desligados_rh_fech7.toPandas()

# Save the Pandas DataFrame to an Excel file
local_path = f'/local_disk0/tmp/TB_DESLIGADOS_FINAL.xlsx'
pandas_df.to_excel(local_path, index=False, sheet_name='DESLIGADOS_RH_F', engine='xlsxwriter')

# Copy the file from the local disk to the desired volume
volume_path = f'/Volumes/databox/juridico_comum/arquivos/trabalhista/bases_indicadores_resultado/TB_DESLIGADOS_FECHAMENTO.xlsx'

copyfile(local_path, volume_path)

# COMMAND ----------

# MAGIC %md
# MAGIC ### 5.3 Faz tratativas se for prévia

# COMMAND ----------

# MAGIC %md
# MAGIC # Validação

# COMMAND ----------

# %sql
# SELECT MES_DESLIGAMENTO, NOVOS, count(*) FROM TB_RH_FECHAMENTO GROUP BY ALL ORDER BY 1, 2

# COMMAND ----------

# MAGIC %md
# MAGIC #### Extração de base desligados com natureza operacional

# COMMAND ----------

df_gerencial_natureza_op_com_matricula = spark.sql('''
    SELECT DISTINCT CAST(MATRICULA AS BIGINT) AS MATRICULA, NATUREZA_OPERACIONAL FROM TRAB_GER_CONSOLIDA
    WHERE MATRICULA IS NOT NULL
    
''')

df_gerencial_natureza_op_com_matricula.createOrReplaceTempView("tb_gerencial_natureza_op_com_matricula")

# COMMAND ----------

df_desligados_com_natureza_operacional = spark.sql('''
    select A.*,
        B.NATUREZA_OPERACIONAL
    from databox.juridico_comum.tb_desligados_rh_historica_final A
    LEFT JOIN tb_gerencial_natureza_op_com_matricula B 
        ON A.MATRICULA = B.MATRICULA
''')

print(df_desligados_com_natureza_operacional.count())

# COMMAND ----------

import pandas as pd
from shutil import copyfile

# Convert PySpark DataFrame to Pandas DataFrame
pandas_df = df_desligados_com_natureza_operacional.toPandas()

# Save the Pandas DataFrame to an Excel file
local_path = f'/local_disk0/tmp/TB_DESLIGADOS_NATUREZA_OP_FINAL.xlsx'
pandas_df.to_excel(local_path, index=False, sheet_name='DESLIGADOS_NATU_OP_RH_F', engine='xlsxwriter')

# Copy the file from the local disk to the desired volume
volume_path = f'/Volumes/databox/juridico_comum/arquivos/trabalhista/bases_indicadores_resultado/TB_DESLIGADOS_NATUREZA_OP.xlsx'

copyfile(local_path, volume_path)

# COMMAND ----------

display(df_desligados_com_natureza_operacional)

# COMMAND ----------

df_desligados_com_natureza_operacional.createOrReplaceTempView("TB_RH_FECHAMENTO_OPERACIONAL")

# COMMAND ----------

# %sql

# SELECT MES_DESLIGAMENTO, COUNT(MATRICULA) AS QTD
# FROM TB_RH_FECHAMENTO_OPERACIONAL
# GROUP BY MES_DESLIGAMENTO
# ORDER BY 1

# COMMAND ----------

# %sql

# select * from --TB_RH_FECHAMENTO_OPERACIONAL 
# --databox.juridico_comum.tb_desligados_rh_historica_final
# tb_gerencial_natureza_op_com_matricula
# where MATRICULA = 553018
