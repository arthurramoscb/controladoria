# Databricks notebook source
# MAGIC %md
# MAGIC # Tratamento do Fechamento Trabalhista Consolidada Final
# MAGIC
# MAGIC **Inputs**\
# MAGIC Tabela:\
# MAGIC juridico_comum.geral_fechamento_trab
# MAGIC
# MAGIC Planilhas:\
# MAGIC Centro de Custo e BU;\
# MAGIC Centro de Custo e Área Funcional;\
# MAGIC De para Cluster Valor;\
# MAGIC De para Terceiro Insolvente
# MAGIC
# MAGIC **Outputs**\
# MAGIC Tabela:\
# MAGIC juridico_comum.tb_fecham_financ_trab_{nmmes}

# COMMAND ----------

# Parametro do formato de data do arquivo. Ex: 20240423
dbutils.widgets.text("nmtabela", "")

# Parametro do formato do nome da tabela gerada ao final. Ex: 202404
dbutils.widgets.text("nmmes", "")

# COMMAND ----------

# MAGIC %md
# MAGIC ### 1 - Carga e tratamentos iniciais

# COMMAND ----------

# MAGIC %run "/Workspace/Jurídico/funcao_tratamento_fechamento/common_functions"

# COMMAND ----------

# Carrega histórico dos fechamentos financeiros
df_ff_trab = spark.sql("select * from databox.juridico_comum.geral_fechamento_trab")

df_ff_trab = adjust_column_names(df_ff_trab)
df_ff_trab = remove_acentos(df_ff_trab)

# COMMAND ----------

df_ff_trab = df_ff_trab.replace(float('NaN'), None)

# COMMAND ----------

# Clasifica NOVO_X_LEGADO
data_corte = "2020-01-01" #YYYY-MM-DD

df_ff_trab = df_ff_trab.withColumn(
    "NOVO_X_LEGADO",
    when(col("DATACADASTRO") >= data_corte, "NOVO").otherwise("LEGADO")
)

# COMMAND ----------

# Trata coluna ESTRATEGIA
df_ff_trab = df_ff_trab.withColumn(
    "ESTRATEGIA_",
    when(col("ESTRATEGIA").isin("ACO", "ACOR", "ACORD", "ACORDO"," ACORDO"), "ACORDO")
    .when(col("ESTRATEGIA").isin("DEF", "DEFE", "DEFES", "DEFESA"," DEFESA"), "DEFESA")
    .otherwise(col("ESTRATEGIA"))
)

df_ff_trab = df_ff_trab.drop(col("ESTRATEGIA"))

df_ff_trab = df_ff_trab.withColumnRenamed("ESTRATEGIA_", "ESTRATEGIA")

# COMMAND ----------

# Ajusta tipo data
lista_data = ['DATACADASTRO']
df_ff_trab = convert_to_date_format(df_ff_trab, lista_data)

# COMMAND ----------

# MAGIC %md
# MAGIC ### 2 - Importa bases com informações adicionais

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

df_ff_trab.createOrReplaceTempView("TB_FECH_FIN_TRAB_CONSOLIDADO")

df_ff_trab_consolidado = spark.sql("""
SELECT A.*
    ,C.`descrição` AS DESCRICAO_CENTRO_CUSTO

    ,(CASE WHEN EMPRESA_M = 'INDÚSTRIA DE MÓVEIS BARTIRA LTDA'
        THEN 'BARTIRA' ELSE B.BU END) AS BU

    ,(CASE WHEN EMPRESA_M = 'INDÚSTRIA DE MÓVEIS BARTIRA LTDA' THEN 'BARTIRA'
        ELSE C.nome2 END ) AS VP

    ,(CASE WHEN EMPRESA_M = 'INDÚSTRIA DE MÓVEIS BARTIRA LTDA' THEN 'BARTIRA'
        ELSE C.nome3 END ) AS DIRETORIA 

    ,(CASE WHEN EMPRESA_M = 'INDÚSTRIA DE MÓVEIS BARTIRA LTDA' THEN 'BARTIRA'
        ELSE UPPER(C.`nome responsavel`) END ) AS RESPONSAVEL_AREA 

    ,(CASE WHEN EMPRESA_M = 'INDÚSTRIA DE MÓVEIS BARTIRA LTDA' THEN 'BART'
        ELSE C.`area funcional` END ) AS AREA_FUNCIONAL_BART

FROM TB_FECH_FIN_TRAB_CONSOLIDADO AS A 
LEFT JOIN TB_DE_PARA_BU AS B ON A.CENTRO_DE_CUSTO_M = B.`Centro de custo`
LEFT JOIN TB_DE_PARA_AREA_FUNCIONAL AS C ON A.CENTRO_DE_CUSTO_M = C.centro
WHERE A.ID_PROCESSO IS NOT NULL 
""")

# COMMAND ----------

df_ff_trab_consolidado.count()

# COMMAND ----------

from pyspark.sql.functions import col, when

df_ff_trab_consolidado = df_ff_trab_consolidado.withColumn(
    "INDICACAO_PROCESSO_ESTRATEGICO",
    when(
        col("INDICACAO_PROCESSO_ESTRATEGICO").contains("AC") | 
        col("INDICACAO_PROCESSO_ESTRATEGICO").contains(" AC"), 
        "ACORDO"
    )
    .when(
        col("INDICACAO_PROCESSO_ESTRATEGICO").contains("DE"), "DEFESA")
    .when(col("INDICACAO_PROCESSO_ESTRATEGICO").contains(" DE"), "DEFESA")
    .otherwise(None)
)

# COMMAND ----------

display(df_ff_trab_consolidado.groupBy("INDICACAO_PROCESSO_ESTRATEGICO").count())

# COMMAND ----------

df_ff_trab_consolidado = df_ff_trab_consolidado.withColumnRenamed("EMPRESA_M", "EMPRESA") \
                        .withColumnRenamed("STATUS_M", "STATUS") \
                        .withColumnRenamed("CENTRO_DE_CUSTO_M", "CENTRO_CUSTO_M") \
                        .withColumnRenamed("EMPRESA:_PROVISAO_MOV_M", "EMPRESA_PROVISAO_MOV_M") \
                        .withColumnRenamed("MEDIA_DE_PAGAMENTO", "MEDIA_PAGAMENTO") \
                        .withColumnRenamed("CENTRO_DE_CUSTO_M", "CENTRO_CUSTO_M") \
                        .withColumnRenamed("FASE_M", "FASE") \
                        .withColumnRenamed("VLR_CAUSA", "VALOR_DA_CAUSA")

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC ### 3 - Importa base gerencial

# COMMAND ----------

nmtabela = dbutils.widgets.get("nmtabela")
nmmes = dbutils.widgets.get("nmmes")

df_ger_consolidado = spark.sql(f"select * from databox.juridico_comum.trab_ger_consolida_{nmtabela}")

# COMMAND ----------

df_ger_consolidado.columns

# COMMAND ----------

#df_ger_consolidado = df_ger_consolidado.drop("NOVO_TERCEIRO")

# COMMAND ----------

display(df_ger_consolidado.limit(10))

# COMMAND ----------

df_ger_consolidado = df_ger_consolidado.withColumnRenamed("NOVO_TERCEIRO_CNPJ_RAZAO_SOCIAL_RAZAO_SOCIAL", "NOVO_TERCEIRO")


# COMMAND ----------

df_ff_trab_consolidado.createOrReplaceTempView("TB_FECH_FIN_TRAB_CONSOLIDADO_2")
df_ger_consolidado.createOrReplaceTempView("TRAB_GER_CONSOLIDA")

df_ff_trab_consolidado = spark.sql("""
    	SELECT A.*
			,DATE_FORMAT(A.DATACADASTRO, 'y-MM-01') AS MES_CADASTRO
			,B.ADVOGADO_RESPONSAVEL
			,B.ESCRITORIO AS ESCRITORIO_RESPONSAVEL
			,B.NUMERO_DO_PROCESSO
			,B.PARTE_CONTRARIA_CPF
			,B.PARTE_CONTRARIA_NOME
			,B.ADVOGADO_DA_PARTE_CONTRARIA AS ADVOGADO_PARTE_CONTRARIA
			,B.MATRICULA
			,B.PROCESSO_ESTADO AS ESTADO
			,B.PROCESSO_COMARCA AS COMARCA
			,B.CLASSIFICACAO
			,B.NATUREZA_OPERACIONAL
			,B.TERCEIRO_PRINCIPAL
			,B.NOVO_TERCEIRO
			,(CASE WHEN B.TERCEIRO_PRINCIPAL IS NULL THEN B.NOVO_TERCEIRO
					ELSE B.TERCEIRO_PRINCIPAL END) AS TERCEIRO_AJUSTADO
			,B.PARTE_CONTRARIA_DATA_ADMISSAO AS DATA_ADMISSAO
			,B.PARTE_CONTRARIA_DATA_DISPENSA AS DATA_DISPENSA
			,B.PARTE_CONTRARIA_CARGO_CARGO_GRUPO AS PARTE_CONTRARIA_CARGO_GRUPO
			,B.PARTE_CONTRARIA_CARGO AS CARGO
			,B.PARTE_CONTRARIA_MOTIVO_DO_DESLIGAMENTO AS MOTIVO_DESLIGAMENTO
			,B.FASE AS FASE_ATUAL
			,B.MOTIVO_DE_ENCERRAMENTO AS MOTIVO_ENCERRAMENTO
   
			,B.FILIAL48 AS FILIAL -- ####################### O NÚMERO É POR CONTA DA REPETIÇÃO DO TITULO NA PLANILHA
   
			,B.BANDEIRA
			,B.CENTRO_DE_CUSTO_AREA_DEMANDANTE_NOME AS NOME_DA_LOJA
 			,B.DATA_REGISTRADO
			,B.DIRETORIA AS DIRETORIA_DP_LOJA 
   
	FROM TB_FECH_FIN_TRAB_CONSOLIDADO_2 AS A
	LEFT JOIN TRAB_GER_CONSOLIDA AS B ON A.ID_PROCESSO = B.PROCESSO_ID;
""")

# COMMAND ----------

df_ff_trab_consolidado.count()

# COMMAND ----------

 display(df_ff_trab_consolidado)

# COMMAND ----------

df_ff_trab_consolidado.createOrReplaceTempView("TB_FECH_FIN_TRAB_CONSOLIDADO_2")

# COMMAND ----------

# MAGIC %md
# MAGIC ### 4 - Expressões regras de negócio

# COMMAND ----------

# MAGIC %md
# MAGIC #### 4.1 Parte contraria, Novos e Fase

# COMMAND ----------

df_ff_trab_consolidado.printSchema()

# COMMAND ----------

df_ff_trab_consolidado = spark.sql("""
SELECT *,
    CASE WHEN PARTE_CONTRARIA_CARGO_GRUPO LIKE '% PARA' THEN SUBSTRING(PARTE_CONTRARIA_CARGO_GRUPO, 1, LENGTH(PARTE_CONTRARIA_CARGO_GRUPO) - 5) ELSE PARTE_CONTRARIA_CARGO_GRUPO END AS PARTE_CONTRARIA_CARGO_GRUPO2
 FROM TB_FECH_FIN_TRAB_CONSOLIDADO_2
 """)

df_ff_trab_consolidado = df_ff_trab_consolidado.drop("PARTE_CONTRARIA_CARGO_GRUPO")

df_ff_trab_consolidado = df_ff_trab_consolidado.withColumnRenamed("PARTE_CONTRARIA_CARGO_GRUPO2", "PARTE_CONTRARIA_CARGO_GRUPO")

# COMMAND ----------

# # Tira os " PARA" do fim da string na coluna PARTE_CONTRARIA_CARGO_GRUPO 
# df_ff_trab_consolidado = df_ff_trab_consolidado.withColumn("PARTE_CONTRARIA_CARGO_GRUPO2", 
#                    expr("""
#                         CASE WHEN `PARTE_CONTRARIA_CARGO_GRUPO` LIKE '% PARA'
#                         THEN SUBSTRING(`PARTE_CONTRARIA_CARGO_GRUPO`, 1, LENGTH(`PARTE_CONTRARIA_CARGO_GRUPO`) - 5)
#                         ELSE `PARTE_CONTRARIA_CARGO_GRUPO` END
#                         """
#                     )
#                    )

# COMMAND ----------

# Trata coluna novos
df_ff_trab_consolidado = df_ff_trab_consolidado.withColumn("NOVOS", 
                   expr("CASE WHEN NOVOS IS NULL THEN REATIVADOS ELSE NOVOS END")
                   )

# COMMAND ----------

df_ff_trab_consolidado = df_ff_trab_consolidado.withColumn("FASE", 
                   expr("""
                        CASE WHEN PARTE_CONTRARIA_NOME IN ('MINISTERIO DO TRABALHO E EMPREGO',
                            'MINISTERIO DO TRABALHO E EMPREGO - 11/07/2019 - 11/07/2019',
                            'MINISTÉRIO PÚBLICO DO TRABALHO',
                            'MINISTÉRIO PÚBLICO DO TRABALHO - 01/01/2014 - 31/12/2014',
                            'MINISTÉRIO PÚBLICO DO TRABALHO - 01/12/2013 - 09/03/2015',
                            'MINISTÉRIO PÚBLICO DO TRABALHO - 22/06/2006 - 31/05/2007',
                            'MINISTERIO PUBLICO DO TRABALHO')
                        THEN "N/A"
                        ELSE FASE END
                        """
                        )
)

# COMMAND ----------

# MAGIC %md
# MAGIC #### 4.2 Periodo reclamado

# COMMAND ----------

# Calcula PERIODO_RECLAMADO
df_ff_trab_cons_pr = df_ff_trab_consolidado.withColumn("PERIODO_RECLAMADO", expr("DISTRIBUICAO - interval 1800 days"))

# Calcula PERIODO1
df_ff_trab_cons_pr = df_ff_trab_cons_pr.withColumn("PERIODO1", when(col("PERIODO_RECLAMADO") >= col("DATA_ADMISSAO"), col("PERIODO_RECLAMADO")).otherwise(col("DATA_ADMISSAO")))

# Calcula PERIODO2
df_ff_trab_cons_pr = df_ff_trab_cons_pr.withColumn("PERIODO2", when(col("DATA_DISPENSA").isNull(), col("DISTRIBUICAO")).otherwise(col("DATA_DISPENSA")))

# Formata PER_1 e PER_2
df_ff_trab_cons_pr = df_ff_trab_cons_pr.withColumn("PER_1", concat_ws("-", lpad(month(col("PERIODO1")), 2, '0'), year(col("PERIODO1"))))
df_ff_trab_cons_pr = df_ff_trab_cons_pr.withColumn("PER_2", concat_ws("-", lpad(month(col("PERIODO2")), 2, '0'), year(col("PERIODO2"))))

# Calcula PERIODO_RECL_GRUPO
df_ff_trab_cons_pr = df_ff_trab_cons_pr.withColumn("PERIODO_RECL_GRUPO", when(col("PERIODO1").isNotNull() & col("PERIODO2").isNotNull(), concat_ws(" a ", col("PER_1"), col("PER_2"))))

# df_ff_trab_cons_pr.select("PERIODO_RECLAMADO","PERIODO_RECL_GRUPO", "DATA_ADMISSAO", "DATA_DISPENSA", "DISTRIBUICAO", "PERIODO1", "PERIODO2", "PER_1", "PER_2").show()

# Drop PER_1 e PER_2
df_ff_trab_cons_pr = df_ff_trab_cons_pr.drop("PER_1", "PER_2")

# COMMAND ----------

# MAGIC %md
# MAGIC ### 5 - Atualização Pagamentos e Garantias

# COMMAND ----------

df_ff_trab_cons_pr66 = df_ff_trab_cons_pr.where("ENCERRADOS = 1")
df_ff_trab_cons_pr66 = df_ff_trab_cons_pr66.orderBy(asc('ID_PROCESSO'), desc('MES_FECH'))
df_ff_trab_cons_pr66 = df_ff_trab_cons_pr66.dropDuplicates(['ID_PROCESSO'])

# COMMAND ----------

# df_ff_trab_cons_pr66

# COMMAND ----------

df_ff_trab_cons_pr66.createOrReplaceTempView("TB_FECH_FIN_TRAB_CONSOLIDADO_66")

df_ff_trab_cons_pr66 = spark.sql("""
        SELECT A.*
			,B.DT_ULT_PGTO AS DT_ULT_PGTO_NOVO
			,B.ACORDO AS ACORDOS_NOVO
			,B.CONDENACAO AS CONDENACAO_NOVO
			,B.PENHORA AS PENHORA_NOVO
			,B.PENSAO AS PENSAO_NOVO
			,B.OUTROS_PAGAMENTOS AS OUTROS_PAGAMENTOS_NOVO
			,B.IMPOSTO AS IMPOSTO_NOVO
			,B.GARANTIA AS GARANTIA_NOVO
	FROM TB_FECH_FIN_TRAB_CONSOLIDADO_66 AS A
	LEFT JOIN global_temp.HIST_PAGAMENTOS_GARANTIA_TRAB_FF AS B ON A.ID_PROCESSO = B.PROCESSO_ID-- AND A.ENCERRADOS = 1
	ORDER BY A.ID_PROCESSO ASC, A.MES_FECH DESC
                                   """)


																	 

# COMMAND ----------

# df_ff_trab_cons_pr66.count()

# COMMAND ----------

df_ff_trab_cons_pr66 = df_ff_trab_cons_pr66.replace(float('NaN'), None)
df_ff_trab_cons_pr66.createOrReplaceTempView("TB_FECH_FIN_TRAB_CONSOLIDADO_77")

# COMMAND ----------

# MAGIC %sql
# MAGIC -- SELECT ID_PROCESSO,
# MAGIC -- ENCERRADOS,
# MAGIC -- DT_ULT_PGTO,
# MAGIC -- DT_ULT_PGTO_NOVO,
# MAGIC -- CONDENACAO,
# MAGIC -- CONDENACAO_NOVO
# MAGIC -- FROM 
# MAGIC -- TB_FECH_FIN_TRAB_CONSOLIDADO_77
# MAGIC -- WHERE CONDENACAO > 0

# COMMAND ----------

# MAGIC %md
# MAGIC ####5.1 Atualização Incremental com os novos valores de pagamento (Base de Pagamentos com atualização)

# COMMAND ----------

df_ff_trab_cons_pr77 = df_ff_trab_cons_pr66.drop("DT_ULT_PGTO").drop("ACORDO").drop("CONDENACAO").drop("PENHORA").drop("OUTROS_PAGAMENTOS").drop("IMPOSTO").drop("GARANTIA").drop("TOTAL_PAGAMENTOS")

rename_cols_pgtos_map = {'DT_ULT_PGTO_NOVO': 'DT_ULT_PGTO',
'ACORDOS_NOVO': 'ACORDO',
'CONDENACAO_NOVO': 'CONDENACAO',
'PENHORA_NOVO': 'PENHORA',
'PENSAO_NOVO': 'PENSAO',
'OUTROS_PAGAMENTOS_NOVO': 'OUTROS_PAGAMENTOS',
'IMPOSTO_NOVO': 'IMPOSTO',
'GARANTIA_NOVO': 'GARANTIA'}

df_ff_trab_cons_pr88 = df_ff_trab_cons_pr77.withColumnsRenamed(rename_cols_pgtos_map)

df_ff_trab_cons_pr88 = df_ff_trab_cons_pr88.fillna({
    'ACORDO': 0,
    'CONDENACAO': 0,
    'PENHORA': 0,
    'PENSAO': 0,
    'OUTROS_PAGAMENTOS': 0,
    'IMPOSTO': 0,
    'GARANTIA': 0
})

# Sum the specified columns to create the 'TOTAL_PAGAMENTOS' column
df_ff_trab_cons_pr88 = df_ff_trab_cons_pr88.withColumn(
    'TOTAL_PAGAMENTOS',
    col('ACORDO') +
    col('CONDENACAO') +
    col('PENHORA') +
    col('OUTROS_PAGAMENTOS') +
    col('IMPOSTO') +
    col('GARANTIA')
)

df_ff_trab_cons_pr88.createOrReplaceTempView("TB_FECH_FIN_TRAB_CONSOLIDADO_88")

# COMMAND ----------

df_ff_trab_cons_pr.createOrReplaceTempView("zdf_ff_trab_cons_pr")

df_ff_trab_cons_pr88.createOrReplaceTempView("zdf_ff_trab_cons_pr88")

# COMMAND ----------

# df_ff_trab_cons_pr99 = spark.sql("""
# SELECT 
#     a.*,
#     b.*  -- ou selecione apenas as colunas desejadas de 'b'
# FROM zdf_ff_trab_cons_pr a
# LEFT JOIN zdf_ff_trab_cons_pr88 b
# ON a.ID_PROCESSO = b.ID_PROCESSO
# AND a.MES_FECH = b.MES_FECH
# """)


# COMMAND ----------

# df_ff_trab_cons_pr99 = merge_dfs(df_ff_trab_cons_pr, df_ff_trab_cons_pr88, ['ID_PROCESSO', 'MES_FECH'],  how='left')

# COMMAND ----------

df_ff_trab_cons_pr99 = df_ff_trab_cons_pr.join(
    df_ff_trab_cons_pr88, 
    on=['ID_PROCESSO', 'MES_FECH'], 
    how='left'
)

# COMMAND ----------

# Supondo que você queira que a lista contenha os nomes das colunas 'coluna1' e 'coluna2'
df_ff_trab_cols = ['MES_FECH','CADASTRO','DATACADASTRO','DISTRIBUICAO','REABERTURA','DATA_ADMISSAO','DATA_DISPENSA','DATA_REGISTRADO','PERIODO_RECLAMADO','PERIODO1','PERIODO2','DT_ULT_PGTO_NOVO']

# Para verificar se essas colunas existem no DataFrame e então criar a lista:
colunas_existentes = df_ff_trab_cons_pr99.columns
df_ff_trab_cols = [col for col in ['MES_FECH','CADASTRO','DATACADASTRO','DISTRIBUICAO','REABERTURA','DATA_ADMISSAO','DATA_DISPENSA','DATA_REGISTRADO','PERIODO_RECLAMADO','PERIODO1','PERIODO2','DT_ULT_PGTO_NOVO'] if col in colunas_existentes]

print(df_ff_trab_cols)

# COMMAND ----------

# # Converte as datas das colunas listadas
# df_ff_trab_cons_pr99 = convert_to_date_format(df_ff_trab_cons_pr99, df_ff_trab_cols)

# COMMAND ----------

df_ff_trab_cons_pr99.createOrReplaceTempView("TB_FECH_TRAB_99")

# COMMAND ----------

# %sql

# select * from TB_FECH_TRAB_99

# COMMAND ----------

df_ff_trab_cons_pr99 = df_ff_trab_cons_pr99.replace(float('NaN'), None)

df_ff_trab_cons_pr99 = deduplica_cols(df_ff_trab_cons_pr99)

df_ff_trab_cons_pr99.createOrReplaceTempView("TB_FECH_FIN_TRAB_CONSOLIDADO_99")

# COMMAND ----------

# MAGIC %md
# MAGIC ### 6 - Ajustes gerais e Aging

# COMMAND ----------

# MAGIC %md
# MAGIC #### 6.1 Ajustes gerais

# COMMAND ----------

df_ff_trab_cons_pr8 = spark.sql("""
SELECT 
     ID_PROCESSO
    ,DATACADASTRO
    ,MES_CADASTRO
    ,AREA_DO_DIREITO
    ,SUB_AREA_DO_DIREITO
    ,VALOR_DA_CAUSA
    ,EMPRESA
    ,STATUS
    ,ADVOGADO_RESPONSAVEL
    ,ESCRITORIO_RESPONSAVEL
    ,NUMERO_DO_PROCESSO
    ,PARTE_CONTRARIA_CPF
    ,PARTE_CONTRARIA_NOME
    ,ADVOGADO_PARTE_CONTRARIA
    ,MATRICULA
    ,BU
    ,VP
    ,DIRETORIA
    ,DESCRICAO_CENTRO_CUSTO
    ,RESPONSAVEL_AREA
    ,AREA_FUNCIONAL
    ,ESTADO
    ,COMARCA
    ,CLASSIFICACAO
    ,NATUREZA_OPERACIONAL
    ,TERCEIRO_PRINCIPAL
    ,NOVO_TERCEIRO
    ,TERCEIRO_AJUSTADO
    ,DATA_ADMISSAO
    ,DATA_DISPENSA
    ,DATA_REGISTRADO
    ,DISTRIBUICAO
    ,PERIODO_RECLAMADO
    ,PERIODO1
    ,PERIODO2
    ,PERIODO_RECL_GRUPO
    ,PARTE_CONTRARIA_CARGO_GRUPO
    ,CARGO
    ,MOTIVO_DESLIGAMENTO
    ,FASE
    ,FASE_ATUAL
    ,FILIAL
    ,INDICACAO_PROCESSO_ESTRATEGICO
    ,BANDEIRA
    ,NOME_DA_LOJA
    ,NOVOS
    ,ENCERRADOS
    ,ESTOQUE
    ,DT_ULT_PGTO
    ,DIRETORIA_DP_LOJA
    ,MES_FECH
    ,NOVO_X_LEGADO

    -- Aliases
    ,CARTEIRA AS PROCESSO_ESTEIRA
    ,DATE_FORMAT(DATA_ADMISSAO, 'y-MM-01') AS MES_DATA_ADMISSAO
    ,DATE_FORMAT(DATA_DISPENSA, 'y-MM-01') AS MES_DATA_DISPENSA
    ,DATEDIFF(month, DATA_ADMISSAO, DATA_DISPENSA) AS TEMPO_EMPRESA_MESES
    ,DATEDIFF(day, DATA_ADMISSAO, DATA_DISPENSA) AS TEMPO_EMPRESA_DIAS
    ,DATEDIFF(month, DATA_DISPENSA, DISTRIBUICAO) AS TEMPO_AJUIZAR_ACAO

    -- COALESCE
    ,COALESCE(`%_SOCIO_M`, 0.00) AS PERC_SOCIO_M
    ,COALESCE(`%_EMPRESA_M`, 0.00) AS PERC_EMPRESA_M
    ,COALESCE(PROVISAO_MOV_M, 0.00) AS PROVISAO_MOV_M
    ,COALESCE(PROVISAO_TOTAL_M_1, 0.00) AS PROVISAO_TOTAL_M_1
    ,COALESCE(PROVISAO_TOTAL_M, 0.00) AS PROVISAO_TOTAL_M
    ,COALESCE(CORRECAO_M_1, 0.00) AS CORRECAO_M_1
    ,COALESCE(CORRECAO_M, 0.00) AS CORRECAO_M
    ,COALESCE(CORRECAO_MOV_M, 0.00) AS CORRECAO_MOV_M
    ,COALESCE(PROVISAO_TOTAL_PASSIVO_M, 0.00) AS PROVISAO_TOTAL_PASSIVO_M
    ,COALESCE(`SOCIO:_PROVISAO_TOTAL_M`, 0.00) AS SOCIO_PROVISAO_TOTAL_M
    ,COALESCE(EMPRESA:_PROV_TOTAL_PASSIVO_M, 0.00) AS EMPRESA_PROV_TOTAL_PASSIVO_M
    ,COALESCE(EMPRESA_PROVISAO_MOV_M, 0.00) AS EMPRESA_PROVISAO_MOV_M
    ,COALESCE(ACORDOS, 0.00) AS ACORDO
    ,COALESCE(CONDENACAO, 0.00) AS CONDENACAO
    ,COALESCE(PENHORA, 0.00) AS PENHORA
--     ,COALESCE(PENSAO, 0.00) AS PENSAO
    ,COALESCE(OUTROS_PAGAMENTOS, 0.00) AS OUTROS_PAGAMENTOS
    ,COALESCE(IMPOSTO, 0.00) AS IMPOSTO
    ,COALESCE(GARANTIA, 0.00) AS GARANTIA
    ,COALESCE(TOTAL_PAGAMENTOS, 0.00) AS TOTAL_PAGAMENTOS
    ,(CASE WHEN ENCERRADOS = 1 THEN DATEDIFF(month, MES_CADASTRO, MES_FECH) END) AS MESES_AGING_ENCERR
    ,(CASE WHEN ENCERRADOS = 1 THEN DATEDIFF(day, MES_CADASTRO, MES_FECH) END) AS ANO_AGING_ENCERR
    ,(CASE WHEN ESTOQUE = 1 THEN DATEDIFF(month, MES_CADASTRO, MES_FECH) END) AS MESES_AGING_ESTOQ
    ,(CASE WHEN ESTOQUE = 1 THEN DATEDIFF(day, MES_CADASTRO, MES_FECH) END) AS ANO_AGING_ESTOQ
    ,(CASE WHEN ESTOQUE = 1 THEN DATEDIFF(month, DATACADASTRO, MES_FECH) END) AS AGING_ESTOQ_MESES
    ,(CASE WHEN ESTOQUE = 1 THEN DATEDIFF(day, DATACADASTRO, MES_FECH) END) AS BASE_ANO_AGING_ESTOQ --ADD
    ,(CASE WHEN ESTOQUE = 1 THEN ""
          WHEN NOVOS = 1 THEN  ""
          ELSE MOTIVO_ENCERRAMENTO 
     END) AS MOTIVO_ENCERRAMENTO

FROM
    TB_FECH_FIN_TRAB_CONSOLIDADO_99;
""")


# COMMAND ----------

# df_ff_trab_cons_pr8.count()

# COMMAND ----------

df_ff_trab_cons_pr8.createOrReplaceTempView("TB_FECH_FIN_TRAB_CONSOLIDADO_8")

# COMMAND ----------

# MAGIC %md
# MAGIC #### 6.2 Agrupamentos de faixas de datas

# COMMAND ----------

# AgingEstoque
data = [
    (-99999, 6, 'até 6 meses'),
    (6, 12, '7 - 12 meses'),
    (12, 18, '13 - 18 meses'),
    (18, 24, '19 - 24 meses'),
    (24, 9999999, '+24 meses')
]

# Define the schema for the DataFrame
schema = StructType([
    StructField("start_range", IntegerType(), False),
    StructField("end_range", IntegerType(), False),
    StructField("label", StringType(), False)
])

df_AgingEstoque = spark.createDataFrame(data, schema)
df_AgingEstoque.createOrReplaceTempView("AgingEstoque")
df_AgingEstoque.show()

# COMMAND ----------

# AgingTrab
data = [
    (-99999, 360, 'até 1 ano'),
    (360, 1080, '1 - 3 anos'),
    (1080, 2160, '3 - 6 anos'),
    (2160, 3240, '6 - 9 anos'),
    (3240, 99999999, '+9 anos')
]

# Define the schema for the DataFrame
schema = StructType([
    StructField("start_range", IntegerType(), False),
    StructField("end_range", IntegerType(), False),
    StructField("label", StringType(), False)
])

df_AgingTrab = spark.createDataFrame(data, schema)
df_AgingTrab.createOrReplaceTempView("AgingTrab")
df_AgingTrab.show()

# COMMAND ----------

# AgingNovoModelo

data = [
    (-99999, 360, 'até 1 ano'),
    (360, 720, '1 - 2 anos'),
    (720, 1080, '2 - 3 anos'),
    (1080, 1440, '3 - 4 anos'),
    (1440, 1800, '4 - 5 anos'),
    (1800, 2160, '5 - 6 anos'),
    (2160, 2520, '6 - 7 anos'),
    (2520, 9999999, '+7 anos')
]

# Define the schema for the DataFrame
schema = StructType([
    StructField("start_range", IntegerType(), False),
    StructField("end_range", IntegerType(), False),
    StructField("label", StringType(), False)
])

df_AgingNovoModelo = spark.createDataFrame(data, schema)
df_AgingNovoModelo.createOrReplaceTempView("AgingNovoModelo")
df_AgingNovoModelo.show()

# COMMAND ----------

# AgingTempoEmpresa

data = [
    (-99999, 360, 'até 1 ano'),
    (360, 720, '1 - 2 anos'),
    (720, 1080, '2 - 3 anos'),
    (1080, 1440, '3 - 4 anos'),
    (1440, 1800, '4 - 5 anos'),
    (1800, 2160, '5 - 6 anos'),
    (2160, 3600, '6 - 10 anos'),
    (3600, 9999999, 'Acima de 10 anos'),
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

df_ff_trab_cons_pr81 = spark.sql("""
          SELECT A.*
          ,C.label AS FX_ANO_AGING_ENCERR
          ,D.label AS FX_MES_AGING_ESTOQ
          ,E.label AS FX_ANO_AGING_ESTOQ
          ,F.label AS FX_ANO_AGING_ESTOQ_2
          ,G.label AS FX_TEMPO_EMPRESA
          FROM 
          TB_FECH_FIN_TRAB_CONSOLIDADO_8 A
          LEFT JOIN AgingEstoque B ON A.MESES_AGING_ENCERR > B.start_range AND A.MESES_AGING_ENCERR <= B.end_range
          LEFT JOIN AgingTrab C ON A.ANO_AGING_ENCERR > C.start_range AND A.ANO_AGING_ENCERR <= C.end_range
          LEFT JOIN AgingEstoque D ON A.AGING_ESTOQ_MESES > D.start_range AND A.AGING_ESTOQ_MESES <= D.end_range
          LEFT JOIN AgingNovoModelo E ON A.BASE_ANO_AGING_ESTOQ > E.start_range AND A.BASE_ANO_AGING_ESTOQ <= E.end_range
          LEFT JOIN AgingTrab F ON A.BASE_ANO_AGING_ESTOQ > F.start_range AND A.BASE_ANO_AGING_ESTOQ <= F.end_range
          LEFT JOIN AgingTempoEmpresa G ON A.TEMPO_EMPRESA_DIAS > G.start_range AND A.TEMPO_EMPRESA_DIAS <= G.end_range;
          """
)

# COMMAND ----------

# Dropando campos usados exclusivamente nos agrupamentos
df_ff_trab_cons_pr81 = df_ff_trab_cons_pr81.drop("TEMPO_EMPRESA_DIAS") \
                                           .drop("BASE_ANO_AGING_ESTOQ")

df_ff_trab_cons_pr81.createOrReplaceTempView("TB_FECH_FIN_TRAB_CONSOLIDADO_81")

# COMMAND ----------

# MAGIC %md
# MAGIC ### 7 - Agrupamentos e regras de negócio

# COMMAND ----------

# MAGIC %md
# MAGIC #### 7.1 Agrupamento campo Fase

# COMMAND ----------

df_ff_trab_cons_pr9 = spark.sql("""
    	SELECT *
			,(CASE WHEN FASE IN ('', 'N/A', 'INATIVO', 'ENCERRAMENTO', 'ADMINISTRATIVO', 'DEMAIS') THEN 'DEMAIS'
					WHEN FASE IN ('EXECUCAO',
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

					WHEN FASE IN ('RECURSAL',
									'RECURSAL - INATIVO',
									'RECURSAL TRT',
									'RECURSAL TRT - INATIVO'
								) THEN 'RECURSAL_TRT'

					WHEN FASE IN ('RECURSAL TST - INATIVO',
								  'RECURSAL TST'
								) THEN 'RECURSAL_TST'
 
					ELSE FASE END) AS DP_FASE
	
	FROM TB_FECH_FIN_TRAB_CONSOLIDADO_81
                                """
)
df_ff_trab_cons_pr9.createOrReplaceTempView("TB_FECH_FIN_TRAB_CONSOLIDADO_9")

# COMMAND ----------

# MAGIC %md
# MAGIC #### 7.2 Grupos áreas funcionais

# COMMAND ----------

df_ff_trab_cons_pr10 = spark.sql(
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
  FROM TB_FECH_FIN_TRAB_CONSOLIDADO_9
  """
)
df_ff_trab_cons_pr10.createOrReplaceTempView("TB_FECH_FIN_TRAB_CONSOLIDADO_10")

# COMMAND ----------

# %sql
# -- SELECT DISTINCT VP, DIRETORIA, DP_VP_DIRETORIA FROM TB_FECH_FIN_TRAB_CONSOLIDADO_10

# COMMAND ----------

# MAGIC %md
# MAGIC #### 7.3 Campo DP natureza

# COMMAND ----------

df_ff_trab_cons_pr12 = spark.sql(
  """
  SELECT *,
    CASE 
      WHEN PROVISAO_TOTAL_PASSIVO_M > 0 THEN 'PASSÍVEL PROVISÃO'
      WHEN NATUREZA_OPERACIONAL = 'TERCEIRO SOLVENTE' THEN 'TERCEIRO SOLVENTE'
      WHEN SUB_AREA_DO_DIREITO = 'CONTENCIOSO COLETIVO' THEN 'ESTRATEGICO'
      WHEN AREA_DO_DIREITO = 'TRABALHISTA COLETIVO' THEN 'ESTRATEGICO'
      WHEN INDICACAO_PROCESSO_ESTRATEGICO = 'DEFESA' THEN 'DEFESA'
     
      ELSE 'PASSÍVEL PROVISÃO'
  END AS DP_NATUREZA
  FROM TB_FECH_FIN_TRAB_CONSOLIDADO_10
  """
)
df_ff_trab_cons_pr12.createOrReplaceTempView("TB_FECH_FIN_TRAB_CONSOLIDADO_12")

# COMMAND ----------

# MAGIC %md
# MAGIC #### 7.4 Trimestre Estoque

# COMMAND ----------

df_ff_trab_cons_pr13 = spark.sql(
  """
  SELECT *,
    CASE 
      WHEN ESTOQUE >= 1 AND month(MES_FECH) IN (3,6,9,12) THEN 'SIM'
  END AS TRIM_ESTOQ
  FROM TB_FECH_FIN_TRAB_CONSOLIDADO_12
  """
)
df_ff_trab_cons_pr13.createOrReplaceTempView("TB_FECH_FIN_TRAB_CONSOLIDADO_13")

# COMMAND ----------

# MAGIC %sql
# MAGIC -- SELECT DISTINCT ESTOQUE, month(MES_FECH), TRIM_ESTOQ FROM TB_FECH_FIN_TRAB_CONSOLIDADO_12

# COMMAND ----------

# MAGIC %md
# MAGIC #### 7.5 Agrupamento Motivo de Encerramento

# COMMAND ----------

df_ff_trab_cons_pr14 = spark.sql(
    """
SELECT
  *,
  CASE
    WHEN ENCERRADOS = 1 THEN CASE
      WHEN ACORDO > 1 THEN 'ACORDO'
      WHEN (
        CONDENACAO  +PENHORA  +OUTROS_PAGAMENTOS+IMPOSTO + GARANTIA
      ) > 1 THEN 'CONDENACAO'
      WHEN (
        ACORDO + CONDENACAO + PENHORA + OUTROS_PAGAMENTOS + IMPOSTO + GARANTIA
      ) <= 1 THEN 'SEM ONUS'
    END
  END AS MOTIVO_ENC_AGRP
FROM
  TB_FECH_FIN_TRAB_CONSOLIDADO_13
"""
)
df_ff_trab_cons_pr14= df_ff_trab_cons_pr14.dropDuplicates(["MES_FECH", "ID_PROCESSO", "NOVOS", "ENCERRADOS", "ESTOQUE"])
df_ff_trab_cons_pr14.createOrReplaceTempView("TB_FECH_FIN_TRAB_CONSOLIDADO_14")

# COMMAND ----------

# MAGIC %md
# MAGIC #### 7.6 Campo assunto cargo

# COMMAND ----------

df_ff_trab_cons_pr15 = spark.sql("""
SELECT
  *,
  CASE
    WHEN PARTE_CONTRARIA_CARGO_GRUPO IN (
      'AJUDANTE',
      'AJUDANTE EXTERNO',
      'ANALISTA',
      'AUXILIAR',
      'CAIXA',
      'GERENTE',
      'MONTADOR',
      'MOTORISTA',
      'OPERADOR',
      'VENDEDOR'
    ) THEN PARTE_CONTRARIA_CARGO_GRUPO
    ELSE 'OUTROS'
  END AS CARGO_TRATADO
FROM
  TB_FECH_FIN_TRAB_CONSOLIDADO_14
  """
)
df_ff_trab_cons_pr15.createOrReplaceTempView("TB_FECH_FIN_TRAB_CONSOLIDADO_15")

# COMMAND ----------

# MAGIC %md
# MAGIC #### 7.7 Cluster aging estoque

# COMMAND ----------

df_ff_trab_cons_pr16 = spark.sql("""
SELECT
    *,
    CASE
        WHEN AGING_ESTOQ_MESES IS NOT NULL THEN
            CASE
                WHEN AGING_ESTOQ_MESES <= 12 THEN 'ATÉ 1 ANO'
                WHEN AGING_ESTOQ_MESES > 12 AND AGING_ESTOQ_MESES <= 24 THEN '1 - 2 ANOS'
                WHEN AGING_ESTOQ_MESES > 24 AND AGING_ESTOQ_MESES <= 36 THEN '2 - 3 ANOS'
                WHEN AGING_ESTOQ_MESES > 36 AND AGING_ESTOQ_MESES <= 48 THEN '3 - 4 ANOS'
                WHEN AGING_ESTOQ_MESES > 48 AND AGING_ESTOQ_MESES <= 60 THEN '4 - 5 ANOS'
                WHEN AGING_ESTOQ_MESES > 60 AND AGING_ESTOQ_MESES <= 72 THEN '5 - 6 ANOS'
                WHEN AGING_ESTOQ_MESES > 72 AND AGING_ESTOQ_MESES <= 84 THEN '6 - 7 ANOS'
                WHEN AGING_ESTOQ_MESES > 84 THEN '+7 ANOS'
            END
        WHEN MESES_AGING_ENCERR IS NOT NULL THEN
            CASE
                WHEN MESES_AGING_ENCERR <= 12 THEN 'ATÉ 1 ANO'
                WHEN MESES_AGING_ENCERR > 12 AND MESES_AGING_ENCERR <= 24 THEN '1 - 2 ANOS'
                WHEN MESES_AGING_ENCERR > 24 AND MESES_AGING_ENCERR <= 36 THEN '2 - 3 ANOS'
                WHEN MESES_AGING_ENCERR > 36 AND MESES_AGING_ENCERR <= 48 THEN '3 - 4 ANOS'
                WHEN MESES_AGING_ENCERR > 48 AND MESES_AGING_ENCERR <= 60 THEN '4 - 5 ANOS'
                WHEN MESES_AGING_ENCERR > 60 AND MESES_AGING_ENCERR <= 72 THEN '5 - 6 ANOS'
                WHEN MESES_AGING_ENCERR > 72 AND MESES_AGING_ENCERR <= 84 THEN '6 - 7 ANOS'
                WHEN MESES_AGING_ENCERR > 84 THEN '+7 ANOS'
            END
        ELSE 'Sem info'
    END AS CLUSTER_AGING
FROM TB_FECH_FIN_TRAB_CONSOLIDADO_15
"""
)
df_ff_trab_cons_pr16.createOrReplaceTempView("TB_FECH_FIN_TRAB_CONSOLIDADO_16")

# COMMAND ----------

# MAGIC %md
# MAGIC ### 8 - Importa base cluster valor

# COMMAND ----------

# MAGIC %md
# MAGIC #### 8.1 Importação e join

# COMMAND ----------

path_cluster_valor = '/Volumes/databox/juridico_comum/arquivos/trabalhista/bases_auxiliares/DE_PARA_CLUSTER_VALOR.xlsx'
df_cluster_valor = read_excel(path_cluster_valor)
df_cluster_valor.createOrReplaceTempView("TB_DP_CLUSTER_VALOR")

# df_cluster_valor.display()

# COMMAND ----------

df_ff_trab_cons_pr17 = spark.sql("""
SELECT
  A.*,
  b.PARA AS CLUSTER_VALOR
FROM
  TB_FECH_FIN_TRAB_CONSOLIDADO_16 A
  LEFT JOIN TB_DP_CLUSTER_VALOR B ON A.ESTADO = B.DE
  """
)
df_ff_trab_cons_pr17.createOrReplaceTempView("TB_FECH_FIN_TRAB_CONSOLIDADO_17")

# COMMAND ----------

# MAGIC %md
# MAGIC #### 8.2 Safra reclamação

# COMMAND ----------

df_ff_trab_cons_pr18 = spark.sql("""
SELECT
A.*,
    CASE
        WHEN DATA_DISPENSA IS NOT NULL THEN
            CASE
                WHEN TEMPO_EMPRESA_MESES <= 60 THEN 
                    to_char(EXTRACT(YEAR FROM DATA_DISPENSA), '9999')
                    --||"-"||to_char(EXTRACT(MONTH FROM DATA_DISPENSA),'09')
                WHEN TEMPO_EMPRESA_MESES > 60 THEN
                  to_char(EXTRACT(YEAR FROM DATA_DISPENSA) - 5, '9999') 
            END
        ELSE 'Sem info'
    END AS SAFRA_RECLAMACAO
FROM TB_FECH_FIN_TRAB_CONSOLIDADO_17 A
"""
)
df_ff_trab_cons_pr18.createOrReplaceTempView("TB_FECH_FIN_TRAB_CONSOLIDADO_18")

# COMMAND ----------

# MAGIC %md
# MAGIC #### 8.3 Faixa tempo de empresa
# MAGIC Comentar que já existe no FX_TEMPO_EMPRESA

# COMMAND ----------

# MAGIC %sql
# MAGIC -- SELECT DISTINCT TEMPO_EMPRESA_MESES, FX_TEMPO_EMPRESA FROM TB_FECH_FIN_TRAB_CONSOLIDADO_21

# COMMAND ----------

# MAGIC %md
# MAGIC ### 9 - Terceiro insolvente

# COMMAND ----------

path_terceiros = '/Volumes/databox/juridico_comum/arquivos/trabalhista/bases_auxiliares/DE_PARA_TERCEIRO_INSOLVENTE_V1.xlsx'
df_terceiros = read_excel(path_terceiros)
df_terceiros.createOrReplaceTempView("TB_DP_TERCEIROS")

# df_terceiros.display()

# COMMAND ----------

df_ff_trab_cons_pr19 = spark.sql("""
	SELECT A.*
			,(CASE WHEN B.TERCEIRO_AJUSTADO IS NULL THEN 'Sem info'
				ELSE  B.TERCEIRO_AJUSTADO END) AS ET

	FROM TB_FECH_FIN_TRAB_CONSOLIDADO_18 A
	LEFT JOIN TB_DP_TERCEIROS B ON A.TERCEIRO_AJUSTADO=B.`EMPRESA TERCEIRIZADA`
 """
)

df_ff_trab_cons_pr19.createOrReplaceTempView("TB_FECH_FIN_TRAB_F")

# COMMAND ----------

df_ff_trab_cons_pr19.select("MES_FECH", "MOTIVO_ENC_AGRP").count()

# COMMAND ----------

display(df_ff_trab_cons_pr19)

# COMMAND ----------

# MAGIC %md
# MAGIC ### 10 - Grava tabela final

# COMMAND ----------

# spark.sql("DROP TABLE databox.juridico_comum.tb_fecham_financ_trab_202406")

# COMMAND ----------

df_ff_trab_cons_pr19.write.format("delta").mode("overwrite").option("overwriteSchema", "true").saveAsTable(f"databox.juridico_comum.tb_fecham_financ_trab_{nmmes}")

# COMMAND ----------

df_ff_trab_cons_pr19.createOrReplaceTempView("TB_FECH_FIN_TRAB_F")

# COMMAND ----------

nmmes = dbutils.widgets.get("nmmes")

df_ff_trab_cons_pr19.write.format("delta").mode("overwrite").saveAsTable(f"databox.juridico_comum.tb_fecham_financ_trab_{nmmes}")

# COMMAND ----------

nmmes = dbutils.widgets.get("nmmes")
df = spark.read.table(f"databox.juridico_comum.tb_fecham_financ_trab_{nmmes}")
display(df)

# COMMAND ----------

# MAGIC %md
# MAGIC # Conferencias

# COMMAND ----------

#Conferência para validar se a volumetria dos dados mensais está correta



trabalhista_total = spark.sql("""
 		SELECT  MES_FECH
	 			,SUM(NOVOS) AS NOVOS
				,SUM(ENCERRADOS) AS ENCERRADOS 
 				,SUM(ESTOQUE) AS ESTOQUE 
				,SUM(ACORDO) AS ACORDO 
 				,SUM(CONDENACAO) AS CONDENACAO 
				,SUM(TOTAL_PAGAMENTOS) AS TOTAL_PAGAMENTOS 
				,SUM(PROVISAO_TOTAL_PASSIVO_M) AS PROVISAO_TOTAL_PASSIVO_M 
				,SUM(PROVISAO_MOV_M) AS PROVISAO_MOV_M 
				
 			FROM TB_FECH_FIN_TRAB_F
 			GROUP BY MES_FECH
	ORDER BY 1
  """)

display(trabalhista_total)

# COMMAND ----------

# MAGIC %md
# MAGIC ####Cria a base de encerrados do INSS e compartilha com o Tributário

# COMMAND ----------

nmmes = dbutils.widgets.get("nmmes")
nmmesano = nmmes[:-2]
nmmes_mes = nmmes[4:]
nmmesdata = f"'{nmmesano}-{nmmes_mes}-01'"

tb_inss_encerr_trab_f = spark.sql(f"""
SELECT
   ID_PROCESSO
	,MES_CADASTRO
	,VALOR_DA_CAUSA
	,EMPRESA
	,NUMERO_DO_PROCESSO
	,PROCESSO_ESTEIRA
	,ESTADO
	,COMARCA
	,CLASSIFICACAO
	,DATA_ADMISSAO
	,MES_DATA_ADMISSAO
	,DATA_DISPENSA
	,MES_DATA_DISPENSA
	,TEMPO_EMPRESA_MESES
	,DATA_REGISTRADO
	,DISTRIBUICAO
	,PERIODO_RECLAMADO
	,PERIODO1
	,PERIODO2
	,PERIODO_RECL_GRUPO
	,PARTE_CONTRARIA_CARGO_GRUPO
	,CARGO
	,FASE
	,PERC_SOCIO_M
	,PERC_EMPRESA_M
	,PROVISAO_MOV_M
	,PROVISAO_TOTAL_M_1
	,PROVISAO_TOTAL_M
	,CORRECAO_M_1
	,CORRECAO_M
	,CORRECAO_MOV_M
	,PROVISAO_TOTAL_PASSIVO_M
	,SOCIO_PROVISAO_TOTAL_M
	,EMPRESA_PROV_TOTAL_PASSIVO_M
	,EMPRESA_PROVISAO_MOV_M
	,MOTIVO_ENCERRAMENTO
	,ACORDO
	,CONDENACAO
	,PENHORA
	,OUTROS_PAGAMENTOS
	,IMPOSTO
	,GARANTIA
	,TOTAL_PAGAMENTOS
	,DT_ULT_PGTO
	,MES_FECH
	,DP_FASE
	,MOTIVO_ENC_AGRP
FROM
  databox.juridico_comum.tb_fecham_financ_trab_{nmmes}
  WHERE ENCERRADOS = 1 AND MES_FECH >= {nmmesdata}
"""
)

tb_inss_encerr_trab_f.createOrReplaceTempView('TB_TEMP_VALIDAR_INSS')

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from TB_TEMP_VALIDAR_INSS

# COMMAND ----------

tb_inss_encerr_trab_ff = spark.sql('''
    SELECT 
    A.ID_PROCESSO
        ,A.MES_CADASTRO
        ,A.VALOR_DA_CAUSA
        ,A.EMPRESA
        ,A.NUMERO_DO_PROCESSO
        ,A.PROCESSO_ESTEIRA
        ,A.ESTADO
        ,A.COMARCA
        ,A.CLASSIFICACAO
        ,A.DATA_ADMISSAO
        ,A.MES_DATA_ADMISSAO
        ,A.DATA_DISPENSA
        ,A.MES_DATA_DISPENSA
        ,A.TEMPO_EMPRESA_MESES
        ,A.DATA_REGISTRADO
        ,A.DISTRIBUICAO
        ,A.PERIODO_RECLAMADO
        ,A.PERIODO1
        ,A.PERIODO2
        ,A.PERIODO_RECL_GRUPO
        ,A.PARTE_CONTRARIA_CARGO_GRUPO
        ,A.CARGO
        ,A.FASE
        ,A.PERC_SOCIO_M
        ,A.PERC_EMPRESA_M
        ,A.PROVISAO_MOV_M
        ,A.PROVISAO_TOTAL_M_1
        ,A.PROVISAO_TOTAL_M
        ,A.CORRECAO_M_1
        ,A.CORRECAO_M
        ,A.CORRECAO_MOV_M
        ,A.PROVISAO_TOTAL_PASSIVO_M
        ,A.SOCIO_PROVISAO_TOTAL_M
        ,A.EMPRESA_PROV_TOTAL_PASSIVO_M
        ,A.EMPRESA_PROVISAO_MOV_M
        ,A.MOTIVO_ENCERRAMENTO
        ,B.ACORDO
        ,B.CONDENACAO
        ,B.PENHORA
        ,B.OUTROS_PAGAMENTOS
        ,B.IMPOSTO
        ,B.GARANTIA
        ,B.ACORDO + B.CONDENACAO + B.PENHORA + B.OUTROS_PAGAMENTOS + B.IMPOSTO + B.GARANTIA AS TOTAL_PAGAMENTOS
        ,B.DT_ULT_PGTO
        ,A.MES_FECH
        ,A.DP_FASE
        ,CASE  WHEN B.ACORDO > 1 THEN 'ACORDO'
        WHEN (
            B.CONDENACAO + B.PENHORA + B.OUTROS_PAGAMENTOS + B.IMPOSTO + B.GARANTIA
            ) > 1 THEN 'CONDENACAO'
            WHEN (
            B.ACORDO + B.CONDENACAO + B.PENHORA + B.OUTROS_PAGAMENTOS + B.IMPOSTO + B.GARANTIA
            ) <= 1 THEN 'SEM ONUS'
        END AS MOTIVO_ENC_AGRP
    FROM TB_TEMP_VALIDAR_INSS A 
    LEFT JOIN global_temp.HIST_PAGAMENTOS_GARANTIA_TRAB_FF AS B 
    ON A.ID_PROCESSO = B.PROCESSO_ID
''')

# COMMAND ----------

# MAGIC %md
# MAGIC ####Cria e salva a base do INSS em excel

# COMMAND ----------

import pandas as pd
import re
from shutil import copyfile

# Converter PySpark DataFrame para Pandas DataFrame
pandas_df = tb_inss_encerr_trab_ff.toPandas()

# Salvar o DataFrame com os nomes corrigidos em um arquivo Excel no disco local primeiro
local_path = f'/local_disk0/tmp/tb_inss_encerr_trab_ftemp.xlsx'
pandas_df.to_excel(local_path, index=False, sheet_name='TB_FECHAM_FINANC_TRAB')

# Copiar o arquivo do disco local para o volume desejado
volume_path = f'/Volumes/databox/juridico_comum/arquivos/trabalhista/bases_indicadores_resultado/tb_inss_encerr_trab_{nmmes}_f.xlsx'

copyfile(local_path, volume_path)
