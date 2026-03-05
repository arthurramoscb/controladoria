# Databricks notebook source
# MAGIC %md
# MAGIC # Tratamento do Fechamento Financeiro
# MAGIC
# MAGIC **Inputs**\
# MAGIC Planilha Fechamento Financeiro (ff) fornecida pelo departamento financeiro
# MAGIC
# MAGIC **Outputs**\
# MAGIC Base Fechamento Finaceiro tratado tb_fechamento_trabalhista

# COMMAND ----------

# Configura campo para que o usuário insira parâmetros

# Parametro do formato de data do arquivo financeiro. Ex: 22.02.2024
dbutils.widgets.text("nmtabela_finan", "")

# Parametro de data da tabela trab_ger_consolidado_ . Ex: 20240423
dbutils.widgets.text("nmtabela_trab_ger_consolidado", "")

# Parametro de data da tabela MESFECH. Ex: 01/04/2024
# Será utilizado como valor de uma coluna no join com o passo anterior
dbutils.widgets.text("mes_fechamento", "")

# COMMAND ----------

from datetime import datetime

# COMMAND ----------

# MAGIC %run "/Workspace/Jurídico/funcao_tratamento_fechamento/common_functions"

# COMMAND ----------

# MAGIC %md
# MAGIC ### 1 - Carga e tratamentos iniciais do Fechamento Financeiro

# COMMAND ----------

# Caminho das pastas e arquivos
nmtabela_finan = dbutils.widgets.get("nmtabela_finan")

path_ff = f'/Volumes/databox/juridico_comum/arquivos/trabalhista/bases_fechamento_financeiro/trabalhista_base_financeiro/{nmtabela_finan} Fechamento Trabalhista.xlsx'

path_ff

# COMMAND ----------

# Carrega a planilha em Spark Data Frame
df_ff = read_excel(path_ff, "'Base Fechamento'!A2:CJ1048576")

# Remove ultimas colunas a direita que causão erro na importação. Ex: 'Cluster Aging Tempo de Empresa99'
#df_ff = df_ff.select(df_ff.columns[:98])

# Remove linhas sem dados
df_ff = df_ff.where("`ID PROCESSO` IS NOT NULL")

# COMMAND ----------

df_ff.count() 

# COMMAND ----------

df_ff.orderBy("ID PROCESSO").display()

# COMMAND ----------

# Ajusta os nomes das colunas. Pontos "." em especial geram muitos erros
df_ff = adjust_column_names(df_ff)

# COMMAND ----------

df_ff.count()

# COMMAND ----------

# Remove exesso de espaços das colunas listadas
compress_colunas = ['DATACADASTRO',
 'Reabertura',
 'Distribuição',
 'Encerrados',
#  'Média_de_Pagamento',
 '%_Sócio_M_1',
 '%_Empresa_M_1',
 '%_Sócio_M',
 '%_Empresa_M',
 'EMPRESA:_Correção_Mov_M',
 'EMPRESA:_Provisão_Mov_Total_M',
 'Centro_de_Custo_M_1',
 'Centro_de_Custo_M']

df_ff = compress_values(df_ff, compress_colunas)

# COMMAND ----------

# Converte as colunas listadas para o tipo data
colunas_data = ['DATACADASTRO','Reabertura','Distribuição']

df_ff = convert_to_date_format(df_ff, colunas_data)
# display(df_ff)

# COMMAND ----------

display(df_ff)

# COMMAND ----------

colunas_numeros = ['Centro_de_Custo_M_1', 'Centro_de_Custo_M']

df_ff = convert_to_float(df_ff, colunas_numeros)
# display(df_ff)

# COMMAND ----------

import pandas as pd
import re
from shutil import copyfile
import openpyxl
# Converter PySpark DataFrame para Pandas DataFrame
pandas_df = df_ff.toPandas()

# Salvar o DataFrame com os nomes corrigidos em um arquivo Excel no disco local primeiro
local_path = f'/local_disk0/tmp/df_fff.xlsx'
pandas_df.to_excel(local_path, index=False, sheet_name='Teste')

# Copiar o arquivo do disco local para o volume desejado
volume_path = f'/Volumes/databox/juridico_comum/arquivos/trabalhista/bases_fechamento_financeiro/trabalhista_base_financeiro/df_ff.xlsx'

copyfile(local_path, volume_path)


# COMMAND ----------

df_ff.count()

# COMMAND ----------

# MAGIC %md
# MAGIC ### 2 - Carga da Trabalhista Gerencial Consolidado e join com a FF

# COMMAND ----------

nmtabela_trab_ger_consolidado = dbutils.widgets.get("nmtabela_trab_ger_consolidado")

df_trab_ger_consolidado = spark.read.table(f"databox.juridico_comum.trab_ger_consolida_{nmtabela_trab_ger_consolidado}")

df_trab_ger_consolidado = adjust_column_names(df_trab_ger_consolidado)

# COMMAND ----------

df_ff.createOrReplaceTempView("FECHAMENTO_TRAB_2")
df_trab_ger_consolidado.createOrReplaceTempView(f"TRAB_GER_CONSOLIDA")

df_fechamento_trab = spark.sql("""
/* CARREGA AS INFORMAÇÕES ADICIONAIS NA BASE DO FECHAMENTO */
SELECT A.*,
    CASE WHEN A.DATACADASTRO IS NULL THEN B.DATA_REGISTRADO
         ELSE A.DATACADASTRO END AS CADASTRO_NOVO,

    CASE WHEN A.STATUS_M IS NULL THEN B.STATUS 
         ELSE A.STATUS_M END AS STATUS_NOVO,

    CASE WHEN B.FASE IS NULL THEN A.FASE_M
         ELSE B.FASE END AS FASE_NOVO,

    B.CARTEIRA,

    CASE WHEN A.`INDICAÇÃO_PROCESSO_ESTRATÉGICO` IS NULL THEN B.`ULTIMA_POSICAO_ESTRATEGIA_JUSTIFICATIVA_ACORDO_DEFESA`
         ELSE A.`INDICAÇÃO_PROCESSO_ESTRATÉGICO` END AS ESTRATEGIA

FROM FECHAMENTO_TRAB_2 AS A
LEFT JOIN TRAB_GER_CONSOLIDA AS B ON A.ID_PROCESSO = B.PROCESSO_ID;
""")

# Add o mes_fechamento
mes_fechamento = dbutils.widgets.get("mes_fechamento")
df_fechamento_trab = df_fechamento_trab.withColumn("MES_FECH", to_date(lit(mes_fechamento), 'dd/MM/yyyy'))

# COMMAND ----------

df_fechamento_trab.count()



# COMMAND ----------

df_fechamento_trab.createOrReplaceTempView("FECHAMENTO_TRAB_3")

df_fechamento_trab_4 = spark.sql("""
/* FAZ OS "DE PARAS" NECESSÁRIOS NA BASE */
SELECT *,
    CASE WHEN FASE_NOVO IN ('', 'N/A', 'INATIVO', 'ENCERRAMENTO', 'ADMINISTRATIVO') THEN 'DEMAIS'
         WHEN FASE_NOVO IN ('EXECUÇÃO', 
                            'EXECUÇÃO - TRT', 
                            'EXECUÇÃO - TST', 
                            'EXECUÇÃO DEFINITIVA', 
                            'EXECUÇÃO DEFINITIVA (TRT)', 
                            'EXECUÇÃO DEFINITIVA (TST)',
                            'EXECUÇÃO DEFINITIVA PROSSEGUIMENTO', 
                            'EXECUÇÃO PROVISORIA (TRT)',
                            'EXECUÇÃO PROVISORIA (TST)',
                            'EXECUÇÃO PROVISÓRIA',
                            'EXECUÇÃO PROVISÓRIA PROSSEGUIMENTO') THEN 'EXECUÇÃO'
         WHEN FASE_NOVO IN ('RECURSAL',
                            'RECURSAL TRT',
                            'RECURSAL TST',
                            'RECURSAL - INATIVO',
                            'RECURSAL TRT - INATIVO') THEN 'RECURSAL' 
         ELSE FASE_NOVO END AS DP_FASE,
    CASE WHEN DOC IN ('ACORDO 1 - PÓS ANÁLISE DE SUBSÍDIOS - DOCUMENTAÇÃO COMPLETA, MAS É DESFAVORÁVEL',
                           'ACORDO 1 - PÓS ANÁLISE DE SUBSÍDIOS - DOCUMENTAÇÃO ENTREGUE FORA DO PRAZO DA TAREFA DE SUBSÍDIOS',
                           'ACORDO 1 - PÓS ANÁLISE DE SUBSÍDIOS - DOCUMENTAÇÃO NÃO ATENDE (DOCUMENTOS INSUFICIENTES PARA DEFESA)',
                           'ACORDO 2 - PÓS AUDIÊNCIA - AUDIÊNCIA ADIADA/CONCILIAÇÃO FRUSTRADA - DOCUMENTAÇÃO NÃO ATENDE OU É DESFAVORÁVEL',
                           'ACORDO 2 - PÓS AUDIÊNCIA - AUDIÊNCIA FAVORÁVEL - MAS DOCUMENTAÇÃO NÃO FOI SUFICIENTE NA APRESENTAÇÃO DA DEFESA',
                           'ACORDO 2 - PÓS AUDIÊNCIA - CONCILIAÇÃO REALIZADA - (DOCUMENTAÇÃO NÃO FOI SUFICIENTE NA APRESENTAÇÃO DA DEFESA)',
                           'ACORDO 3 - PÓS ACÓRDÃO - NULIDADE DA SENTENÇA/RETORNO AO CONHECIMENTO - DOCUMENTAÇÃO NÃO ATENDE') THEN 'SIM' ELSE 'NÃO' END AS PROCESSO_COM_DOCUMENTO
FROM FECHAMENTO_TRAB_3
""")

# COMMAND ----------

df_fechamento_trab_4.count()

# COMMAND ----------

# MAGIC %md
# MAGIC ### 3 - Carga e join dos Pagamentos e Garantias

# COMMAND ----------

df_pgto_garantias = spark.sql("select * from databox.juridico_comum.stg_trab_pgto_garantias")

# COMMAND ----------

df_fechamento_trab_4.createOrReplaceTempView("FECHAMENTO_TRAB_4")

df_fechamento_trab_5 = spark.sql(f"""
/* CARREGA OS PAGAMENTOS DE ACORDOS, CONDENAÇÕES E GARANTIAS NA BASE */
SELECT A.ID_PROCESSO
		,A.MES_FECH
		,B.ACORDO
		,B.CONDENACAO
		,B.PENHORA
		,B.GARANTIA
		,B.IMPOSTO
		,B.OUTROS_PAGAMENTOS
		,A.ENCERRADOS
		,B.TOTAL_PAGAMENTOS
		,CAST(B.DT_ULT_PGTO AS DATE) AS DATA_EFETIVA_DO_PAGAMENTO

	FROM FECHAMENTO_TRAB_4 AS A
	LEFT JOIN databox.juridico_comum.tb_trab_pgto_garantias_ff AS B ON A.ID_PROCESSO = B.PROCESSO_ID
	WHERE A.ID_PROCESSO IS NOT NULL AND A.ENCERRADOS = 1
""")

# COMMAND ----------

df_fechamento_trab_5.count()

# COMMAND ----------

# MAGIC %md
# MAGIC ### 4 - Adição do Mes Contábil

# COMMAND ----------

# MAGIC %run 
# MAGIC "./mes_contabil"

# COMMAND ----------

df_fechamento_trab_5.createOrReplaceTempView("FECHAMENTO_TRAB_51")
df_mes_contabil.createOrReplaceTempView("TB_MES_CONTABIL")

df_fechamento_trab_5 = spark.sql("""
    SELECT
    A.ID_PROCESSO,
    A.MES_FECH,
    A.ACORDO,
    A.CONDENACAO,
    A.PENHORA,
    A.GARANTIA,
    A.IMPOSTO,
    A.OUTROS_PAGAMENTOS,
    A.ENCERRADOS,
    A.TOTAL_PAGAMENTOS,
    A.DATA_EFETIVA_DO_PAGAMENTO,
    B.mes_contabil AS MES_CONTABIL
FROM
    FECHAMENTO_TRAB_51 A
LEFT JOIN
    TB_MES_CONTABIL B
ON
    A.DATA_EFETIVA_DO_PAGAMENTO BETWEEN B.dt_contabil_inicio AND B.dt_contabil_fim
""")

# COMMAND ----------

df_fechamento_trab_5.createOrReplaceTempView("FECHAMENTO_TRAB_5")

df_fechamento_trab_6 = spark.sql(f"""
	SELECT ID_PROCESSO
			,MES_FECH
			,ENCERRADOS
			,MAX(MES_CONTABIL) AS DT_ULT_PGTO
			,SUM(ACORDO) AS ACORDOS
			,SUM(CONDENACAO) AS `CONDENAÇÃO`
			,SUM(PENHORA) AS PENHORA
			,SUM(GARANTIA) AS GARANTIA
			,SUM(IMPOSTO) AS IMPOSTO
			,SUM(OUTROS_PAGAMENTOS) AS OUTROS_PAGAMENTOS
			,SUM(TOTAL_PAGAMENTOS) AS TOTAL_PAGAMENTOS

	FROM FECHAMENTO_TRAB_5
	WHERE MES_CONTABIL <= MES_FECH
	GROUP BY 1, 2, 3
	ORDER BY ID_PROCESSO
 """)

# COMMAND ----------

df_fechamento_trab_5.count()

# COMMAND ----------

df_fechamento_trab_6.createOrReplaceTempView("FECHAMENTO_TRAB_6")

# COMMAND ----------

df_fechamento_trab_6.createOrReplaceTempView("FECHAMENTO_TRAB_6")

df_fechamento_trab_final = spark.sql(f"""
SELECT A.*
      ,B.DT_ULT_PGTO
      ,B.ACORDOS
      ,B.`CONDENAÇÃO`
      ,B.PENHORA
      ,B.GARANTIA
      ,B.IMPOSTO
      ,B.OUTROS_PAGAMENTOS
      ,B.TOTAL_PAGAMENTOS
FROM FECHAMENTO_TRAB_4 A
LEFT JOIN FECHAMENTO_TRAB_6 B
ON A.ID_PROCESSO = B.ID_PROCESSO
   AND A.MES_FECH = B.MES_FECH
   AND A.ENCERRADOS = B.ENCERRADOS
ORDER BY ID_PROCESSO
""")

# COMMAND ----------

df_fechamento_trab_final.count()

# COMMAND ----------

# MAGIC %md
# MAGIC ### 5 - Tratamentos finais

# COMMAND ----------

df_fechamento_trab_final = df_fechamento_trab_final.withColumn("FASE_FINAL", when(col("FASE_M") == "", col("FASE_NOVO")) \
                                                               .otherwise(col("FASE_M")))

# COMMAND ----------

df_fechamento_trab_final = df_fechamento_trab_final.drop("DATACADASTRO", "STATUS", "FASE", "FASE_NOVO", "FASE_M", "STATUS_M")

# COMMAND ----------

df_fechamento_trab_final = df_fechamento_trab_final.withColumnRenamed('CADASTRO_NOVO', 'DATACADASTRO') \
                                                   .withColumnRenamed('STATUS_NOVO', 'STATUS_M') \
                                                   .withColumnRenamed('FASE_FINAL', 'FASE_M')

# COMMAND ----------

nmtabela = nmtabela_trab_ger_consolidado[:6]
print(nmtabela)

# COMMAND ----------

# Tira a duplicidade de campos da base. Mantém a primeira coluna encontrada na base
df_fechamento_trab_final = deduplica_cols(df_fechamento_trab_final)

# COMMAND ----------

# Remove acentos

df_fechamento_trab_final = remove_acentos(df_fechamento_trab_final)

# COMMAND ----------

df_fechamento_trab_final.count() #31254 | 30370

# COMMAND ----------

df_fechamento_trab_final.createOrReplaceGlobalTempView('df_fechamento_trab_final')

# COMMAND ----------

c = df_fechamento_trab_final.columns

print(c)

# COMMAND ----------

df_fechamento_trab_final.write.format("delta") \
    .option("mergeSchema", "true") \
    .option("overwriteSchema", "true") \
    .mode("overwrite") \
    .saveAsTable(f"databox.juridico_comum.tb_fecham_trab_{nmtabela}")

# COMMAND ----------

df_tb_fecham_f = spark.sql(f'''
                 SELECT * FROM databox.juridico_comum.tb_fecham_trab_{nmtabela}   
                           
                           
                           ''')

df_tb_fecham_f.createOrReplaceTempView('TB_FECH_FIN_TRAB_F')

# COMMAND ----------

trabalhista_total = spark.sql("""
		SELECT  MES_FECH
	 			,SUM(NOVOS) AS NOVOS
                ,SUM(REATIVADOS) AS REATIVADOS
				,SUM(ENCERRADOS) AS ENCERRADOS 
				,SUM(ESTOQUE) AS ESTOQUE 
				,SUM(ACORDOS) AS ACORDO 
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

msg = f"tb_trab_fecham_{nmtabela}"
dbutils.notebook.exit(msg)
