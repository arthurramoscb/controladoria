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
df_ff = read_excel(path_ff, "'Base Fechamento'!A2")

# Remove ultimas colunas a direita que causão erro na importação. Ex: 'Cluster Aging Tempo de Empresa99'
#df_ff = df_ff.select(df_ff.columns[:98])

# Remove linhas sem dados
df_ff = df_ff.where("`ID PROCESSO` IS NOT NULL")

# COMMAND ----------

# Ajusta os nomes das colunas. Pontos "." em especial geram muitos erros
df_ff = adjust_column_names(df_ff)

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

colunas_numeros = ['Centro_de_Custo_M_1', 'Centro_de_Custo_M']

df_ff = convert_to_float(df_ff, colunas_numeros)
# display(df_ff)

# COMMAND ----------

import pandas as pd
import re
from shutil import copyfile

# Converter PySpark DataFrame para Pandas DataFrame
pandas_df = df_ff.toPandas()

# Salvar o DataFrame com os nomes corrigidos em um arquivo Excel no disco local primeiro
local_path = f'/local_disk0/tmp/df_ff.xlsx'
pandas_df.to_excel(local_path, index=False, sheet_name='Teste')

# Copiar o arquivo do disco local para o volume desejado
volume_path = f'/Volumes/databox/juridico_comum/arquivos/trabalhista/bases_fechamento_financeiro/trabalhista_base_financeiro/df_ff.xlsx'

copyfile(local_path, volume_path)


# COMMAND ----------

# MAGIC %md
# MAGIC ### 2 - Carga da Trabalhista Gerencial Consolidado e join com a FF

# COMMAND ----------

nmtabela_trab_ger_consolidado = dbutils.widgets.get("nmtabela_trab_ger_consolidado")

df_trab_ger_consolidado = spark.read.table(f"databox.juridico_comum.trab_ger_consolida_{nmtabela_trab_ger_consolidado}")

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

    CASE WHEN A.`INDICAÇÃO_PROCESSO_ESTRATÉGICO` IS NULL THEN B.`ULTIMA_POSICAO_ESTRATEGIA_JUSTIFICATIVA_ACORDO_DEFESA`    ---ULTIMA_POSICAO_|           _ESTRATEGIA_JUSTIFICATIVA_ACORDO_DEFESA
         ELSE A.`INDICAÇÃO_PROCESSO_ESTRATÉGICO` END AS ESTRATEGIA

FROM FECHAMENTO_TRAB_2 AS A
LEFT JOIN TRAB_GER_CONSOLIDA AS B ON A.ID_PROCESSO = B.PROCESSO_ID;
""")

# Add o mes_fechamento
mes_fechamento = dbutils.widgets.get("mes_fechamento")
df_fechamento_trab = df_fechamento_trab.withColumn("MES_FECH", to_date(lit(mes_fechamento), 'dd/MM/yyyy'))

# COMMAND ----------

# Seleciona o ID (para referência) e a coluna nova para visualizar
display(df_fechamento_trab.select("ID_PROCESSO", "MES_FECH").limit(10))

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

# MAGIC %md
# MAGIC ### 3 - Carga e join dos Pagamentos e Garantias

# COMMAND ----------

df_pgto_garantias = spark.sql("select * from global_temp.HIST_PAGAMENTOS_GARANTIA_TRAB_F")

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
		,B.DATA_EFETIVA_DO_PAGAMENTO

	FROM FECHAMENTO_TRAB_4 AS A
	LEFT JOIN global_temp.HIST_PAGAMENTOS_GARANTIA_TRAB_F AS B ON A.ID_PROCESSO = B.PROCESSO_ID
	WHERE A.ID_PROCESSO IS NOT NULL AND A.ENCERRADOS = 1
""")

# COMMAND ----------

# MAGIC %md
# MAGIC ### 4 - Adição do Mes Contábil

# COMMAND ----------

# MAGIC %run "./mes_contabil"

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

display(df_fechamento_trab_final)

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

name_mapping_final = {
    "ÁREA_DO_DIREITO": "AREA_DO_DIREITO", 
    "SUB_ÁREA_DO_DIREITO": "SUB_AREA_DO_DIREITO", 
    "ÓRGÃO_OFENSOR_FLUXO_M_1": "ORGAO_OFENSOR_FLUXO_M_1", 
    "ÓRGÃO_OFENSOR_FLUXO_M": "ORGAO_OFENSOR_FLUXO_M", 
    "Distribuição": "Distribuicao", 
    "Nº_PROCESSO": "No_PROCESSO", 
    "PROVISÃO_M_1": "PROVISAO_M_1", 
    "CLASSIFICAÇÃO_MOV_M": "CLASSIFICACAO_MOV_M", 
    "PROVISÃO_MOV_M": "PROVISAO_MOV_M", 
    "PROVISÃO_MOV_TOTAL_M": "PROVISAO_MOV_TOTAL_M", 
    "PROVISÃO_TOTAL_M": "PROVISAO_TOTAL_M", 
    "PROVISÃO_TOTAL_PASSIVO_M": "PROVISAO_TOTAL_PASSIVO_M", 
    "SOCIO:_PROVISÃO_M_1": "SOCIO:_PROVISAO_M_1", 
    "SOCIO:_CORREÇÃO_M_1": "SOCIO:_CORRECAO_M_1", 
    "SOCIO:_PROVISÃO_TOTAL_M_1": "SOCIO:_PROVISAO_TOTAL_M_1", 
    "SOCIO:_CLASSIFICAÇÃO_MOV_M": "SOCIO:_CLASSIFICACAO_MOV_M", 
    "SOCIO:_PROVISÃO_MOV_M": "SOCIO:_PROVISAO_MOV_M", 
    "SOCIO:_CORREÇÃO_MOV_M": "SOCIO:_CORRECAO_MOV_M", 
    "SOCIO:_PROVISÃO_MOV_TOTAL_M": "SOCIO:_PROVISAO_MOV_TOTAL_M", 
    "SOCIO:_PROVISÃO_TOTAL_M": "SOCIO:_PROVISAO_TOTAL_M", 
    "SOCIO:_CORREÇÃO_M_1_0001": "SOCIO:_CORRECAO_M_1_0001",
    "SOCIO:_CORREÇÃO_M": "SOCIO:_CORRECAO_M", 
    "SOCIO:_PROV_TOTAL_PASSIVO_M": "SOCIO:_PROV_TOTAL_PASSIVO_M", 
    "EMPRESA:_PROVISÃO_M_1": "EMPRESA:_PROVISAO_M_1",
    "EMPRESA:_CORREÇÃO_M_1": "EMPRESA:_CORRECAO_M_1", 
    "EMPRESA:_PROVISÃO_TOTAL_M_1": "EMPRESA:_PROVISAO_TOTAL_M_1", 
    "EMPRESA:_CLASSIFICAÇÃO_MOV_M": "EMPRESA:_CLASSIFICACAO_MOV_M",
    "EMPRESA:_PROVISÃO_MOV_M": "EMPRESA:_PROVISAO_MOV_M", 
    "EMPRESA:_Correção_Mov_M": "EMPRESA:_Correcao_Mov_M", 
    "EMPRESA:_Provisão_Mov_Total_M": "EMPRESA:_Provisao_Mov_Total_M",
    "EMPRESA:_PROVISÃO_TOTAL_M": "EMPRESA:_PROVISAO_TOTAL_M", 
    "EMPRESA:_CORREÇÃO_M_1_0001": "EMPRESA:_CORRECAO_M_1_0001",
    "EMPRESA:_CORREÇÃO_M": "EMPRESA:_CORRECAO_M",
    "EMPRESA:_PROV_TOTAL_PASSIVO_M": "EMPRESA:_PROV_TOTAL_PASSIVO_M", 
    "DEMITIDO_POR_REESTRUTURAÇÃO": "DEMITIDO_POR_REESTRUTURACAO", 
    "INDICAÇÃO_PROCESSO_ESTRATÉGICO": "INDICACAO_PROCESSO_ESTRATEGICO", 
    "PARCELAMENTO_CONDENAÇÃO": "PARCELAMENTO_CONDENACAO", 
    "PARCELAMENTO_ACORDO": "PARCELAMENTO_ACORDO", 
    "TIPO_DE_CÁLCULO_M": "TIPO_DE_CALCULO_M", 
    "ESTRATÉGIA_M_1": "ESTRATEGIA_M_1", 
    "TIPO_DE_CÁLCULO_M_1": "TIPO_DE_CALCULO_M_1", 
    "MOTIVO_MOVIMENTAÇÃO": "MOTIVO_MOVIMENTACAO", 
    "MOTIVO_PAGAMENTO": "MOTIVO_PAGAMENTO", 
    "NOVO_X_LEGADO87": "NOVO_X_LEGADO87", 
    "MÊS_FECH": "MES_FECH", 
    "DT_ÚLT_PGTO": "DT_ULT_PGTO", 
    "CONDENAÇÃO": "CONDENACAO", 
    # Colunas adicionadas na sua query final que precisam de mapeamento
    # Se houver mais, adicione aqui.
}

def rename_columns_from_mapping(df, mapping):
    for old_name, new_name in mapping.items():
        if old_name in df.columns:
            df = df.withColumnRenamed(old_name, new_name)
    return df

# Aplica a limpeza final no df_fechamento_trab_final
df_fechamento_trab_final = rename_columns_from_mapping(df_fechamento_trab_final, name_mapping_final)
# df_fechamento_trab_final.printSchema() # Descomente para verificar o schema limpo

# COMMAND ----------

df_fechamento_trab_final.count()

# COMMAND ----------

df_fechamento_trab_final.createOrReplaceTempView("VERIFICACAO_FINAL")

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Verifica contagem por mês/ano
# MAGIC SELECT 
# MAGIC   MES_FECH, 
# MAGIC   count(*) as total_linhas 
# MAGIC FROM VERIFICACAO_FINAL 
# MAGIC WHERE month(MES_FECH) = 11
# MAGIC GROUP BY MES_FECH

# COMMAND ----------

display(df_fechamento_trab_final.select("ID_PROCESSO", "MES_FECH").limit(10))

# COMMAND ----------

nmtabela = nmtabela_trab_ger_consolidado[:6]
print(nmtabela)

# COMMAND ----------

# Tira a duplicidade de campos da base. Mantém a primeira coluna encontrada na base
df_fechamento_trab_final = deduplica_cols(df_fechamento_trab_final)

# COMMAND ----------

df_fechamento_trab_final.count()

# COMMAND ----------

display(df_fechamento_trab_final)

# COMMAND ----------

df_fechamento_trab_final.write \
    .format("delta") \
    .mode("overwrite") \
    .option("overwriteSchema", "true") \
    .saveAsTable(f"databox.juridico_comum.tb_fecham_trab_{nmtabela}")

# COMMAND ----------

#df_fechamento_trab_final.write.format("delta").mode("overwrite").saveAsTable(f"databox.juridico_comum.tb_fecham_trab_{nmtabela}")

# COMMAND ----------

display(df_fechamento_trab_final)

# COMMAND ----------

# %sql
# SELECT * FROM databox.juridico_comum.tb_fecham_trab_202404 LIMIT 10

# df_tb_fech = spark.sql("SELECT * FROM databox.juridico_comum.tb_fecham_trab_202404")
# display(df_tb_fech)

df_fechamento_trab_final.count()


# COMMAND ----------

msg = f"tb_trab_fecham_{nmtabela}"
dbutils.notebook.exit(msg)

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT count(*) FROM databox.juridico_comum.tb_fecham_trab_202511   ---tb_fecham_trab_202406

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM databox.juridico_comum.tb_fecham_trab_202511 WHERE ID_PROCESSO = 34537
