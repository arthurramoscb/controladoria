# Databricks notebook source
# MAGIC %md
# MAGIC # Relatórios Entradas, Saídas e Estoque

# COMMAND ----------

# MAGIC %run "/Workspace/Jurídico/funcao_tratamento_fechamento/common_functions"

# COMMAND ----------

# Configura campos para que o usuário insira parâmetros

# Parametro do nome da tabela da competência atual. Ex: 202404
dbutils.widgets.text("nmmes", "")

# Parametro de data da tabela GERENCIAL CONSOLIDADA (Passo 1). Ex: 20240423
dbutils.widgets.text("nmtabela", "")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 1 - TB_ENTR_TRAB_&NMMES + TRAB_GER_CONSOLIDA_

# COMMAND ----------

nmmes = dbutils.widgets.get("nmmes") # Fechamento de referencia
nmtabela = dbutils.widgets.get("nmtabela") # Gerencial de referencia

df_entr_trab_ff = spark.sql(f"""
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

	FROM databox.juridico_comum.tb_entradas_trab_2017_{nmmes} A
	LEFT JOIN databox.juridico_comum.trab_ger_consolida_{nmtabela} B ON A.ID_PROCESSO = B.PROCESSO_ID WHERE NOVOS = 1 
	ORDER BY B.MATRICULA, MES_FECH DESC
"""
)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 2 - TB_FECHAM_FINANC_TRAB_&NMMES._F => TB_FECHAM_FINANC_TRAB_2021

# COMMAND ----------

nmmes = dbutils.widgets.get("nmmes")

df_fecham_financ_trab = spark.sql(f"""
	SELECT ID_PROCESSO
			,MES_FECH::DATE
			,EMPRESA
			,SUB_AREA_DO_DIREITO
			,NATUREZA_OPERACIONAL
			,INDICACAO_PROCESSO_ESTRATEGICO
			,ESCRITORIO_RESPONSAVEL
			,PROCESSO_ESTEIRA
			,CLASSIFICACAO
			,MOTIVO_ENC_AGRP
			,ESTADO
			,PARTE_CONTRARIA_CARGO_GRUPO
			,BU
			,VP
			,DIRETORIA
			,DESCRICAO_CENTRO_CUSTO
			,RESPONSAVEL_AREA
			,AREA_FUNCIONAL
			,DP_VP_DIRETORIA
			,MESES_AGING_ENCERR
			,FX_ANO_AGING_ENCERR
			,ANO_AGING_ENCERR
			,FX_ANO_AGING_ENCERR
			,FASE_ATUAL
			,DP_FASE
			,DP_NATUREZA
			,TRIM_ESTOQ
			-- ,FX_ANO_AGING_NOVO # Conferir
			,NOVO_X_LEGADO
			,SUM(NOVOS) AS NOVOS
			,SUM(ENCERRADOS) AS ENCERRADOS
			,SUM(ESTOQUE) AS ESTOQUE
			,SUM(ACORDO) AS ACORDO
			,SUM(CONDENACAO) AS CONDENACAO
			,SUM(TOTAL_PAGAMENTOS) AS TOTAL_PAGAMENTOS
			,SUM(PROVISAO_TOTAL_M_1) AS PROVISAO_TOTAL_M_1
			,SUM(PROVISAO_TOTAL_PASSIVO_M) AS PROVISAO_TOTAL_PASSIVO_M
			,SUM(PROVISAO_MOV_M) AS PROVISAO_MOV_M

	FROM databox.juridico_comum.tb_fecham_financ_trab_{nmmes}
	WHERE MES_FECH >= '2023-01-01'
	GROUP BY ALL
"""
)

# COMMAND ----------

df_fecham_financ_trab.createOrReplaceTempView("TB_FECHAM_FINANC_TRAB_2021")

# COMMAND ----------

# display(df_fecham_financ_trab)

# COMMAND ----------

import pandas as pd
import re
from shutil import copyfile

nmmes = dbutils.widgets.get("nmmes")

# Converter PySpark DataFrame para Pandas DataFrame
pandas_df = df_fecham_financ_trab.toPandas()

# Salvar o DataFrame com os nomes corrigidos em um arquivo Excel no disco local primeiro
local_path = f'/local_disk0/tmp/df_fecham_financ_trab_{nmmes}_2.xlsx'
pandas_df.to_excel(local_path, index=False, sheet_name='TB_FECHAM_FINANC_TRAB')

# Copiar o arquivo do disco local para o volume desejado
volume_path = f'/Volumes/databox/juridico_comum/arquivos/trabalhista/bases_fechamento_financeiro/trabalhista_base_financeiro/df_fecham_financ_trab_{nmmes}_2.xlsx'

copyfile(local_path, volume_path)

# COMMAND ----------

# Calcula a frequência da coluna 'sua_coluna' usando SQL
frequencia = spark.sql("""
    SELECT FX_ANO_AGING_ENCERR, COUNT(*) AS Frequencia
    FROM TB_FECHAM_FINANC_TRAB_2021
    GROUP BY FX_ANO_AGING_ENCERR
    ORDER BY Frequencia DESC
""")

display(frequencia)

# COMMAND ----------

df_fecham_financ_trab.createOrReplaceTempView("TB_FECH_ENTRADAS_SAIDAS_TRAB")

# COMMAND ----------

trabalhista_valid_entradas_saidas = spark.sql("""
		SELECT  MES_FECH
	 			,SUM(NOVOS) AS NOVOS
				,SUM(ENCERRADOS) AS ENCERRADOS 
				,SUM(ESTOQUE) AS ESTOQUE 
				,SUM(ACORDO) AS ACORDO 
				,SUM(CONDENACAO) AS CONDENACAO 
				,SUM(TOTAL_PAGAMENTOS) AS TOTAL_PAGAMENTOS 
				,SUM(PROVISAO_TOTAL_PASSIVO_M) AS PROVISAO_TOTAL_PASSIVO_M 
				,SUM(PROVISAO_MOV_M) AS PROVISAO_MOV_M 
				
			FROM TB_FECH_ENTRADAS_SAIDAS_TRAB
			GROUP BY MES_FECH
	ORDER BY 1
 """)


display(trabalhista_valid_entradas_saidas)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 3 - ESTOQUE

# COMMAND ----------

df_estq_fin_trab = spark.sql(f"""
SELECT
  ID_PROCESSO,
  AREA_DO_DIREITO,
  SUB_AREA_DO_DIREITO,
  FX_ANO_AGING_ESTOQ_2 AS FX_ANO_AGING_ESTOQ,
  VALOR_DA_CAUSA,
  PROCESSO_ESTEIRA,
  NATUREZA_OPERACIONAL,
  FASE,
  FASE_ATUAL,
  INDICACAO_PROCESSO_ESTRATEGICO,
  PROVISAO_TOTAL_M,
  PROVISAO_TOTAL_PASSIVO_M,
  ESTOQUE,
  MES_FECH,
  DP_FASE,
  NOVO_X_LEGADO,
  CASE
    WHEN PROVISAO_TOTAL_PASSIVO_M > 0 THEN 'PASSÍVEL PROVISÃO'
    WHEN INDICACAO_PROCESSO_ESTRATEGICO = 'DEFESA' THEN 'DEFESA'
    WHEN NATUREZA_OPERACIONAL = 'TERCEIRO SOLVENTE' THEN 'TERCEIRO SOLVENTE'
    WHEN SUB_AREA_DO_DIREITO = 'CONTENCIOSO COLETIVO' THEN 'ESTRATEGICO'
    WHEN AREA_DO_DIREITO = 'TRABALHISTA COLETIVO' THEN 'ESTRATEGICO'
    ELSE 'PASSÍVEL PROVISÃO'
  END AS DP_NATUREZA
FROM
  databox.juridico_comum.tb_fecham_financ_trab_{nmmes}
  WHERE MES_FECH >= '2023-01-01'
"""
)

# COMMAND ----------

display(df_estq_fin_trab)

# COMMAND ----------

df_estq_fin_trab.createOrReplaceTempView("TB_FECH_ESTOQUE_TRAB")

# COMMAND ----------

trabalhista_total = spark.sql("""
		SELECT  DP_NATUREZA, COUNT(ID_PROCESSO) AS QTD 
			FROM TB_FECH_ESTOQUE_TRAB
			GROUP BY DP_NATUREZA


 """)

display(trabalhista_total)

# COMMAND ----------

# Calcula a frequência da coluna 'sua_coluna' usando SQL
frequencia = spark.sql("""
    SELECT FX_ANO_AGING_ESTOQ, COUNT(*) AS Frequencia
    FROM TB_FECH_ESTOQUE_TRAB
    
    GROUP BY FX_ANO_AGING_ESTOQ
    
    ORDER BY Frequencia DESC
    
""")

display(frequencia)

# COMMAND ----------

c = df_estq_fin_trab.count()

print(c)

# COMMAND ----------

# MAGIC %md
# MAGIC ###Exportação arquivos indicadores

# COMMAND ----------

import pandas as pd
import re
from shutil import copyfile

nmmes = dbutils.widgets.get("nmmes")

# Converter PySpark DataFrame para Pandas DataFrame
df_estq_fin_trab_pandas = df_estq_fin_trab.toPandas()

# Salvar o DataFrame com os nomes corrigidos em um arquivo Excel no disco local primeiro
local_path = f'/local_disk0/tmp/df_estoq_fin_trab_{nmmes}_2.xlsx'
df_estq_fin_trab_pandas.to_excel(local_path, index=False, sheet_name='TB_FECH_ESTOQUE_TRAB')

# Copiar o arquivo do disco local para o volume desejado
volume_path = f'/Volumes/databox/juridico_comum/arquivos/trabalhista/bases_fechamento_financeiro/trabalhista_base_financeiro/df_estoq_fin_trab_{nmmes}_2.xlsx'

copyfile(local_path, volume_path)

# COMMAND ----------

# MAGIC %md
# MAGIC ### Cria base de estoque para compartilhar com o MIS
# MAGIC

# COMMAND ----------

# Estoque
nmmes = dbutils.widgets.get("nmmes")
anonm = nmmes[:4]
mesnm = nmmes[4:]
dtmes = f'{anonm}-{mesnm}-01'

# Defini e nome da tabela
table_name = f"tb_estq_financ_trab_{nmmes}_f"

# Use the constructed table name in your query
query = f"""
SELECT *
FROM
  databox.juridico_comum.tb_fecham_financ_trab_{nmmes}
  WHERE ESTOQUE = 1 AND MES_FECH = '{dtmes}'
"""       

# Execute the query
tb_estq_financ_trab_f = spark.sql(query)

# COMMAND ----------

# Estoque
nmmes = dbutils.widgets.get("nmmes")

import pandas as pd
import re
from shutil import copyfile

# Converter PySpark DataFrame para Pandas DataFrame
pandas_df = tb_estq_financ_trab_f.toPandas()

# Salvar o DataFrame com os nomes corrigidos em um arquivo Excel no disco local primeiro
local_path = f'/local_disk0/tmp/{table_name}.xlsx'
pandas_df.to_excel(local_path, index=False, sheet_name='ESTOQUE')

# Copiar o arquivo do disco local para o volume desejado
volume_path = f'/Volumes/databox/juridico_comum/arquivos/trabalhista/bases_indicadores_resultado/{table_name}.xlsx'

copyfile(local_path, volume_path)

# COMMAND ----------

# MAGIC %md
# MAGIC ### Cria base de ENCERRADOS para compartilhar com o MIS
# MAGIC

# COMMAND ----------

from datetime import datetime
import calendar

# Encerrados
nmmes = dbutils.widgets.get("nmmes")
anonm = nmmes[:4]
mesnm = nmmes[4:]
dtmes = f'{anonm}-{mesnm}-01'

data_base = datetime.strptime(dtmes, '%Y-%m-%d')
    
# Obtém o último dia do mês
ultimo_dia = calendar.monthrange(data_base.year, data_base.month)[1]

# Cria o objeto datetime para o último dia do mês
fim_mes = datetime(data_base.year, data_base.month, ultimo_dia)

dtfimmes = "'" + fim_mes.strftime('%Y-%m-%d') + "'"

# Defini e nome da tabela
table_name = f"tb_encerr_financ_trab_{nmmes}_f"

tb_encerr_row_number = spark.sql(f'''
   SELECT 
    ID_PROCESSO,
    MES_FECH,
    ROW_NUMBER() OVER (PARTITION BY ID_PROCESSO ORDER BY MES_FECH) AS NUM_FECH
  FROM databox.juridico_comum.tb_fecham_financ_trab_{nmmes}
  WHERE ENCERRADOS = 1
  ORDER BY ID_PROCESSO, MES_FECH;                             
''')

tb_encerr_row_number.createOrReplaceTempView('tb_encerr_row_number')

tb_encerr_row_number_f = spark.sql(f'''
    SELECT *,
    CASE WHEN NUM_FECH = MAX(NUM_FECH) OVER (PARTITION BY ID_PROCESSO) 
        THEN "ULTIMO" ELSE "ANTIGO" END AS TIPO_FECHAMENTO

    FROM tb_encerr_row_number
    ORDER BY ID_PROCESSO DESC
''')

tb_encerr_row_number_f.createOrReplaceTempView('tb_encerr_row_number_f')

# Use the constructed table name in your query
tb_encerr_financ_trab_f = spark.sql(f"""
  SELECT A.*,
    last_day(A.MES_FECH) AS MES_FECH_LAST
    ,B.NUM_FECH
    ,B.TIPO_FECHAMENTO
  FROM
    databox.juridico_comum.tb_fecham_financ_trab_{nmmes} A
    LEFT JOIN tb_encerr_row_number_f B
    ON A.ID_PROCESSO = B.ID_PROCESSO AND A.MES_FECH = B.MES_FECH
    WHERE A.ENCERRADOS = 1 AND A.MES_FECH <= '{dtmes}'
""")


tb_encerr_financ_trab_f.createOrReplaceTempView('tb_encerr_financ_trab_f')

# COMMAND ----------

df_pagamentos_ate_fechamento = spark.sql(f"""
    SELECT A.ID_PROCESSO
        ,A.MES_FECH
        ,MAX(CASE WHEN A.TIPO_FECHAMENTO = 'ULTIMO' THEN B.DATA_EFETIVA_DO_PAGAMENTO 
            ELSE NULL END) AS DATA_ULTIMO_PAGAMENTO
        ,SUM(CASE 
                WHEN A.TIPO_FECHAMENTO = 'ULTIMO' THEN B.ACORDO        
            ELSE 0 END) AS ACORDO 
        ,SUM(CASE 
                WHEN A.TIPO_FECHAMENTO = 'ULTIMO' THEN B.CONDENACAO     
            ELSE 0 END) AS CONDENACAO
        ,SUM(CASE 
                WHEN A.TIPO_FECHAMENTO = 'ULTIMO' THEN B.PENHORA        
            ELSE 0 END) AS PENHORA
        ,SUM(CASE 
                WHEN A.TIPO_FECHAMENTO = 'ULTIMO' THEN B.OUTROS_PAGAMENTOS         
            ELSE 0 END) AS OUTROS_PAGAMENTOS
        ,SUM(CASE 
                WHEN A.TIPO_FECHAMENTO = 'ULTIMO' THEN B.IMPOSTO      
            ELSE 0 END) AS IMPOSTO
        ,SUM(CASE 
                WHEN A.TIPO_FECHAMENTO = 'ULTIMO' THEN B.GARANTIA      
            ELSE 0 END) AS GARANTIA
        ,SUM(CASE 
                WHEN A.TIPO_FECHAMENTO = 'ULTIMO' THEN B.PENSAO         
            ELSE 0 END) AS PENSAO
        ,SUM(CASE 
                WHEN A.TIPO_FECHAMENTO = 'ULTIMO' THEN B.TOTAL_PAGAMENTOS      
            ELSE 0 END) AS TOTAL_PAGAMENTOS
    FROM tb_encerr_financ_trab_f A
    LEFT JOIN databox.juridico_comum.tb_pgto_garantias_desagrupada B
    ON A.ID_PROCESSO = B.PROCESSO_ID
    GROUP BY A.ID_PROCESSO, A.MES_FECH
    ORDER BY A.MES_FECH DESC
""")

df_pagamentos_ate_fechamento = df_pagamentos_ate_fechamento.fillna({
    'DATA_ULTIMO_PAGAMENTO': ''    
    ,'ACORDO': 0.0   
    ,'CONDENACAO': 0.0
    ,'PENHORA': 0.0
    ,'PENSAO': 0.0
    ,'OUTROS_PAGAMENTOS': 0.0
    ,'IMPOSTO': 0.0
    ,'GARANTIA': 0.0
    ,'TOTAL_PAGAMENTOS': 0.0
})

df_pagamentos_ate_fechamento.createOrReplaceTempView('df_pagamentos_ate_fechamento')


# COMMAND ----------

df_agrupamento_para_marcacao_1 = spark.sql(f'''
    select ID_PROCESSO
        ,MAX(DATA_ULTIMO_PAGAMENTO) AS DATA_ULTIMO_PAGAMENTO
        ,SUM(COALESCE(ACORDO, 0)) AS ACORDO
        ,SUM(COALESCE(CONDENACAO, 0)) AS CONDENACAO
        ,SUM(COALESCE(PENHORA, 0)) AS PENHORA
        ,SUM(COALESCE(OUTROS_PAGAMENTOS, 0)) AS OUTROS_PAGAMENTOS
        ,SUM(COALESCE(IMPOSTO, 0)) AS IMPOSTO
        ,SUM(COALESCE(GARANTIA, 0)) AS GARANTIA
        ,SUM(COALESCE(PENSAO, 0)) AS PENSAO
        ,SUM(COALESCE(TOTAL_PAGAMENTOS, 0)) AS TOTAL_PAGAMENTOS
    from df_pagamentos_ate_fechamento
    GROUP BY ID_PROCESSO
''')

df_agrupamento_para_marcacao_1.createOrReplaceTempView('df_agrupamento_para_marcacao_1')


# COMMAND ----------

df_agrupamento_para_marcacao_f = spark.sql('''
    select *,
    CASE WHEN ACORDO > 1 THEN 'ACORDO'
        WHEN (CONDENACAO + PENHORA + OUTROS_PAGAMENTOS + IMPOSTO + GARANTIA + PENSAO) > 1 THEN 'CONDENACAO'
        WHEN (ACORDO + CONDENACAO + PENHORA + OUTROS_PAGAMENTOS + IMPOSTO + GARANTIA + PENSAO) <= 1 THEN 'SEM ONUS'
    END AS MOTIVO_ENC_AGRP_ALT  
    from df_agrupamento_para_marcacao_1
''')

df_agrupamento_para_marcacao_f.createOrReplaceTempView('df_agrupamento_para_marcacao_f')

# COMMAND ----------

nmmes = dbutils.widgets.get("nmmes")

df_encerr_trab_alt = spark.sql(f'''
    SELECT *
    FROM
    databox.juridico_comum.tb_fecham_financ_trab_{nmmes} A 
    WHERE ENCERRADOS = 1 AND MES_FECH <= {dtfimmes}
''')

df_encerr_trab_alt.createOrReplaceTempView('df_encerr_trab')

# COMMAND ----------

tb_encerr_financ_trab_f = spark.sql('''

SELECT A.*
  ,B.DATA_ULTIMO_PAGAMENTO AS DATA_ULT_PGTO_ALT
  ,COALESCE(B.ACORDO, 0) AS ACORDO_ALT
  ,COALESCE(B.CONDENACAO, 0) AS CONDENACAO_ALT
  ,COALESCE(B.PENHORA, 0) AS PENHORA_ALT
  ,COALESCE(B.PENSAO, 0) AS PENSAO_ALT
  ,COALESCE(B.OUTROS_PAGAMENTOS, 0) AS OUTROS_PAGAMENTOS_ALT
  ,COALESCE(B.IMPOSTO, 0) AS IMPOSTO_ALT
  ,COALESCE(B.GARANTIA, 0) AS GARANTIA_ALT
  ,COALESCE(B.TOTAL_PAGAMENTOS, 0) AS TOTAL_PAGAMENTOS_ALT
  ,C.MOTIVO_ENC_AGRP_ALT
 FROM df_encerr_trab A 
LEFT JOIN df_pagamentos_ate_fechamento B 
ON A.ID_PROCESSO = B.ID_PROCESSO AND A.MES_FECH = B.MES_FECH
LEFT JOIN df_agrupamento_para_marcacao_f C
ON A.ID_PROCESSO = C.ID_PROCESSO
''')

tb_encerr_financ_trab_f.createOrReplaceTempView('''tb_encerr_financ_trab_f''')

# COMMAND ----------

df_reorganizado = tb_encerr_financ_trab_f.select(
    'ID_PROCESSO',
'DATACADASTRO',
'MES_CADASTRO',
'AREA_DO_DIREITO',
'SUB_AREA_DO_DIREITO',
'VALOR_DA_CAUSA',
'EMPRESA',
'STATUS',
'ADVOGADO_RESPONSAVEL',
'ESCRITORIO_RESPONSAVEL',
'NUMERO_DO_PROCESSO',
'PARTE_CONTRARIA_CPF',
'PARTE_CONTRARIA_NOME',
'ADVOGADO_PARTE_CONTRARIA',
'MATRICULA',
'BU',
'VP',
'DIRETORIA',
'DESCRICAO_CENTRO_CUSTO',
'RESPONSAVEL_AREA',
'AREA_FUNCIONAL',
'ESTADO',
'COMARCA',
'CLASSIFICACAO',
'NATUREZA_OPERACIONAL',
'TERCEIRO_PRINCIPAL',
'NOVO_TERCEIRO',
'TERCEIRO_AJUSTADO',
'DATA_ADMISSAO',
'DATA_DISPENSA',
'DATA_REGISTRADO',
'DISTRIBUICAO',
'PERIODO_RECLAMADO',
'PERIODO1',
'PERIODO2',
'PERIODO_RECL_GRUPO',
'PARTE_CONTRARIA_CARGO_GRUPO',
'CARGO',
'MOTIVO_DESLIGAMENTO',
'FASE',
'FASE_ATUAL',
'FILIAL',
'INDICACAO_PROCESSO_ESTRATEGICO',
'BANDEIRA',
'NOME_DA_LOJA',
'NOVOS',
'ENCERRADOS',
'ESTOQUE',
'DATA_ULT_PGTO_ALT',
'DIRETORIA_DP_LOJA',
'MES_FECH',
'NOVO_X_LEGADO',
'PROCESSO_ESTEIRA',
'MES_DATA_ADMISSAO',
'MES_DATA_DISPENSA',
'TEMPO_EMPRESA_MESES',
'TEMPO_AJUIZAR_ACAO',
'PERC_SOCIO_M',
'PERC_EMPRESA_M',
'PROVISAO_MOV_M',
'PROVISAO_TOTAL_M_1',
'PROVISAO_TOTAL_M',
'CORRECAO_M_1',
'CORRECAO_M',
'CORRECAO_MOV_M',
'PROVISAO_TOTAL_PASSIVO_M',
'SOCIO_PROVISAO_TOTAL_M',
'EMPRESA_PROV_TOTAL_PASSIVO_M',
'EMPRESA_PROVISAO_MOV_M',
'ACORDO_ALT',
'CONDENACAO_ALT',
'PENHORA_ALT',
'PENSAO_ALT',
'OUTROS_PAGAMENTOS_ALT',
'IMPOSTO_ALT',
'GARANTIA_ALT',
'TOTAL_PAGAMENTOS_ALT',
'MESES_AGING_ENCERR',
'ANO_AGING_ENCERR',
'MESES_AGING_ESTOQ',
'ANO_AGING_ESTOQ',
'AGING_ESTOQ_MESES',
'MOTIVO_ENCERRAMENTO',
'FX_ANO_AGING_ENCERR',
'FX_MES_AGING_ESTOQ',
'FX_ANO_AGING_ESTOQ',
'FX_ANO_AGING_ESTOQ_2',
'FX_TEMPO_EMPRESA',
'DP_FASE',
'DP_VP_DIRETORIA',
'DP_NATUREZA',
'TRIM_ESTOQ',
'MOTIVO_ENC_AGRP_ALT',
'CARGO_TRATADO',
'CLUSTER_AGING',
'CLUSTER_VALOR',
'SAFRA_RECLAMACAO',
'ET',
)

# COMMAND ----------

renomear_colunas = {
    'DATA_ULT_PGTO_ALT': 'DATA_ULT_PGTO',
    'MOTIVO_ENC_AGRP_ALT': 'MOTIVO_ENC_AGRP',
    'ACORDO_ALT': 'ACORDO',
    'CONDENACAO_ALT': 'CONDENACAO',
    'PENHORA_ALT': 'PENHORA',
    'PENSAO_ALT': 'PENSAO',
    'OUTROS_PAGAMENTOS_ALT': 'OUTROS_PAGAMENTOS',
    'IMPOSTO_ALT': 'IMPOSTO',
    'GARANTIA_ALT': 'GARANTIA',
    'TOTAL_PAGAMENTOS_ALT': 'TOTAL_PAGAMENTOS'

}

# Iterar sobre o dicionário para renomear as colunas
for coluna_antiga, coluna_nova in renomear_colunas.items():
    df_reorganizado = df_reorganizado.withColumnRenamed(coluna_antiga, coluna_nova)

# COMMAND ----------

from pyspark.sql.functions import when, col

# Suponha que 'df' seja o seu DataFrame e 'coluna' seja a coluna que você quer modificar

df_reorganizado_2 = df_reorganizado.withColumn(
    'GARANTIA',  # Nome da coluna que será alterada
    when(col('GARANTIA') <= 0.5, 0)  # Se o valor da coluna for <= 0,5, o valor se torna 0
    .otherwise(col('GARANTIA'))  # Caso contrário, mantém o valor original
)

df_reorganizado_3 = df_reorganizado_2.withColumn(
    'TOTAL_PAGAMENTOS',  # Nome da coluna que será alterada
    when(col('TOTAL_PAGAMENTOS') <= 0.5, 0)  # Se o valor da coluna for <= 0,5, o valor se torna 0
    .otherwise(col('TOTAL_PAGAMENTOS'))  # Caso contrário, mantém o valor original
)

colunas_para_converter = ['ACORDO', 'CONDENACAO', 'PENHORA', 'PENSAO', 'OUTROS_PAGAMENTOS','IMPOSTO', 'GARANTIA', 'TOTAL_PAGAMENTOS' ]  # Lista de colunas

for coluna in colunas_para_converter:
    df_reorganizado_3 = df_reorganizado_3.withColumn(coluna, col(coluna).cast("double"))

df_reorganizado_f = df_reorganizado_3

display(df_reorganizado_f)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Exporta os arquivos

# COMMAND ----------

# Encerrados
nmmes = dbutils.widgets.get("nmmes")

import pandas as pd
import re
from shutil import copyfile

# Converter PySpark DataFrame para Pandas DataFrame
pandas_df = df_reorganizado_f.toPandas()

# Salvar o DataFrame com os nomes corrigidos em um arquivo Excel no disco local primeiro
local_path = f'/local_disk0/tmp/{table_name}.xlsx'
pandas_df.to_excel(local_path, index=False, sheet_name='ENCERRADOS')

# Copiar o arquivo do disco local para o volume desejado
volume_path = f'/Volumes/databox/juridico_comum/arquivos/trabalhista/bases_indicadores_resultado/{table_name}.xlsx'

copyfile(local_path, volume_path)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Cria o arquivo de ENTRADAS

# COMMAND ----------

# Entradas
nmmes = dbutils.widgets.get("nmmes")
anonm = nmmes[:4]
mesnm = nmmes[4:]
dtmes = f'{anonm}-{mesnm}-01'

# Defini e nome da tabela
table_name = f"tb_entr_financ_trab_{nmmes}_f"

# Use the constructed table name in your query
query = f"""
SELECT *
FROM
  databox.juridico_comum.tb_fecham_financ_trab_{nmmes}
  WHERE NOVOS = 1 AND MES_FECH <= '{dtmes}'
"""

# Execute the query
tb_entr_financ_trab_f = spark.sql(query)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Exporta para excel o arquivo de ENTRADAS

# COMMAND ----------

#Volumes/databox/juridico_comum/arquivos/trabalhista/bases_fechamento_financeiro/tb_fecham_financ_civel_202409.xlsx# Entradas
nmmes = dbutils.widgets.get("nmmes")

import pandas as pd
import re
from shutil import copyfile

# Converter PySpark DataFrame para Pandas DataFrame
pandas_df = tb_entr_financ_trab_f.toPandas()

# Salvar o DataFrame com os nomes corrigidos em um arquivo Excel no disco local primeiro
local_path = f'/local_disk0/tmp/{table_name}.xlsx'
pandas_df.to_excel(local_path, index=False, sheet_name='ENTRADAS')

# Copiar o arquivo do disco local para o volume desejado
volume_path = f'/Volumes/databox/juridico_comum/arquivos/trabalhista/bases_indicadores_resultado/{table_name}.xlsx'

copyfile(local_path, volume_path)

# COMMAND ----------

print("Concluido !!!!")
