# Databricks notebook source
# MAGIC %md
# MAGIC # Tratamento dos dados de Pagamentos e Garantias
# MAGIC
# MAGIC **Inputs**\
# MAGIC Planilhas fornecidas pelo ELAW e tratadas pela área finaceira. Pagamentos e Garantias.
# MAGIC
# MAGIC **Outputs**\
# MAGIC Uma ~Global View~ tabela (stg_trab_pgto_garantias) com os dados de Pagamentos e Garantias tratados e unidos para utilização pelo passo 3.

# COMMAND ----------

# MAGIC %md
# MAGIC ### 1- Carga e tratamento inicial das duas bases

# COMMAND ----------

from pyspark.sql.functions import coalesce, row_number, lit

# Configura campo para que o usuário insira parâmetros

# Parametro do formato de data do arquivo. Ex: 20240423
dbutils.widgets.text("nmtabela_pgto", "")
dbutils.widgets.text("nmtabela_garantias", "")

# COMMAND ----------

# MAGIC %run "/Workspace/Jurídico/funcao_tratamento_fechamento/common_functions"

# COMMAND ----------

# Caminho das pastas e arquivos
nmtabela_pgto = dbutils.widgets.get("nmtabela_pgto")
nmtabela_garantias = dbutils.widgets.get("nmtabela_garantias")

path_pagamentos = f'/Volumes/databox/juridico_comum/arquivos/bases_pagamentos/HISTORICO_DE-PAGAMENTOS_{nmtabela_pgto}.xlsx'
path_garantias = f'/Volumes/databox/juridico_comum/arquivos/bases_garantias/GARANTIAS_{nmtabela_garantias}.xlsx'

# COMMAND ----------

# Carrega as planilhas em Spark Data Frames
df_pgto = read_excel(path_pagamentos, "A6",)
df_garantias = read_excel(path_garantias, "A6")

df_pgto.createOrReplaceTempView("df_pgto_original")
df_garantias.createOrReplaceTempView("df_garantias_original")

# COMMAND ----------

# Lista as colunas com datas
pgto_data_cols = find_columns_with_word(df_pgto, 'DATA ')
garantias_data_cols = find_columns_with_word(df_garantias, 'DATA ')

print("pgto_data_cols")
print(pgto_data_cols)
print("\n")
print("garantias_data_cols")
print(garantias_data_cols)

# COMMAND ----------

# Converte as datas das colunas listadas
df_pgto = convert_to_date_format(df_pgto, pgto_data_cols)
df_garantias = convert_to_date_format(df_garantias, garantias_data_cols)

# COMMAND ----------

# df_garantias.count() #165887

# COMMAND ----------

# MAGIC %md
# MAGIC ### 2 - Tratamento base de Pagamentos

# COMMAND ----------

df_pgto.createOrReplaceTempView("HISTORICO_DE_PAGAMENTOS_1")

# COMMAND ----------

df_pgto = spark.sql("""
SELECT HISTORICO_DE_PAGAMENTOS_1.*,
    CASE 
        WHEN `SUB TIPO` IN ('ACORDO - CÍVEL', 'ACORDO - CÍVEL ESTRATÉGICO', 'ACORDO - CÍVEL MASSA', 'ACORDO - TRABALHISTA', 'PAGAMENTO DE ACORDO - TRABALHISTA (FK)', 'RNO - ACORDO - TRABALHISTA', 'ACORDO - CÍVEL MASSA - CARTÕES', 'ACORDO - CÍVEL MASSA - FORNECEDOR', 'ACORDO - CÍVEL MASSA - MKTPLACE', 'ACORDO - CÍVEL MASSA - SEGURO', 'ACORDO - IMOBILIÁRIO', 'ACORDO - MEDIAÇÃO', 'ACORDO - REGULATÓRIO - PROCON COM AUDIÊNCIA', 'ACORDO/ PROGRAMAS DE PARCELAMENTO - REGULATÓRIO', 'MULTA ACORDO - TRABALHISTA','RNO - MULTA ACORDO - TRABALHISTA')
        THEN VALOR
    END AS ACORDO,
    CASE 
        WHEN `SUB TIPO` IN ('CONDENAÇÃO - CÍVEL', 'CONDENAÇÃO - CÍVEL ESTRATÉGICO', 'CONDENAÇÃO - CÍVEL MASSA', 'CONDENAÇÃO - CÍVEL MASSA (INCONTROVERSO)', 'CONDENAÇÃO - REGULATÓRIO', 'CONDENAÇÃO - TRABALHISTA', 'CONDENAÇÃO - TRABALHISTA (INCONTROVERSO)', 'CONDENAÇÃO - CÍVEL MASSA (INCONTROVERSO) - CARTÕES', 'CONDENAÇÃO - CÍVEL MASSA (INCONTROVERSO) - FORNECEDOR', 'CONDENAÇÃO - CÍVEL MASSA (INCONTROVERSO) - MKTPLACE', 'CONDENAÇÃO - CÍVEL MASSA (INCONTROVERSO) - SEGURO', 'CONDENAÇÃO - CÍVEL MASSA - CARTÕES', 'CONDENAÇÃO - CÍVEL MASSA - FORNECEDOR', 'CONDENAÇÃO - CÍVEL MASSA - MKTPLACE', 'CONDENAÇÃO - CÍVEL MASSA - SEGURO', 'CONDENAÇÃO - IMOBILIÁRIO', 'CONDENAÇÃO - IMOBILIÁRIO (INCONTROVERSO)', 'CONDENAÇÃO - REGULATÓRIO', 'MULTA - REGULATÓRIO', 'MULTA PROCESSUAL - CÍVEL', 'MULTA PROCESSUAL - TRABALHISTA', 'MULTA PROCESSUAL - CÍVEL ESTRATÉGICO', 'MULTA PROCESSUAL - IMOBILIÁRIO', 'RNO - CONDENAÇÃO - TRABALHISTA', 'RNO - CONDENAÇÃO - TRABALHISTA (INCONTROVERSO)', 'RNO - MULTA PROCESSUAL - TRABALHISTA','MULTA POR EMBRAGOS PROTELATÓRIOS - TRABALHISTA')
        THEN VALOR
    END AS CONDENACAO,
    CASE 
        WHEN `SUB TIPO` IN ('HONORÁRIOS CONCILIADOR - CIVEL MASSA', 'HONORÁRIOS PERICIAIS - CÍVEL ESTRATÉGICO', 'HONORÁRIOS PERICIAIS - CÍVEL MASSA', 'HONORÁRIOS PERICIAIS - IMOBILIÁRIO', 'HONORÁRIOS PERICIAIS - TRABALHISTA', 'HONORÁRIOS CONCILIADOR - CIVEL MASSA - CARTÕES', 'HONORÁRIOS CONCILIADOR - CIVEL MASSA - FORNECEDOR', 'HONORÁRIOS CONCILIADOR - CIVEL MASSA - MKTPLACE', 'HONORÁRIOS CONCILIADOR - CIVEL MASSA - SEGURO', 'HONORÁRIOS PERICIAIS - CÍVEL MASSA - CARTÕES', 'HONORÁRIOS PERICIAIS - CÍVEL MASSA - FORNECEDOR', 'HONORÁRIOS PERICIAIS - CÍVEL MASSA - MKTPLACE', 'HONORÁRIOS PERICIAIS - CÍVEL MASSA - SEGURO', 'HONORÁRIOS SUCUMBENCIAIS - REGULATÓRIO', 'PAGAMENTO DE HONORÁRIOS PERICIAIS - TRABALHISTA (FK)', 'RNO - HONORÁRIOS PERICIAIS -TRABALHISTA', 'CUSTAS FK - TRABALHISTA', 'CUSTAS PERITOS - CÍVEL', 'CUSTAS PERITOS - IMOBILIÁRIO', 'CUSTAS PROCESSUAIS - CÍVEL', 'CUSTAS PROCESSUAIS - CÍVEL ESTRATÉGICO', 'CUSTAS PROCESSUAIS - CÍVEL MASSA', 'CUSTAS PROCESSUAIS - IMOBILIÁRIO', 'CUSTAS PROCESSUAIS - REGULATÓRIO', 'CUSTAS PROCESSUAIS - TRABALHISTA', 'CUSTAS PROCESSUAIS - CÍVEL MASSA - CARTÕES', 'CUSTAS PROCESSUAIS - CÍVEL MASSA - FORNECEDOR', 'CUSTAS PROCESSUAIS - CÍVEL MASSA - MKTPLACE', 'CUSTAS PROCESSUAIS - CÍVEL MASSA - SEGURO', 'PAGAMENTO DE CUSTAS - TRIBUTÁRIO', 'PERITOS - REGULATÓRIO', 'RNO - CUSTAS PROCESSUAIS - TRABALHISTA')
        THEN VALOR
    END AS CUSTAS,
    CASE 
        WHEN `SUB TIPO` IN ('RNO - INSS/IR - DARF E-SOCIAL','INSS - TRABALHISTA', 'IR - TRABALHISTA', 'PAGAMENTO DE INSS - INDENIZAÇÃO TRABALHISTA (FK)', 'PAGAMENTO DE IR - INDENIZAÇÃO TRABALHISTA (FK)', 'RNO - INSS - TRABALHISTA', 'RNO - IR - TRABALHISTA ', 'RNO - FGTS', 'FGTS', 'INSS/IR - DARF E-SOCIAL')
        THEN VALOR
    END AS IMPOSTO,
    CASE 
        WHEN `SUB TIPO` IN ('LIBERAÇÃO DE PENHORA - MASSA', 'LIBERAÇÃO DE PENHORA MASSA', 'LIBERAÇÃO DE PENHORA - REGULATÓRIO')
        THEN VALOR
    END AS PENHORA,
    CASE 
        WHEN `SUB TIPO` IN ('PENSÃO - CÍVEL', 'PENSÃO - CÍVEL ESTRATÉGICO', 'PENSÃO - CÍVEL MASSA')
        THEN VALOR
    END AS PENSAO,
    CASE 
        WHEN `SUB TIPO` IN ('ACERTO CONTÁBIL: BLOQUEIO COM TRANSFERÊNCIA BANCÁRIA - DEP. JUDICIAL  (PAG)', 'ACERTO CONTÁBIL: BLOQUEIO COM TRANSFERÊNCIA BANCÁRIA - PAGAMENTO', 'ACERTO CONTÁBIL: PAGAMENTO DE ACORDO COM BAIXA DE DEP. JUDICIAL', 'LIMINAR (CÍV. MASSA)', 'PAGAMENTO', 'PAGAMENTO AUTUAÇÃO MINISTÉRIO DO TRABALHO - TRABALHISTA', 'PAGAMENTO DE EXECUÇÃO - TRABALHISTA', 'PAGAMENTOS - ALL - VV', 'PAGAMENTOS FK')
        THEN VALOR
    END AS OUTROS_PAGAMENTOS
FROM HISTORICO_DE_PAGAMENTOS_1
WHERE `SUB TIPO` IN ('RNO - INSS/IR - DARF E-SOCIAL','MULTA POR EMBRAGOS PROTELATÓRIOS - TRABALHISTA','RNO - MULTA ACORDO - TRABALHISTA','MULTA ACORDO - TRABALHISTA','ACORDO - CÍVEL', 'ACORDO - CÍVEL ESTRATÉGICO', 'ACORDO - CÍVEL MASSA', 'ACORDO - TRABALHISTA', 'PAGAMENTO DE ACORDO - TRABALHISTA (FK)', 'RNO - ACORDO - TRABALHISTA', 'ACORDO - CÍVEL MASSA - CARTÕES', 'ACORDO - CÍVEL MASSA - FORNECEDOR', 'ACORDO - CÍVEL MASSA - MKTPLACE', 'ACORDO - CÍVEL MASSA - SEGURO', 'ACORDO - IMOBILIÁRIO', 'ACORDO - MEDIAÇÃO', 'ACORDO - REGULATÓRIO - PROCON COM AUDIÊNCIA', 'ACORDO/ PROGRAMAS DE PARCELAMENTO - REGULATÓRIO', 'CONDENAÇÃO - CÍVEL', 'CONDENAÇÃO - CÍVEL ESTRATÉGICO', 'CONDENAÇÃO - CÍVEL MASSA', 'CONDENAÇÃO - CÍVEL MASSA (INCONTROVERSO)', 'CONDENAÇÃO - REGULATÓRIO', 'CONDENAÇÃO - TRABALHISTA', 'CONDENAÇÃO - TRABALHISTA (INCONTROVERSO)', 'CONDENAÇÃO - CÍVEL MASSA (INCONTROVERSO) - CARTÕES', 'CONDENAÇÃO - CÍVEL MASSA (INCONTROVERSO) - FORNECEDOR', 'CONDENAÇÃO - CÍVEL MASSA (INCONTROVERSO) - MKTPLACE', 'CONDENAÇÃO - CÍVEL MASSA (INCONTROVERSO) - SEGURO', 'CONDENAÇÃO - CÍVEL MASSA - CARTÕES', 'CONDENAÇÃO - CÍVEL MASSA - FORNECEDOR', 'CONDENAÇÃO - CÍVEL MASSA - MKTPLACE', 'CONDENAÇÃO - CÍVEL MASSA - SEGURO', 'CONDENAÇÃO - IMOBILIÁRIO', 'CONDENAÇÃO - IMOBILIÁRIO (INCONTROVERSO)', 'CONDENAÇÃO - REGULATÓRIO', 'MULTA - REGULATÓRIO', 'MULTA PROCESSUAL - CÍVEL', 'MULTA PROCESSUAL - TRABALHISTA', 'MULTA PROCESSUAL - CÍVEL ESTRATÉGICO', 'MULTA PROCESSUAL - IMOBILIÁRIO', 'RNO - CONDENAÇÃO - TRABALHISTA', 'RNO - CONDENAÇÃO - TRABALHISTA (INCONTROVERSO)', 'RNO - MULTA PROCESSUAL - TRABALHISTA', 'HONORÁRIOS CONCILIADOR - CIVEL MASSA', 'HONORÁRIOS PERICIAIS - CÍVEL ESTRATÉGICO', 'HONORÁRIOS PERICIAIS - CÍVEL MASSA', 'HONORÁRIOS PERICIAIS - IMOBILIÁRIO', 'HONORÁRIOS PERICIAIS - TRABALHISTA', 'HONORÁRIOS CONCILIADOR - CIVEL MASSA - CARTÕES', 'HONORÁRIOS CONCILIADOR - CIVEL MASSA - FORNECEDOR', 'HONORÁRIOS CONCILIADOR - CIVEL MASSA - MKTPLACE', 'HONORÁRIOS CONCILIADOR - CIVEL MASSA - SEGURO', 'HONORÁRIOS PERICIAIS - CÍVEL MASSA - CARTÕES', 'HONORÁRIOS PERICIAIS - CÍVEL MASSA - FORNECEDOR', 'HONORÁRIOS PERICIAIS - CÍVEL MASSA - MKTPLACE', 'HONORÁRIOS PERICIAIS - CÍVEL MASSA - SEGURO', 'HONORÁRIOS SUCUMBENCIAIS - REGULATÓRIO', 'PAGAMENTO DE HONORÁRIOS PERICIAIS - TRABALHISTA (FK)', 'RNO - HONORÁRIOS PERICIAIS -TRABALHISTA', 'CUSTAS FK - TRABALHISTA', 'CUSTAS PERITOS - CÍVEL', 'CUSTAS PERITOS - IMOBILIÁRIO', 'CUSTAS PROCESSUAIS - CÍVEL', 'CUSTAS PROCESSUAIS - CÍVEL ESTRATÉGICO', 'CUSTAS PROCESSUAIS - CÍVEL MASSA', 'CUSTAS PROCESSUAIS - IMOBILIÁRIO', 'CUSTAS PROCESSUAIS - REGULATÓRIO', 'CUSTAS PROCESSUAIS - TRABALHISTA', 'CUSTAS PROCESSUAIS - CÍVEL MASSA - CARTÕES', 'CUSTAS PROCESSUAIS - CÍVEL MASSA - FORNECEDOR', 'CUSTAS PROCESSUAIS - CÍVEL MASSA - MKTPLACE', 'CUSTAS PROCESSUAIS - CÍVEL MASSA - SEGURO', 'PAGAMENTO DE CUSTAS - TRIBUTÁRIO', 'PERITOS - REGULATÓRIO', 'RNO - CUSTAS PROCESSUAIS - TRABALHISTA', 'INSS - TRABALHISTA', 'IR - TRABALHISTA', 'PAGAMENTO DE INSS - INDENIZAÇÃO TRABALHISTA (FK)', 'PAGAMENTO DE IR - INDENIZAÇÃO TRABALHISTA (FK)', 'RNO - INSS - TRABALHISTA', 'RNO - IR - TRABALHISTA ', 'RNO - FGTS', 'FGTS', 'LIBERAÇÃO DE PENHORA - MASSA', 'LIBERAÇÃO DE PENHORA MASSA', 'LIBERAÇÃO DE PENHORA - REGULATÓRIO', 'PENSÃO - CÍVEL', 'PENSÃO - CÍVEL ESTRATÉGICO', 'PENSÃO - CÍVEL MASSA', 'ACERTO CONTÁBIL: BLOQUEIO COM TRANSFERÊNCIA BANCÁRIA - DEP. JUDICIAL  (PAG)', 'ACERTO CONTÁBIL: BLOQUEIO COM TRANSFERÊNCIA BANCÁRIA - PAGAMENTO', 'ACERTO CONTÁBIL: PAGAMENTO DE ACORDO COM BAIXA DE DEP. JUDICIAL', 'LIMINAR (CÍV. MASSA)', 'PAGAMENTO', 'PAGAMENTO AUTUAÇÃO MINISTÉRIO DO TRABALHO - TRABALHISTA', 'PAGAMENTO DE EXECUÇÃO - TRABALHISTA', 'PAGAMENTOS - ALL - VV', 'PAGAMENTOS FK')
    AND UPPER(`STATUS DO PAGAMENTO`) NOT IN ('REMOVIDO', 'CANCELADO', 'REJEITADO', 'EM CORREÇÃO', 'PENDENTE')
    AND UPPER(`STATUS DO PAGAMENTO`) NOT LIKE ('%PAGAMENTO DEVOLVIDO%')
""")

# COMMAND ----------

# Correção da DATA EFETIVA PAGAMENTO
df_pgto = df_pgto.withColumn('DATA EFETIVA DO PAGAMENTO', coalesce('DATA EFETIVA DO PAGAMENTO', 'DATA SOLICITAÇÃO (ESCRITÓRIO)'))


# COMMAND ----------

df_pgto = adjust_column_names(df_pgto)

# COMMAND ----------

df_pgto = df_pgto.select(
    'PROCESSO_ID',
'ID_DO_PAGAMENTO',
'STATUS_DO_PROCESSO',
'ÁREA_DO_DIREITO',
'DATA_REGISTRADO',
'EMPRESA',
'DATA_SOLICITAÇÃO_ESCRITÓRIO',
'DATA_LIMITE_DE_PAGAMENTO',
'DATA_DE_VENCIMENTO',
'DATA_EFETIVA_DO_PAGAMENTO',
'STATUS_DO_PAGAMENTO',
'TIPO',
'SUB_TIPO',
'VALOR',
'RESPONSÁBILIDADE_COMPRADOR_PERCENTUAL',
'RESPONSÁBILIDADE_VENDEDOR_PERCENTUAL',
'VALOR_DA_MULTA_POR_DESCUMPRIMENTO_DA_OBRIGAÇÃO',
'PAGAMENTO_ATRAVÉS_DE',
'NÚMERO_DO_DOCUMENTO_SAP',
'INDICAR_O_MOMENTO_DA_REALIZAÇÃO_DO_ACORDO',
'PARCELAMENTO_ACORDO',
'PARCELAMENTO_CONDENAÇÃO',
'PARTE_CONTRÁRIA_CPF',
'PARTE_CONTRÁRIA_NOME',
'FAVORECIDO_NOME',
'PARTE_CONTRÁRIA_CARGO_CARGO_GRUPO',
'CÍVEL_MASSA_RESPONSABILIDADE26',
'ESCRITÓRIO',
'CÍVEL_MASSA_RESPONSABILIDADE28',
'CARTEIRA',
'ADVOGADO_RESPONSÁVEL',
'NOVO_TERCEIRO',
'ESTADO',
'COMARCA',
'FILIAL_BANDEIRA_CADASTRO_BANDEIRA',
'FILIAL_BANDEIRA_CADASTRO_RESPONSÁVEL_DIRETORIA',
'FILIAL_BANDEIRA_CADASTRO_DIRETORIA',
'FILIAL_BANDEIRA_CADASTRO_REGIONAL',
'FILIAL_BANDEIRA_CADASTRO_FILIAL',
'OUTRAS_PARTES_NÃO_CLIENTES',
'OBJETO_VINCULADO_AO_RATEIO',
'DISTRIBUIÇÃO',
'ACORDO',
'CONDENACAO',
'CUSTAS',
'IMPOSTO',
'PENHORA',
'PENSAO',
'OUTROS_PAGAMENTOS'
)

# COMMAND ----------

df_pgto.createOrReplaceTempView("HISTORICO_DE_PAGAMENTOS_JUD_2")
df_pgto.write.format("delta").mode("overwrite").saveAsTable(f"databox.juridico_comum.hist_pgto_f")

# COMMAND ----------

df_pgto_parc_cond = spark.sql('''
    select *
    ,CASE WHEN `PARCELAMENTO_CONDENAÇÃO` LIKE "%PARCELAMENTO 916 - 30%%" THEN VALOR / 0.3 ELSE VALOR END AS VALOR_TOTAL
    ,CASE WHEN `PARCELAMENTO_CONDENAÇÃO` LIKE "%PARCELAMENTO 916 - 30%%" THEN ((VALOR / 0.3) - VALOR) ELSE 0 END AS VALOR_RESTANTE
    from databox.juridico_comum.hist_pgto_f
    where `PARCELAMENTO_CONDENAÇÃO` IS NOT NULL AND `PARCELAMENTO_CONDENAÇÃO` NOT IN ('PAGAMENTO SEM PARCELAMENTO','SALDO - PARCELAMENTO INDEFERIDO')
''')

df_pgto_parc_cond.createOrReplaceTempView('df_pgto_parc_cond')


# COMMAND ----------

from pyspark.sql import functions as F
from pyspark.sql.functions import when, col
from pyspark.sql.window import Window

window_spec = Window.partitionBy("PROCESSO_ID")

# Suponha que 'df' seja o seu DataFrame
df_pgto_parc_cond = df_pgto_parc_cond.withColumn("NUMERO_PARCELA", F.regexp_extract("PARCELAMENTO_CONDENAÇÃO", r'(\d+)\s*ª\s*PARCELA', 1))
df_pgto_parc_cond = df_pgto_parc_cond.withColumn("NUMERO_PARCELA", col("NUMERO_PARCELA").cast("int"))
df_pgto_parc_cond = df_pgto_parc_cond.withColumn(
    "NUMERO_PARCELA", 
    when(col("PARCELAMENTO_CONDENAÇÃO") == "PARCELAMENTO 916 - 30%", 0.5).otherwise(col("NUMERO_PARCELA"))
)
df_pgto_parc_cond = df_pgto_parc_cond.withColumn("MAIOR_PARCELA", F.max("NUMERO_PARCELA").over(window_spec))
df_pgto_parc_cond = df_pgto_parc_cond.withColumn(
    "É_MAIOR_PARCELA", 
    F.when(col("MAIOR_PARCELA") == col("NUMERO_PARCELA"), "SIM").otherwise("NÃO")
)
df_pgto_parc_cond = df_pgto_parc_cond.withColumn(
    "PARCELAS_FALTANTES", 
    F.when((col("NUMERO_PARCELA") == 0.5) & (col("É_MAIOR_PARCELA") == "SIM"), 6)
    .when((col("É_MAIOR_PARCELA") == "SIM") & (col("NUMERO_PARCELA") >= 1), 6 - col("NUMERO_PARCELA"))
    .otherwise(0)
)
df_pgto_parc_cond = df_pgto_parc_cond.withColumn(
    "VALOR_POR_PARCELA", 
    F.when((col("VALOR_RESTANTE") > 0), col("VALOR_RESTANTE") / col("PARCELAS_FALTANTES"))
    .otherwise(col("VALOR"))
)

df_pgto_parc_cond = df_pgto_parc_cond.withColumn("PARCELAS_FALTANTES", df_pgto_parc_cond["PARCELAS_FALTANTES"].cast(IntegerType()))

df_pgto_parc_cond_parc_falta = df_pgto_parc_cond.filter(df_pgto_parc_cond["PARCELAS_FALTANTES"] >= 1)


# COMMAND ----------

from pyspark.sql import functions as F

# Suponha que 'df' seja o seu DataFrame original

# Criar uma nova coluna com uma lista de números de parcela para cada linha
df_expanded = df_pgto_parc_cond_parc_falta.withColumn(
    "parcelas", 
    F.expr("sequence(1, PARCELAS_FALTANTES)").cast("array<int>")
)

# Explodir a lista de parcelas para criar uma linha para cada parcela
df_expanded = df_expanded.withColumn(
    "NUMERO_PARCELA", 
    F.explode(df_expanded["parcelas"])
).drop("parcelas")  # Remover a coluna 'parcelas', já que não é mais necessária

# Exibir o resultado
display(df_expanded)


# COMMAND ----------

df_pgto = spark.sql("""
/* AGRUPA TABELA DE PAGAMENTOS DE ACORDOS E CONDENAÇÕES */
WITH CTE AS (
    SELECT PROCESSO_ID
           ,DATA_EFETIVA_DO_PAGAMENTO
           ,COALESCE(SUM(ACORDO), 0) AS ACORDO
           ,COALESCE(SUM(CONDENACAO), 0) AS CONDENACAO
           ,COALESCE(SUM(PENHORA), 0) AS PENHORA
           ,COALESCE(SUM(PENSAO), 0) AS PENSAO
           ,COALESCE(SUM(OUTROS_PAGAMENTOS), 0) AS OUTROS_PAGAMENTOS
           ,COALESCE(SUM(IMPOSTO), 0) AS IMPOSTO
           --,COALESCE(SUM(CUSTAS), 0) AS CUSTAS
    FROM HISTORICO_DE_PAGAMENTOS_JUD_2
    GROUP BY 1, 2)
    SELECT CTE.*
    FROM CTE
    WHERE (ACORDO + CONDENACAO + PENHORA + OUTROS_PAGAMENTOS + IMPOSTO) > 0 -- + PENSAO + CUSTAS
"""
)

# COMMAND ----------

df_pgto.createOrReplaceTempView("HISTORICO_DE_PAGAMENTOS_JUD_3")

# COMMAND ----------

# display(df_pgto.sort("PROCESSO_ID", ascending=True))

# COMMAND ----------

df_pgto.count() # 587962 | 593780 | 598773

# COMMAND ----------

# MAGIC %md
# MAGIC ### 3- Tratamento base de Garantias
# MAGIC

# COMMAND ----------

# Ajusta os nomes das colunas com '_' no lugar de ' '
df_garantias = adjust_column_names(df_garantias)

# COMMAND ----------

df_garantias.createOrReplaceTempView("TB_GARANTIAS_F")

# COMMAND ----------

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
						'PENHORA - REGULATÓRIO' );
""")
# df_garantias.count() #75479

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

# COMMAND ----------

df_garantias.count() # 42123 | 42762 | 43235

# COMMAND ----------

# MAGIC %md
# MAGIC ### 4 - Junta as bases de Pagamentos e Garantias

# COMMAND ----------

df_pgto.createOrReplaceTempView("HISTORICO_DE_PAGAMENTOS_JUD_4")
df_garantias.createOrReplaceTempView("TB_GARANTIAS_F")

# COMMAND ----------

# Faz o Join das duas tabelas
df_pgto_garantias = spark.sql("""
    SELECT 
    coalesce(A.PROCESSO_ID, B.PROCESSO_ID) AS PROCESSO_ID
    ,coalesce(A.DATA_EFETIVA_DO_PAGAMENTO, B.DATA_EFETIVA_DO_PAGAMENTO) AS DATA_EFETIVA_DO_PAGAMENTO
    ,coalesce(ACORDO, 0) as ACORDO
    ,coalesce(CONDENACAO, 0) as CONDENACAO
    ,coalesce(PENHORA, 0) as PENHORA
    ,coalesce(PENSAO, 0) as PENSAO
    ,coalesce(OUTROS_PAGAMENTOS, 0) as OUTROS_PAGAMENTOS
    ,coalesce(IMPOSTO, 0) as IMPOSTO
    --,CUSTAS
    ,coalesce(GARANTIA, 0) AS GARANTIA
    FROM HISTORICO_DE_PAGAMENTOS_JUD_4 A
    FULL OUTER JOIN TB_GARANTIAS_F B
ON A.PROCESSO_ID = B.PROCESSO_ID AND A.DATA_EFETIVA_DO_PAGAMENTO = B.DATA_EFETIVA_DO_PAGAMENTO;
""")

# COMMAND ----------

df_pgto_garantias = df_pgto_garantias.withColumn("TOTAL_PAGAMENTOS",col("ACORDO") + 
                                                                    col("CONDENACAO") +
                                                                    col("PENHORA") +
                                                                    col("PENSAO") +
                                                                    col("OUTROS_PAGAMENTOS") +
                                                                    col("IMPOSTO") +
                                                                    col("GARANTIA"))

# COMMAND ----------

df_pgto_garantias.createOrReplaceGlobalTempView("HIST_PAGAMENTOS_GARANTIA_TRAB_F")

# COMMAND ----------

df_pgto_garantias.write.format("delta").mode("overwrite").saveAsTable(f"databox.juridico_comum.tb_trab_pgto_garantias_ff")

# COMMAND ----------

df_pgto_garantias.write.format("delta").mode("overwrite").option("mergeSchema", "true").saveAsTable(f"databox.juridico_comum.stg_trab_pgto_garantias")

# COMMAND ----------

df_pgto_garantias.write.format("delta").mode("overwrite").option("mergeSchema", "true").saveAsTable(f"databox.juridico_comum.tb_pgto_garantias_desagrupada")

# COMMAND ----------

# Agrupa as infromações
df_pgto_garantias = spark.sql("""
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
    FROM global_temp.HIST_PAGAMENTOS_GARANTIA_TRAB_F
    GROUP BY 1 
    ORDER BY 1;
 """)

df_pgto_garantias.createOrReplaceTempView("df_pgto_garantias_validar")

# COMMAND ----------

from pyspark.sql.functions import month, year, to_date, lit, concat

df_pgto_garantia_conf = df_pgto_garantias.withColumn("MES", month("DT_ULT_PGTO"))

df_pgto_garantia_conf = df_pgto_garantia_conf.withColumn(
    "DATA_MES_ANO",
    to_date(
        concat(lit("1/"), df_pgto_garantia_conf["MES"], lit("/"), year("DT_ULT_PGTO")),
        "d/M/yyyy"
    )
)

display(df_pgto_garantia_conf)

# COMMAND ----------

df_pgto_garantia_conf.createOrReplaceTempView("HIST_PAGAMENTOS_GARANTIA_TRAB_FFFF")

# COMMAND ----------

# # Agrupa as infromações
# df_pgto_garantia_check = spark.sql("""
#     SELECT DATA_MES_ANO
        
#         ,SUM(coalesce(ACORDO, 0)) AS ACORDO
#         ,SUM(coalesce(CONDENACAO, 0)) AS CONDENACAO
#         ,SUM(coalesce(PENHORA, 0)) AS PENHORA
#         ,SUM(coalesce(PENSAO, 0)) AS PENSAO
#         ,SUM(coalesce(OUTROS_PAGAMENTOS, 0)) AS OUTROS_PAGAMENTOS
#         ,SUM(coalesce(IMPOSTO, 0)) AS IMPOSTO
#         ,SUM(coalesce(GARANTIA, 0)) AS GARANTIA
#         ,SUM(coalesce(TOTAL_PAGAMENTOS, 0)) AS TOTAL_PAGAMENTOS
#     FROM HIST_PAGAMENTOS_GARANTIA_TRAB_FFFF
#    WHERE DATA_MES_ANO >= '2021-01-01'
#       GROUP BY 1 
#     ORDER BY 1;
#  """)

# COMMAND ----------

df_pgto_garantias.createOrReplaceGlobalTempView('HIST_PAGAMENTOS_GARANTIA_TRAB_FF')

df_pgto_garantias.write.format("delta").option("mergeSchema", "true").mode("overwrite").saveAsTable("databox.juridico_comum.tb_trab_pgto_garantias_ff")

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from databox.juridico_comum.tb_trab_pgto_garantias_ff

# COMMAND ----------

# dbutils.notebook.exit("Success")

# COMMAND ----------

# MAGIC %md
# MAGIC # Fim

# COMMAND ----------

# from datetime import datetime
# from dateutil.relativedelta import relativedelta

# nmmes = '202411'
# mesbase = nmmes + '01'
# database = datetime.strptime(mesbase, '%Y%m%d')
# dt_corte_inicio = database - relativedelta(months=23)
# data_corte_inicio = "'" + dt_corte_inicio.strftime('%Y-%m-%d') + "'"
# print(data_corte_inicio)

# df_encerrados_202406 = spark.sql(f'''
#     SELECT * 
#     FROM databox.juridico_comum.tb_fecham_financ_trab_202406 
#     WHERE MES_FECH >= {data_corte_inicio} AND ENCERRADOS = 1
# ''')

# df_encerrados_202406.createOrReplaceTempView('df_encerrados_valid')

# COMMAND ----------

# df_garantias_export = spark.sql('''

#     select * from df_garantias_original
#     WHERE `STATUS` NOT IN ('REMOVIDO') AND
#     `ÁREA DO DIREITO` IN ('TRABALHISTA') AND
#     `STATUS DA GARANTIA` IN ('LEVANTADA') AND
#     `TIPO GARANTIA` IN ('PENHORA - GARANTIA',
#     'DEPÓSITO RECURSAL - AIRR - TRABALHISTA',
#     'DEPÓSITO RECURSAL - RO - TRABALHISTA',
#     'DEPÓSITO JUDICIAL - TRABALHISTA',
#     'DEPÓSITO RECURSAL - RR - TRABALHISTA') AND

#     (`DATA DE LEVANTAMENTO (EMPRESA)` is null or `DATA DE LEVANTAMENTO (EMPRESA)` >= '2024-11-01') AND 
#     (`DATA DE LEVANTAMENTO (PARTE CONTRÁRIA)` is null or 
#     `DATA DE LEVANTAMENTO (PARTE CONTRÁRIA)` BETWEEN '2021-09-01' AND '2024-11-20')
# ''')




# COMMAND ----------

# import pandas as pd
# import re
# from shutil import copyfile

# # Converter PySpark DataFrame para Pandas DataFrame
# pandas_df = df_garantias_export.toPandas()

# # Salvar o DataFrame com os nomes corrigidos em um arquivo Excel no disco local primeiro
# local_path = f'/local_disk0/tmp/df_garantias_export.xlsx'
# pandas_df.to_excel(local_path, index=False, sheet_name='df_garantias_export')

# # Copiar o arquivo do disco local para o volume desejado
# volume_path = f'/Volumes/databox/juridico_comum/arquivos/trabalhista/bases_indicadores_resultado/df_garantias_export.xlsx'

# copyfile(local_path, volume_path)
