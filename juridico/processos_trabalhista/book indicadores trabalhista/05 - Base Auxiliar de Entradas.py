# Databricks notebook source
# MAGIC %md
# MAGIC # Base Auxiliar de Entradas
# MAGIC
# MAGIC **Inputs**\
# MAGIC Tabela:\
# MAGIC juridico_comum.tb_fecham_financ_trab_{nmmes}
# MAGIC
# MAGIC Planilhas:\
# MAGIC TB_FECHAM_FINANC_TRAB_202201_F.xlsx \
# MAGIC Trabalhista_Gerencial_(Ativos)-20191220.xlsx \
# MAGIC Trabalhista_Gerencial_(Encerrados)-20191230.xlsx
# MAGIC
# MAGIC
# MAGIC **Outputs**\
# MAGIC Tabela:\
# MAGIC juridico_comum.tb_entradas_trab_2017_{nmmes}

# COMMAND ----------

# Parametro do nome da tabela da competência atual. Ex: 202404
dbutils.widgets.text("nmmes", "")

# COMMAND ----------

from datetime import datetime

# COMMAND ----------

# MAGIC %run "/Workspace/Jurídico/funcao_tratamento_fechamento/common_functions"

# COMMAND ----------

# MAGIC %md
# MAGIC ## 1 - Imports

# COMMAND ----------

# MAGIC %md
# MAGIC ### 1.1 Importa a base atual

# COMMAND ----------

# Traz a tb_fecham_financ_trab_{nmmes}. Gerada pelo Notebook "4 - Base Consolidada Final"
nmmes = dbutils.widgets.get("nmmes")
df_fecham_financ_trab = spark.sql(f"""
    SELECT ID_PROCESSO
		,AREA_DO_DIREITO
		,SUB_AREA_DO_DIREITO
		,PROCESSO_ESTEIRA
		,DATACADASTRO
		,PARTE_CONTRARIA_NOME
		,PARTE_CONTRARIA_CPF
		,PARTE_CONTRARIA_CARGO_GRUPO
		,VALOR_DA_CAUSA
		,CLASSIFICACAO
		,NATUREZA_OPERACIONAL
		,ESCRITORIO_RESPONSAVEL
		,DISTRIBUICAO
		,ESTADO
		,COMARCA
		,FASE
		,MES_CADASTRO
		,NOVOS
		,MES_FECH 
		,DATA_ADMISSAO
		,MES_DATA_ADMISSAO
		,DATA_DISPENSA
		,MES_DATA_DISPENSA
		,NOVO_X_LEGADO
		,'ATUAL' AS SOURCE
    FROM databox.juridico_comum.tb_fecham_financ_trab_{nmmes}
    WHERE NOVOS = 1
"""
)

df_fecham_financ_trab.createOrReplaceTempView("TB_FECHAM_FINANC_ENTR_2020_22")

# COMMAND ----------

# df_fecham_financ_trab.count()


# COMMAND ----------

# MAGIC %md
# MAGIC ### 1.2 Importa base de entradas 2020

# COMMAND ----------

df_fecham_financ_trab_2020 = read_excel('/Volumes/databox/juridico_comum/arquivos/trabalhista/bases_auxiliares/TB_FECHAM_FINANC_TRAB_202201_F.xlsx')
df_fecham_financ_trab_2020.createOrReplaceTempView("TB_FECHAM_FINANC_ENTR_2020")
df_fecham_financ_trab_2020 = spark.sql(f"""
    SELECT ID_PROCESSO
		,AREA_DO_DIREITO
		,SUB_AREA_DO_DIREITO
		,PROCESSO_ESTEIRA
		,CADASTRO
		,PARTE_CONTRARIA_NOME
		,PARTE_CONTRARIA_CPF
		,PARTE_CONTRARIA_CARGO_GRUPO
		,VALOR_DA_CAUSA
		,CLASSIFICACAO
		,NATUREZA_OPERACIONAL
		,ESCRITORIO_RESPONSAVEL
		,DISTRIBUICAO
		,ESTADO
		,COMARCA
		,FASE
		,MES_CADASTRO
		,NOVOS
		,MES_FECH 
		,DATA_ADMISSAO
		,MES_DATA_ADMISSAO
		,DATA_DISPENSA
		,MES_DATA_DISPENSA
		,CASE WHEN CADASTRO >= '2020-01-01' THEN 'NOVO' ELSE 'LEGADO' END AS NOVO_X_LEGADO
		,'2020' AS SOURCE
    FROM TB_FECHAM_FINANC_ENTR_2020
    WHERE NOVOS = 1 AND MES_FECH <= '2021-07-01'
    """
)
df_fecham_financ_trab_2020.createOrReplaceTempView("TB_FECHAM_FINANC_ENTR_2020")

# COMMAND ----------

# display(df_fecham_financ_trab_2020)

# COMMAND ----------

# df_fecham_financ_trab_2020.count()

# COMMAND ----------

# MAGIC %md
# MAGIC ## 2 - Tratamento estoque de dezembro 2019

# COMMAND ----------

# MAGIC %md
# MAGIC ### 2.1 ATIVOS

# COMMAND ----------

# MAGIC %md
# MAGIC #### Importa base de ATIVOS de dezembro 2019

# COMMAND ----------

### ATIVOS
path_ativos = '/Volumes/databox/juridico_comum/arquivos/trabalhista/bases_gerenciais/external/Trabalhista_Gerencial_(Ativos)-20191220.xlsx'
df_trab_ger_ativos_2019_raw = read_excel(path_ativos)
df_trab_ger_ativos_2019_raw = adjust_column_names(df_trab_ger_ativos_2019_raw)
df_trab_ger_ativos_2019_raw = remove_acentos(df_trab_ger_ativos_2019_raw)
df_trab_ger_ativos_2019_raw.createOrReplaceTempView("TRAB_GER_ATIVOS_20191220")

# COMMAND ----------

# df_trab_ger_ativos_2019_raw.count()

# COMMAND ----------

# MAGIC %md
# MAGIC #### Ajuste nomes de colunas

# COMMAND ----------

df_trab_ger_ativos_2019 = spark.sql("""
SELECT PROCESSO_ID AS ID_PROCESSO
		,AREA_DO_DIREITO
		,SUB_AREA_DO_DIREITO
		,PROCESSO_ESTEIRA
		,DATA_REGISTRADO
		,PARTE_CONTRARIA_NOME
		,PARTE_CONTRARIA_CPF
		,PARTE_CONTRARIA_CARGO_CARGO_GRUPO AS PARTE_CONTRARIA_CARGO_GRUPO
		,VALOR_DA_CAUSA
		,CLASSIFICACAO
		,FASE
		,DATE_FORMAT(DATA_REGISTRADO, 'y-MM-01') AS MES_CADASTRO
		,1 AS NOVOS
--		,MES_FECH
		,PARTE_CONTRARIA_DATA_ADMISSAO AS DATA_ADMISSAO
		,PARTE_CONTRARIA_DATA_DISPENSA AS DATA_DISPENSA
		,DATE_FORMAT(PARTE_CONTRARIA_DATA_ADMISSAO, 'y-MM-01') AS MES_DATA_ADMISSAO
		,DATE_FORMAT(PARTE_CONTRARIA_DATA_DISPENSA, 'y-MM-01') AS MES_DATA_DISPENSA
		,MATRICULA
		,PAGE_REPORT_ESCRITORIORESPONSAVEL AS ESCRITORIO_RESPONSAVEL
		,DISTRIBUICAO
		,PROCESSO_ESTADO AS ESTADO
		,PROCESSO_COMARCA AS COMARCA
		,CENTRO_DE_CUSTO_AREA_DEMANDANTE_NOME
		,CASE 
        	WHEN (FILIAL = '' OR FILIAL IS NULL) THEN SUBSTRING(CENTRO_DE_CUSTO_AREA_DEMANDANTE_NOME, 3, 4) 
         	ELSE FILIAL 
        END AS FILIAL
		,CASE 
			WHEN DATA_REGISTRADO >= '2020-01-01' THEN 'NOVO'
			ELSE 'LEGADO'
		END AS NOVO_X_LEGADO
		, 'ATIVOS <2019' AS SOURCE
FROM TRAB_GER_ATIVOS_20191220
""")

# COMMAND ----------

# MAGIC %run "./mes_contabil"

# COMMAND ----------

# Add MES_FECH
df_mes_contabil.createOrReplaceTempView("TB_MES_CONTABIL")
df_trab_ger_ativos_2019.createOrReplaceTempView("TRAB_GER_ATIVOS_20191220")

df_trab_ger_ativos_2019 = spark.sql("""
    SELECT
    A.*
    ,B.mes_contabil AS MES_FECH
FROM
    TRAB_GER_ATIVOS_20191220 A
LEFT JOIN
    TB_MES_CONTABIL B
ON
    A.DATA_REGISTRADO BETWEEN B.dt_contabil_inicio AND B.dt_contabil_fim
    """
)

# COMMAND ----------

# df_trab_ger_ativos_2019.count()

# COMMAND ----------

# MAGIC %md
# MAGIC ### 2.2 ENCERRADOS

# COMMAND ----------

# MAGIC %md
# MAGIC #### Importa base de ENCERRADOS de dezembro 2019

# COMMAND ----------

### ENCERRADOS
df_trab_ger_encerrados_2019_raw = read_excel('/Volumes/databox/juridico_comum/arquivos/trabalhista/bases_gerenciais/external/Trabalhista_Gerencial_(Encerrados)-20191230.xlsx')
df_trab_ger_encerrados_2019_raw = adjust_column_names(df_trab_ger_encerrados_2019_raw)
df_trab_ger_encerrados_2019_raw = remove_acentos(df_trab_ger_encerrados_2019_raw)
df_trab_ger_encerrados_2019_raw.createOrReplaceTempView("TRAB_GERENCIAL_ENCERR_20191230")

# COMMAND ----------

# df_trab_ger_encerrados_2019_raw.count()

# COMMAND ----------

# MAGIC %md
# MAGIC #### Ajuste nomes de colunas

# COMMAND ----------

df_trab_ger_encerrados_2019 = spark.sql("""
SELECT PROCESSO_ID AS ID_PROCESSO
		,AREA_DO_DIREITO
		,SUB_AREA_DO_DIREITO
		,PROCESSO_ESTEIRA
		,DATA_REGISTRADO
		,PARTE_CONTRARIA_NOME
		,PARTE_CONTRARIA_CPF
		,PARTE_CONTRARIA_CARGO_CARGO_GRUPO AS PARTE_CONTRARIA_CARGO_GRUPO
		,VALOR_DA_CAUSA
		,CLASSIFICACAO
		,FASE
		,DATE_FORMAT(DATA_REGISTRADO, 'y-MM-01') AS MES_CADASTRO
		,1 AS NOVOS
--		,MES_FECH
		,PARTE_CONTRARIA_DATA_ADMISSAO AS DATA_ADMISSAO
		,PARTE_CONTRARIA_DATA_DISPENSA AS DATA_DISPENSA
		,DATE_FORMAT(PARTE_CONTRARIA_DATA_ADMISSAO, 'y-MM-01') AS MES_DATA_ADMISSAO
		,DATE_FORMAT(PARTE_CONTRARIA_DATA_DISPENSA, 'y-MM-01') AS MES_DATA_DISPENSA
		,MATRICULA
		,PAGE_REPORT_ESCRITORIORESPONSAVEL AS ESCRITORIO_RESPONSAVEL
		,DISTRIBUICAO
		,PROCESSO_ESTADO AS ESTADO
		,PROCESSO_COMARCA AS COMARCA
		,CENTRO_DE_CUSTO_AREA_DEMANDANTE_NOME
		,CASE 
        	WHEN (FILIAL = '' OR FILIAL IS NULL) THEN SUBSTRING(CENTRO_DE_CUSTO_AREA_DEMANDANTE_NOME, 3, 4) 
         	ELSE FILIAL 
        END AS FILIAL
		,CASE 
			WHEN DATA_REGISTRADO >= '2020-01-01' THEN 'NOVO'
			ELSE 'LEGADO'
		END AS NOVO_X_LEGADO
		, 'ENCERRADOS <2019' AS SOURCE
FROM TRAB_GERENCIAL_ENCERR_20191230
""")

# COMMAND ----------

# Add MES_FECH
df_mes_contabil.createOrReplaceTempView("TB_MES_CONTABIL")
df_trab_ger_encerrados_2019.createOrReplaceTempView("TRAB_GER_ENCERR_20191230_1")

df_trab_ger_encerrados_2019 = spark.sql("""
    SELECT
    A.*
    ,B.mes_contabil AS MES_FECH
FROM
    TRAB_GER_ENCERR_20191230_1 A
LEFT JOIN
    TB_MES_CONTABIL B
ON
    A.DATA_REGISTRADO BETWEEN B.dt_contabil_inicio AND B.dt_contabil_fim
    """
)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 3 - Junta as bases

# COMMAND ----------

# MAGIC %md
# MAGIC ### 3.1 Cria a TB_ENTRADAS_2017_2019

# COMMAND ----------

df_2017_2019 = df_trab_ger_ativos_2019.unionByName(df_trab_ger_encerrados_2019, allowMissingColumns=True).where("MES_FECH >= '2019-12-01'").sort(asc("ID_PROCESSO"))
# print("df_2017_2019")
# print(df_2017_2019.count())

# COMMAND ----------

# df_2017_2019.count()

# COMMAND ----------

df_2017_2020 = df_2017_2019.unionByName(df_fecham_financ_trab_2020, allowMissingColumns=True)
# print("df_2017_2020")
# print(df_2017_2020.count())

df_entradas = df_2017_2020.unionByName(df_fecham_financ_trab, allowMissingColumns=True)
# print("df_entradas")
# print(df_entradas.count())

# COMMAND ----------

df_entradas.sort(asc("ID_PROCESSO")).sort(desc("MES_FECH"))

# COMMAND ----------

 df_entradas.count()

# COMMAND ----------

display(df_entradas)

# COMMAND ----------

df_entradas.write.format("delta").mode("overwrite").saveAsTable(f"databox.juridico_comum.tb_entradas_trab_2017_{nmmes}")

# COMMAND ----------

# df_entradas.display()

# COMMAND ----------

df_validar = spark.sql(f"""
    SELECT MES_FECH, count(ID_PROCESSO) as QTD
    FROM databox.juridico_comum.tb_entradas_trab_2017_{nmmes}
    WHERE MES_FECH >= '2020-01-01 00:00:00'
    GROUP BY ALL
    ORDER BY MES_FECH DESC
""")

display(df_validar)



# COMMAND ----------

import pandas as pd
import matplotlib.pyplot as plt

# Exemplo de consulta usando PySpark
df = spark.sql(f"""
    SELECT date_format(MES_FECH, 'y') AS MES_FECH, COUNT(ID_PROCESSO) AS NUM_PROCESSOS
    FROM databox.juridico_comum.tb_entradas_trab_2017_{nmmes}
    GROUP BY MES_FECH
""").toPandas()

# Gerando o gráfico com Matplotlib
df.plot(x='MES_FECH', y='NUM_PROCESSOS', kind='bar', legend=False)
plt.xlabel('Mês de Fechamento')
plt.ylabel('Número de Processos')
plt.title('Número de Processos por Mês de Fechamento')
plt.show()

