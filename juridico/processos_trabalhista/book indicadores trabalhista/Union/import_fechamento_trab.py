# Databricks notebook source
# MAGIC %md
# MAGIC # Fechamento Financeiro

# COMMAND ----------

# MAGIC %run "/Workspace/Jurídico/funcao_tratamento_fechamento/common_functions"

# COMMAND ----------

import re

# COMMAND ----------

path = '/Volumes/databox/juridico_comum/arquivos/trabalhista/bases_fechamento_financeiro/trabalhista_base_financeiro_tratada/'

arquivos = dbutils.fs.ls(path)

lista_arquivos = [arquivo.name for arquivo in arquivos]
lista_arquivos

# COMMAND ----------

# for arquivo in lista_arquivos:
#     pattern = r"(\d{6})\.xlsx$"
#     nmmes = re.search(pattern, arquivo).group(1)
#     print(f"DROP TABLE databox.juridico_comum.tb_fecham_financ_trab_{nmmes}")
#     spark.sql(f"DROP TABLE databox.juridico_comum.tb_fecham_financ_trab_{nmmes}")

# COMMAND ----------

lista_dfs = []

for arquivo in lista_arquivos:
    # Trata os nomes de coluna
    df = read_excel(f"{path}{arquivo}")
    df = adjust_column_names(df)
    df = remove_acentos(df)
    df = deduplica_cols(df)
    # df = df.withColumn("SOURCE", lit(arquivo))

    # Pega nome da tabela
    pattern = r"(\d{6})\.xlsx$"
    nmmes = re.search(pattern, arquivo).group(1)

    # Grava a tabela
    df.write.format("delta").mode("overwrite").saveAsTable(f"databox.juridico_comum.tb_fecham_trab_{nmmes}")
    print(f"tb_fecham_trab_{nmmes} CONCLUIDA")


# COMMAND ----------

lista_comps = []

for arquivo in lista_arquivos:
    pattern = r"(\d{6})\.xlsx$"
    nmmes = re.search(pattern, arquivo).group(1)
    lista_comps.append(nmmes)

# COMMAND ----------

print(lista_comps)
