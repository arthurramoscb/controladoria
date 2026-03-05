# Databricks notebook source
# MAGIC %md
# MAGIC # Tratamento das Bases Gerenciais
# MAGIC
# MAGIC **Inputs**\
# MAGIC Planilhas fornecidas pelo ELAW. Gerencial, Ativos e Encerrados.
# MAGIC
# MAGIC **Outputs**\
# MAGIC Dados tratados de cada planilha.

# COMMAND ----------

# Configura campo para que o usuário insira parâmetros

# Parametro do formato de data do arquivo. Ex: 20240423
dbutils.widgets.text("nmtabela", "")

# COMMAND ----------

# MAGIC %run "/Workspace/Jurídico/funcao_tratamento_fechamento/common_functions"

# COMMAND ----------

# Caminho das pastas e arquivos

nmtabela = dbutils.widgets.get("nmtabela")
diretorio_origem = '/Volumes/databox/juridico_comum/arquivos/trabalhista/bases_gerenciais/external/'

arquivo_consolidado = f'TRABALHISTA_GERENCIAL_(CONSOLIDADO)-{nmtabela}.xlsx'
arquivo_ativos = 'Trabalhista_Gerencial_(Ativos)-20191220.xlsx'
arquivo_encerrados = 'Trabalhista_Gerencial_(Encerrados)-20191230.xlsx'

path_consolidado = diretorio_origem + arquivo_consolidado
path_ativos = diretorio_origem + arquivo_ativos
path_encerrados = diretorio_origem + arquivo_encerrados

path_consolidado

# COMMAND ----------

# Carrega as planilhas em Spark Data Frames
df_consolidado = read_excel(path_consolidado, "'TRABALHISTA'!A6")
df_ativos = read_excel(path_ativos, "A1")
df_encerrados = read_excel(path_encerrados, "A1")

# COMMAND ----------

#df_ativos.printSchema()
#df_ativos.display()

# COMMAND ----------

# Lista as colunas com datas
consolidado_data_cols = find_columns_with_word(df_consolidado, 'DATA ')
consolidado_data_cols.append('DISTRIBUIÇÃO')

ativos_data_cols = find_columns_with_word(df_ativos, 'DATA ')
ativos_data_cols.append('DISTRIBUIÇÃO')

encerrados_data_cols = find_columns_with_word(df_encerrados, 'DATA ')
ativos_data_cols.append('DISTRIBUIÇÃO')

print("consolidado_data_cols")
print(consolidado_data_cols)
print("\n")
print("ativos_data_cols")
print(ativos_data_cols)
print("\n")
print("encerrados_data_cols")
print(encerrados_data_cols)

# COMMAND ----------

# Converte as datas das colunas listadas
df_consolidado = convert_to_date_format(df_consolidado, consolidado_data_cols)
df_ativos = convert_to_date_format(df_ativos, ativos_data_cols)
df_encerrados = convert_to_date_format(df_encerrados, encerrados_data_cols)

# COMMAND ----------

#df_consolidado.display()

# COMMAND ----------

# Lista colunas com percentual
consolidado_percent_cols = ["RESPONSÁBILIDADE EMPRESA PERCENTUAL", "RESPONSÁBILIDADE SÓCIO PERCENTUAL"]
ativos_percent_cols = find_columns_with_word(df_ativos, 'PERCENTUAL')
encerrados_percent_cols = find_columns_with_word(df_encerrados, 'PERCENTUAL')

print("consolidado_percent_cols")
print(consolidado_percent_cols)
print("\n")
print("ativos_percent_cols")
print(ativos_percent_cols)
print("\n")
print("encerrados_data_cols")
print(encerrados_percent_cols)

# COMMAND ----------

# Ajuste dos valores percetuais
df_consolidado = convert_to_float(df_consolidado, consolidado_percent_cols)
df_ativos = convert_to_float(df_ativos, ativos_percent_cols)
df_encerrados = convert_to_float(df_encerrados, encerrados_percent_cols)

# COMMAND ----------

# Ajusta os nomes das colunas
df_consolidado = adjust_column_names(df_consolidado)
df_consolidado = remove_acentos(df_consolidado)

# COMMAND ----------

df_consolidado.createOrReplaceTempView("consolidado")

# COMMAND ----------

df_consolidado.write.format("delta").mode("overwrite").saveAsTable(f"databox.juridico_comum.trab_ger_consolida_{nmtabela}")
