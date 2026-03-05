# Databricks notebook source
# MAGIC %md
# MAGIC

# COMMAND ----------

# MAGIC %md
# MAGIC ###Fechamento Imobiliário

# COMMAND ----------

pip install openpyxl


# COMMAND ----------

import os
import re
import pandas as pd

# COMMAND ----------

# MAGIC %pip install dbutils

# COMMAND ----------

# MAGIC %md
# MAGIC ### Importação e concatenação bases Imobiliário

# COMMAND ----------

import os
import re
import pandas as pd

# Defina o caminho da pasta de origem e destino
diretorio_origem = "/Volumes/databox/juridico_comum/arquivos/imobiliário/bases_fechamento_financeiro/"
diretorio_destino = "/Volumes/databox/juridico_comum/arquivos/imobiliário/bases_fechamento_financeiro/"

# Lista para armazenar os dataframes
imobiliario = []

# Percorrer todos os arquivos na pasta
for file_name in os.listdir(diretorio_origem):
    file_path = os.path.join(diretorio_origem, file_name)

    # Substitua o nome do arquivo, se necessário, antes de extrair a data
    if file_name == "02.05.2023 Fechamento Imobiliário.xlsx":
        file_name_adjusted = "01.04.2023 Fechamento Imobiliário.xlsx"
    else:
        file_name_adjusted = file_name
    
    # Verificar se o arquivo (ajustado) é um Excel (.xlsx ou .xlsm) e contém a data no nome
    if file_name_adjusted.endswith(('.xlsx', '.xlsm')) and re.search(r"\d{2}.\d{2}.\d{4}", file_name_adjusted):
        # Extrair a data do padrão de nomeação
        date = re.search(r"\d{2}.\d{2}.\d{4}", file_name_adjusted).group()
        date = pd.to_datetime(date, format="%d.%m.%Y").replace(day=1).strftime("%Y-%m-%d")
        
        # Ler o arquivo utilizando pandas
        xl = pd.ExcelFile(file_path)
        
        # Percorrer as abas do arquivo
        for sheet_name in xl.sheet_names:
            # Verificar se a aba contém 'Base' ou 'Base Fechamento' no nome
            if 'Base' in sheet_name or 'Base Fechamento' in sheet_name:
                # Determinar a linha do cabeçalho de forma dinâmica
                header_line = None
                for i, row in enumerate(pd.read_excel(file_path, sheet_name, header=None).itertuples(), start=1):
                    if 'LINHA' in row:
                        header_line = i
                        break
                
                if header_line is not None:
                    # Ler a aba como DataFrame, pulando as linhas de cabeçalho incorretas
                    df = xl.parse(sheet_name, skiprows=header_line-1)

                    # Renomear a coluna LINHA para Data
                    df = df.rename(columns={"LINHA": "MES_FECH"})
                    
                    # Inserir a data no dataframe
                    df["MES_FECH"] = date

                    # Adicionar o dataframe à lista
                    imobiliario.append(df)

# Fazer append de todos os dataframes
fechamento_imobiliario_final = pd.concat(imobiliario, ignore_index=True)


# Converter a coluna MES_FECH para o formato de data
fechamento_imobiliario_final["MES_FECH"] = pd.to_datetime(fechamento_imobiliario_final["MES_FECH"], format="%Y-%m-%d")
# Criar o diretório de destino se não existir


os.makedirs(diretorio_destino, exist_ok=True)

# Salvar o DataFrame resultante em um arquivo CSV
fechamento_imobiliario_final.to_csv(os.path.join(diretorio_destino, "bases_concatenadas.csv"), index=False)

print("Bases concatenadas foram salvas em:", os.path.join(diretorio_destino, "bases_concatenadas.csv"))


# COMMAND ----------

print(fechamento_imobiliario_final.head(10))

# COMMAND ----------

fechamento_imobiliario_final.count()

# COMMAND ----------

# Define the absolute path for saving the table
# Read the CSV file as a Spark DataFrame
fechamento_imobiliario_final_spark = spark.read.csv("/Volumes/databox/juridico_comum/arquivos/imobiliário/bases_fechamento_financeiro/bases_concatenadas.csv", header=True, inferSchema=True)

# COMMAND ----------

import re

def clean_column_names(df):
    """
    Limpa os caracteres irregulares nas colunas do DataFrame.
    
    Args:
        df (pyspark.sql.DataFrame): DataFrame Spark.
    
    Returns:
        pyspark.sql.DataFrame: DataFrame com nomes de colunas limpos.
    """
    # Define os caracteres irregulares
    invalid_chars = " ,;{}()\n\t="
    
    # Limpa os caracteres irregulares das colunas
    cleaned_columns = []
    for column in df.columns:
        cleaned_column = re.sub(r'[{}]+'.format(re.escape(invalid_chars)), '_', column)
        cleaned_columns.append(cleaned_column)
    
    # Renomeia as colunas no DataFrame
    df_cleaned = df.toDF(*cleaned_columns)
    
    return df_cleaned

# COMMAND ----------

# Chama a função para limpar os nomes das colunas
fechamento_imobiliario_final = clean_column_names(fechamento_imobiliario_final_spark)

# Salva o DataFrame limpo como uma tabela Delta
fechamento_imobiliario_final.write.format("delta").mode("overwrite").saveAsTable("databox.juridico_comum.fechamento_imobiliario_final")

# COMMAND ----------

display(spark.sql("SELECT * FROM databox.juridico_comum.fechamento_imobiliario_final"))

# COMMAND ----------

fechamento_imobiliario_final.count()

# COMMAND ----------

imobiliario_sorted = fechamento_imobiliario_final.orderBy("MES_FECH")
imobiliario_sorted.show()

# COMMAND ----------

# Ler a tabela Delta em um DataFrame
imobiliario_sorted = spark.read.format("delta").table("databox.juridico_comum.fechamento_imobiliario_final")



# COMMAND ----------

imobiliario_sorted = imobiliario.orderBy("MES_FECH")

# COMMAND ----------

from pyspark.sql.functions import sum as spark_sum

imobiliario = imobiliario_sorted.groupBy("MES_FECH").agg(
    spark_sum("NOVOS").alias("NOVOS"),
    spark_sum("ENCERRADOS").alias("ENCERRADOS"),
    spark_sum("ESTOQUE").alias("ESTOQUE"),
    spark_sum("Provisão_Total_Passivo_M_43").alias("Provisão_Total_Passivo__M")
)

imobiliario_sorted = imobiliario.sort("MES_FECH")
imobiliario.show()

# COMMAND ----------

imobiliario_sorted = imobiliario.sort("MES_FECH")

# COMMAND ----------

dbutils.library.restartPython()

# COMMAND ----------

# MAGIC %md
# MAGIC

# COMMAND ----------


