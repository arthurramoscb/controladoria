# Databricks notebook source
# MAGIC %md
# MAGIC

# COMMAND ----------

# MAGIC %md
# MAGIC ###Fechamento Procon

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
# MAGIC ### Importação e concatenação bases Regulatório

# COMMAND ----------

import os
import re
import pandas as pd

# Defina o caminho da pasta de origem e destino
diretorio_origem = "/Volumes/databox/juridico_comum/arquivos/regulatório/bases_fechamento_financeiro/"
diretorio_destino = "/Volumes/databox/juridico_comum/arquivos/regulatório/bases_fechamento_financeiro/"

# Lista para armazenar os dataframes
dfs = []

# Percorrer todos os arquivos na pasta
for file_name in os.listdir(diretorio_origem):
    file_path = os.path.join(diretorio_origem, file_name)
    
    # Verificar se o arquivo é um Excel (.xlsx ou .xlsm) e contém a data no nome
    if file_name.endswith(('.xlsx', '.xlsm')) and re.search(r"\d{2}.\d{2}.\d{4}", file_name):
        # Extrair a data do padrão de nomeação
        date = re.search(r"\d{2}.\d{2}.\d{4}", file_name).group()
        # Definir o dia como 1
        date = pd.to_datetime(date, format="%d.%m.%Y").replace(day=1).strftime("%d/%m/%Y")
        
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
                    df = df.rename(columns={"LINHA": "Data"})
                    
                    # Inserir a data no dataframe
                    df["Data"] = date

                    # Adicionar o dataframe à lista
                    dfs.append(df)

# Fazer append de todos os dataframes
df_final_reg = pd.concat(dfs, ignore_index=True)

# Criar o diretório de destino se não existir
os.makedirs(diretorio_destino, exist_ok=True)

# Salvar o DataFrame resultante em um arquivo CSV
df_final_reg.to_csv(os.path.join(diretorio_destino, "bases_concatenadas.csv"), index=False)

print("Bases concatenadas foram salvas em:", os.path.join(diretorio_destino, "bases_concatenadas.csv"))


# COMMAND ----------

display(df_final_reg)

# COMMAND ----------

df_final_reg.count()

# COMMAND ----------

import pyspark.sql.functions as F

# Ler o arquivo CSV para um DataFrame
df = spark.read.csv("/Volumes/databox/juridico_comum/arquivos/regulatório/bases_fechamento_financeiro/bases_concatenadas.csv", header=True, inferSchema=True)
df = df.withColumn("Data", F.to_date(F.col("Data"), "dd/MM/yyyy"))
df = df.withColumn("DATACADASTRO", F.to_date(F.col("DATACADASTRO"), "dd/MM/yyyy"))
df = df.withColumn("Distribuição", F.to_date(F.col("Distribuição"), "dd/MM/yyyy"))
df = df.withColumn("REABERTURA", F.to_date(F.col("REABERTURA"), "dd/MM/yyyy"))
df = df.toDF(*[re.sub('[ ,;{}()\n\t=]', '_', col) for col in df.columns])


# COMMAND ----------


from pyspark.sql.functions import sum as spark_sum, col

Regulatorio= df.groupBy("Data").agg(
    spark_sum("NOVOS").alias("NOVOS"),
    spark_sum("ENCERRADOS").alias("ENCERRADOS"),
    spark_sum("ESTOQUE").alias("ESTOQUE"),
    spark_sum("Provisão_Total_Passivo__M_").alias("Provisão_Total_Passivo__M")

)
Regulatorio = Regulatorio.orderBy('Data')

# Exibir DataFrame resultante
Regulatorio.show()

# COMMAND ----------

display(df)

# COMMAND ----------

# Leitura Tabela Delta Regulatório Arquivo Histórico
display(spark.read.format("delta").load("/Volumes/databox/juridico_comum/arquivos/regulatório/bases_fechamento_financeiro/tabela_delta"))

# COMMAND ----------

# MAGIC %md
# MAGIC ###Base de Pagamentos

# COMMAND ----------

pip install openpyxl

# COMMAND ----------

import pandas as pd

# COMMAND ----------

#Importação base de Pagamentos 
pagamentos_1 = pd.read_excel("/Volumes/databox/juridico_comum/arquivos/bases_pagamentos/HISTORICO_DE-PAGAMENTOS_20240401.xlsx", sheet_name = "PAGAMENTOS", header=5)

# COMMAND ----------

print(pagamentos_1.dtypes)

# COMMAND ----------

import pandas as pd

# Convertendo para datetime e definindo erros como 'coerce' para tratar datas inválidas
pagamentos_1['DATA LIMITE DE PAGAMENTO'] = pd.to_datetime(pagamentos_1['DATA LIMITE DE PAGAMENTO'], errors='coerce')
pagamentos_1['DATA DE VENCIMENTO'] = pd.to_datetime(pagamentos_1['DATA DE VENCIMENTO'], errors='coerce')
pagamentos_1['DATA EFETIVA DO PAGAMENTO'] = pd.to_datetime(pagamentos_1['DATA EFETIVA DO PAGAMENTO'], errors='coerce')

# Print dos tipos de dados para verificar se os dados foram convertidos corretamente



# COMMAND ----------

#Schema manual para a conversão do Dataframe Pyspark


from pyspark.sql.types import StructType, StructField, StringType, IntegerType, DateType, FloatType

# Definir o schema manualmente
schema = StructType([
    StructField("PROCESSO - ID", IntegerType(), True),
    StructField("ID DO PAGAMENTO", IntegerType(), True),
    StructField("STATUS DO PROCESSO", StringType(), True),
    StructField("ÁREA DO DIREITO", StringType(), True),
    StructField("DATA REGISTRADO", DateType(), True),
    StructField("EMPRESA", StringType(), True),
    StructField("DATA SOLICITAÇÃO (ESCRITÓRIO)", DateType(), True),
    StructField("DATA LIMITE DE PAGAMENTO", DateType(), True),
    StructField("DATA DE VENCIMENTO", DateType(), True),
    StructField("DATA EFETIVA DO PAGAMENTO", DateType(), True),
    StructField("STATUS DO PAGAMENTO", StringType(), True),
    StructField("TIPO", StringType(), True),
    StructField("SUB TIPO", StringType(), True),
    StructField("VALOR", FloatType(), True),
    StructField("RESPONSÁBILIDADE COMPRADOR PERCENTUAL", FloatType(), True),
    StructField("RESPONSÁBILIDADE VENDEDOR PERCENTUAL", FloatType(), True),
    StructField("VALOR DA MULTA POR DESCUMPRIMENTO DA OBRIGAÇÃO", FloatType(), True),
    StructField("PAGAMENTO ATRAVÉS DE", FloatType(), True),
    StructField("NÚMERO DO DOCUMENTO SAP", FloatType(), True),
    StructField("INDICAR O MOMENTO DA REALIZAÇÃO DO ACORDO", FloatType(), True),
    StructField("PARCELAMENTO ACORDO", StringType(), True),
    StructField("PARCELAMENTO CONDENAÇÃO", StringType(), True),
    StructField("PARTE CONTRÁRIA CPF", StringType(), True),
    StructField("PARTE CONTRÁRIA NOME", StringType(), True),
    StructField("FAVORECIDO (NOME)", StringType(), True),
    StructField("PARTE CONTRÁRIA - CARGO - CARGO (GRUPO)", StringType(), True),
    StructField("CÍVEL MASSA - RESPONSABILIDADE",StringType(), True),
    StructField("ESCRITÓRIO", StringType(), True),
    StructField("CÍVEL MASSA - RESPONSABILIDADE.1", StringType(), True),
    StructField("CARTEIRA", StringType(), True),
    StructField("ADVOGADO RESPONSÁVEL", StringType(), True),
    StructField("NOVO TERCEIRO", StringType(), True)
])

# Converter DataFrame Pandas para DataFrame PySpark com o esquema definido
hist_pag = spark.createDataFrame(pagamentos_1, schema=schema)

# Exibir o esquema do DataFrame Spark resultante
hist_pag.printSchema()





# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC ### Transformação Base de Pagamentos

# COMMAND ----------

# Renomear e tratar campos da Base de Pagamentos para formato Pyspark
hist_pag = hist_pag.toDF(*[col.replace(' ', '_') for col in hist_pag.columns])
hist_pag = hist_pag.toDF(*[col.replace('(', '') for col in hist_pag.columns])
hist_pag = hist_pag.toDF(*[col.replace(')', '') for col in hist_pag.columns])

# COMMAND ----------

from pyspark.sql.functions import col, when, regexp_replace, translate, trim
from pyspark.sql.functions import udf
from pyspark.sql.types import StringType, DoubleType, DecimalType
from pyspark.sql.types import FloatType
from pyspark.sql.window import Window
import pyspark.sql.functions as F

# COMMAND ----------

acordo = ['ACORDO - MEDIAÇÃO', 'ACORDO - REGULATÓRIO - PROCON COM AUDIÊNCIA', 'ACORDO/ PROGRAMAS DE PARCELAMENTO - REGULATÓRIO']
condenacao = ['CONDENAÇÃO - REGULATÓRIO', 'MULTA - REGULATÓRIO']
custas = ['CUSTAS PROCESSUAIS - REGULATÓRIO']
penhora = ['LIBERAÇÃO DE PENHORA - REGULATÓRIO']

# COMMAND ----------

#dbutils.library.restartPython()

# COMMAND ----------

# Adicione a coluna 'Valor Final' com base na coluna 'SUB TIPO'
hist_pag = hist_pag.withColumn("Valor_Final", 
                                when(hist_pag["SUB_TIPO"].isin(acordo), "Acordo")
                               .when(hist_pag["SUB_TIPO"].isin(condenacao), "Condenação")
                               .when(hist_pag["SUB_TIPO"].isin(custas), "Custas")
                               .when(hist_pag["SUB_TIPO"].isin(penhora), "Penhora")
                               .otherwise("")
                              )


# COMMAND ----------

display(hist_pag)

# COMMAND ----------

from pyspark.sql.functions import col, when
from pyspark.sql.types import IntegerType

# Create new columns using when and otherwise functions
hist_pag = hist_pag.withColumn("Acordo", when(col("Valor_Final") == "Acordo", col("VALOR")).otherwise(0))
hist_pag = hist_pag.withColumn("Condenação", when(col("Valor_Final") == "Condenação", col("VALOR")).otherwise(0))
hist_pag = hist_pag.withColumn("Custas", when(col("Valor_Final") == "Custas", col("VALOR")).otherwise(0))
hist_pag = hist_pag.withColumn("Penhora", when(col("Valor_Final") == "Penhora", col("VALOR")).otherwise(0))

# Filter rows based on conditions
status_pag = ['CANCELADO', 'REMOVIDO', 'REJEITADO']
hist_pag_a = hist_pag.filter(~col("STATUS_DO_PAGAMENTO").isin(status_pag) &
                             ~col("STATUS_DO_PAGAMENTO").contains("PAGAMENTO DEVOLVIDO") &
                             ~col("STATUS_DO_PAGAMENTO").isin(status_pag) &
                             ~col("STATUS_DO_PAGAMENTO").contains("PAGAMENTO DEVOLVIDO"))


# If needed, cast the newly created columns to IntegerType
hist_pag_a = hist_pag_a.withColumn("Acordo", col("Acordo").cast(IntegerType()))
hist_pag_a = hist_pag_a.withColumn("Condenação", col("Condenação").cast(IntegerType()))
hist_pag_a = hist_pag_a.withColumn("Custas", col("Custas").cast(IntegerType()))
hist_pag_a = hist_pag_a.withColumn("Penhora", col("Penhora").cast(IntegerType()))

# COMMAND ----------


from pyspark.sql.types import StructType, StructField, StringType, ArrayType

# Dados
d = [
     
    {'01/11/2018':[('01/01/1900','20/11/2018 23:59:59')],
    '01/12/2018':[('21/11/2018','17/12/2018 23:59:59')],
    '01/01/2019':[('18/12/2018','21/01/2019 23:59:59')],
    '01/02/2019':[('22/01/2019','18/02/2019 23:59:59')],
    '01/03/2019':[('19/02/2019','19/03/2019 23:59:59')],
    '01/04/2019':[('20/03/2019','21/04/2019 23:59:59')],
    '01/05/2019':[('22/04/2019','21/05/2019 23:59:59')],
    '01/06/2019':[('22/05/2019','17/06/2019 23:59:59')],
    '01/07/2019':[('18/06/2019','21/07/2019 23:59:59')],
    '01/08/2019':[('22/07/2019','20/08/2019 23:59:59')],
    '01/09/2019':[('21/08/2019','22/09/2019 23:59:59')],
    '01/10/2019':[('23/09/2019','23/10/2019 23:59:59')],
    '01/11/2019':[('24/10/2019','21/11/2019 23:59:59')],
    '01/12/2019':[('22/11/2019','19/12/2020 23:59:59')],  
    '01/01/2020':[('20/12/2019','26/01/2020 23:59:59')],
    '01/02/2020':[('27/01/2020','28/02/2020 23:59:59')],
    '01/03/2020':[('29/02/2020','24/03/2020 23:59:59')],
    '01/04/2020':[('25/03/2020','20/04/2020 23:59:59')],
    '01/05/2020':[('21/04/2020','19/05/2020 23:59:59')],
    '01/06/2020':[('20/05/2020','23/06/2020 23:59:59')],
    '01/07/2020':[('24/06/2020','29/07/2020 23:59:59')],
    '01/08/2020':[('30/07/2020','21/08/2020 23:59:59')],
    '01/09/2020':[('22/08/2020','22/09/2020 23:59:59')],
    '01/10/2020':[('23/09/2020','22/10/2020 23:59:59')],
    '01/11/2020':[('23/10/2020','24/11/2020 23:59:59')],
    '01/12/2020':[('25/11/2020','18/12/2020 23:59:59')],
    '01/01/2021':[('19/12/2020','23/01/2021 23:59:59')],
    '01/02/2021':[('24/01/2021','22/02/2021 23:59:59')],
    '01/03/2021':[('23/02/2021','21/03/2021 23:59:59')],
    '01/04/2021':[('22/03/2021','21/04/2021 23:59:59')],
    '01/05/2021':[('22/04/2021','23/05/2021 23:59:59')],
    '01/06/2021':[('24/05/2021','21/06/2021 23:59:59')],
    '01/07/2021':[('22/06/2021','22/07/2021 23:59:59')],
    '01/08/2021':[('23/07/2021','22/08/2021 23:59:59')],
    '01/10/2022':[('22/09/2022','21/10/2022 23:59:59')],
    '01/09/2022':[('24/08/2022','21/09/2022 23:59:59')],
    '01/11/2022':[('22/10/2022','18/11/2022 23:59:59')],
    '01/12/2022':[('19/11/2022','22/12/2022 23:59:59')],
    '01/01/2023':[('23/12/2022','20/01/2023 23:59:59')],
    '01/02/2023':[('21/01/2023','17/02/2023 23:59:59')],
    '01/03/2023':[('18/02/2023','21/03/2023 23:59:59')],
    '01/04/2023':[('22/03/2023','02/05/2023 23:59:59')],
    '01/05/2023':[('03/05/2023','22/05/2023 23:59:59')],
    '01/06/2023':[('23/05/2023','22/06/2023 23:59:59')],
    '01/07/2023':[('23/06/2023','23/07/2023 23:59:59')],
    '01/08/2023':[('24/07/2023','22/08/2023 23:59:59')],
    '01/09/2023':[('23/08/2023','22/09/2023 23:59:59')],
    '01/10/2023':[('23/09/2023','23/10/2023 23:59:59')],
    '01/11/2023':[('24/10/2023','22/11/2023 23:59:59')],
    '01/12/2023':[('23/11/2023','15/12/2023 23:59:59')],
    '01/01/2024':[('16/12/2023','22/01/2024 23:59:59')],
    '01/02/2024':[('23/01/2024','20/02/2024 23:59:59')]}
]

# COMMAND ----------

display(hist_pag_a)

# COMMAND ----------


from pyspark.sql.functions import col, when, to_date


# Inicializar a coluna 'Data Levantamento F' com NoneType
hist_pag_a = hist_pag_a.withColumn("DATA_EFETIVA_PAGAMENTO_F", when(col("DATA_EFETIVA_DO_PAGAMENTO").isNull(), None).otherwise(col("DATA_EFETIVA_DO_PAGAMENTO")))

# Iterar sobre os itens da lista 'd' e aplicar a lógica necessária
for item in d:
    for data, intervalos in item.items():
        for s, e in intervalos:
            # Usar a função withColumn do PySpark para atualizar o DataFrame 'hist_pag_a'
            hist_pag_a = hist_pag_a.withColumn(
                "DATA_EFETIVA_PAGAMENTO_F",
                when(
                    (col("DATA_EFETIVA_DO_PAGAMENTO").between(s, e)),
                    data
                ).otherwise(col("DATA_EFETIVA_PAGAMENTO_F"))
            )
#hist_pag_a = hist_pag_a.withColumn('DATA_EFETIVA_DO_PAGAMENTO', to_date(hist_pag_a['DATA_EFETIVA_PAGAMENTO_F'], 'yyyy-MM-dd'))
# Exemplo de como mostrar o DataFrame resultante
display(hist_pag_a)

# COMMAND ----------

# Sort DataFrame by 'PROCESSO - ID' and 'Processo - Assunto (CÍVEL) - Ass' in descending order
sorted_df = df.orderBy(['PROCESSO - ID', 'Processo - Assunto (CÍVEL) - Ass'], ascending=[False, False])

# Remove duplicate rows based on 'PROCESSO - ID' column
deduplicated_df = sorted_df.dropDuplicates(['PROCESSO - ID'])

# Perform frequency analysis on 'DISTRIBUICAO' column
freq_result = deduplicated_df.groupBy('DISTRIBUICAO').count()

# Display frequency analysis result
freq_result.show()

# COMMAND ----------


# Salvar o DataFrame como tabela Delta na camada Databox.juridico_schema
hist_pag_a_group.write.format("delta").mode("overwrite").saveAsTable("databox.juridico_comum.pagamento_historico")

# COMMAND ----------

# MAGIC %sql
# MAGIC DROP TABLE IF EXISTS databox.juridico_comum.pagamento_historicos;

# COMMAND ----------

# MAGIC %md
# MAGIC ###Base de Garantias

# COMMAND ----------

# MAGIC %pip install pyspark
# MAGIC

# COMMAND ----------

import pandas as pd



# COMMAND ----------

#Importação base de Pagamentos 
garantias_1 = pd.read_excel("/Volumes/databox/juridico_comum/arquivos/regulatório/bases_garantias/GARANTIAS_20240304.xlsx", sheet_name = "gerencial_garantias", header=5)

# COMMAND ----------

# Convertendo para datetime e definindo erros como 'coerce' para tratar datas inválidas e tratamento das datas com comandos da biblioteca Pandas
garantias_1['DATA DE LEVANTAMENTO (PARTE CONTRÁRIA)'] = pd.to_datetime(garantias_1['DATA DE LEVANTAMENTO (PARTE CONTRÁRIA)'], errors='coerce')
garantias_1['DATA DE LEVANTAMENTO (EMPRESA)'] = pd.to_datetime(garantias_1['DATA DE LEVANTAMENTO (EMPRESA)'], errors='coerce')
garantias_1['DATA DO ALVARÁ'] = pd.to_datetime(garantias_1['DATA DO ALVARÁ'], errors='coerce')
garantias_1['DATA DA CI'] = pd.to_datetime(garantias_1['DATA DA CI'], errors='coerce')
garantias_1['DATA DA VIGÊNCIA - JUDICIAL'] = pd.to_datetime(garantias_1['DATA DA VIGÊNCIA - JUDICIAL'], errors='coerce')

# COMMAND ----------

print(garantias_1.dtypes)

# COMMAND ----------

#Criação do Dataframe Spark e criação do Schema da tabela
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, DateType, FloatType

schema = StructType([
    StructField("PROCESSO_ID", IntegerType(), True),
    StructField("DATA_REGISTRADO_PROCESSO", DateType(), True),
    StructField("PASTA", StringType(), True),
    StructField("STATUS", StringType(), True),
    StructField("GRUPO", StringType(), True),
    StructField("ÁREA_DO_DIREITO", StringType(), True),
    StructField("NÚMERO_DO_PROCESSO", StringType(), True),
    StructField("CENTRO_DE_CUSTO_ÁREA_DEMANDANTE_CÓDIGO", StringType(), True),
    StructField("CC_DO_SÓCIO", StringType(), True),
    StructField("EMPRESA", StringType(), True),
    StructField("ESCRITÓRIO_RESPONSÁVEL", StringType(), True),
    StructField("PARTE_CONTRÁRIA_NOME", StringType(), True),
    StructField("STATUS_DA_GARANTIA", StringType(), True),
    StructField("ID", IntegerType(), True),
    StructField("TIPO_GARANTIA", StringType(), True),
    StructField("VALOR", FloatType(), True),
    StructField("VALOR_ATUALIZADO", FloatType(), True),
    StructField("RESPONSÁBILIDADE_COMPRADOR_PERCENTUAL", FloatType(), True),
    StructField("RESPONSÁBILIDADE_VENDEDOR_PERCENTUAL", FloatType(), True),
    StructField("DATA_DE_LEVANTAMENTO_EMPRESA", DateType(), True),
    StructField("VALOR_LEVANTADO_EMPRESA", FloatType(), True),
    StructField("DATA_DE_LEVANTAMENTO_PARTE_CONTRÁRIA", DateType(), True),
    StructField("VALOR_LEVANTADO_PARTE_CONTRÁRIA", FloatType(), True),
    StructField("SALDO", FloatType(), True),
    StructField("JUROS", FloatType(), True),
    StructField("INDICE", StringType(), True),
    StructField("ID_1", IntegerType(), True),
    StructField("BANCO_DEPÓSITO_JUDICIAL", StringType(), True),
    StructField("NÚMERO_DA_CONTA_JUDICIAL", StringType(), True),
    StructField("DATA_DA_SOLICITAÇÃO_DO_PAGAMENTO", DateType(), True),
    StructField("DATA_LIMITE_DE_PAGAMENTO", DateType(), True),
    StructField("DATA_DE_VENCIMENTO", DateType(), True),
    StructField("DATA_EFETIVA_DO_PAGAMENTO", DateType(), True),
    StructField("NÚMERO_DO_DOCUMENTO_SAP", StringType(), True),
    StructField("ID_JUDICIAL", StringType(), True),
    StructField("CONTA_JUDICIAL_ID_JUDICIAL", StringType(), True),
    StructField("PROCESSO_ESTADO", StringType(), True),
    StructField("DATA_DO_ALVARÁ", DateType(), True),
    StructField("DATA_DA_CI", DateType(), True),
    StructField("NÚMERO_DA_CI", FloatType(), True),
    StructField("TIPO_DO_SEGURO", StringType(), True),
    StructField("NÚMERO_DA_APÓLICE", StringType(), True),
    StructField("NÚMERO_DO_ENDOSSO", StringType(), True),
    StructField("NOME_DA_SEGURADORA", StringType(), True),
    StructField("CARTEIRA", StringType(), True),
    StructField("FASE", StringType(), True),
    StructField("ESTÁ_NA_BASE_DE_NÃO_IDENTIFICADOS", StringType(), True),
    StructField("INDICAR_EMPRESA", StringType(), True),
    StructField("DATA_REGISTRADO_JUDICIAL", DateType(), True),
    StructField("NÚMERO_DA_GARANTIA_JUDICIAL", StringType(), True),
    StructField("DATA_DA_EMISSÃO_JUDICIAL", DateType(), True),
    StructField("NOME_DA_EMPRESA_JUDICIAL", StringType(), True),
    StructField("DATA_DA_VIGÊNCIA_JUDICIAL", DateType(), True),
    StructField("VALORES_DATA_DA_VIGÊNCIA", StringType(), True),
    StructField("VALORES_DATA_DA_VIGÊNCIA_DA_APÓLICE", StringType(), True),
    StructField("VALORES_DATA_DA_EMISSÃO_DA_APÓLICE", DateType(), True),
    StructField("PROCESSO_VARA_ÓRGÃO", StringType(), True),
    StructField("VALORES_MOTIVOS_DA_PENHORA", StringType(), True),
    StructField("ADVOGADO_RESPONSÁVEL", StringType(), True)
])

# Converter DataFrame Pandas para DataFrame PySpark com o esquema definido
hist_gar = spark.createDataFrame(garantias_1, schema=schema)

# Exibir o esquema do DataFrame Spark resultante
hist_gar.printSchema()

# COMMAND ----------

display(hist_gar)

# COMMAND ----------

# MAGIC %md
# MAGIC ###Transformação Base de Garantias

# COMMAND ----------

# Renomear e tratar campos da Base de Garantias para formato Pyspark
hist_gar = hist_gar.toDF(*[col.replace(' ', '_') for col in hist_gar.columns])
hist_gar = hist_gar.toDF(*[col.replace('(', '') for col in hist_gar.columns])
hist_gar = hist_gar.toDF(*[col.replace(')', '') for col in hist_gar.columns])
hist_gar = hist_gar.toDF(*[col.replace('´', '') for col in hist_gar.columns])

# COMMAND ----------

hist_gar.printSchema()

# COMMAND ----------

from pyspark.sql.functions import col, when, regexp_replace, translate, trim
from pyspark.sql.functions import udf
from pyspark.sql.types import StringType, DoubleType, DecimalType
from pyspark.sql.types import FloatType
from pyspark.sql.window import Window
import pyspark.sql.functions as F

# COMMAND ----------

from pyspark.sql.functions import col


# Defina a lista de tipos de garantia
tipo_garantia = ['ACERTO CONTÁBIL: BLOQUEIO COM TRANSFERÊNCIA BANCÁRIA - DEP. JUDICIAL',
                 'BEM MÓVEL - CÍVEL',
                 'BEM MÓVEL - CÍVEL MASSA',
                 'BEM MÓVEL - REGULATÓRIO',
                 'CARTA DE FIANÇA - CÍVEL',
                 'CARTA DE FIANÇA - CÍVEL MASSA',
                 'CARTA DE FIANÇA - REGULATÓRIO',
                 'DEPÓSITO',
                 'DEPÓSITO GARANTIA DE JUIZO - CÍVEL',
                 'DEPÓSITO JUDICIAL',
                 'DEPÓSITO JUDICIAL - CÍVEL',
                 'DEPÓSITO JUDICIAL - CÍVEL ESTRATÉGICO',
                 'DEPÓSITO JUDICIAL - CÍVEL MASSA',
                 'DEPÓSITO JUDICIAL - REGULATÓRIO',
                 'DEPÓSITO JUDICAL REGULATÓRIO',
                 'DEPÓSITO JUDICIAL TRIBUTÁRIO',
                 'DEPÓSITO RECURSAL - AIRR',
                 'DESPÓSITO RECURSAL - CÍVEL MASSA',
                 'DEPÓSITO RECURSAL - EMBARGOS TST',
                 'DEPÓSITO RECURSAL AIRR',
                 'DEPÓSITO RECURSAL RO',
                 'IMÓVEL - CÍVEL MASSA',
                 'IMÓVEL - REGULATÓRIO',
                 'INATIVO',
                 'LEVANTAMENTO DE CRÉDITO',
                 'LEVANTAMENTO DE CRÉDITO - CÍVEL MASSA',
                 'PENHORA - GARANTIA',
                 'PENHORA - REGULATÓRIO']

# Carregar o DataFrame 'gar' - ajuste o caminho e o formato conforme necessário
#gar = spark.read.table("hist_gar")

# Filtrar DataFrame 'gar' conforme as condições especificadas
gar_a = hist_gar.filter(~col('STATUS_DA_GARANTIA').isin('CANCELADO')) \
           .filter(~col('STATUS_DA_GARANTIA').contains('PAGAMENTO DEVOLVIDO')) \
           .filter(~col('STATUS').isin('REMOVIDO')) \
           .filter(col('TIPO_GARANTIA').isin(tipo_garantia))

# COMMAND ----------

gar_a = gar_a.filter(gar_a['DATA_DE_LEVANTAMENTO_PARTE_CONTRÁRIA'].isNull() == False)
gar_b = gar_a.withColumnRenamed("DATA_DE_LEVANTAMENTO_PARTE_CONTRÁRIA", "DATA_DE_LEVANTAMENTO_PARTE_CONTRARIA")

# COMMAND ----------

# MAGIC %md
# MAGIC

# COMMAND ----------

display(gar_b)

# COMMAND ----------


from pyspark.sql.types import StructType, StructField, StringType, ArrayType

# Dados
d = [
     
    {'01/11/2018':[('01/01/1900','20/11/2018 23:59:59')],
    '01/12/2018':[('21/11/2018','17/12/2018 23:59:59')],
    '01/01/2019':[('18/12/2018','21/01/2019 23:59:59')],
    '01/02/2019':[('22/01/2019','18/02/2019 23:59:59')],
    '01/03/2019':[('19/02/2019','19/03/2019 23:59:59')],
    '01/04/2019':[('20/03/2019','21/04/2019 23:59:59')],
    '01/05/2019':[('22/04/2019','21/05/2019 23:59:59')],
    '01/06/2019':[('22/05/2019','17/06/2019 23:59:59')],
    '01/07/2019':[('18/06/2019','21/07/2019 23:59:59')],
    '01/08/2019':[('22/07/2019','20/08/2019 23:59:59')],
    '01/09/2019':[('21/08/2019','22/09/2019 23:59:59')],
    '01/10/2019':[('23/09/2019','23/10/2019 23:59:59')],
    '01/11/2019':[('24/10/2019','21/11/2019 23:59:59')],
    '01/12/2019':[('22/11/2019','19/12/2020 23:59:59')],  
    '01/01/2020':[('20/12/2019','26/01/2020 23:59:59')],
    '01/02/2020':[('27/01/2020','28/02/2020 23:59:59')],
    '01/03/2020':[('29/02/2020','24/03/2020 23:59:59')],
    '01/04/2020':[('25/03/2020','20/04/2020 23:59:59')],
    '01/05/2020':[('21/04/2020','19/05/2020 23:59:59')],
    '01/06/2020':[('20/05/2020','23/06/2020 23:59:59')],
    '01/07/2020':[('24/06/2020','29/07/2020 23:59:59')],
    '01/08/2020':[('30/07/2020','21/08/2020 23:59:59')],
    '01/09/2020':[('22/08/2020','22/09/2020 23:59:59')],
    '01/10/2020':[('23/09/2020','22/10/2020 23:59:59')],
    '01/11/2020':[('23/10/2020','24/11/2020 23:59:59')],
    '01/12/2020':[('25/11/2020','18/12/2020 23:59:59')],
    '01/01/2021':[('19/12/2020','23/01/2021 23:59:59')],
    '01/02/2021':[('24/01/2021','22/02/2021 23:59:59')],
    '01/03/2021':[('23/02/2021','21/03/2021 23:59:59')],
    '01/04/2021':[('22/03/2021','21/04/2021 23:59:59')],
    '01/05/2021':[('22/04/2021','23/05/2021 23:59:59')],
    '01/06/2021':[('24/05/2021','21/06/2021 23:59:59')],
    '01/07/2021':[('22/06/2021','22/07/2021 23:59:59')],
    '01/08/2021':[('23/07/2021','22/08/2021 23:59:59')],
    '01/10/2022':[('22/09/2022','21/10/2022 23:59:59')],
    '01/09/2022':[('24/08/2022','21/09/2022 23:59:59')],
    '01/11/2022':[('22/10/2022','18/11/2022 23:59:59')],
    '01/12/2022':[('19/11/2022','22/12/2022 23:59:59')],
    '01/01/2023':[('23/12/2022','20/01/2023 23:59:59')],
    '01/02/2023':[('21/01/2023','17/02/2023 23:59:59')],
    '01/03/2023':[('18/02/2023','21/03/2023 23:59:59')],
    '01/04/2023':[('22/03/2023','02/05/2023 23:59:59')],
    '01/05/2023':[('03/05/2023','22/05/2023 23:59:59')],
    '01/06/2023':[('23/05/2023','22/06/2023 23:59:59')],
    '01/07/2023':[('23/06/2023','23/07/2023 23:59:59')],
    '01/08/2023':[('24/07/2023','22/08/2023 23:59:59')],
    '01/09/2023':[('23/08/2023','22/09/2023 23:59:59')],
    '01/10/2023':[('23/09/2023','23/10/2023 23:59:59')],
    '01/11/2023':[('24/10/2023','22/11/2023 23:59:59')],
    '01/12/2023':[('23/11/2023','15/12/2023 23:59:59')],
    '01/01/2024':[('16/12/2023','22/01/2024 23:59:59')],
    '01/02/2024':[('23/01/2024','20/02/2024 23:59:59')]}
]

# COMMAND ----------


from pyspark.sql.functions import col, when, to_date


# Inicializar a coluna 'Data Levantamento F' com NoneType
gar_b = gar_b.withColumn("DATA_LEVANTAMENTO_F", when(col("DATA_DE_LEVANTAMENTO_PARTE_CONTRARIA").isNull(), None).otherwise(col("DATA_DE_LEVANTAMENTO_PARTE_CONTRARIA")))

# Iterar sobre os itens da lista 'd' e aplicar a lógica necessária
for item in d:
    for data, intervalos in item.items():
        for s, e in intervalos:
            # Usar a função withColumn do PySpark para atualizar o DataFrame 'gar_b'
            gar_b = gar_b.withColumn(
                "DATA_LEVANTAMENTO_F",
                when(
                    (col("DATA_DE_LEVANTAMENTO_PARTE_CONTRARIA").between(s, e)),
                    data
                ).otherwise(col("DATA_LEVANTAMENTO_F"))
            )
gar_b = gar_b.withColumn('DATA_LEVANTAMENTO_F', to_date(gar_b['DATA_LEVANTAMENTO_F'], 'yyyy-MM-dd'))
# Exemplo de como mostrar o DataFrame resultante
display(gar_b)




# COMMAND ----------

from pyspark.sql.functions import col
import pyspark.sql.functions as F

group_b = gar_b.orderBy('DATA_LEVANTAMENTO_F').groupBy(['PROCESSO_ID', 'STATUS_DA_GARANTIA']).agg(
    F.sum('VALOR_LEVANTADO_PARTE_CONTRÁRIA').alias('SUM_VALOR_LEVANTADO_PARTE_CONTRÁRIA'),
    F.max('DATA_LEVANTAMENTO_F').alias('DATA_LEVANTAMENTO')
)
group_b = group_b.orderBy('PROCESSO_ID')

# COMMAND ----------

# Importar biblioteca
from pyspark.sql.functions import col

# Renomear coluna
# Importar biblioteca
from pyspark.sql.functions import col

# Importar biblioteca
from pyspark.sql.functions import col

# Renomear coluna
pagamentos_base = hist_pag_a_group.withColumnRenamed("PROCESSO_-_ID", "PROCESSO_ID")

# COMMAND ----------

# MAGIC %md
# MAGIC ###Consolidação Base Regulatório
# MAGIC

# COMMAND ----------

garantias_base = group_b.select([col(c).alias("ID_PROCESSO" if c == "PROCESSO_ID" else c) for c in group_b.columns])

# COMMAND ----------

pagamentos_base  = hist_pag_a_group.select([col(c).alias("ID_PROCESSO" if c == "PROCESSO_-_ID" else c) for c in hist_pag_a_group.columns])

# COMMAND ----------

from pyspark.sql.functions import col

# Renomear coluna
pagamentos_base = hist_pag_a_group.withColumnRenamed("PROCESSO_-_ID", "ID_PROCESSO")

# COMMAND ----------

reg_pag_base = df.join(pagamentos_base, on="ID_PROCESSO", how="left")
reg_pag_gar_final = reg_pag_base.join(garantias_base, on="ID_PROCESSO", how="left")

# COMMAND ----------

from pyspark.sql.functions import col, coalesce, lit

# Calculate the columns 'Total Cond' and 'Total Pagamentos'
reg_pag_gar_final = reg_pag_gar_final.withColumn(
    "Total Cond",
    coalesce(col("Condenacao"), lit(0)) + coalesce(col("Penhora"), lit(0)) + coalesce(col("SUM_VALOR_LEVANTADO_PARTE_CONTRÁRIA"), lit(0))
)
reg_pag_gar_final = reg_pag_gar_final.withColumn(
    "Total Pagamentos",
    coalesce(col("Condenacao"), lit(0)) + coalesce(col("Penhora"), lit(0)) + coalesce(col("SUM_VALOR_LEVANTADO_PARTE_CONTRÁRIA"), lit(0)) + coalesce(col("Acordo"), lit(0))
)

# COMMAND ----------

from pyspark.sql.functions import when, col

# Defina as condições e use a função `when` para atribuir valores com base nas condições
reg_pag_gar_final = reg_pag_gar_final.withColumn('Motivo_Enc_Agrp',
                                     when((col('ENCERRADOS') == 1) & (col('Acordo') > 1), "ACORDO")
                                     .when((col('ENCERRADOS') == 1) & (col('Total Cond') > 1), "CONDENAÇÃO")
                                     .when((col('ENCERRADOS') == 1) & (col('Total Pagamentos') == 0) | (col('Total Pagamentos').isNull()), "SEM ONUS")
                                     .otherwise(""))

# COMMAND ----------

# Remova a coluna 'Total Cond' usando o método drop
drop_columns = ['Total Cond']
reg_pag_gar_final = reg_pag_gar_final.drop(*drop_columns)

# COMMAND ----------

display(reg_pag_gar_final)

# COMMAND ----------

#Importação base de Pagamentos 
gerencial_regulatorio = pd.read_excel("/Volumes/databox/juridico_comum/arquivos/regulatório/bases_gerenciais/BASE_REGULATORIO_E_PROCON_COM_AUDIENCIA-171108775833016980774432746390091.xlsx", sheet_name = "REG_PROCON", header=5)

# COMMAND ----------

colunas_regulatorio = ["(Processo) ID", "Pasta", "Grupo", "Status", "Área do Direito", 
                     "Sub-área do Direito", "Processo - Esteira", "Ação","Órgão ofensor","Processo - Assunto 1 (Regulatório) - Principal","Processo - Assunto 1 (Regulatório) - Secundário","(Processo) Comarca","(Processo) Estado","INDICAÇÃO DE PROCESSO ESTRATÉGICO?"]
gerencial_regulatorio_1 = gerencial_regulatorio[colunas_regulatorio]

# COMMAND ----------

from pyspark.sql.types import StructType, StructField, StringType, IntegerType, DateType

schema = StructType([
    StructField("(Processo) ID", IntegerType(), True),
    StructField("Pasta", StringType(), True),
    StructField("Grupo", StringType(), True),
    StructField("Status", StringType(), True),
    StructField("Área do Direito", StringType(), True),
    StructField("Sub-área do Direito", StringType(), True),
    StructField("Processo - Esteira", StringType(), True),
    StructField("Ação", StringType(), True),
    StructField("Órgão ofensor", StringType(), True),
    StructField("Processo - Assunto 1 (Regulatório) - Principal", StringType(), True),
    StructField("Processo - Assunto 1 (Regulatório) - Secundário", StringType(), True),
    StructField("(Processo) Comarca", StringType(), True),
    StructField("(Processo) Estado", StringType(), True),
    StructField("INDICAÇÃO DE PROCESSO ESTRATÉGICO?", StringType(), True)
])
# Converter DataFrame Pandas para DataFrame PySpark com o esquema definido
gerencial_regulatorio_1 = spark.createDataFrame(gerencial_regulatorio_1, schema=schema)

# Exibir o esquema do DataFrame Spark resultante
gerencial_regulatorio_1.printSchema()


# COMMAND ----------

# Renomear e tratar campos da Base de Garantias para formato Pyspark
gerencial_regulatorio_1 = gerencial_regulatorio_1.toDF(*[col.replace(' ', '_') for col in gerencial_regulatorio_1.columns])
gerencial_regulatorio_1 = gerencial_regulatorio_1.toDF(*[col.replace('(', '') for col in gerencial_regulatorio_1.columns])
gerencial_regulatorio_1 = gerencial_regulatorio_1.toDF(*[col.replace(')', '') for col in gerencial_regulatorio_1.columns])
gerencial_regulatorio_1 = gerencial_regulatorio_1.toDF(*[col.replace('´', '') for col in gerencial_regulatorio_1.columns])

# COMMAND ----------

display(gerencial_regulatorio_1)

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC ###Criação da Base Final do Regulatório

# COMMAND ----------

gerencial_regulatorio_1 = gerencial_regulatorio_1.withColumnRenamed("Processo_ID", "ID_PROCESSO")

# COMMAND ----------

regulatorio_final = reg_pag_gar_final.join(gerencial_regulatorio_1, on="ID_PROCESSO", how="left")

# COMMAND ----------

display(regulatorio_final)

# COMMAND ----------


