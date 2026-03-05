# Databricks notebook source
# MAGIC %md
# MAGIC # Testes
# MAGIC
# MAGIC Validações simples de funções

# COMMAND ----------

# imports
from pyspark.sql import DataFrame
from pyspark.sql.functions import *
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, DateType, TimestampType, FloatType
from typing import List

# COMMAND ----------

# MAGIC %run "/Workspace/Jurídico/funcao_tratamento_fechamento/common_functions"

# COMMAND ----------

# MAGIC %md
# MAGIC #### Teste conversão de data
# MAGIC

# COMMAND ----------

def convert_to_date_format_americano(df: DataFrame, columns: List[str]) -> DataFrame:
    """
    Converts columns in a PySpark DataFrame to date format.

    Args:
    - df: PySpark DataFrame
    - columns: List of column names to convert to date format

    Returns:
    - PySpark DataFrame with specified columns converted to date format
    """
    for column in columns:
        column_type = dict(df.dtypes)[column]
        try:
            if "timestamp" in column_type:
                df = df.withColumn(column, to_date(col(column), 'yyyy-MM-dd'))
            else:
                df = df.withColumn(column, when(
                                                length(col(column)) == 10,
                                                to_date(col(column), 'dd/MM/yyyy')
                                            ).otherwise(
                                                to_date(col(column), 'M/d/yy')
                                            )
                )
        except:
            print(f"Column '{column}' does not exist. Skipping...")
    return df

# COMMAND ----------

from pyspark.sql import Row

# Sample data
data = [
    Row(id=1, date_time='2009-10-26T00:00:00.000+00:00' , date_str='25/11/1991'),
    Row(id=2, date_time=None , date_str=None),
    Row(id=3, date_time='2022-03-23T00:00:00.000+00:00' , date_str='6/5/22'),
    Row(id=4, date_time='2022-01-12T00:00:00.000+00:00' , date_str='12/31/20'),
    Row(id=5, date_time='2022-01-12T00:00:00.000+00:00' , date_str='22/032022'),
    Row(id=6, date_time='2022-01-12T00:00:00.000+00:00' , date_str='22/03//2022')
]


# Create DataFrame
df1 = spark.createDataFrame(data)

df1 = df1.withColumn('date_time', df1['date_time'].cast(TimestampType()))

# Show DataFrame
df1.show()

df2 = convert_to_date_format_americano(df1,["date_time"])
df3 = convert_to_date_format_americano(df2,["date_str"])
df3.show()

# COMMAND ----------

def convert_to_date_format(df: DataFrame, columns: List[str]) -> DataFrame:
    """
    Converts columns in a PySpark DataFrame to date format.

    Args:
    - df: PySpark DataFrame
    - columns: List of column names to convert to date format

    Returns:
    - PySpark DataFrame with specified columns converted to date format
    """
    for column in columns:
        column_type = dict(df.dtypes)[column]
        try:
            if "timestamp" in column_type:
                df = df.withColumn(column, to_date(col(column), 'yyyy-MM-dd'))
            else:
                df = df.withColumn(column, when(
                                                length(col(column)) == 10,
                                                to_date(col(column), 'dd/MM/yyyy')
                                            ).otherwise(
                                                to_date(col(column), 'd/M/yy')
                                            )
                )
        except:
            print(f"Column '{column}' does not exist. Skipping...")
    return df

# COMMAND ----------

from pyspark.sql import Row

# Sample data
data = [
    Row(id=1, date_time='2009-10-26T00:00:00.000+00:00' , date_str='25/11/1991'),
    Row(id=2, date_time=None , date_str=None),
    Row(id=3, date_time='2022-03-23T00:00:00.000+00:00' , date_str='6/5/22'),
    Row(id=4, date_time='2022-01-12T00:00:00.000+00:00' , date_str='31/12/97'),
    Row(id=5, date_time='2022-01-12T00:00:00.000+00:00' , date_str='22/032022'),
    Row(id=6, date_time='2022-01-12T00:00:00.000+00:00' , date_str='22/03//2022')
]

# Create DataFrame
df1 = spark.createDataFrame(data)

df1 = df1.withColumn('date_time', df1['date_time'].cast(TimestampType()))

# Show DataFrame
df1.show()

df2 = convert_to_date_format(df1,["date_time"])
df3 = convert_to_date_format(df2,["date_str"])
df3.show()

# COMMAND ----------

# MAGIC %md
# MAGIC #### Teste compress

# COMMAND ----------

# data = [
#     Row(id=1, text1="  Hello  " , text2="  World  ", Pagamento = 120),
#     Row(id=2, text1="  How     are you?  " , text2="  Fine, thank you!  ", Pagamento = 120)]
# df = spark.createDataFrame(data)
data = [("  Hello  ", "  World  ", 120), ("  How are you?  ", "  Fine, thank you!  ", '85 ')]
df = spark.createDataFrame(data, ["text1", "text2", "Pagamento COm é %e"])


df.show(truncate=True)
# List of columns to compress
columns_to_compress = ["text1", "text2","Pagamento COm é %e"]

# Compressing columns
df = compress_values(df, columns_to_compress)

# Show the result
df.show(truncate=True)

# COMMAND ----------

from pyspark.sql import Row
from pyspark.sql.types import StringType
from pyspark.sql.functions import *

# Sample data
data = [
    Row(id=1, num=1029.2123 , date_str='25/11/1991'),
    Row(id=2, num=None , date_str=None),
    Row(id=3, num=1029.2123 , date_str='13/05/2022'),
    Row(id=4, num=1029.2123 , date_str='12/31/20'),
    Row(id=5, num=2921.23 , date_str='22/032022')
]

# Create DataFrame
df = spark.createDataFrame(data)

df_replace = df.withColumn('num', round('num', 2))

df_replace = df_replace.withColumn('num', df_replace['num'].cast(StringType()))

df_replace = df.withColumn('num', round('num', 2)) \
    .withColumn('num',regexp_replace('num', ',' , '@')) \
    .withColumn('num', regexp_replace('num', '\.' , ',')) \
    .withColumn('num', regexp_replace('num', "@", "."))

display(df_replace)

# COMMAND ----------

# MAGIC %md
# MAGIC ### Test Merge 

# COMMAND ----------

# Create sample data for source_table
source_data = [
    ("jan", "Alice", 100),
    ("jan", "Bob", 200),
    ("jan", "Charlie", 300)
]

# Create sample data for target_table
target_data = [
    ("fev", "Alice", 50),
    ("fev", "David", 400)
]

target_data2 = [
    ("mar", "Alice", 50),
    ("mar", "David", 400)
]

# Define schema
schema = ["competencia", "name", "value"]
schema2 = ["competencia", "name", "%ótra"]


# Create DataFrames
source_df = spark.createDataFrame(source_data, schema)
target_df = spark.createDataFrame(target_data, schema)
target_df2 = spark.createDataFrame(target_data2, schema2)

# Create temporary views
source_df.createOrReplaceTempView("source_table")
target_df.createOrReplaceTempView("target_table")
target_df2.createOrReplaceTempView("target_table2")


# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT competencia, name, value FROM source_table
# MAGIC UNION ALL 
# MAGIC SELECT competencia, name, value FROM target_table

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT competencia, name, value, null as `%ótra` FROM source_table
# MAGIC UNION ALL
# MAGIC SELECT competencia, name, value, null as `%ótra` FROM target_table
# MAGIC UNION ALL
# MAGIC SELECT competencia, name, null as value, `%ótra` FROM target_table2

# COMMAND ----------

# MAGIC %md
# MAGIC #### Teste SUM

# COMMAND ----------

data = [
    ("mar", "Alice", 50, 90),
    ("mar", "David", 400, 80),
    ("mar", "Lis", 100, None)
]

# Define schema
schema = ["competencia", "name", "value1", "value2"]

df = spark.createDataFrame(data, schema)

df.show()

df = df.withColumn('value_sum', expr("value1 + value2"))

df.show()

df = df.withColumn('value_sum', sum())
