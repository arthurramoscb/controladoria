# Databricks notebook source
from pyspark.sql import Row

# COMMAND ----------

# MAGIC %run "/Workspace/Jurídico/funcao_tratamento_fechamento/common_functions"

# COMMAND ----------

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

# COMMAND ----------

df2 = df1.withColumn('ANO_YY', substring("date_str", -2, 2))
# df2.show()
df3 = df2.withColumn('ANO_YY_int', col('ANO_YY').cast("int"))
df3.show()


df4 = df3.withColumn('ANO_YYYY_exp',

# COMMAND ----------

df3.createOrReplaceTempView('df3')

# COMMAND ----------

# %sql
# select *, 
# case when ANO_YY_int > 50 then concat('19', ANO_YY)
#   when 

#  end as concat1
# from df3

# COMMAND ----------

# spark.conf.set("spark.sql.legacy.timeParserPolicy", "CORRECTED")
spark.sql("select to_date('2/10/60', 'M/d/yy') as date").show()

# COMMAND ----------

df2 = convert_to_date_format(df1, ['date_str'])
df2.display()
