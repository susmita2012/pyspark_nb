# Databricks notebook source
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, lit
spark = SparkSession.builder.appName('Quiz 1').getOrCreate()

# COMMAND ----------

df = spark.read.options(header='True').csv('/FileStore/tables/StudentData.csv')
df.show()

# COMMAND ----------

df2 = df.withColumn("total_marks", lit('120'))
df2.show()

# COMMAND ----------

df3 = df2.withColumn('average', (col('marks') / col('total_marks')) * 100 )
df3.show()

# COMMAND ----------

df4 = df3.filter( (df3.average>80 ) & (df3.course == 'OOP') )
df4.show()

# COMMAND ----------

df5 = df3.filter( (df3.average>60 ) & (df3.course == 'Cloud') )
df5.show()

# COMMAND ----------

df44 = df4.select(col('name'), col('marks'))
df44.show()

# COMMAND ----------


