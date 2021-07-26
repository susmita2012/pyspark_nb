# Databricks notebook source
from pyspark.sql import SparkSession
from pyspark.sql.functions import lit, col, sum, avg, max, min, mean, count
spark = SparkSession.builder.appName('group by filtering').getOrCreate()

# COMMAND ----------

df = spark.read.options(header='True', inferSchema='True').csv('/FileStore/tables/StudentData.csv')
df.show()

# COMMAND ----------

df2 = df.filter(df.gender=='Male').groupBy('gender').agg(count('*'))
df2.show()

# COMMAND ----------

df3 = df.filter(df.gender == 'Male').groupBy("course", "gender").agg(count('*').alias('total_enrollments'))
df3.show()

# COMMAND ----------

df4 = df3.where(df3.total_enrollments > 85)
df4.show()

# COMMAND ----------


