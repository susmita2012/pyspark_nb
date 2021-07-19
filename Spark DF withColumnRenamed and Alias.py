# Databricks notebook source
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, lit

spark = SparkSession.builder.appName('Spark DF withColumnRenamed and Alias').getOrCreate()

# COMMAND ----------

df = spark.read.options(header='True', inferSchema='True').csv('/FileStore/tables/StudentData.csv')
# df.show()
df.select(col("name").alias('Full Name')).show()

# COMMAND ----------

df2 = df.withColumnRenamed('gender', 'sex').withColumnRenamed('roll', '#ID')
df2.show()

# COMMAND ----------


