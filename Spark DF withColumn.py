# Databricks notebook source
from pyspark.sql import SparkSession
# lit allows any hardcoded string / value inside newly created col or existing col
from pyspark.sql.functions import col, lit
spark = SparkSession.builder.appName('Spark DF withColumn').getOrCreate()

# COMMAND ----------

df = spark.read.options(header='True', inferSchema='True').csv('/FileStore/tables/StudentData.csv')
df.show()

# COMMAND ----------

df2 = df.withColumn('roll', col('roll').cast('String'))
df2.show()

# COMMAND ----------

df2.printSchema()

# COMMAND ----------

df3 = df2.withColumn('marks', col('marks') - 10)
df3.show()

# COMMAND ----------

# based on existing col we want to create a new column
df4 = df3.withColumn('Aggregated marks', col('marks') - 20)
df4.show()

# COMMAND ----------

# lit assigns USA to each row

df5 = df4.withColumn('Country', lit('USA'))
df5.show()

# COMMAND ----------


