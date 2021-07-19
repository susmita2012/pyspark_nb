# Databricks notebook source
from pyspark.sql import SparkSession
# lit allows any hardcoded string / value inside newly created col or existing col
from pyspark.sql.functions import col, lit
spark = SparkSession.builder.appName('Spark DF').getOrCreate()

# COMMAND ----------

df = spark.read.options(header='True', inferSchema='True').csv('/FileStore/tables/StudentData.csv')
df.show()

# COMMAND ----------

df.filter(df.course == 'DB').count()

# COMMAND ----------

df.select('gender', 'age').distinct().count()


# COMMAND ----------

df.select('gender').distinct().show()

# COMMAND ----------

df.show()

# COMMAND ----------

df2 = df.select(df.age, df.gender, df.course).distinct()
df2.count()

# COMMAND ----------

df3 = df.dropDuplicates(['age', 'gender', 'course'])
df3.show()

# COMMAND ----------


