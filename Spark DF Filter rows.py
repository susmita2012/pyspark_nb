# Databricks notebook source
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, lit

spark = SparkSession.builder.appName('Spark DF Filter rows').getOrCreate()

# COMMAND ----------

df = spark.read.options(header='True').csv('/FileStore/tables/StudentData.csv')
df.show()

# COMMAND ----------

df2 = df.filter(df.course == 'MVC')
df2.show()

# COMMAND ----------

df3 = df.filter(col('course') == 'DB')
df3.show()

# COMMAND ----------

df4 = df.filter( (df.course == 'DB') & (df.marks > 50) )
df4.show()

# COMMAND ----------

courses = ['DB', 'Cloud', 'MVC', 'DSA']

df5 = df.filter( df.course.isin(courses) )
df5.show()

# COMMAND ----------

df6 = df.filter( df.course.startswith('M') )
df6.show()

# COMMAND ----------

df7 = df.filter( df.name.endswith('e') )
df7.show()

# COMMAND ----------

df8 = df.filter( df.name.contains('nna') )
df8.show()

# COMMAND ----------

df9 = df.filter( df.name.like('%oo%') )
df8.show()

# COMMAND ----------


