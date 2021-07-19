# Databricks notebook source
from pyspark.sql import SparkSession
spark = SparkSession.builder.appName("Spark DF Columns").getOrCreate()

# COMMAND ----------

from pyspark.sql.types import StructType, StructField, StringType, IntegerType
schema = StructType([
        StructField('age', IntegerType(), True),
        StructField('gender', StringType(), True),
        StructField('name', StringType(), True),
        StructField('course', StringType(), True),
        StructField('roll', StringType(), True),
        StructField('marks', IntegerType(), True),
        StructField('email', StringType(), True),
])



df = spark.read.options(header='True').schema(schema).csv('/FileStore/tables/StudentData.csv')
df.printSchema()
df.show()

# COMMAND ----------

df.select('gender', 'name').show()

# COMMAND ----------

df.select(df.name, df.email).show()

# COMMAND ----------

from pyspark.sql.functions import col
df.select(col('roll'), col('name')).show()

# COMMAND ----------

df.select('*').show()

# COMMAND ----------

df.columns

# COMMAND ----------

df.select('age', 'gender', 'name').show()

# COMMAND ----------

df.select(df.columns[:3]).show()

# COMMAND ----------

df.select(df.columns[3]).show()

# COMMAND ----------

df.select(df.columns).show()

# COMMAND ----------

df.select(df.columns[2:6]).show()

# COMMAND ----------


