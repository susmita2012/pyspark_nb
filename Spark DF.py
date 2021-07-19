# Databricks notebook source
from pyspark.sql import SparkSession

spark = SparkSession.builder.appName("Spark DF").getOrCreate()

# COMMAND ----------

df = spark.read.options(inferSchema='True',header='True').csv('/FileStore/tables/StudentData.csv')
df.printSchema()

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

# COMMAND ----------

df = spark.read.options(header='True').schema(schema).csv('/FileStore/tables/StudentData.csv')
df.show()
df.printSchema()

# COMMAND ----------

from pyspark import SparkContext, SparkConf
conf = SparkConf().setAppName('RDD')
sc = SparkContext.getOrCreate(conf = conf)

rdd = sc.textFile('/FileStore/tables/StudentData.csv')
header = rdd.first()
rdd2 = rdd.filter(lambda x: x!=header)
rdd2.collect()

rdd3 = rdd2.map(lambda x: x.split(','))
rdd4 = rdd3.map(lambda x: [int(x[0]), x[1], x[2], x[3], x[4], int(x[5]), x[6]])
rdd4.collect()

# COMMAND ----------

columns = header.split(',')
rddDf = rdd3.toDF(columns)
rddDf.show()

# COMMAND ----------



# COMMAND ----------

dfRdd = spark.createDataFrame(rdd4, schema=schema)
dfRdd.printSchema()
dfRdd.show()

# COMMAND ----------


