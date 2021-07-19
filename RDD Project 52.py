# Databricks notebook source
from pyspark import SparkConf, SparkContext
conf = SparkConf().setAppName("47 48")
sc = SparkContext.getOrCreate(conf=conf)

rdd = sc.textFile('/FileStore/tables/StudentData.csv')
header = rdd.first()
rdd=rdd.filter(lambda x: x != header)

rdd2 = rdd.map(lambda x: ( x.split(',')[3], (int( x.split(',')[5] ),1) ))
rdd2.collect() 


# COMMAND ----------

rdd3 = rdd2.reduceByKey( lambda x,y: ( (x[0] + y[0]), (x[1] + y[1])))
rdd3.collect() 

# COMMAND ----------

rdd4 = rdd3.map(lambda x: (x[0], x[1][0] / x[1][1]))
rdd4.collect()

# COMMAND ----------


