# Databricks notebook source
from pyspark import SparkConf, SparkContext

# COMMAND ----------

conf = SparkConf().setAppName("My Pyspark Project")
print(conf)

# COMMAND ----------

sc = SparkContext.getOrCreate(conf=conf)
print(sc)

# COMMAND ----------

rdd = sc.textFile("/FileStore/tables/sample.txt")
rdd.collect()

# COMMAND ----------

def foo(x):
  l = x.split(' ')
  l2 = []
  for s in l:
    l2.append( int(s) + 2 )
  return l2
rdd2 = rdd.map(foo)
rdd2.collect()

# COMMAND ----------

mappedRdd = rdd.map(lambda x: x.split(' '))
mappedRdd.collect()

# COMMAND ----------

flatMappedRdd = rdd.flatMap(lambda x: x.split(' '))
flatMappedRdd.collect()

# COMMAND ----------


