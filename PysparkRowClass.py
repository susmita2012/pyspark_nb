# Databricks notebook source
from pyspark.sql import SparkSession, Row
spark = SparkSession.builder.master("local[1]").appName("Row class").getOrCreate()

# COMMAND ----------

row=Row("Susmita","1000")
print("Name: " + row[0])
print("salary: " + row[1])

# COMMAND ----------

row = Row(name="Susmita", age=11)
print("Name: " + row.name)
print("salary: " + str(row.age))

# COMMAND ----------

person = Row("name","age")
p1=person("Susmita","41")
p2=person("Puchu","42")
print("P1: " + p1.name + ", " + p1.age)

# COMMAND ----------

data=[
  Row(name="John, Smith", lang=["java","python","C++"], states="US"),
  Row(name="Alice,,Wonderland", lang=["PHP","C","VB"], states="NY"),
  Row(name="Robert, Williams,", lang=["CSharp","Ruby"], states="NV")
]
rdd = spark.sparkContext.parallelize(data)
rdd.collect()

# COMMAND ----------

df = spark.createDataFrame(data)
df.show()

# COMMAND ----------

columns = ["firstname","lastname","country","states"]
data = [
  ("James","Smith","USA","MD"),
  ("Peter","Cat","Japan","Kung")
]

df = spark.createDataFrame(data=data, schema=columns)
df.show()

# COMMAND ----------

df.select("firstname").show()

# COMMAND ----------

df2 = df.select(df.lastname, df.states)
df2.show()

# COMMAND ----------

df3 = df.select(df["firstname"], df["country"])
df3.show()

# COMMAND ----------

df.select(*columns).show()

# COMMAND ----------

df.select([col for col in df.columns]).show()

# COMMAND ----------

df.select("*").show()

# COMMAND ----------


