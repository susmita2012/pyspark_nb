# Databricks notebook source
# dbutils.fs.rm("/FileStore/tables", True)

# COMMAND ----------

from pyspark.sql import SparkSession
from pyspark.sql.functions import col,lit,explode
import pyspark.sql.functions as f
spark = SparkSession.builder.appName("ETL").getOrCreate()

# COMMAND ----------

# Extract
df = spark.read.text("/FileStore/tables/WordData.txt")
df.show()

# COMMAND ----------

display(df)

# COMMAND ----------

x = "When he encountered maize for the first time he thought it incredibly corny"
x.split(" ")

# COMMAND ----------

# Transformation
df2 = df.withColumn("splittedData", f.split(df.value, " "))
df3 = df2.withColumn("words", explode("splittedData"))
# display(df3)

wordsDF = df3.select("words")
# wordsDF.show()

wc = wordsDF.groupBy("words").count()

display(wc)

# wordsDF.createOrReplaceTempView("wordsTab")
# spark.sql("select words, count('words') as numWords from wordsTab group by words").show()


# COMMAND ----------

# Load

driver = "org.postgresql.Driver"
url = "jdbc:postgresql://database-1.cxo32uwlxxwz.us-east-2.rds.amazonaws.com/"
table = "sus_schema_pyspark.WordCount"
user = "postgres"
password = "isusmitabiswas3880"

# COMMAND ----------

wc.write.format("jdbc").option("driver", driver).option("url", url).option("dbtable", table).mode("append").option("user", user).option("password", password).save()

# COMMAND ----------


