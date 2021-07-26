# Databricks notebook source
from pyspark.sql import SparkSession
from pyspark.sql.functions import col,lit,udf,count,max,min,avg
from pyspark.sql.types import IntegerType
spark = SparkSession.builder.appName("Mini project").getOrCreate()

# COMMAND ----------

df = spark.read.options(header="True", inferSchema="True").csv("/FileStore/tables/OfficeDataProject.csv")
df.show()

# COMMAND ----------

df.createOrReplaceTempView("officeDataTable")
spark.sql("select * from officeDataTable").show()

# COMMAND ----------

# print total number of employees in the company
# using SQL
spark.sql("SELECT count('*') as totalNumberOfEmployee from officeDataTable").show()

# using DF API
# df.count()

# COMMAND ----------

# total number of dept in the company
# spark.sql("select COUNT(DISTINCT department) as dept from officeDataTable").show()

# spark.sql("SELECT department, count(department) as countDept from officeDataTable group by department").show()

# df.select("department").dropDuplicates(["department"]).show()
# df.select("department").dropDuplicates(["department"]).count()




# COMMAND ----------

# dept names of the company
# spark.sql("select DISTINCT department as dept from officeDataTable").show()

# df.select("department").distinct().show()

df.select("department").dropDuplicates(["department"]).show()

# COMMAND ----------

#4 total number of employees in each department
spark.sql("select department, count('employee_id') as empNum from officeDataTable group by department").show()

df.groupBy("department").count().show()

# COMMAND ----------

#4 total number of employees in each state
spark.sql("select state, count('employee_id') as numEmp from officeDataTable group by state").show()

df.groupBy("state").count().show()

# COMMAND ----------

#6 total number of employees in each state, department
# spark.sql("select state, department, count('employee_id') from officeDataTable group by state, department").show()

df.groupBy("state", "department").agg(count('*').alias('totalNumRec')).show()

# COMMAND ----------

#7
spark.sql("select department, max(salary), min(salary) from officeDataTable group by department").show()

df.groupBy("department").agg(max("salary"), min("salary")).show()

# COMMAND ----------

#8 
spark.sql("select employee_id, employee_name from officeDataTable where state='NY' and department='Finance' and bonus > (select avg(bonus) from officeDataTable where state='NY')").show()

avgBonus=df.filter(df.state == 'NY').groupBy('state').agg(avg('bonus').alias("avg_bonus")).select("avg_bonus").collect()[0]["avg_bonus"]

df.filter((df.state == 'NY') & (df.department == 'Finance') & (df.bonus > avgBonus)).show()

# COMMAND ----------

#9
def updateSalary(age, salary):
  if age > 45:
    return salary + 500
  return salary

salaryUDF = udf(lambda x,y: updateSalary(x,y), IntegerType())

df.withColumn("new_salary", salaryUDF(df.age, df.salary)).show()
  

# COMMAND ----------

# 10
df.filter( col("age") > 45 ).write.csv("/FileStore/tables/output_45")

# COMMAND ----------


