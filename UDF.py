# Databricks notebook source
data = [(1,'asif',1000,10),(2,'khan',2000,20),(3,'kaleja',3000,30)]
schema = ['id','name','salary','bonous']

df = spark.createDataFrame(data,schema)
df.show()

def TotalPay(a,b):
    return a+b
from pyspark.sql.functions import udf
totalPay = udf( lambda a,b : TotalPay(a,b))
df.select('*',totalPay(df.salary,df.bonous).alias('TotalPay')).show()

# COMMAND ----------

from pyspark.sql.types import IntegerType
@udf(returnType=IntegerType())
def TotalPay(a,b):
    return a+b
df.select('*',TotalPay(df.salary,df.bonous)).show()
df.createOrReplaceTempView('emps')
spark.udf.register('totalPay',TotalPay)

# COMMAND ----------

# MAGIC %sql
# MAGIC select * ,totalPay(salary,bonous) as TotalPay from emps

# COMMAND ----------


