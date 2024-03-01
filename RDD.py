# Databricks notebook source
data = [(1,'nag',1000),(2,'rao',2000)]
rdd=spark.sparkContext.parallelize(data)
print(type(data))
print(type(rdd))
df = rdd.toDF(['id','name','salary'])
df.show()
help(rdd)
df1 = spark.createDataFrame(rdd,)
