# Databricks notebook source
help(when)

# COMMAND ----------

from pyspark.sql.functions import when
data = [(1,'mahe','f',200),(2,'kanna','m',400),(3,'lol','',300)]

schema = ['id','name','gender','salary']
df = spark.createDataFrame(data,schema)

# df= df.select(df.id,df.name,when(condition=df.gender=='f',value='female')\
#     .when(condition=df.gender=='m',value='male')\
#         .otherwise('unknown').alias('gender')).show()

df1= df.select(df.id,df.name,when(df.gender=='f','female')\
    .when(df.gender=='m','male')\
        .otherwise('unknown').alias('gender')).show()


# COMMAND ----------

df.show()
df2= df.sort(df.salary.asc())
df2.show()

df3 = df.select(df.name.alias('emp_name'),df.id.alias('emp_id'))
df4 = df.filter(df.name.like('m%')).show()
df3.show()

# COMMAND ----------


df.where((df.salary==200) & (df.name=='mahe')).show()
df.where(("salary=200") and "name='mahe'").show()
df.filter(("salary=200") and "name='mahe'").show()
df.filter(df.name.contains('a')).show()
df.filter(df.gender.isNotNull()).show()
df.filter(df.name.isin('mahe')).show()
df.filter(~df.name.isin('mahe')).show()
df.filter(df.salary.between(100,200)).show()
# df1= df.filter((df.salary==200) & (df.name=='mahe')).collect()
# print('df1')
# df1.show()

# COMMAND ----------

help(df.filter)

df.filter((df.age > 3) & (df.subject == "Physics")).show()
df.filter("age > 3 AND name = 'Bob'").show()
df.filter((df.age == 2) | (df.subject == "Chemistry")).show()
df.filter(df.age.between(2, 5)).show()
df.filter(df.name.contains("i")).show()
df.filter(df.name.isin("Alice", "Bob")).show()
df.filter(df.subject.isin(["Math", "Physics"])).show()
df.filter(~df.name.isin(["Alice", "Charlie"])).show()
df.filter(df.name.isNotNull()).show()
df.filter(df.name.like("Al%")).show()

# COMMAND ----------

data = [(1,'mahe','f',400),(2,'mahe','m',400),(3,'lol','',300)]

schema = ['id','name','gender','salary']
df = spark.createDataFrame(data,schema)
df.distinct().show()
df.select('name','salary').distinct().filter('name = "mahe"').show()
df.dropDuplicates(('name','salary')).display()

# COMMAND ----------

from pyspark.sql.functions import asc,desc
df.show()
df.sort('salary').show()
df.sort(desc('salary')).show()
df.sort(df.salary.asc()).show()
df.sort(df.salary,ascending=False).show()
df.orderBy(["age", "name"], ascending=[False, False]).show()
df.orderBy([1, "name"], ascending=[False, False]).show()


# COMMAND ----------

help(df.orderBy)

# COMMAND ----------

data1 = [(1,'hari',100),(2,'nag',200)]
data2 = [(1,'hari',100),(3,'kesav',300)]
schema= ['id','name','salary']
df3 = spark.createDataFrame(data1,schema)
df4 = spark.createDataFrame(data2,schema)

df3.show()
df4.show()

final_df = df3.union(df4).show()

# COMMAND ----------

help(df3.union)

# COMMAND ----------



# COMMAND ----------

df1
df2

df3= df1.join(df2,df2.Id=df1.Id and df2.Loc=df1.Loc,'')

# COMMAND ----------

from pyspark.sql.functions import *
df3.withColumn('NewDate',current_timestamp().cast('date')).show()
               #to_date(current_timestamp())).show()

# COMMAND ----------


