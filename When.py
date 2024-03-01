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



# COMMAND ----------

data = [(1,'mahe','f',400),(2,'mahe','m',400),(3,'lol','',300)]

schema = ['id','name','gender','salary']
df = spark.createDataFrame(data,schema)
# df.distinct().show()
# df.select('name','salary').distinct().filter('name = "mahe"').show()
# df.dropDuplicates(('name','salary')).display()

df.dtypes[0][1]
df.columns

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

final_df = df3.union(df4).distinct()
final_df.show()

# COMMAND ----------


from pyspark.sql.functions import col
final_df.select('*').show()
final_df.select('id','name').show()
final_df.select(['id','name']).show()
final_df.select(final_df['id']).show()
final_df.select(col('id')).show()
final_df.select(('id')).show()

# COMMAND ----------

df3.show()
df4.show()
join_df = df3.join(df4,['id','name'],'left').show()
join_df = df3.join(df4,df3.id==df4.id).show()
join_df = df3.join(df4,['id']).show()
df3.join(df4, "name", "right_outer").show()
df3.join(df4, "name", "leftanti").show()
data = [(1,'Nag',0),(2,'Rao',1),(3,'Bhuma',)]
schema = ['id','name','ManagerId']


# COMMAND ----------



# COMMAND ----------

from pyspark.sql.functions import col,when
data = [(1,'Nag',0),(2,'Rao',1),(3,'Bhuma',2)]
schema = ['id','name','ManagerId']
df = spark.createDataFrame(data,schema)
df.alias('EmpData').join(df.alias('MangerData'),col('EmpData.id')==col('MangerData.ManagerId'),'full')\
    .select(when (col('EmpData.name').alias('Emp_Name').isNull(),'PRESIDENT').otherwise(col('EmpData.name')).alias('Emp_Name')\
        ,col('MangerData.name').alias('Manager_Name')).show()


# COMMAND ----------

df=spark.read.csv('dbfs:/mnt/blobstorage/test/employees.csv',header=True)

# COMMAND ----------

df.show()

# COMMAND ----------

df.groupBy('DEPARTMENT_ID').pivot('JOB_ID',['IT_PROG']).count().show()

# COMMAND ----------

from pyspark.sql.functions import expr
data = [('IT',2,4),('PAYROLL',3,6),('HR',3,6)]
schema = ['Department','male','female']
df = spark.createDataFrame(data,schema)
df= df.select('Department', expr("stack(2,'m',male,'f',female) as (gender,count)"))
df.show()
df= df.select('Department', expr("length(Department)"))
df.show()



# COMMAND ----------

data = [(1,'nag',None),(None,'kesav','rct'),(2,None,'kdp')]
schema = ['id','name','address']
df = spark.createDataFrame(data,schema)
df.show()
df.fillna(1999,['id']).show()
df.fillna('unknown',['name']).show()
df.na.fill('unknown',['address']).show()

# COMMAND ----------

data = [(1,'nag',None),(None,'kesav','rct'),(2,None,'kdp')]
schema = ['id','name','address']
df = spark.createDataFrame(data,schema)
df.show()
df1=df.collect()
print(df1[0][2])
print(df1[2][2])
print(df[2])

# COMMAND ----------

from pyspark.sql.functions import upper
df.show()
def ConvertUppper(df):
    return df.withColumn('name',upper('name'))
def MutiplyId(df):
    return df.withColumn('id',df.id*2)
df1 = df.transform(ConvertUppper)
df2 = df.transform(MutiplyId)
final_df = df.transform(ConvertUppper).transform(MutiplyId)
df1.show()
df2.show()
final_df.show()
print(df.columns)

# COMMAND ----------

help(df.transform)

# COMMAND ----------

from pyspark.sql.functions import transform
data = [(1,['nag','hari'],None),(None,['kesav,''chinna'],'rct'),(2,['king'],'kdp')]
schema = ['id','name','address']
df = spark.createDataFrame(data,schema)
df.show()
df.printSchema()
df.select('id',transform('name',lambda x : upper(x)).alias('UpperName')).show()

def ConvertUppperf(x):
    return upper(x)

df1 =df.select('id',transform('name',ConvertUppperf).alias('Uppder'))
df1.show()
# > from pyspark.sql.functions import col
#     >>> df = spark.createDataFrame([(1, 1.0), (2, 2.0)], ["int", "float"])
#     >>> def cast_all_to_int(input_df):
#     ...     return input_df.select([col(col_name).cast("int") for col_name in input_df.columns])


# COMMAND ----------

df.show()
df.createOrReplaceTempView('employeeTemp')
df1 = spark.sql("select name from employeeTemp")
df1.show()

# COMMAND ----------

# MAGIC %scala
# MAGIC spark

# COMMAND ----------

df.createOrReplaceGlobalTempView('employeeGobal')

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from employee

# COMMAND ----------

#spark.catalog.listTables('global_temp')
#spark.catalog.dropTempView('employee')
spark.catalog.dropGlobalTempView('global_temp')

# COMMAND ----------



# COMMAND ----------



# COMMAND ----------

from pyspark.sql.functions import * 
df3.withColumn('NewDate',current_timestamp().cast('date')).show()
               #to_date(current_timestamp())).show()

# COMMAND ----------

help(df.transform)

# COMMAND ----------

df.show()
