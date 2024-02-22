# Databricks notebook source
df = spark.createDataFrame([
    ['red', 'banana', 1, 10], ['blue', 'banana', 2, 20], ['red', 'carrot', 3, 30],
    ['blue', 'grape', 4, 40], ['red', 'carrot', 5, 50], ['black', 'carrot', 6, 60],
    ['red', 'banana', 7, 70], ['red', 'grape', 8, 80]], schema=['color', 'fruit', 'id', 'v2'])
df.show()

# COMMAND ----------

df.groupBy('color').sum('v2').show()

# COMMAND ----------

#df.write.csv('dbfs:/mnt/blobstorage/test/writecsv.csv',header=True)
spark.read.csv('dbfs:/mnt/blobstorage/test/writecsv.csv', header=True).show()

# COMMAND ----------

from pyspark.sql import Row

person = Row('1','nag')
person1 = Row('2','king')
data=[person,person1]
df = spark.createDataFrame(data)
df.show()
df.printSchema()
# +---+----+
# | _1|  _2|
# +---+----+
# |  1| nag|
# |  2|king|
# +---+----+

# root
#  |-- _1: string (nullable = true)
#  |-- _2: string (nullable = true)


# COMMAND ----------

person = Row(id='1',name='nag')
person1 = Row(id='2',name='king')
data=[person,person1]
df = spark.createDataFrame(data)
df.show()
df.printSchema()
# +---+----+
# | id|name|
# +---+----+
# |  1| nag|
# |  2|king|
# +---+----+

# root
#  |-- id: string (nullable = true)
#  |-- name: string (nullable = true)


# COMMAND ----------

mainperson=Row('id','name')
person = mainperson('1','nag')
person1 = mainperson('2','king')
print(person.id)

data=[person,person1]

df = spark.createDataFrame(data)
df.show()
df.printSchema()

# 1
# +---+----+
# | id|name|
# +---+----+
# |  1| nag|
# |  2|king|
# +---+----+

# root
#  |-- id: string (nullable = true)
#  |-- name: string (nullable = true)

# COMMAND ----------

from pyspark.sql.functions import col
df.select(df.name).show()
# +----+
# |name|
# +----+
# | nag|
# |king|
# +----+
df.select(df['name']).show()
# +----+
# |name|
# +----+
# | nag|
# |king|
# +----+
df.select(col('name')).show()
# +----+
# |name|
# +----+
# | nag|
# |king|
# +----+

# COMMAND ----------

from pyspark.sql.types import * 
data = [('red', 'banana',('black','blue')),('blue', 'banana',('white','red'))]
StruType=StructType([StructField('hair',StringType()),StructField('eye',StringType())])
schema = StructType([StructField('colour',StringType()),StructField('fruit',StringType()),\
                    StructField('props',StruType)])
df= spark.createDataFrame(data,schema)
df.select(df.props.hair).show()
df.select(df['props']['hair']).show()
df.select(df['props.hair']).show()
df.select(col('props.hair')).show()
#df.select(df.)

# colour:string
# fruit:string
# props:struct
# hair:string
# eye:string
# +----------+
# |props.hair|
# +----------+
# |     black|
# |     white|
# +----------+

# +----------+
# |props.hair|
# +----------+
# |     black|
# |     white|
# +----------+

# +-----+
# | hair|
# +-----+
# |black|
# |white|
# +-----+

# +-----+
# | hair|
# +-----+
# |black|
# |white|
# +-----+

# COMMAND ----------

help(Row)
