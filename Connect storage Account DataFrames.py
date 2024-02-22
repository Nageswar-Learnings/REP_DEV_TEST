# Databricks notebook source


# COMMAND ----------

spark.conf.set("fs.azure.account.key.rakeshtest123.blob.core.windows.net","OywIBtiWjOAVui/ZfQWMh0LODebcxD3X7OVzKzgZSwjIQLnYJqglMU/W6xgs6sdB2b6nTf6D3fXJ+AStxm1Z3w==")
spark.conf.set("fs.azure.createRemoteFileSystemDuringInitialization", "true")

# COMMAND ----------


dbutils.fs.mount(
source='wasbs://nagcontainer@rakeshtest123.blob.core.windows.net/',
mount_point = '/mnt/blobstorage',
extra_configs = {'fs.azure.account.key.rakeshtest123.blob.core.windows.net':'OywIBtiWjOAVui/ZfQWMh0LODebcxD3X7OVzKzgZSwjIQLnYJqglMU/W6xgs6sdB2b6nTf6D3fXJ+AStxm1Z3w=='}
)
df=spark.read.csv('dbfs:/mnt/blobstorage/test/employees.csv',header=True)

# COMMAND ----------

df=spark.read.csv('dbfs:/mnt/blobstorage/test/employees.csv',header=True)

# COMMAND ----------

df.groupBy('DEPARTMENT_ID')

# COMMAND ----------

df=spark.read.json('dbfs:/mnt/blobstorage/input/JsonDir/employees.json').cache()

# COMMAND ----------

df= spark.read.json('dbfs:/mnt/blobstorage/input/JsonDir/employees.json')

# COMMAND ----------

help(df)

# COMMAND ----------

corrupt_count = df.filter("_corrupt_record is not null").count()

# COMMAND ----------

df.createOrReplaceTempView('EmployeesJs')

# COMMAND ----------

df=spark.read.csv('dbfs:/mnt/blobstorage/test/employees.csv',header=True)

# COMMAND ----------

df.display()

# COMMAND ----------

df.filter(df.LAST_NAME=='Whalen').display()

# COMMAND ----------

df.collect()

# COMMAND ----------

from pyspark import Row
df=spark.createDataFrame([Row(EMPLOYEE_ID='198', FIRST_NAME='Donald', LAST_NAME='OConnell', EMAIL='DOCONNEL', PHONE_NUMBER='650.507.9833', HIRE_DATE='21-JUN-07', JOB_ID='SH_CLERK', SALARY='2600', COMMISSION_PCT=' - ', MANAGER_ID='124', DEPARTMENT_ID='50'),
 Row(EMPLOYEE_ID='199', FIRST_NAME='Douglas', LAST_NAME='Grant', EMAIL='DGRANT', PHONE_NUMBER='650.507.9844', HIRE_DATE='13-JAN-08', JOB_ID='SH_CLERK', SALARY='2600', COMMISSION_PCT=' - ', MANAGER_ID='124', DEPARTMENT_ID='50'),
 Row(EMPLOYEE_ID='200', FIRST_NAME='Jennifer', LAST_NAME='Whalen', EMAIL='JWHALEN', PHONE_NUMBER='515.123.4444', HIRE_DATE='17-SEP-03', JOB_ID='AD_ASST', SALARY='4400', COMMISSION_PCT=' - ', MANAGER_ID='101', DEPARTMENT_ID='10'),
 Row(EMPLOYEE_ID='201', FIRST_NAME='Michael', LAST_NAME='Hartstein', EMAIL='MHARTSTE', PHONE_NUMBER='515.123.5555', HIRE_DATE='17-FEB-04', JOB_ID='MK_MAN', SALARY='13000', COMMISSION_PCT=' - ', MANAGER_ID='100', DEPARTMENT_ID='20'),
 Row(EMPLOYEE_ID='202', FIRST_NAME='Pat', LAST_NAME='Fay', EMAIL='PFAY', PHONE_NUMBER='603.123.6666', HIRE_DATE='17-AUG-05', JOB_ID='MK_REP', SALARY='6000', COMMISSION_PCT=' - ', MANAGER_ID='201', DEPARTMENT_ID='20'),
 Row(EMPLOYEE_ID='203', FIRST_NAME='Susan', LAST_NAME='Mavris', EMAIL='SMAVRIS', PHONE_NUMBER='515.123.7777', HIRE_DATE='07-JUN-02', JOB_ID='HR_REP', SALARY='6500', COMMISSION_PCT=' - ', MANAGER_ID='101', DEPARTMENT_ID='40'),
 Row(EMPLOYEE_ID='204', FIRST_NAME='Hermann', LAST_NAME='Baer', EMAIL='HBAER', PHONE_NUMBER='515.123.8888', HIRE_DATE='07-JUN-02', JOB_ID='PR_REP', SALARY='10000', COMMISSION_PCT=' - ', MANAGER_ID='101', DEPARTMENT_ID='70'),
 Row(EMPLOYEE_ID='205', FIRST_NAME='Shelley', LAST_NAME='Higgins', EMAIL='SHIGGINS', PHONE_NUMBER='515.123.8080', HIRE_DATE='07-JUN-02', JOB_ID='AC_MGR', SALARY='12008', COMMISSION_PCT=' - ', MANAGER_ID='101', DEPARTMENT_ID='110'),
 Row(EMPLOYEE_ID='206', FIRST_NAME='William', LAST_NAME='Gietz', EMAIL='WGIETZ', PHONE_NUMBER='515.123.8181', HIRE_DATE='07-JUN-02', JOB_ID='AC_ACCOUNT', SALARY='8300', COMMISSION_PCT=' - ', MANAGER_ID='205', DEPARTMENT_ID='110'),
 Row(EMPLOYEE_ID='100', FIRST_NAME='Steven', LAST_NAME='King', EMAIL='SKING', PHONE_NUMBER='515.123.4567', HIRE_DATE='17-JUN-03', JOB_ID='AD_PRES', SALARY='24000', COMMISSION_PCT=' - ', MANAGER_ID=' - ', DEPARTMENT_ID='90'),
 Row(EMPLOYEE_ID='101', FIRST_NAME='Neena', LAST_NAME='Kochhar', EMAIL='NKOCHHAR', PHONE_NUMBER='515.123.4568', HIRE_DATE='21-SEP-05', JOB_ID='AD_VP', SALARY='17000', COMMISSION_PCT=' - ', MANAGER_ID='100', DEPARTMENT_ID='90'),
 Row(EMPLOYEE_ID='102', FIRST_NAME='Lex', LAST_NAME='De Haan', EMAIL='LDEHAAN', PHONE_NUMBER='515.123.4569', HIRE_DATE='13-JAN-01', JOB_ID='AD_VP', SALARY='17000', COMMISSION_PCT=' - ', MANAGER_ID='100', DEPARTMENT_ID='90'),
 Row(EMPLOYEE_ID='103', FIRST_NAME='Alexander', LAST_NAME='Hunold', EMAIL='AHUNOLD', PHONE_NUMBER='590.423.4567', HIRE_DATE='03-JAN-06', JOB_ID='IT_PROG', SALARY='9000', COMMISSION_PCT=' - ', MANAGER_ID='102', DEPARTMENT_ID='60'),
 Row(EMPLOYEE_ID='104', FIRST_NAME='Bruce', LAST_NAME='Ernst', EMAIL='BERNST', PHONE_NUMBER='590.423.4568', HIRE_DATE='21-MAY-07', JOB_ID='IT_PROG', SALARY='6000', COMMISSION_PCT=' - ', MANAGER_ID='103', DEPARTMENT_ID='60'),
 Row(EMPLOYEE_ID='105', FIRST_NAME='David', LAST_NAME='Austin', EMAIL='DAUSTIN', PHONE_NUMBER='590.423.4569', HIRE_DATE='25-JUN-05', JOB_ID='IT_PROG', SALARY='4800', COMMISSION_PCT=' - ', MANAGER_ID='103', DEPARTMENT_ID='60'),
 Row(EMPLOYEE_ID='106', FIRST_NAME='Valli', LAST_NAME='Pataballa', EMAIL='VPATABAL', PHONE_NUMBER='590.423.4560', HIRE_DATE='05-FEB-06', JOB_ID='IT_PROG', SALARY='4800', COMMISSION_PCT=' - ', MANAGER_ID='103', DEPARTMENT_ID='60'),
 Row(EMPLOYEE_ID='107', FIRST_NAME='Diana', LAST_NAME='Lorentz', EMAIL='DLORENTZ', PHONE_NUMBER='590.423.5567', HIRE_DATE='07-FEB-07', JOB_ID='IT_PROG', SALARY='4200', COMMISSION_PCT=' - ', MANAGER_ID='103', DEPARTMENT_ID='60'),
 Row(EMPLOYEE_ID='108', FIRST_NAME='Nancy', LAST_NAME='Greenberg', EMAIL='NGREENBE', PHONE_NUMBER='515.124.4569', HIRE_DATE='17-AUG-02', JOB_ID='FI_MGR', SALARY='12008', COMMISSION_PCT=' - ', MANAGER_ID='101', DEPARTMENT_ID='100'),
 Row(EMPLOYEE_ID='109', FIRST_NAME='Daniel', LAST_NAME='Faviet', EMAIL='DFAVIET', PHONE_NUMBER='515.124.4169', HIRE_DATE='16-AUG-02', JOB_ID='FI_ACCOUNT', SALARY='9000', COMMISSION_PCT=' - ', MANAGER_ID='108', DEPARTMENT_ID='100'),
 Row(EMPLOYEE_ID='110', FIRST_NAME='John', LAST_NAME='Chen', EMAIL='JCHEN', PHONE_NUMBER='515.124.4269', HIRE_DATE='28-SEP-05', JOB_ID='FI_ACCOUNT', SALARY='8200', COMMISSION_PCT=' - ', MANAGER_ID='108', DEPARTMENT_ID='100'),
 Row(EMPLOYEE_ID='111', FIRST_NAME='Ismael', LAST_NAME='Sciarra', EMAIL='ISCIARRA', PHONE_NUMBER='515.124.4369', HIRE_DATE='30-SEP-05', JOB_ID='FI_ACCOUNT', SALARY='7700', COMMISSION_PCT=' - ', MANAGER_ID='108', DEPARTMENT_ID='100'),
 Row(EMPLOYEE_ID='112', FIRST_NAME='Jose Manuel', LAST_NAME='Urman', EMAIL='JMURMAN', PHONE_NUMBER='515.124.4469', HIRE_DATE='07-MAR-06', JOB_ID='FI_ACCOUNT', SALARY='7800', COMMISSION_PCT=' - ', MANAGER_ID='108', DEPARTMENT_ID='100'),
 Row(EMPLOYEE_ID='113', FIRST_NAME='Luis', LAST_NAME='Popp', EMAIL='LPOPP', PHONE_NUMBER='515.124.4567', HIRE_DATE='07-DEC-07', JOB_ID='FI_ACCOUNT', SALARY='6900', COMMISSION_PCT=' - ', MANAGER_ID='108', DEPARTMENT_ID='100'),
 Row(EMPLOYEE_ID='114', FIRST_NAME='Den', LAST_NAME='Raphaely', EMAIL='DRAPHEAL', PHONE_NUMBER='515.127.4561', HIRE_DATE='07-DEC-02', JOB_ID='PU_MAN', SALARY='11000', COMMISSION_PCT=' - ', MANAGER_ID='100', DEPARTMENT_ID='30'),
 Row(EMPLOYEE_ID='115', FIRST_NAME='Alexander', LAST_NAME='Khoo', EMAIL='AKHOO', PHONE_NUMBER='515.127.4562', HIRE_DATE='18-MAY-03', JOB_ID='PU_CLERK', SALARY='3100', COMMISSION_PCT=' - ', MANAGER_ID='114', DEPARTMENT_ID='30'),
 Row(EMPLOYEE_ID='116', FIRST_NAME='Shelli', LAST_NAME='Baida', EMAIL='SBAIDA', PHONE_NUMBER='515.127.4563', HIRE_DATE='24-DEC-05', JOB_ID='PU_CLERK', SALARY='2900', COMMISSION_PCT=' - ', MANAGER_ID='114', DEPARTMENT_ID='30'),
 Row(EMPLOYEE_ID='117', FIRST_NAME='Sigal', LAST_NAME='Tobias', EMAIL='STOBIAS', PHONE_NUMBER='515.127.4564', HIRE_DATE='24-JUL-05', JOB_ID='PU_CLERK', SALARY='2800', COMMISSION_PCT=' - ', MANAGER_ID='114', DEPARTMENT_ID='30'),
 Row(EMPLOYEE_ID='118', FIRST_NAME='Guy', LAST_NAME='Himuro', EMAIL='GHIMURO', PHONE_NUMBER='515.127.4565', HIRE_DATE='15-NOV-06', JOB_ID='PU_CLERK', SALARY='2600', COMMISSION_PCT=' - ', MANAGER_ID='114', DEPARTMENT_ID='30'),
 Row(EMPLOYEE_ID='119', FIRST_NAME='Karen', LAST_NAME='Colmenares', EMAIL='KCOLMENA', PHONE_NUMBER='515.127.4566', HIRE_DATE='10-AUG-07', JOB_ID='PU_CLERK', SALARY='2500', COMMISSION_PCT=' - ', MANAGER_ID='114', DEPARTMENT_ID='30'),
 Row(EMPLOYEE_ID='120', FIRST_NAME='Matthew', LAST_NAME='Weiss', EMAIL='MWEISS', PHONE_NUMBER='650.123.1234', HIRE_DATE='18-JUL-04', JOB_ID='ST_MAN', SALARY='8000', COMMISSION_PCT=' - ', MANAGER_ID='100', DEPARTMENT_ID='50'),
 Row(EMPLOYEE_ID='121', FIRST_NAME='Adam', LAST_NAME='Fripp', EMAIL='AFRIPP', PHONE_NUMBER='650.123.2234', HIRE_DATE='10-APR-05', JOB_ID='ST_MAN', SALARY='8200', COMMISSION_PCT=' - ', MANAGER_ID='100', DEPARTMENT_ID='50'),
 Row(EMPLOYEE_ID='122', FIRST_NAME='Payam', LAST_NAME='Kaufling', EMAIL='PKAUFLIN', PHONE_NUMBER='650.123.3234', HIRE_DATE='01-MAY-03', JOB_ID='ST_MAN', SALARY='7900', COMMISSION_PCT=' - ', MANAGER_ID='100', DEPARTMENT_ID='50'),
 Row(EMPLOYEE_ID='123', FIRST_NAME='Shanta', LAST_NAME='Vollman', EMAIL='SVOLLMAN', PHONE_NUMBER='650.123.4234', HIRE_DATE='10-OCT-05', JOB_ID='ST_MAN', SALARY='6500', COMMISSION_PCT=' - ', MANAGER_ID='100', DEPARTMENT_ID='50'),
 Row(EMPLOYEE_ID='124', FIRST_NAME='Kevin', LAST_NAME='Mourgos', EMAIL='KMOURGOS', PHONE_NUMBER='650.123.5234', HIRE_DATE='16-NOV-07', JOB_ID='ST_MAN', SALARY='5800', COMMISSION_PCT=' - ', MANAGER_ID='100', DEPARTMENT_ID='50'),
 Row(EMPLOYEE_ID='125', FIRST_NAME='Julia', LAST_NAME='Nayer', EMAIL='JNAYER', PHONE_NUMBER='650.124.1214', HIRE_DATE='16-JUL-05', JOB_ID='ST_CLERK', SALARY='3200', COMMISSION_PCT=' - ', MANAGER_ID='120', DEPARTMENT_ID='50'),
 Row(EMPLOYEE_ID='126', FIRST_NAME='Irene', LAST_NAME='Mikkilineni', EMAIL='IMIKKILI', PHONE_NUMBER='650.124.1224', HIRE_DATE='28-SEP-06', JOB_ID='ST_CLERK', SALARY='2700', COMMISSION_PCT=' - ', MANAGER_ID='120', DEPARTMENT_ID='50'),
 Row(EMPLOYEE_ID='127', FIRST_NAME='James', LAST_NAME='Landry', EMAIL='JLANDRY', PHONE_NUMBER='650.124.1334', HIRE_DATE='14-JAN-07', JOB_ID='ST_CLERK', SALARY='2400', COMMISSION_PCT=' - ', MANAGER_ID='120', DEPARTMENT_ID='50'),
 Row(EMPLOYEE_ID='128', FIRST_NAME='Steven', LAST_NAME='Markle', EMAIL='SMARKLE', PHONE_NUMBER='650.124.1434', HIRE_DATE='08-MAR-08', JOB_ID='ST_CLERK', SALARY='2200', COMMISSION_PCT=' - ', MANAGER_ID='120', DEPARTMENT_ID='50'),
 Row(EMPLOYEE_ID='129', FIRST_NAME='Laura', LAST_NAME='Bissot', EMAIL='LBISSOT', PHONE_NUMBER='650.124.5234', HIRE_DATE='20-AUG-05', JOB_ID='ST_CLERK', SALARY='3300', COMMISSION_PCT=' - ', MANAGER_ID='121', DEPARTMENT_ID='50'),
 Row(EMPLOYEE_ID='130', FIRST_NAME='Mozhe', LAST_NAME='Atkinson', EMAIL='MATKINSO', PHONE_NUMBER='650.124.6234', HIRE_DATE='30-OCT-05', JOB_ID='ST_CLERK', SALARY='2800', COMMISSION_PCT=' - ', MANAGER_ID='121', DEPARTMENT_ID='50'),
 Row(EMPLOYEE_ID='131', FIRST_NAME='James', LAST_NAME='Marlow', EMAIL='JAMRLOW', PHONE_NUMBER='650.124.7234', HIRE_DATE='16-FEB-05', JOB_ID='ST_CLERK', SALARY='2500', COMMISSION_PCT=' - ', MANAGER_ID='121', DEPARTMENT_ID='50'),
 Row(EMPLOYEE_ID='132', FIRST_NAME='TJ', LAST_NAME='Olson', EMAIL='TJOLSON', PHONE_NUMBER='650.124.8234', HIRE_DATE='10-APR-07', JOB_ID='ST_CLERK', SALARY='2100', COMMISSION_PCT=' - ', MANAGER_ID='121', DEPARTMENT_ID='50'),
 Row(EMPLOYEE_ID='133', FIRST_NAME='Jason', LAST_NAME='Mallin', EMAIL='JMALLIN', PHONE_NUMBER='650.127.1934', HIRE_DATE='14-JUN-04', JOB_ID='ST_CLERK', SALARY='3300', COMMISSION_PCT=' - ', MANAGER_ID='122', DEPARTMENT_ID='50'),
 Row(EMPLOYEE_ID='134', FIRST_NAME='Michael', LAST_NAME='Rogers', EMAIL='MROGERS', PHONE_NUMBER='650.127.1834', HIRE_DATE='26-AUG-06', JOB_ID='ST_CLERK', SALARY='2900', COMMISSION_PCT=' - ', MANAGER_ID='122', DEPARTMENT_ID='50'),
 Row(EMPLOYEE_ID='135', FIRST_NAME='Ki', LAST_NAME='Gee', EMAIL='KGEE', PHONE_NUMBER='650.127.1734', HIRE_DATE='12-DEC-07', JOB_ID='ST_CLERK', SALARY='2400', COMMISSION_PCT=' - ', MANAGER_ID='122', DEPARTMENT_ID='50'),
 Row(EMPLOYEE_ID='136', FIRST_NAME='Hazel', LAST_NAME='Philtanker', EMAIL='HPHILTAN', PHONE_NUMBER='650.127.1634', HIRE_DATE='06-FEB-08', JOB_ID='ST_CLERK', SALARY='2200', COMMISSION_PCT=' - ', MANAGER_ID='122', DEPARTMENT_ID='50'),
 Row(EMPLOYEE_ID='137', FIRST_NAME='Renske', LAST_NAME='Ladwig', EMAIL='RLADWIG', PHONE_NUMBER='650.121.1234', HIRE_DATE='14-JUL-03', JOB_ID='ST_CLERK', SALARY='3600', COMMISSION_PCT=' - ', MANAGER_ID='123', DEPARTMENT_ID='50'),
 Row(EMPLOYEE_ID='138', FIRST_NAME='Stephen', LAST_NAME='Stiles', EMAIL='SSTILES', PHONE_NUMBER='650.121.2034', HIRE_DATE='26-OCT-05', JOB_ID='ST_CLERK', SALARY='3200', COMMISSION_PCT=' - ', MANAGER_ID='123', DEPARTMENT_ID='50'),
 Row(EMPLOYEE_ID='139', FIRST_NAME='John', LAST_NAME='Seo', EMAIL='JSEO', PHONE_NUMBER='650.121.2019', HIRE_DATE='12-FEB-06', JOB_ID='ST_CLERK', SALARY='2700', COMMISSION_PCT=' - ', MANAGER_ID='123', DEPARTMENT_ID='50'),
 Row(EMPLOYEE_ID='140', FIRST_NAME='Joshua', LAST_NAME='Patel', EMAIL='JPATEL', PHONE_NUMBER='650.121.1834', HIRE_DATE='06-APR-06', JOB_ID='ST_CLERK', SALARY='2500', COMMISSION_PCT=' - ', MANAGER_ID='123', DEPARTMENT_ID='50')])

# COMMAND ----------

df=spark.createDataFrame()

# COMMAND ----------

df.show(2,vertical=True)

# COMMAND ----------

spark.conf.set('spark.sql.repl.eagerEval.enabled', True)
df.show()

# COMMAND ----------

l1=[]
for i in df.columns:
    l1.append(i)

print(l1)


# COMMAND ----------

df.select('EMPLOYEE_ID', 'FIRST_NAME', 'LAST_NAME').describe().show()

# COMMAND ----------

df.toPandas()

# COMMAND ----------

df.EMPLOYEE_ID

# COMMAND ----------

df.show(truncate=False,vertical=True)

# COMMAND ----------

from pyspark.sql.functions import *
data = [[1,'nag','10000'],[2,'kutty','50000'],[3,'funny','1000']]
schema = ['id','name','salary']
df=spark.createDataFrame(data,schema)
df.show(truncate=False)
# +---+-----+------+
# |id |name |salary|
# +---+-----+------+
# |1  |nag  |10000 |
# |2  |kutty|50000 |
# |3  |funny|1000  |
# +---+-----+------+


df1 = df.withColumn('salary',col('salary').cast('int'))
df1.printSchema()
# root
#  |-- id: long (nullable = true)
#  |-- name: string (nullable = true)
#  |-- salary: integer (nullable = true)
df1.show(truncate=False)
# +---+-----+------+
# |id |name |salary|
# +---+-----+------+
# |1  |nag  |10000 |
# |2  |kutty|50000 |
# |3  |funny|1000  |
# +---+-----+------+

df2 = df.withColumn('salary',col('salary')*2).filter(df.salary<10000)
df2.show(truncate=False)

# +---+-----+------+
# |id |name |salary|
# +---+-----+------+
# |3  |funny|2000.0|
# +---+-----+------+
df21 = df.withColumn('salary',df.salary.cast('float'))
df21.printSchema()
# root
#  |-- id: long (nullable = true)
#  |-- name: string (nullable = true)
#  |-- salary: float (nullable = true)

df3 = df.withColumn('country',lit('india'))
df3.show(truncate=False)
# +---+-----+------+-------+
# |id |name |salary|country|
# +---+-----+------+-------+
# |1  |nag  |10000 |india  |
# |2  |kutty|50000 |india  |
# |3  |funny|1000  |india  |
# +---+-----+------+-------+
df3 = df.withColumn('copiedcolumn',col('salary'))
df3.show(truncate=False)
# +---+-----+------+------------+
# |id |name |salary|copiedcolumn|
# +---+-----+------+------------+
# |1  |nag  |10000 |10000       |
# |2  |kutty|50000 |50000       |
# |3  |funny|1000  |1000        |
# +---+-----+------+------------+
df4 = df3.withColumnRenamed('copiedcolumn','Newcopiedcolumn')
df4.show(truncate=False)
# +---+-----+------+---------------+
# |id |name |salary|Newcopiedcolumn|
# +---+-----+------+---------------+
# |1  |nag  |10000 |10000          |
# |2  |kutty|50000 |50000          |
# |3  |funny|1000  |1000           |
# +---+-----+------+---------------+
df4 = df4.withColumnsRenamed({'Newcopiedcolumn':'RenamedcopiedColumn','salary':'Rsal'})

df4.show(truncate=False)

# +---+-----+-----+-------------------+
# |id |name |Rsal |RenamedcopiedColumn|
# +---+-----+-----+-------------------+
# |1  |nag  |10000|10000              |
# |2  |kutty|50000|50000              |
# |3  |funny|1000 |1000               |
# +---+-----+-----+-------------------+


# COMMAND ----------

from pyspark.sql.types import *

data = [(1,('nag','bala'),200.0),(2,('singh','kesav'),100.0),(3,('king','kohli'),300.0)]
strucName = StructType([StructField('Fist_Name',StringType()),\
    StructField('Last_Name',StringType())])
schema=StructType([StructField(name='id',dataType = IntegerType()),\
                    StructField(name='name',dataType=strucName),
                        StructField(name='salary',dataType=FloatType())])
df= spark.createDataFrame(data=data,schema=schema)              
df.show(truncate=False)
# +---+--------------+------+
# |id |name          |salary|
# +---+--------------+------+
# |1  |{nag, bala}   |200.0 |
# |2  |{singh, kesav}|100.0 |
# |3  |{king, kohli} |300.0 |
# +---+--------------+------+
df1 = df.withColumns({'First_Name':df.name['Fist_Name'],'Last_Name':df.name['Last_Name']})
df1.show(truncate=False)
# +---+--------------+------+----------+---------+
# |id |name          |salary|First_Name|Last_Name|
# +---+--------------+------+----------+---------+
# |1  |{nag, bala}   |200.0 |nag       |bala     |
# |2  |{singh, kesav}|100.0 |singh     |kesav    |
# |3  |{king, kohli} |300.0 |king      |kohli    |
# +---+--------------+------+----------+---------+

# COMMAND ----------

df.display()

# COMMAND ----------



# COMMAND ----------

help(df.withColumns)

# COMMAND ----------

data=[(1,[2,3],'nag'),(2,[4,5],'king')]
schema = StructType([StructField('id',IntegerType()),StructField('numbers',ArrayType(IntegerType())),\
    StructField('name',StringType())])
df=spark.createDataFrame(data,schema)
df.show(truncate=False)
# +---+-------+----+
# |id |numbers|name|
# +---+-------+----+
# |1  |[2, 3] |nag |
# |2  |[4, 5] |king|
# +---+-------+----+
df1 = df.withColumns({'Number1':df.numbers[0]})
df1.show(truncate=False)
+---+-------+----+-------+
|id |numbers|name|Number1|
+---+-------+----+-------+
|1  |[2, 3] |nag |2      |
|2  |[4, 5] |king|4      |
+---+-------+----+-------+
df2 = df1.withColumn('Combined',array(df1.id,df1.Number1))

df2.show(truncate=False)
+---+-------+----+-------+--------+
|id |numbers|name|Number1|Combined|
+---+-------+----+-------+--------+
|1  |[2, 3] |nag |2      |[1, 2]  |
|2  |[4, 5] |king|4      |[2, 4]  |
+---+-------+----+-------+--------+
df.display()


# COMMAND ----------

from pyspark.sql.functions import explode
data=[(1,['azure','dotnet'],'nag'),(2,['aws','lambda'],'king')]
schema = ['id','skills','name']
df1 =  spark.createDataFrame(data,schema)
df1.show(truncate=False)
# +---+---------------+----+
# |id |skills         |name|
# +---+---------------+----+
# |1  |[azure, dotnet]|nag |
# |2  |[aws, lambda]  |king|
# +---+---------------+----+
df2 = df1.withColumn('skill',explode('skills'))
df2.show(truncate=False)
# +---+---------------+----+------+
# |id |skills         |name|skill |
# +---+---------------+----+------+
# |1  |[azure, dotnet]|nag |azure |
# |1  |[azure, dotnet]|nag |dotnet|
# |2  |[aws, lambda]  |king|aws   |
# |2  |[aws, lambda]  |king|lambda|
# +---+---------------+----+------+

# COMMAND ----------

data=[(1,['azure','dotnet'],'nag'),(2,['aws','lambda'],'king')]
schema = ['id','skills','name']
df = spark.createDataFrame(data,schema)
df.show()
df.display()


# COMMAND ----------

data=[(1,'azure,dotnet','nag'),(2,'aws,lambda','king')]
schema = ['id','skills','name']
df = spark.createDataFrame(data,schema)
df.show()
# +---+------------+----+
# | id|      skills|name|
# +---+------------+----+
# |  1|azure,dotnet| nag|
# |  2|  aws,lambda|king|
# +---+------------+----+

df1 = df.withColumn('SkillArray',split(df.skills,','))
df1.show(truncate=False)
# +---+------------+----+---------------+
# |id |skills      |name|SkillArray     |
# +---+------------+----+---------------+
# |1  |azure,dotnet|nag |[azure, dotnet]|
# |2  |aws,lambda  |king|[aws, lambda]  |
# +---+------------+----+---------------+
df2 = df.withColumn('SkillArray',split(col('skills'),','))
df2.show(truncate=False)
# +---+------------+----+---------------+
# |id |skills      |name|SkillArray     |
# +---+------------+----+---------------+
# |1  |azure,dotnet|nag |[azure, dotnet]|
# |2  |aws,lambda  |king|[aws, lambda]  |
# +---+------------+----+---------------+
df3 = df2.withColumn('HaveAzureSkilled',array_contains(col('SkillArray'),'azure'))
df3.show(truncate=False)
# +---+------------+----+---------------+----------------+
# |id |skills      |name|SkillArray     |HaveAzureSkilled|
# +---+------------+----+---------------+----------------+
# |1  |azure,dotnet|nag |[azure, dotnet]|true            |
# |2  |aws,lambda  |king|[aws, lambda]  |false           |
# +---+------------+----+---------------+----------------+
df3.display(truncate=False)


# COMMAND ----------

data = [('nag',{'hair':'blak','eyes':'brown'}),('nag',{'hair':'white','eyes':'black'})]
schema = StructType([StructField('Name',StringType()),StructField('Properties',MapType(StringType(),StringType()))])
df= spark.createDataFrame(data,schema)
df.printSchema()
df.show(truncate=False)
df.display()

# df:pyspark.sql.dataframe.DataFrame
# Name:string
# Properties:map
# key:string
# value:string
# root
#  |-- Name: string (nullable = true)
#  |-- Properties: map (nullable = true)
#  |    |-- key: string
#  |    |-- value: string (valueContainsNull = true)

# +----+------------------------------+
# |Name|Properties                    |
# +----+------------------------------+
# |nag |{eyes -> brown, hair -> blak} |
# |nag |{eyes -> black, hair -> white}|
# +----+------------------------------+

# COMMAND ----------

df2 = df.withColumn('hair',df.Properties['hair'])
#OR
df2 = df.withColumn('hair',df.Properties.hair)
# +----+------------------------------+-----+
# |Name|Properties                    |hair |
# +----+------------------------------+-----+
# |nag |{eyes -> brown, hair -> blak} |blak |
# |nag |{eyes -> black, hair -> white}|white|
# +----+------------------------------+-----+
df2.show(truncate=False)
#OR
df2 = df.withColumn('eyes',df.Properties.getItem("eyes"))
# +----+------------------------------+-----+
# |Name|Properties                    |eyes |
# +----+------------------------------+-----+
# |nag |{eyes -> brown, hair -> blak} |brown|
# |nag |{eyes -> black, hair -> white}|black|
# +----+------------------------------+-----+
df2.show(truncate=False)
df3 = df.select('Name','Properties',explode(df.Properties))
# +----+------------------------------+----+-----+
# |Name|Properties                    |key |value|
# +----+------------------------------+----+-----+
# |nag |{eyes -> brown, hair -> blak} |eyes|brown|
# |nag |{eyes -> brown, hair -> blak} |hair|blak |
# |nag |{eyes -> black, hair -> white}|eyes|black|
# |nag |{eyes -> black, hair -> white}|hair|white|
# +----+------------------------------+----+-----+
# df3.show(truncate=False)
df3 = df.select('Name','Properties',map_keys(df.Properties))
# +----+------------------------------+--------------------+
# |Name|Properties                    |map_keys(Properties)|
# +----+------------------------------+--------------------+
# |nag |{eyes -> brown, hair -> blak} |[eyes, hair]        |
# |nag |{eyes -> black, hair -> white}|[eyes, hair]        |
# +----+------------------------------+--------------------+
df3 = df.select('Name','Properties',map_keys(df.Properties))
# +----+------------------------------+--------------------+
# |Name|Properties                    |map_keys(Properties)|
# +----+------------------------------+--------------------+
# |nag |{eyes -> brown, hair -> blak} |[eyes, hair]        |
# |nag |{eyes -> black, hair -> white}|[eyes, hair]        |
# +----+------------------------------+--------------------+
df3.show(truncate=False)
df2.display()
df3 = df.withColumn('eyes',getValue.(df.Properties.))
