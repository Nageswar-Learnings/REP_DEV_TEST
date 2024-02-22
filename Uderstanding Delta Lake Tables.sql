-- Databricks notebook source

CREATE TABLE Emp
(ID INT,NAME STRING,SALARY DOUBLE)

-- COMMAND ----------

INSERT INTO emp VALUES(1,'HARI',1000),(2,'CHANTI',20000)

-- COMMAND ----------

select * from emp

-- COMMAND ----------

drop table emp

-- COMMAND ----------

DESCRIBE DETAIL EMP

-- COMMAND ----------

-- MAGIC %fs
-- MAGIC ls 'dbfs:/user/hive/warehouse/emp/_delta_log/'

-- COMMAND ----------

-- MAGIC %fs
-- MAGIC head 'dbfs:/user/hive/warehouse/emp/_delta_log/00000000000000000001.json'

-- COMMAND ----------

describe history emp

-- COMMAND ----------


