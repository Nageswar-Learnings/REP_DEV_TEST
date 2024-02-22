-- Databricks notebook source
select * from emp

-- COMMAND ----------

describe detail emp

-- COMMAND ----------

describe history emp

-- COMMAND ----------

INSERT INTO EMP VALUES (11,'dUMMY',1111)

-- COMMAND ----------

select * from EMP VERSION as of 2


-- COMMAND ----------

delete from emp

-- COMMAND ----------

restore table emp to version as of 3

-- COMMAND ----------

select * from emp

-- COMMAND ----------

describe history emp

-- COMMAND ----------

select * from emp version as of 2

-- COMMAND ----------

restore table emp to version as of 2

-- COMMAND ----------

select * from emp

-- COMMAND ----------


