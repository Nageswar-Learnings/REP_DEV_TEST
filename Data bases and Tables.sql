-- Databricks notebook source
create table if not exists test1
(id int, name string);

-- COMMAND ----------

describe extended test1

-- COMMAND ----------

-- MAGIC %fs
-- MAGIC ls 'dbfs:/user/hive/warehouse/test1'

-- COMMAND ----------

CREATE TABLE if not EXISTS External_default_table
(id int,name STRING,salary DOUBLE)
LOCATION 'dbfs:/mnt/demo/external_default';

insert into External_default_table
values (1 ,'hari',1000),(2 ,'CC',3000)

-- COMMAND ----------

DESC HISTO External_default_table

-- COMMAND ----------

drop table External_default_table;
drop table test1;

-- COMMAND ----------

-- MAGIC %fs
-- MAGIC ls 'dbfs:/user/hive/warehouse/test1' 
-- MAGIC

-- COMMAND ----------

select * from external_default_table

-- COMMAND ----------

restore table external_default_table TO VERSION 0

-- COMMAND ----------



-- COMMAND ----------

-- MAGIC %fs
-- MAGIC ls 'dbfs:/mnt/demo/external_default/_delta_log'

-- COMMAND ----------



-- COMMAND ----------


