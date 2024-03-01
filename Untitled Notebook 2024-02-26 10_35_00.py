# Databricks notebook source
# MAGIC %scala
# MAGIC spark

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from global_temp.employee

# COMMAND ----------

#spark.catalog.currentDatabase()
#spark.catalog.listTables('global_temp')
spark.catalog.listTables()

# COMMAND ----------


