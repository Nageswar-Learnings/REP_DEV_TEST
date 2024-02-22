# Databricks notebook source
dbutils.secrets.listScopes()

# COMMAND ----------

dbutils.secrets.getBytes('testScope21','azurekeyvaulttestnag')

# COMMAND ----------

spark.conf.set('fs.azure.account.')
