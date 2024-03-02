# Databricks notebook source
# MAGIC %md
# MAGIC
# MAGIC ### Using Databricks Secrets utilities to get the access_key value from the keyvault and scope

# COMMAND ----------

dbutils.secrets.help()

# COMMAND ----------

dbutils.secrets.listScopes()

# COMMAND ----------

dbutils.secrets.list(scope = 'MaxDev_Scope')

# COMMAND ----------

access_key = dbutils.secrets.get(scope = 'MaxDev_Scope', key = 'MaxDev-AccessKey')

# COMMAND ----------

spark.conf.set("fs.azure.account.key.maxdev00datalake.dfs.core.windows.net",access_key)

# COMMAND ----------

display(dbutils.fs.ls("abfss://default@maxdev00datalake.dfs.core.windows.net"))

# COMMAND ----------

spark.sparkContext.getConf().getAll()

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC ### Using Databricks Secrets utilities to get the SAS token value from the keyvault and scope

# COMMAND ----------

SAS_token = dbutils.secrets.get(scope='MaxDev_Scope', key = 'MaxDev-SAStoken')

# COMMAND ----------

spark.conf.set("fs.azure.account.auth.type.maxdev00datalake.dfs.core.windows.net", "SAS")
spark.conf.set("fs.azure.sas.token.provider.type.maxdev00datalake.dfs.core.windows.net", "org.apache.hadoop.fs.azurebfs.sas.FixedSASTokenProvider")
spark.conf.set("fs.azure.sas.fixed.token.maxdev00datalake.dfs.core.windows.net", SAS_token)

# COMMAND ----------

display(dbutils.fs.ls("abfss://default@maxdev00datalake.dfs.core.windows.net"))

# COMMAND ----------


