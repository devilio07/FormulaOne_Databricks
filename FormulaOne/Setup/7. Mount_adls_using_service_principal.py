# Databricks notebook source
# MAGIC %md
# MAGIC #### Access Azure Data Lake using Service Principal
# MAGIC 1. Get client_id, tenant_id and client_secret from the key valut.
# MAGIC 2. Set Spark Config with App/Client ID, Directory/ Tenant Id and Secret.
# MAGIC 3. Call file system utility mount to mount the storge.
# MAGIC 4. Access other file system utilities related to mount(list all mount, unmount)

# COMMAND ----------

# We can get these values from Microsoft Entra ID
client_id = dbutils.secrets.get(scope="MaxDev_formulaOnescope", key="maxdev00client00id")
tenant_id = dbutils.secrets.get(scope="MaxDev_formulaOnescope", key="maxdev00tenant00id")
client_secret = dbutils.secrets.get(scope="MaxDev_formulaOnescope", key="maxdev00client00secret")

# COMMAND ----------

configs = {"fs.azure.account.auth.type": "OAuth",
          "fs.azure.account.oauth.provider.type": "org.apache.hadoop.fs.azurebfs.oauth2.ClientCredsTokenProvider",
          "fs.azure.account.oauth2.client.id": client_id,
          "fs.azure.account.oauth2.client.secret": client_secret,
          "fs.azure.account.oauth2.client.endpoint": f"https://login.microsoftonline.com/{tenant_id}/oauth2/token"}

# COMMAND ----------

dbutils.fs.mount(source="abfss://raw@maxdev00storage.dfs.core.windows.net/",
                 mount_point="/mnt/formula_one/raw",
                 extra_configs=configs)

# COMMAND ----------

display(dbutils.fs.ls("/mnt/formula_one/raw"))

# COMMAND ----------

display(spark.read.format('csv').options(header=True, delimeter=",", path="/mnt/formula_one/raw/circuits.csv").load())

# COMMAND ----------

display(dbutils.fs.mounts())

# COMMAND ----------

dbutils.fs.unmount("/mnt/formula_one/raw")

# COMMAND ----------


