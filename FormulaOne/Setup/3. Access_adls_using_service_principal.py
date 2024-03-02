# Databricks notebook source
# MAGIC %md
# MAGIC #### Access Azure Data Lake using Service Principal
# MAGIC 1. Register Azure AD Application/Service Principal (Now called Microsoft Entra ID).
# MAGIC 2. Generate a secret/password for the Applicaition.
# MAGIC 3. Set Spark Conig with App/Client ID, Directory/ Tenant Id and Secret.
# MAGIC 4. Assign Role 'Strorage Blob Data Contributor' to the Data Lake.

# COMMAND ----------

# We can get these values from Microsoft Entra ID
client_id = dbutils.secrets.get(scope="MaxDev_formulaOnescope", key="maxdev00client00id")
tenant_id = dbutils.secrets.get(scope="MaxDev_formulaOnescope", key="maxdev00tenant00id")
client_secret = dbutils.secrets.get(scope="MaxDev_formulaOnescope", key="maxdev00client00secret")

# COMMAND ----------

# service_credential = dbutils.secrets.get(scope="<secret-scope>",key="<service-credential-key>")

spark.conf.set("fs.azure.account.auth.type.maxdev00datalake.dfs.core.windows.net", "OAuth")
spark.conf.set("fs.azure.account.oauth.provider.type.maxdev00datalake.dfs.core.windows.net", "org.apache.hadoop.fs.azurebfs.oauth2.ClientCredsTokenProvider")
spark.conf.set("fs.azure.account.oauth2.client.id.maxdev00datalake.dfs.core.windows.net", client_id)
spark.conf.set("fs.azure.account.oauth2.client.secret.maxdev00datalake.dfs.core.windows.net", client_secret)
spark.conf.set("fs.azure.account.oauth2.client.endpoint.maxdev00datalake.dfs.core.windows.net", fr"https://login.microsoftonline.com/{tenant_id}/oauth2/token")

# COMMAND ----------

display(dbutils.fs.ls("abfss://raw@maxdev00storage.dfs.core.windows.net"))
