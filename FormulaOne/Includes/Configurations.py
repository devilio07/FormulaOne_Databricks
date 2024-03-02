# Databricks notebook source
# def mount_storage(storage_account :str, container :str) -> None:
#     client_id :str = dbutils.secrets.get(scope="MaxDev_formulaOnescope", key="maxdev00client00id")
#     tenant_id :str  = dbutils.secrets.get(scope="MaxDev_formulaOnescope", key="maxdev00tenant00id")
#     client_secret :str = dbutils.secrets.get(scope="MaxDev_formulaOnescope", key="maxdev00client00secret")

#     configs :dict(str,str) = {"fs.azure.account.auth.type": "OAuth",
#           "fs.azure.account.oauth.provider.type": "org.apache.hadoop.fs.azurebfs.oauth2.ClientCredsTokenProvider",
#           "fs.azure.account.oauth2.client.id": client_id,
#           "fs.azure.account.oauth2.client.secret": client_secret,
#           "fs.azure.account.oauth2.client.endpoint": f"https://login.microsoftonline.com/{tenant_id}/oauth2/token"}
    
#     if any(mount.mountPoint==fr"/mnt/{storage_account}/{container}" for mount in dbutils.fs.mounts()):
#         dbutils.fs.unmount(fr"/mnt/{storage_account}/{container}")

#     dbutils.fs.mount(source=fr"abfss://{container}@{storage_account}.dfs.core.windows.net/",
#                  mount_point=fr"/mnt/{storage_account}/{container}",
#                  extra_configs=configs)
    
#     return fr"/mnt/{storage_account}/{container}"

# COMMAND ----------

# raw_path :str = mount_storage("maxdev00storage","raw")
# processed_path :str = mount_storage("maxdev00storage","processed")
# presentation_path :str = mount_storage("maxdev00storage","presentation")
# default_path :str = mount_storage("maxdev00storage","default")

# COMMAND ----------

raw_path :str = "/mnt/maxdev00storage/raw/"
processed_path :str = "/mnt/maxdev00storage/processed/"
presentation_path :str = "/mnt/maxdev00storage/presentation/"
default_path :str = "/mnt/maxdev00storage/default"

# COMMAND ----------


