# Databricks notebook source
# MAGIC %md
# MAGIC #### Access Azure Data Lake using SAS Token
# MAGIC 1. Set the spark config for SAS Token.
# MAGIC 2. List files from "default" container.
# MAGIC 3. Read data from circuit.csv file.

# COMMAND ----------

spark.conf.set("fs.azure.account.auth.type.maxdev00datalake.dfs.core.windows.net", "SAS")
spark.conf.set("fs.azure.sas.token.provider.type.maxdev00datalake.dfs.core.windows.net", "org.apache.hadoop.fs.azurebfs.sas.FixedSASTokenProvider")
spark.conf.set("fs.azure.sas.fixed.token.maxdev00datalake.dfs.core.windows.net", "sv=2023-01-03&st=2024-01-20T07%3A05%3A03Z&se=2024-01-21T07%3A05%3A03Z&sr=c&sp=rl&sig=ckX%2Fxt5PJh46zjrlGmeNBmxzughS%2BmGtnHYRj9Vn%2FJg%3D")

# COMMAND ----------

display(dbutils.fs.ls("abfss://default@maxdev00datalake.dfs.core.windows.net"))

# COMMAND ----------

from pyspark.sql import *
from pyspark.sql.functions import *
from pyspark.sql.types import *

# COMMAND ----------

df_raw = spark.read.format("csv").options(header=True, delimeter=",", path=fr"abfss://default@maxdev00datalake.dfs.core.windows.net/circuits.csv").load()

# COMMAND ----------

df_raw.display()

# COMMAND ----------

df_proc = df_raw.withColumn("alt", 
                  col("alt").cast("double"))

# df_proc2 = df_raw.withColumn("alt", 
#                   col("alt").cast(DoubleType()))

# COMMAND ----------

df_proc.printSchema()

# COMMAND ----------


