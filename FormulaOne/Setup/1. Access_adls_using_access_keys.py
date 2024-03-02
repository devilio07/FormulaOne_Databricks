# Databricks notebook source
# MAGIC %md
# MAGIC #### Access Azure Data Lake using access keys
# MAGIC 1. Set the spark config fs.azure.account.key
# MAGIC 2. List files from "default" container
# MAGIC 3. Read data from circuit.csv file

# COMMAND ----------

spark.conf.set("fs.azure.account.key.maxdev00datalake.dfs.core.windows.net",
               "tidjzTRS9I0j3rQCrjL9GL1Zc4yvtHBoDtglGC054Kw69dFgXbUzaAp1NmtY19bQsDhqzYAeCXG7+ASt7vVDjg==")

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


