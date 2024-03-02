# Databricks notebook source
# MAGIC %md
# MAGIC #### Access Azure Data Lake using Cluster Level Auhentication (using access keys)
# MAGIC 1. Set the spark config fs.azure.account.key in the Spark config at cluster level
# MAGIC 2. List files from "default" container
# MAGIC 3. Read data from circuit.csv file

# COMMAND ----------

display(dbutils.fs.ls("abfss://default@maxdev00datalake.dfs.core.windows.net"))|

# COMMAND ----------

from pyspark.sql import *
from pyspark.sql.functions import *
from pyspark.sql.types import *

# COMMAND ----------

df_raw = spark.read.format("csv").options(header=True, inferSchema = True, delimeter=",", path=fr"abfss://default@maxdev00datalake.dfs.core.windows.net/circuits.csv").load()

# COMMAND ----------

df_proc = df_raw.withColumn("alt", 
                  col("alt").cast("double"))

# df_proc2 = df_raw.withColumn("alt", 
#                   col("alt").cast(DoubleType()))

# COMMAND ----------

df_proc.display()

# COMMAND ----------


