# Databricks notebook source
# MAGIC %md
# MAGIC
# MAGIC ##### We'll create managed and external tables using DELTA lake and draw similarities between what we already know and how delta fits into that.

# COMMAND ----------

# MAGIC %run "/Workspace/MaxDev_Practice/FormulaOne/Includes/Configurations"

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC create database if not exists delta_demo
# MAGIC location '/mnt/maxdev00storage/default';

# COMMAND ----------

result_df = spark.read.options(inferSchema = True).json("/mnt/maxdev00storage/raw/2021-03-28/results.json")

# COMMAND ----------

result_df.write.format("delta").mode("overwrite").saveAsTable("delta_demo.results_managed")
result_df.write.format("delta").mode("overwrite").save("/mnt/maxdev00storage/default/results_external")

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC create table if not exists delta_demo.results_external
# MAGIC using delta
# MAGIC location "/mnt/maxdev00storage/default/results_external";

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC select * from delta_demo.results_external;

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC ##### Datalake(parquet file format) and SPARK SQL did not support update and Delete. But Delta Lake does. We'll explore that a little bit here.
# MAGIC
# MAGIC - We can use direct SQL syntax or in python we'll have to use Delta Lake API.

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC update delta_demo.results_managed 
# MAGIC set points = 11-position
# MAGIC where position <=10;

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC select * from delta_demo.results_managed;

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC I used the following link to see how the update and delete works in pyspark using delta lake
# MAGIC
# MAGIC https://docs.delta.io/latest/delta-update.html#update-a-table

# COMMAND ----------

from delta.tables import DeltaTable
from pyspark.sql.functions import *

result_delta = DeltaTable.forPath(spark,"/mnt/maxdev00storage/default/results_managed")


result_delta.update(
  condition = col('position') <= 10,
  set = { 'points': expr('21-position') }
)

# COMMAND ----------

# MAGIC %sql 
# MAGIC  select * from delta_demo.results_managed;

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC delete from delta_demo.results_managed where position >10;

# COMMAND ----------

from delta.tables import DeltaTable
from pyspark.sql.functions import *

result_delta = DeltaTable.forPath(spark,"/mnt/maxdev00storage/default/results_managed")


result_delta.delete(
  condition = col('points').isNull()
)

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC ##### Using Merge to Upsert data into tables using delta lake.

# COMMAND ----------


