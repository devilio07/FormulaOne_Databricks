# Databricks notebook source
# MAGIC %run "/Workspace/MaxDev_Practice/FormulaOne/Includes/Configurations"

# COMMAND ----------

# MAGIC %run "/Workspace/MaxDev_Practice/FormulaOne/Includes/common_functions"

# COMMAND ----------

dbutils.widgets.text(name="file_date", defaultValue="2021-03-21")
g_file_date :str = dbutils.widgets.get("file_date")

# COMMAND ----------

# MAGIC %md
# MAGIC #### Formula One Drivers Standings 

# COMMAND ----------

from pyspark.sql.functions import *
from pyspark.sql.window import Window

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC ### Making Driver analysis incremental.
# MAGIC
# MAGIC Should only run for the concerned filedate.

# COMMAND ----------

race_year_list :list[str] = spark.read.format("parquet").options(path = f"{presentation_path}/race_results").load() \
    .filter(col("file_date")==g_file_date) \
    .select(col("race_year")) \
    .distinct() \
    .collect()

# COMMAND ----------

race_year_list :list[str] = [_.race_year for _ in race_year_list]

# COMMAND ----------

race_results = spark.read.format("parquet").options(path = f"{presentation_path}/race_results").load() \
.filter(col("race_year").isin(race_year_list))

# COMMAND ----------

driver_stand_df = race_results.groupBy(col("race_year"),col("driver_name"),col("driver_nationality"),col("team")) \
    .agg(sum(col("points")).alias("total_points"), count(when(col("position")==1, True)).alias("wins"))

# COMMAND ----------

driver_rank_spec = Window.partitionBy(col("race_year")).orderBy(desc(col("total_points")),desc(col("wins")))

driver_standing = driver_stand_df.withColumn("rank", dense_rank().over(driver_rank_spec))

# COMMAND ----------

# driver_standing.write.mode("overwrite").parquet(f"{presentation_path}/driver_standings")

# COMMAND ----------

# MAGIC %md 
# MAGIC
# MAGIC #### NOTE: Following change is being made to make the managed tables using pyspark.
# MAGIC
# MAGIC This essentially creates the same folders in the processed directory, the only difference is that now there datasets will be linked to managed table as well. So, we can read them into dataframes as well as read/query them using Spark SQL.

# COMMAND ----------

# driver_standing.write.mode("overwrite").format("parquet").saveAsTable("f1_presentation.driver_standings")

incremental_load(driver_standing,"race_year","f1_presentation","driver_standings")

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC select * from f1_presentation.driver_standings;
