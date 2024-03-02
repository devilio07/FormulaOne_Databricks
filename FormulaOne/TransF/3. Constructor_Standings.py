# Databricks notebook source
# MAGIC %run "/Workspace/MaxDev_Practice/FormulaOne/Includes/Configurations"

# COMMAND ----------

# MAGIC %run "/Workspace/MaxDev_Practice/FormulaOne/Includes/common_functions"

# COMMAND ----------

dbutils.widgets.text(name="file_date", defaultValue="2021-03-21")
g_file_date :str = dbutils.widgets.get("file_date")

# COMMAND ----------

# MAGIC %md
# MAGIC #### Formula One Constructor Standings 

# COMMAND ----------

from pyspark.sql.functions import *
from pyspark.sql.window import Window

# COMMAND ----------

race_results = spark.read.format("parquet").options(path = f"{presentation_path}/race_results").load() \
.filter(col("file_date")==g_file_date)

# COMMAND ----------

race_year_list = df_to_col(race_results, "race_year")

# COMMAND ----------

race_results = spark.read.format("parquet").options(path = f"{presentation_path}/race_results").load() \
.filter(col("race_year").isin(race_year_list))

# COMMAND ----------

const_stand_df = race_results.groupBy(col("race_year"),col("team")) \
    .agg(sum(col("points")).alias("total_points"), count(when(col("position")==1, True)).alias("wins"))

# COMMAND ----------

const_rank_spec = Window.partitionBy(col("race_year")).orderBy(desc(col("total_points")),desc(col("wins")))

constructor_standing = const_stand_df.withColumn("rank", dense_rank().over(const_rank_spec))

# COMMAND ----------

# constructor_standing.write.mode("overwrite").parquet(f"{presentation_path}/constructor_standings")

# COMMAND ----------

# MAGIC %md 
# MAGIC
# MAGIC #### NOTE: Following change is being made to make the managed tables using pyspark.
# MAGIC
# MAGIC This essentially creates the same folders in the processed directory, the only difference is that now there datasets will be linked to managed table as well. So, we can read them into dataframes as well as read/query them using Spark SQL.

# COMMAND ----------

# constructor_standing.write.mode("overwrite").format("parquet").saveAsTable("f1_presentation.constructor_standings")

incremental_load(constructor_standing,"race_year","f1_presentation","constructor_standings")
