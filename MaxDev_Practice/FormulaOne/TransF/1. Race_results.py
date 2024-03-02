# Databricks notebook source
# MAGIC %run "/Workspace/MaxDev_Practice/FormulaOne/Includes/Configurations"

# COMMAND ----------

# MAGIC %run "/Workspace/MaxDev_Practice/FormulaOne/Includes/common_functions"

# COMMAND ----------

dbutils.widgets.text(name="file_date", defaultValue="2021-03-21")
g_file_date :str = dbutils.widgets.get("file_date")

# COMMAND ----------

# MAGIC %md
# MAGIC #### Importing all the Files that are required for the final dataset - Race Results

# COMMAND ----------

from pyspark.sql.functions import *

# COMMAND ----------

races_df = spark.read.parquet(f"{processed_path}/races") \
    .select(col("race_id"), col("circuit_id"),col("race_year"),col("name").alias("race_name"), col("race_timestamp"))

# COMMAND ----------

circuits_df = spark.read.parquet(f"{processed_path}/circuits") \
    .select(col("circuit_id"), col("location").alias("circuit_location"))

# COMMAND ----------

drivers_df = spark.read.parquet(f"{processed_path}/drivers") \
    .select(col("driver_id"), col("name").alias("driver_name"), col("number").alias("driver_number"), col('nationality').alias("driver_nationality"))

# COMMAND ----------

const_df= spark.read.parquet(f"{processed_path}/constructors") \
    .select(col("constructor_id"), col("name").alias("team"))

# COMMAND ----------

results_df = spark.read.parquet(f"{processed_path}/results") \
    .select(col("race_id").alias("result_race_id"),col("driver_id"),col("constructor_id"), col("grid"), col("fastest_lap"),col("time").alias("race_time"), col("points"), col("position"), col("file_date").alias("result_file_date")) \
    .filter(col("file_date") == g_file_date)

# COMMAND ----------

display(results_df.show())

# COMMAND ----------

race_cir_df = races_df.join(circuits_df, races_df.circuit_id==circuits_df.circuit_id, "inner") \
    .select(col("race_id"), col("race_year"), col("race_name"),col("race_timestamp").alias("race_date"), col("circuit_location"))

# COMMAND ----------

results_data_df = results_df.join(race_cir_df, results_df.result_race_id==race_cir_df.race_id,"inner") \
    .join(drivers_df, results_df.driver_id==drivers_df.driver_id, "inner") \
    .join(const_df, results_df.constructor_id==const_df.constructor_id,"inner") \
        .select(col("race_id"),col("race_year"),col("race_name"), col("race_date"), "circuit_location", col("driver_name"),col("driver_number"), col("driver_nationality"),col("team"),col("grid"),col("fastest_lap"),col("race_time"),col("points"), col("position"), col("result_file_date").alias("file_date")) \
        .withColumn("created_date",current_timestamp())

# COMMAND ----------

#  display(results_data_df.filter((col("race_name")=="Abu Dhabi Grand Prix") & (col("race_year")==2020)).orderBy(col("points").desc()))

# COMMAND ----------

# results_data_df.write.mode("overwrite").format("parquet").options(path = f"{presentation_path}/race_results").save()

# COMMAND ----------

# MAGIC %md 
# MAGIC
# MAGIC #### NOTE: Following change is being made to make the managed tables using pyspark.
# MAGIC
# MAGIC This essentially creates the same folders in the processed directory, the only difference is that now there datasets will be linked to managed table as well. So, we can read them into dataframes as well as read/query them using Spark SQL.

# COMMAND ----------

incremental_load(results_data_df,"race_id","f1_presentation","race_results")
