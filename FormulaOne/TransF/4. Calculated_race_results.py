# Databricks notebook source
# %sql
# drop table if exists f1_presentation.calculated_race_results ;
# create table if not exists f1_presentation.calculated_race_results 
# using parquet
# as (select
# d.race_year,
# c.name as team,
# b.name as driver_name,
# a.position,
# a.points,
# 11-a.position as calculated_points
# from f1_processed.results as a
# inner join f1_processed.drivers as b on a.driver_id = b.driver_id
# inner join f1_processed.constructors as c on a.constructor_id = c.constructor_id
# inner join f1_processed.races as d on a.race_id = d.race_id
# where a.position <=10
# and a.file_date = '2021-03-28');

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC #### Turning the above SQL code into Pyspark code, so that it can be made incremental and gives more control over it.

# COMMAND ----------

# MAGIC %md
# MAGIC ### Update 
# MAGIC > We are going to convert this back to SQL based (with a mix of pyspark) table creation.
# MAGIC >> We will incorporate delta lake merge capabilities.

# COMMAND ----------

# MAGIC %run "../Includes/Configurations"

# COMMAND ----------

# MAGIC %run "../Includes/common_functions"

# COMMAND ----------

dbutils.widgets.text(name="file_date", defaultValue="2021-03-21")
g_file_date :str = dbutils.widgets.get("file_date")

# COMMAND ----------


spark.sql(
        fr""" create table if not exists f1_presentation.calculated_race_results (
        race_year int,
        team_name string,
        driver_id int,
        driver_name string,
        race_id int,
        position int,
        points int,
        calculated_points int,
        created_date timestamp,
        updated_date timestamp
        )
        using delta """
)

# COMMAND ----------

spark.sql(fr"""
create or replace temp view  race_result_update as
select
d.race_year,
c.name as team_name,
b.driver_id,
b.name as driver_name,
d.race_id,
a.position,
a.points,
11-a.position as calculated_points
from f1_processed.results as a
inner join f1_processed.drivers as b on a.driver_id = b.driver_id
inner join f1_processed.constructors as c on a.constructor_id = c.constructor_id
inner join f1_processed.races as d on a.race_id = d.race_id
where a.position <=10
and a.file_date = '{g_file_date}'
"""
)

# COMMAND ----------

spark.sql(fr"""
merge into f1_presentation.calculated_race_results old
using race_result_update upd
on (old.race_id=upd.race_id and old.driver_id=upd.driver_id)
when matched then
update set  old.position = upd.position,
            old.points = upd.points,
            old.calculated_points = upd.calculated_points,
            old.updated_date = current_timestamp
when not matched then 
insert (race_year,team_name,driver_id,driver_name,race_id,position,points,calculated_points,created_date) 
values(race_year,team_name,driver_id,driver_name,race_id,position,points,calculated_points, current_timestamp );
""")


# COMMAND ----------

# from pyspark.sql.functions import *
# from pyspark.sql.window import Window

# COMMAND ----------

# races_df = spark.read.parquet(f"{processed_path}/races") \
#     .select(col("race_id"), col("circuit_id"),col("race_year"),col("name").alias("race_name"), col("race_timestamp"))

# COMMAND ----------

# circuits_df = spark.read.parquet(f"{processed_path}/circuits") \
#     .select(col("circuit_id"), col("location").alias("circuit_location"))

# COMMAND ----------

# drivers_df = spark.read.parquet(f"{processed_path}/drivers") \
#     .select(col("driver_id"), col("name").alias("driver_name"), col("number").alias("driver_number"), col('nationality').alias("driver_nationality"))

# COMMAND ----------

# const_df= spark.read.parquet(f"{processed_path}/constructors") \
#     .select(col("constructor_id"), col("name").alias("team"))

# COMMAND ----------

# results_df = spark.read.parquet(f"{processed_path}/results") \
#     .select(col("race_id").alias("result_race_id"),col("driver_id"),col("constructor_id"), col("grid"), col("fastest_lap"),col("time").alias("race_time"), col("points"), col("position"), col("file_date").alias("result_file_date")) \
#     .filter(col("file_date") == g_file_date)

# COMMAND ----------

# race_cir_df = races_df.join(circuits_df, races_df.circuit_id==circuits_df.circuit_id, "inner") \
#     .select(col("race_id"), col("race_year"), col("race_name"),col("race_timestamp").alias("race_date"), col("circuit_location"))

# COMMAND ----------

# results_data_df = results_df.join(race_cir_df, results_df.result_race_id==race_cir_df.race_id,"inner") \
#     .join(drivers_df, results_df.driver_id==drivers_df.driver_id, "inner") \
#     .join(const_df, results_df.constructor_id==const_df.constructor_id,"inner") \
#         .select(col("race_year"), col("team"), col("driver_name"), col("position"), col("points"), lit(11-col("position")).alias("calculated_points"),col("result_file_date").alias("file_date")) \
#         .withColumn("created_date",current_timestamp()) \
#             .filter(col("position")<=10)

# COMMAND ----------

# display(results_data_df.orderBy(col("calculated_points").desc()))

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC select count(1) from f1_presentation.calculated_race_results;

# COMMAND ----------

# MAGIC %sql
# MAGIC select count(1) from race_result_update
