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

# COMMAND ----------

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
# MAGIC - For this we will split a file into three parts mimicing incremental data which may include old records as well.
# MAGIC - Then We will create a delta table and use SQL version and then ultimately the pyspark version of the Merge statement.

# COMMAND ----------

# First Split of the data
drivers_day1_df = spark.read.options(inferSchema = True) \
  .format('json') \
  .options(inferSchema = True, path = "/mnt/maxdev00storage/raw/2021-03-28/drivers.json") \
  .load() \
  .filter(col('driverId')<=10) \
  .select(col("driverId"), col('dob'), col('name.forename').alias("forename"), col('name.surname').alias("surname"))

# COMMAND ----------

# Second split of the data with some overlap and change in previous data.
drivers_day2_df = spark.read.options(inferSchema = True) \
  .format('json') \
  .options(inferSchema = True, path = "/mnt/maxdev00storage/raw/2021-03-28/drivers.json") \
  .load() \
  .filter("driverId between 6 and 15") \
  .select(col("driverId"), col('dob'), upper(col('name.forename')).alias("forename"), upper(col('name.surname')).alias("surname"))

# COMMAND ----------

# Third split of the data with some overlap and change in previous data.
drivers_day3_df = spark.read.options(inferSchema = True) \
  .format('json') \
  .options(inferSchema = True, path = "/mnt/maxdev00storage/raw/2021-03-28/drivers.json") \
  .load() \
  .filter("driverId between 1 and 5 or driverId between 16 and 20") \
  .select(col("driverId"), col('dob'), upper(col('name.forename')).alias("forename"), upper(col('name.surname')).alias("surname"))

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC #### Using SQL approach
# MAGIC - Will need to create temp views and a delta lake based managed table to perform the insert.

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC create table if not exists delta_demo.drivers_merge (
# MAGIC driverId int,
# MAGIC dob date,
# MAGIC forename string,
# MAGIC surname string,
# MAGIC create_date date,
# MAGIC update_date date
# MAGIC )
# MAGIC using delta;

# COMMAND ----------

drivers_day1_df.createOrReplaceGlobalTempView('drivers_day1')
drivers_day2_df.createOrReplaceGlobalTempView('drivers_day2')

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC #### Used this for Merge syntax for both SQL and pyspark.
# MAGIC
# MAGIC https://docs.delta.io/latest/delta-update.html#-delta-merge

# COMMAND ----------

# MAGIC %md
# MAGIC Day 1

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Writing merge query to perfrom upsert, in this case since the table is initially empty, the records will only get inserted to the delta table.
# MAGIC
# MAGIC merge into delta_demo.drivers_merge tgt
# MAGIC using global_temp.drivers_day1 upd 
# MAGIC on tgt.driverId = upd.driverId
# MAGIC when matched then 
# MAGIC   update set tgt.dob = upd.dob,
# MAGIC              tgt.forename = upd.forename,
# MAGIC              tgt.surname = upd.surname,
# MAGIC              tgt.update_date = current_timestamp 
# MAGIC when not matched then
# MAGIC   insert (driverId, dob, forename, surname, create_date) values (driverId, dob, forename, surname, current_timestamp);

# COMMAND ----------

# MAGIC %md
# MAGIC Day 2

# COMMAND ----------

# MAGIC %sql
# MAGIC merge into delta_demo.drivers_merge tgt
# MAGIC using global_temp.drivers_day2 upd 
# MAGIC on tgt.driverId = upd.driverId
# MAGIC when matched then 
# MAGIC   update set tgt.dob = upd.dob,
# MAGIC              tgt.forename = upd.forename,
# MAGIC              tgt.surname = upd.surname,
# MAGIC              tgt.update_date = current_timestamp + interval '1' day   
# MAGIC when not matched then
# MAGIC   insert (driverId, dob, forename, surname, create_date) values (driverId, dob, forename, surname, current_timestamp + interval '1' day);

# COMMAND ----------

# MAGIC %md
# MAGIC Day 3 - We will use pyspark for this

# COMMAND ----------

from delta.tables import *

deltaTable = DeltaTable.forPath(spark, '/mnt/maxdev00storage/default/drivers_merge/')

deltaTable.alias('tgt') \
  .merge(
    drivers_day3_df.alias('upd'),
    'tgt.driverId = upd.driverId'
  ) \
  .whenMatchedUpdate(set =
    {
      "dob": "upd.dob",
      "forename": "upd.forename",
      "surname": "upd.surname",
      "update_date": "date_add(current_timestamp(),2)"
    }
  ) \
  .whenNotMatchedInsert(values =
    {
      "driverId" :"upd.driverId",
      "dob": "upd.dob",
      "forename": "upd.forename",
      "surname": "upd.surname",
      "create_date": "date_add(current_timestamp(),2)"
    }
  ) \
  .execute()

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC select * from delta_demo.drivers_merge order by driverId asc;

# COMMAND ----------

# MAGIC %md
# MAGIC ###### Now we will explore the following functionalities
# MAGIC
# MAGIC 1. History and Versioning.
# MAGIC 2. Time Travel
# MAGIC 3. Vacuum

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC describe history delta_demo.drivers_merge;

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC select * from delta_demo.drivers_merge version as of 1;

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC select * from delta_demo.drivers_merge timestamp as of '2024-03-02T18:15:26Z';

# COMMAND ----------

# MAGIC %md
# MAGIC  Using Pyspark syntax

# COMMAND ----------

df = spark.read.format("delta").options(versionAsOf=2, path = "/mnt/maxdev00storage/default/drivers_merge/").load()

display(df)

# COMMAND ----------

df = spark.read.format("delta").options(timestampAsOf='2024-03-02T18:27:24Z', path = "/mnt/maxdev00storage/default/drivers_merge/").load()

display(df)

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC To compy with the GDPR guidelines if the user want his/her information to be deleted, we have to comply. Since, we're able to time travel and look at the previous data, the user data is not actually deleted even if we delete it from the current version.
# MAGIC
# MAGIC > therefore, we will use vacuum command to clean up the older version (default is 7 days, but this can  be changed).

# COMMAND ----------

# MAGIC %sql
# MAGIC set spark.databricks.delta.retentionDurationCheck.enabled = false;
# MAGIC vacuum delta_demo.drivers_merge retain 0 hours;

# COMMAND ----------

# MAGIC %md
# MAGIC Pyspark way

# COMMAND ----------

from delta.tables import DeltaTable

deltaTable = DeltaTable.forName(spark, 'delta_demo.drivers_merge')    # Hive metastore-based tables

deltaTable.vacuum(0)  

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC >If we want to rollback to the previous version due to any underlying reasons, we can do that by using merge and referencing to the previous versions.
# MAGIC >
# MAGIC >> For that we'll delete some records first and then try to load them back.

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC delete from delta_demo.drivers_merge where driverId <=2;

# COMMAND ----------

# MAGIC %md
# MAGIC ####SQL Syntax
# MAGIC
# MAGIC Here we will only add the one of the deleted rows; and then we'll add the remaining using pyspark syntax.

# COMMAND ----------

# MAGIC %sql
# MAGIC merge into delta_demo.drivers_merge lts
# MAGIC using delta_demo.drivers_merge version as of 17 old
# MAGIC on lts.driverId = old.driverId 
# MAGIC when not matched and old.driverId = 2 then
# MAGIC insert *;

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC select * from delta_demo.drivers_merge order by driverId;

# COMMAND ----------

# MAGIC %md
# MAGIC #### Pyspark Syntax
# MAGIC
# MAGIC Adding the remaining row from the previous version.
# MAGIC > https://docs.delta.io/latest/delta-update.html#language-python

# COMMAND ----------

deltaTable = DeltaTable.forName(spark, 'delta_demo.drivers_merge')

oldDeltaTable = spark.read.format('delta').options(versionAsOf=17, path = "/mnt/maxdev00storage/default/drivers_merge/").load()

deltaTable.alias("cur") \
    .merge(oldDeltaTable.alias("old"),
           "cur.driverId=old.driverId") \
    .whenNotMatchedInsertAll(condition="old.driverId=1") \
    .execute()

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC select * from delta_demo.drivers_merge order by driverId;

# COMMAND ----------

# MAGIC %md
# MAGIC #### Converting Parquet into Delta tables
# MAGIC >We'll convert both parquet tables as well as parquet files (just file no table).

# COMMAND ----------

# MAGIC %sql
# MAGIC drop table if exists delta_demo.convertParquet_toDelta;
# MAGIC create table delta_demo.convertParquet_toDelta using parquet as (
# MAGIC   select * from delta_demo.drivers_merge
# MAGIC   );

# COMMAND ----------

# MAGIC %md
# MAGIC Converting using SQL

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC convert to delta delta_demo.convertParquet_toDelta;

# COMMAND ----------

# MAGIC %md
# MAGIC Creating a parquet file instead of a table and then converting it into delta.
# MAGIC

# COMMAND ----------

df = spark.table("delta_demo.drivers_merge")
df.write.parquet("/mnt/maxdev00storage/default/converParquet_toDelta_new");

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC convert to delta parquet.`/mnt/maxdev00storage/default/converParquet_toDelta_new`;
