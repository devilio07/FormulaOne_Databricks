# Databricks notebook source
# MAGIC %md
# MAGIC
# MAGIC ### Ingesting results.json file

# COMMAND ----------

dbutils.widgets.text(name="data_source", defaultValue="")
g_data_source = dbutils.widgets.get("data_source")

# COMMAND ----------

dbutils.widgets.text(name="file_date", defaultValue="2021-03-28")
g_file_date = dbutils.widgets.get("file_date")

# COMMAND ----------

# MAGIC %run "../Includes/Configurations"

# COMMAND ----------

# MAGIC %run "../Includes/common_functions"

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC ##### Step 1 - Read the JSON file using the spark dataframe reader

# COMMAND ----------

from pyspark.sql.types import *
from pyspark.sql.functions import *

# COMMAND ----------

results_schema = StructType(fields=[StructField("resultId",IntegerType(),False),
                                    StructField("raceId",StringType(),True),
                                    StructField("driverId",IntegerType(),True),
                                    StructField("constructorId",IntegerType(),True),
                                    StructField("number",IntegerType(),True),
                                    StructField("grid",IntegerType(),True),
                                    StructField("position",IntegerType(),True),
                                    StructField("positionText",StringType(),True),
                                    StructField("positionOrder",IntegerType(),True),
                                    StructField("points",FloatType(),True),
                                    StructField("laps",IntegerType(),True),
                                    StructField("time",StringType(),True),
                                    StructField("milliseconds",IntegerType(),True),
                                    StructField("fastestLap",IntegerType(),True),
                                    StructField("rank",IntegerType(),True),
                                    StructField("fastestLapTime",StringType(),True),
                                    StructField("fastestLapSpeed",FloatType(),True),
                                    StructField("statusId",StringType(),True),
                                    ])

# COMMAND ----------

results_raw = spark.read.format('json').schema(results_schema).options(path = f'{raw_path}/{g_file_date}/results.json').load()

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC ##### Step 2 - Renaming + adding + dropping columns in a single go.

# COMMAND ----------

results_fnl= add_ingest_date(results_raw.withColumnRenamed('resultId','result_id') \
  .withColumnRenamed('raceId','race_id') \
  .withColumnRenamed('driverId','driver_id') \
  .withColumnRenamed('constructorId','constructor_id') \
  .withColumnRenamed('positionText','position_text') \
  .withColumnRenamed('positionOrder','position_order') \
  .withColumnRenamed('fastestLap','fastest_lap') \
  .withColumnRenamed('fastestLapTime','fastest_lap_time') \
  .withColumnRenamed('fastestLapSpeed','fastest_lap_speed') \
  .drop(col('statusId')) \
  .withColumn("data_source", lit(g_data_source)) \
  .withColumn("file_date", lit(g_file_date))   
  )

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC ##### Step 3 - Saving the processed file in parquet format and in processed folder.

# COMMAND ----------

# results_fnl.write.mode('overwrite').format('parquet').options(path=f"{processed_path}results").partitionBy('race_id').save()

# COMMAND ----------

# MAGIC %md 
# MAGIC
# MAGIC #### NOTE: Following change is being made to make the managed tables using pyspark.
# MAGIC
# MAGIC This essentially creates the same folders in the processed directory, the only difference is that now there datasets will be linked to managed table as well. So, we can read them into dataframes as well as read/query them using Spark SQL.
# MAGIC
# MAGIC ###### NOTE: Changes to ingest only the incremental data is being made. 
# MAGIC We will also handle duplicate data ingestion i.e. in case due to some reasons we have to ingest the previous/same date file. Since, datalake architecture and spark  doesn't provide Delete or Update. We have to take other measures. 

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC #### Method 1: Dropping Partition
# MAGIC
# MAGIC Since we are partitioning the data on the basis of race_id, since our data is incremental on races itself. This would mean that all the data for a particular race or race_id will be under the same partition. Dropping that should remove the whole data of that particular date or file.
# MAGIC
# MAGIC
# MAGIC ###### NOTE: This functionality is only available in sql format and not pyspark or python format.

# COMMAND ----------

# for race_id_list in results_fnl.select(col("race_id")).distinct().collect():
#     #Checking if the table itself exists. For the very first run the table might not be present.
#     if spark._jsparkSession.catalog().tableExists("f1_processed.results"):
#         spark.sql(f"ALTER TABLE f1_processed.results DROP IF EXISTS PARTITION (race_id = {race_id_list.race_id})")

# COMMAND ----------

# results_fnl.write.mode('append').format('parquet').partitionBy('race_id').saveAsTable("f1_processed.results")

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC #### Method 2: Overwrite -- Insertinto functionality by spark
# MAGIC
# MAGIC Instead of droppping the partition and then appending the data, we will overwrite the data (not append like in previous mathod) and use insert into functionality withtin spark.  
# MAGIC
# MAGIC --While using insertInto() there is no way of telling the order of in which the columns are to inserted, so for the first run we will have use the normal partitionBy and saveAsTable statement. <br>
# MAGIC --Also, insertInto() expects the last column to be the partition Column. So, we will need to take care of that as well.
# MAGIC
# MAGIC ##### Important !!!#####
# MAGIC We need to set spark config of parition overwriting to Dynamic, otherwise insertInto() will also overwrite the whole data instead of the partition in concern.

# COMMAND ----------

incremental_load(results_fnl,"race_id","f1_processed","results")

# COMMAND ----------

dbutils.notebook.exit("True")

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC select race_id, count(1)
# MAGIC from f1_processed.results
# MAGIC group by race_id;
