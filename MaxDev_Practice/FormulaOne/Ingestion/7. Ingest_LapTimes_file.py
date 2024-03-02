# Databricks notebook source
# MAGIC %md
# MAGIC
# MAGIC ### Ingesting split files from lap_times folder.

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
# MAGIC ##### Step 1 - Read the CSV files using the spark dataframe reader

# COMMAND ----------

from pyspark.sql.types import *
from pyspark.sql.functions import *

# COMMAND ----------

lap_times_schema = StructType(fields=[
                                    StructField("raceId",IntegerType(),True),
                                    StructField("driverId",IntegerType(),True),
                                    StructField("lap",IntegerType(),True),
                                    StructField("position",IntegerType(),True),
                                    StructField("time",StringType(),True),
                                    StructField("milliseconds",IntegerType(),True)
                                    ])

# COMMAND ----------

lap_times_raw = spark.read.format('csv').schema(lap_times_schema).options(path = f'{raw_path}/{g_file_date}/lap_times/', pathGlobFilter='*.csv').load()

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC ##### Step 2 - Renaming + adding + dropping columns in a single go.

# COMMAND ----------

lap_times_fnl= add_ingest_date(lap_times_raw.withColumnRenamed('raceId','race_id') \
  .withColumnRenamed('driverId','driver_id') \
  .withColumn("data_source", lit(g_data_source)) \
  .withColumn("file_date", lit(g_file_date))
  )

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC ##### Step 3 - Saving the processed file in parquet format and in processed folder.

# COMMAND ----------

# lap_times_fnl.write.mode('overwrite').format('parquet').options(path=f"{processed_path}lap_times").save()

# COMMAND ----------

# MAGIC %md 
# MAGIC
# MAGIC #### NOTE: Following change is being made to make the managed tables using pyspark.
# MAGIC
# MAGIC This essentially creates the same folders in the processed directory, the only difference is that now there datasets will be linked to managed table as well. So, we can read them into dataframes as well as read/query them using Spark SQL.

# COMMAND ----------

# lap_times_fnl.write.mode('overwrite').format('parquet').saveAsTable("f1_processed.lap_times")

# COMMAND ----------

incremental_load(lap_times_fnl, "race_id", "f1_processed", "lap_times")

# COMMAND ----------

dbutils.notebook.exit("True")
