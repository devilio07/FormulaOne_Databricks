# Databricks notebook source
# MAGIC %md
# MAGIC
# MAGIC ### Ingesting split files from qualifying folder.

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
# MAGIC ##### Step 1 - Read the multiline JSON files using the spark dataframe reader

# COMMAND ----------

from pyspark.sql.types import *
from pyspark.sql.functions import *

# COMMAND ----------

qualifying_schema = StructType(fields=[
                                    StructField("qualifyId",IntegerType(),False),
                                    StructField("raceId",IntegerType(),True),
                                    StructField("driverId",IntegerType(),True),
                                    StructField("constructorId",IntegerType(),True),
                                    StructField("number",IntegerType(),True),
                                    StructField("position",IntegerType(),True),
                                    StructField("q1", StringType(), True),
                                    StructField("q2", StringType(), True),
                                    StructField("q3", StringType(), True)
                                    ])

# COMMAND ----------

qualify_raw = spark.read.format('json').schema(qualifying_schema).options(multiline = True, path = f'{raw_path}/{g_file_date}/qualifying/', pathGlobFilter='*.json').load()

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC ##### Step 2 - Renaming + adding + dropping columns in a single go.

# COMMAND ----------

qualify_fnl= add_ingest_date(qualify_raw.withColumnRenamed('qualifyId','qualify_id') \
  .withColumnRenamed('raceId','race_id') \
  .withColumnRenamed('driverId','driver_id') \
  .withColumnRenamed('constructorId','constructor_id') \
  .withColumn("data_source", lit(g_data_source)) \
  .withColumn("file_path", lit(g_file_date)))

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC ##### Step 3 - Saving the processed file in parquet format and in processed folder.

# COMMAND ----------

# qualify_fnl.write.mode('overwrite').format('parquet').options(path=f"{processed_path}qualifying").save()

# COMMAND ----------

# MAGIC %md 
# MAGIC
# MAGIC #### NOTE: Following change is being made to make the managed tables using pyspark.
# MAGIC
# MAGIC This essentially creates the same folders in the processed directory, the only difference is that now there datasets will be linked to managed table as well. So, we can read them into dataframes as well as read/query them using Spark SQL.

# COMMAND ----------

# qualify_fnl.write.mode('overwrite').format('parquet').saveAsTable("f1_processed.qualifying")

# COMMAND ----------

#incremental_load(qualify_fnl,"race_id","f1_processed","qualifying")

# COMMAND ----------

merge_condition = fr"old.qualify_id=upd.qualify_id and old.race_id=upd.race_id"
conv_to_delta(qualify_fnl, "race_id",merge_condition,"f1_processed","qualifying")

# COMMAND ----------

dbutils.notebook.exit("True")
