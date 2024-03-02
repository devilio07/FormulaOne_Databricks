# Databricks notebook source
# MAGIC %md
# MAGIC
# MAGIC ### Ingesting races.csv file

# COMMAND ----------

dbutils.widgets.text(name="data_source", defaultValue="")
g_data_source = dbutils.widgets.get("data_source")

# COMMAND ----------

dbutils.widgets.text(name="file_date", defaultValue="")
g_file_date = dbutils.widgets.get("file_date")

# COMMAND ----------

# MAGIC %run "../Includes/Configurations"

# COMMAND ----------

# MAGIC %run "../Includes/common_functions"

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC ##### Step 1 - Read the CSV file using the spark dataframe reader

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC ##### Note
# MAGIC In an actual Production project, it is not recommended to use "InferSchema()".
# MAGIC Since, it results into another job, since the driver has to go through the data again to be able to tell/guess the datatype and then apply the same.
# MAGIC
# MAGIC -- We would rather want to define our own schema and fail the job if the data doesn't conform to the defined norms.

# COMMAND ----------

from pyspark.sql.types import *
from pyspark.sql.functions import *

# COMMAND ----------

# MAGIC %md Defining our very own schema

# COMMAND ----------

schema_def = StructType(fields=[
    StructField("raceId", IntegerType(),False),
    StructField("year", IntegerType(),True),
    StructField("round", IntegerType(),True),
    StructField("circuitId", IntegerType(),True),
    StructField("name", StringType(),True),
    StructField("date", StringType(),True),
    StructField("time", StringType(),True),
    StructField("url", StringType(),True),
]) 
print(schema_def)
 # Creating a StructType object in another way
# schema_def.add("db_id","integer",True)      # Adding column 1 to StructType
# schema_def.add("db_name","string",True)     # Adding column 2 to StructType
# schema_def.add("db_type_cd","string",True)  # Adding column 3 to StructType
# print(schema_def)

# COMMAND ----------

races_raw = spark.read.format('csv').schema(schema_def).options(header=True, path = f"{raw_path}/{g_file_date}/races.csv").load()

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC ##### Step 2 - Renaming columns into meaningful names

# COMMAND ----------

races_raw2=races_raw.withColumnRenamed("raceId", "race_id") \
    .withColumnRenamed("year","race_year") \
    .withColumnRenamed("circuitId","circuit_id")

# COMMAND ----------

# MAGIC %md 
# MAGIC ##### Step 3 - Adding additional column and making a new column based on the existing columns

# COMMAND ----------

races_raw3=races_raw2.withColumn("race_timestamp", to_timestamp(concat(col("date"),lit(' '), col("time")), 'yyyy-MM-dd HH:mm:ss'))

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC ##### Step 4 - Drop columns that are not required now

# COMMAND ----------

races_raw4 = races_raw3.drop(col("date"),col("time"),col("url")) \
    .withColumn("data_source", lit(g_data_source))  \
    .withColumn("file_date", lit(g_file_date))

# COMMAND ----------

races_fnl = add_ingest_date(races_raw4)

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC ##### Step 5 - Saving the processed file in parquet format and in processed folder.

# COMMAND ----------

# races_fnl.write.mode('overwrite').format('parquet').options(path=f"{processed_path}races").partitionBy('race_year').save()

# COMMAND ----------

# MAGIC %md 
# MAGIC
# MAGIC #### NOTE: Following change is being made to make the managed tables using pyspark.
# MAGIC
# MAGIC This essentially creates the same folders in the processed directory, the only difference is that now there datasets will be linked to managed table as well. So, we can read them into dataframes as well as read/query them using Spark SQL.

# COMMAND ----------

races_fnl.write.mode('overwrite').format('parquet').partitionBy('race_year').saveAsTable("f1_processed.races")

# COMMAND ----------

dbutils.notebook.exit("True")
