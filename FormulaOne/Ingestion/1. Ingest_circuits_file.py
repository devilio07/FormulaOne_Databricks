# Databricks notebook source
# MAGIC %md
# MAGIC
# MAGIC ### Ingesting circuits.csv file

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC Including other notebook to parameterise some hardcorded values.
# MAGIC
# MAGIC ###### NOTE: These changes have been made after these notebooks were created, so basically some changes are being made and the previous version is not being preserved. Most hardcoded and redundent values are to be replaced.

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
    StructField("circuitId", IntegerType(),False),
    StructField("circuitRef", StringType(),True),
    StructField("name", StringType(),True),
    StructField("location", StringType(),True),
    StructField("country", StringType(),True),
    StructField("lat", DoubleType(),True),
    StructField("lng", DoubleType(),True),
    StructField("alt", IntegerType(),True),
    StructField("url", StringType(),True),
]) 
print(schema_def)
 # Creating a StructType object in another way
# schema_def.add("db_id","integer",True)      # Adding column 1 to StructType
# schema_def.add("db_name","string",True)     # Adding column 2 to StructType
# schema_def.add("db_type_cd","string",True)  # Adding column 3 to StructType
# print(schema_def)

# COMMAND ----------

circuits_raw = spark.read.format('csv').schema(schema_def).options(header=True, path = f"{raw_path}/{g_file_date}/circuits.csv").load()

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC ##### Step 2 - Selecting only the required columns (or dropping the one not required)

# COMMAND ----------

circuits_raw2 = circuits_raw.drop(col("url"))

# circuits_raw2=circuits_raw.select(col("circuitId"), col("circuitRef"),
#                                 col("name"), col("location"), col("country"),
#                                 col("lat"), col("lng"), col("alt"))

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC ##### Step 3 - Renaming columns into meaningful names

# COMMAND ----------

circuits_raw3=circuits_raw2.withColumnRenamed("circuitId", "circuit_id") \
    .withColumnRenamed("circuitRef","circuit_ref") \
    .withColumnRenamed("lat","latitude") \
    .withColumnRenamed("lng","longitude") \
    .withColumnRenamed("alt","altitude") \
    .withColumn("data_source", lit(g_data_source)) \
    .withColumn("file_date", lit(g_file_date))

# COMMAND ----------

# MAGIC %md 
# MAGIC ##### Step 4 - Adding additional column

# COMMAND ----------

# circuits_raw4=circuits_raw3.withColumn("Ingestion_date", current_timestamp())

circuits_raw4 = add_ingest_date(circuits_raw3)

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC ##### Step 5 - Saving the processed file in parquet format and in processed folder.

# COMMAND ----------

# circuits_raw4.write.mode('overwrite').format('parquet').options(path=f"{processed_path}circuits").save()

# COMMAND ----------

# MAGIC %md 
# MAGIC
# MAGIC #### NOTE: Following change is being made to make the managed tables using pyspark.
# MAGIC
# MAGIC This essentially creates the same folders in the processed directory, the only difference is that now there datasets will be linked to managed table as well. So, we can read them into dataframes as well as read/query them using Spark SQL.

# COMMAND ----------

circuits_raw4.write.mode('overwrite').format('parquet').saveAsTable("f1_processed.circuits")

# COMMAND ----------

dbutils.notebook.exit("True")
