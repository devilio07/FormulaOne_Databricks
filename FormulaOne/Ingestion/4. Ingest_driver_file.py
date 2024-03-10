# Databricks notebook source
# MAGIC %md
# MAGIC
# MAGIC ### Ingesting drivers.json file (single line json but with nested columns)

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
# MAGIC ##### Step 1 - Read the JSON file using the spark dataframe reader

# COMMAND ----------

from pyspark.sql.types import *
from pyspark.sql.functions import *

# COMMAND ----------

# MAGIC %md 
# MAGIC
# MAGIC ###### Defining our very own schema
# MAGIC -- Since the JSON file has nested structure.
# MAGIC -- Need to define the nested structure first and use that in the main schema

# COMMAND ----------

name_schema = StructType(fields=[StructField("forename",StringType(),True),
                                StructField("surname",StringType(),True)])

drivers_schema = StructType(fields=[StructField("driverId",IntegerType(),False),
                                    StructField("driverRef",StringType(),True),
                                    StructField("number",IntegerType(),True),
                                    StructField("code",StringType(),True),
                                    StructField("name",name_schema,True),
                                    StructField("dob",DateType(),True),
                                    StructField("nationality",StringType(),True),
                                    StructField("url",StringType(),True),
                                    ])

# COMMAND ----------

drivers_raw = spark.read.format('json').schema(drivers_schema).options(path = f'{raw_path}/{g_file_date}/drivers.json').load()

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC ##### Step 2 - Renaming + adding + dropping columns in a single go.

# COMMAND ----------

drivers_fnl = add_ingest_date(drivers_raw.withColumnRenamed('driverId','driver_id') \
  .withColumnRenamed('driverRef','driver_ref') \
  .withColumn('name',concat(col('name').forename,lit(' '),col('name').surname)) \
  .drop(col('url')) \
  .withColumn("data_source", lit(g_data_source))   \
    .withColumn("file_date", lit(g_file_date))
  )

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC ##### Step 3 - Saving the processed file in parquet format and in processed folder.

# COMMAND ----------

# drivers_fnl.write.mode('overwrite').format('parquet').options(path=f"{processed_path}drivers").save()

# COMMAND ----------

# MAGIC %md 
# MAGIC
# MAGIC #### NOTE: Following change is being made to make the managed tables using pyspark.
# MAGIC
# MAGIC This essentially creates the same folders in the processed directory, the only difference is that now there datasets will be linked to managed table as well. So, we can read them into dataframes as well as read/query them using Spark SQL.

# COMMAND ----------

drivers_fnl.write.mode('overwrite').format('delta').saveAsTable("f1_processed.drivers")

# COMMAND ----------

dbutils.notebook.exit("True")
