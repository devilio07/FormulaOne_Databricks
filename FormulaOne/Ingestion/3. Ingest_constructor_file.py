# Databricks notebook source
# MAGIC %md
# MAGIC
# MAGIC ### Ingesting constructor.json file (single line json)

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
# MAGIC
# MAGIC --This time we will use DDL based schema instead of Struct based schema like done in the previous notebooks
# MAGIC --Although we can stick with the SturctType based approach.

# COMMAND ----------

constructor_schema = "constructorId INT, constructorRef STRING, name STRING, nationality STRING, url STRING"
print(constructor_schema)

# COMMAND ----------

constructor_raw = spark.read.format('json').schema(constructor_schema).options(path = f'{raw_path}/{g_file_date}/constructors.json').load()

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC ##### Step 2 - Renaming + adding + dropping columns in a single go.

# COMMAND ----------

constructor_fnl = add_ingest_date(constructor_raw.withColumnRenamed("constructorId",'constructor_id') \
  .withColumnRenamed('constructorRef','constructor_ref') \
  .drop(col('url')) \
  .withColumn("data_source", lit(g_data_source))   \
    .withColumn("file_date", lit(g_file_date))
  ) 

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC ##### Step 3 - Saving the processed file in parquet format and in processed folder.

# COMMAND ----------

# constructor_fnl.write.mode('overwrite').format('parquet').options(path=f"{processed_path}constructors").save()

# COMMAND ----------

# MAGIC %md 
# MAGIC
# MAGIC #### NOTE: Following change is being made to make the managed tables using pyspark.
# MAGIC
# MAGIC This essentially creates the same folders in the processed directory, the only difference is that now there datasets will be linked to managed table as well. So, we can read them into dataframes as well as read/query them using Spark SQL.

# COMMAND ----------

constructor_fnl.write.mode('overwrite').format('delta').saveAsTable("f1_processed.constructors")

# COMMAND ----------

dbutils.notebook.exit("True")
