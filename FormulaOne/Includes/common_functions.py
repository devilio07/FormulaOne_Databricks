# Databricks notebook source
from pyspark.sql.functions import *
from delta.tables import DeltaTable

# COMMAND ----------

def add_ingest_date(input_df):
    out_df=input_df.withColumn("ingestion_date", current_timestamp())
    return out_df

# COMMAND ----------

def col_reorder(df, partition_id):
    col_names = []
    for cols in df.schema.names:
        if cols!=partition_id:
            col_names.append(cols)
    col_names.append(partition_id)
    return df.select(col_names)

# COMMAND ----------

def incremental_load(df,partition_id,databasename,tablename):
    df_reordered = col_reorder(df, partition_id)

    spark.conf.set("spark.sql.sources.partitionOverwriteMode","dynamic") 

    if spark._jsparkSession.catalog().tableExists(f"{databasename}.{tablename}"):
        df_reordered.write.mode('overwrite').insertInto(f"{databasename}.{tablename}")
    else:
        df_reordered.write.mode('overwrite').format('parquet').partitionBy(f"{partition_id}").saveAsTable(f"{databasename}.{tablename}")

# COMMAND ----------

def df_to_col(df, dist_col :str)-> list:
    race_year_list :list[str] = df\
        .select(col("race_year")) \
        .distinct() \
        .collect()

    return [_.race_year for _ in race_year_list]

# COMMAND ----------

from pyspark.sql.functions import *
from delta.tables import DeltaTable

def conv_to_delta(df,partition_id :str, merge_condition :str, databasename: str,tablename :str) -> None:

    spark.conf.set("spark.databricks.optimizer.dynamicPartitionPruning","true")

    if spark._jsparkSession.catalog().tableExists(f"{databasename}.{tablename}"):
        df_old = DeltaTable.forName(spark,f"{databasename}.{tablename}") 
        
        df_old.alias("old").merge(
            df.alias("upd"),
            merge_condition
        ) \
        .whenMatchedUpdateAll() \
        .whenNotMatchedInsertAll() \
        .execute()
        
    else:
        df.write.mode('overwrite').format('delta').partitionBy(f"{partition_id}").saveAsTable(f"{databasename}.{tablename}")

# COMMAND ----------


