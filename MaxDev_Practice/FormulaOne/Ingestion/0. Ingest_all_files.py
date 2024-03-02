# Databricks notebook source
notebook_list = ["1. Ingest_circuits_file","2. Ingest_races_file",
                "3. Ingest_constructor_file", "4. Ingest_driver_file",
                "5. Ingest_results_file", "6. Ingest_pitstops_file",
                "7. Ingest_LapTimes_file", "8. Ingest_Qualifying_file"]

# COMMAND ----------

for notebook in notebook_list:
    dbutils.notebook.run(notebook,0,{"data_source": "Ergast API", "file_date":"2021-04-18"})
    print(f"{notebook} has been run successfully.")
