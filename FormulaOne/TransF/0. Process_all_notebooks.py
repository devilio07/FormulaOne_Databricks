# Databricks notebook source
notebook_list = ["1. Race_results","2. Driver_Standings", "3. Constructor_Standings", "4. Calculated_race_results"]

# COMMAND ----------

for notebook in notebook_list:
    dbutils.notebook.run(notebook,0,{"file_date":"2021-04-18"})
    print(f"{notebook} has been run successfully.")
