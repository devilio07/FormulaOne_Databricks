-- Databricks notebook source
create database if not exists demo;

-- COMMAND ----------

show databases;

-- COMMAND ----------

describe database extended demo;

-- COMMAND ----------

select current_database();

-- COMMAND ----------

show tables in demo;

-- COMMAND ----------

use demo;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC
-- MAGIC ### Learning Objectives
-- MAGIC
-- MAGIC 1. Creating managed tables using Python
-- MAGIC 2. Creating managed tables using SQL
-- MAGIC 3. Effect of dropping the managed table
-- MAGIC 4. Describe table

-- COMMAND ----------

-- MAGIC %run "/Workspace/MaxDev_Practice/FormulaOne/Includes/Configurations"

-- COMMAND ----------

-- MAGIC %python
-- MAGIC
-- MAGIC race_results = spark.read.format('parquet').options(path=f"{presentation_path}/race_results").load()

-- COMMAND ----------

-- MAGIC %python
-- MAGIC
-- MAGIC race_results.write.format("parquet").saveAsTable("demo.race_resutls_py")

-- COMMAND ----------

desc table extended race_resutls_py;

-- COMMAND ----------

select count(*) from demo.race_resutls_py where race_year = 2020;

-- COMMAND ----------

create table demo.race_result_sql 
as 
select * from demo.race_resutls_py where race_year = 2020;

-- COMMAND ----------

show tables;

-- COMMAND ----------

desc table extended race_result_sql;

-- COMMAND ----------

drop table demo.race_result_sql;

-- COMMAND ----------

show tables;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC
-- MAGIC ### Learning Objectives
-- MAGIC
-- MAGIC 1. Creating external tables using Python
-- MAGIC 2. Creating external tables using SQL
-- MAGIC 3. Effect of dropping the external table

-- COMMAND ----------

-- MAGIC %python
-- MAGIC
-- MAGIC # Creating an external table in presentation layer.
-- MAGIC
-- MAGIC race_results.write.mode("overwrite").format("parquet").options(path=f"{presentation_path}/race_results_ext_py").saveAsTable("demo.race_results_ext_py")

-- COMMAND ----------

desc extended demo.race_results_ext_py;

-- COMMAND ----------

create table demo.race_results_ext_sql
(
race_year integer,
race_name string,
race_date timestamp,
circuit_location string,
driver_name string,
driver_number integer,
driver_nationality string,
team string,
grid integer,
fastest_lap integer,
race_time string,
points float,
position integer,
created_date timestamp
)
using parquet
location "abfss://presentation@maxdev00datalake.dfs.core.windows.net/race_results_ext_sql"

-- COMMAND ----------

show tables;

-- COMMAND ----------

-- MAGIC %md 
-- MAGIC
-- MAGIC ###### NOTE:
-- MAGIC
-- MAGIC Even though the table is visible in the database, there won't be any directory in the ADLS (location given in the above cell). It will only appear when there will be some data in it.

-- COMMAND ----------

INSERT into demo.race_results_ext_sql 
select * FROM demo.race_results_ext_py where race_year = 2020;

-- COMMAND ----------

select count(*) from demo.race_results_ext_sql;

-- COMMAND ----------

drop table demo.race_results_ext_sql;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC
-- MAGIC ###### NOTE:
-- MAGIC Dropping external tables only removes the metadata from hive metastore, the data files in the external storage remains intact. 
-- MAGIC Beauty of this is that, we can recreate the table pointing to the same external location and we can fetch the data without having to insert the data again.

-- COMMAND ----------

-- MAGIC %md
-- MAGIC
-- MAGIC ####Views on Tables
-- MAGIC
-- MAGIC ###### Learning Objectives
-- MAGIC
-- MAGIC 1. Create Temp View.
-- MAGIC 2. Create Global Temp View.
-- MAGIC 3. Create Permanent View.

-- COMMAND ----------

create or replace temp view v_driver_standings as 
select *, dense_rank() over(partition by(race_year) order by total_points desc, wins desc ) as rank 
from(
SELECT race_year, driver_name, driver_nationality, team, 
sum(points) as total_points,
count(case when position=1 then 1 END) as wins
FROM demo.race_resutls_py
where race_year = 2020
group by race_year, driver_name, driver_nationality, team
) ref

-- COMMAND ----------

select * from v_driver_standings;

-- COMMAND ----------

create or replace global temp view gv_driver_standings as 
select *, dense_rank() over(partition by(race_year) order by total_points desc, wins desc ) as rank 
from(
SELECT race_year, driver_name, driver_nationality, team, 
sum(points) as total_points,
count(case when position=1 then 1 END) as wins
FROM demo.race_resutls_py
where race_year = 2020
group by race_year, driver_name, driver_nationality, team
) ref

-- COMMAND ----------

-- Important -- global temp views or tables are created in global_temp schema/db

select * from global_temp.gv_driver_standings;

-- COMMAND ----------

create or replace view demo.pv_driver_standings as 
select *, dense_rank() over(partition by(race_year) order by total_points desc, wins desc ) as rank 
from(
SELECT race_year, driver_name, driver_nationality, team, 
sum(points) as total_points,
count(case when position=1 then 1 END) as wins
FROM demo.race_resutls_py
where race_year = 2020
group by race_year, driver_name, driver_nationality, team
) ref

-- COMMAND ----------

show tables in demo;

-- COMMAND ----------

show databases;

-- COMMAND ----------

show tables in f1_presentation;

-- COMMAND ----------

create temporary view v_driver_standing_2020
as
select * from f1_presentation.driver_standings where race_year = 2020;

create temporary view v_driver_standing_2018
as
select * from f1_presentation.driver_standings where race_year = 2018;

-- COMMAND ----------

select * from 
v_driver_standing_2020 as a
inner join v_driver_standing_2018 as b
on a.driver_name = b.driver_name;

-- COMMAND ----------

-- MAGIC %python
-- MAGIC
-- MAGIC display(dbutils.fs.ls("abfss://raw@maxdev00storage.dfs.core.windows.net/"))

-- COMMAND ----------


