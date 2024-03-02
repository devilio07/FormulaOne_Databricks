-- Databricks notebook source
-- MAGIC %md
-- MAGIC
-- MAGIC We are going to create external tables for the raw layer

-- COMMAND ----------

create database if not exists f1_raw;

-- COMMAND ----------

drop table if exists f1_raw.circuits;
create table if not exists f1_raw.circuits(
circuitId  integer,
circuitRef  string,
name  string,
location  string,
country  string,
lat  double,
lng  double,
alt  integer,
url  string
)
using csv
options (path "/mnt/maxdev00storage/raw/circuits.csv", header True );

-- COMMAND ----------

drop table if exists f1_raw.races;
create table if not exists f1_raw.races(
raceid integer,   
year integer,   
round integer,   
circuitid integer,   
name string,   
date string,   
time string,   
url string
)
using csv
options (path "/mnt/maxdev00storage/raw/races.csv", header True );

-- COMMAND ----------

drop table if exists f1_raw.constructor;
create table if not exists f1_raw.constructor(
constructorId INT, 
constructorRef STRING, 
name STRING, 
nationality STRING, 
url STRING
)
using json
options (path "/mnt/maxdev00storage/raw/constructors.json");

-- COMMAND ----------

drop table if exists f1_raw.drivers;
create table if not exists f1_raw.drivers(
driverId integer,
driverRef string,
number integer,
code string,
name struct<forename: string, surname : string>,
forename string,
surname string,
dob date,
nationality string,
url string
)
using json 
options (path "/mnt/maxdev00storage/raw/drivers.json");

-- COMMAND ----------

drop table if exists f1_raw.results;
create table if not exists f1_raw.results(
resultId integer,
raceId string,
driverId integer,
constructorId integer,
number integer,
grid integer,
position integer,
positionText string,
positionOrder integer,
points float,
laps integer,
time string,
milliseconds integer,
fastestLap integer,
rank integer,
fastestLapTime string,
fastestLapSpeed float,
statusId string
)
using json 
options (path "/mnt/maxdev00storage/raw/results.json");

-- COMMAND ----------

drop table if exists f1_raw.pit_stops;
create table if not exists f1_raw.pit_stops(
raceId integer,
driverId integer,
stop string,
lap integer,
time string,
duration string,
milliseconds integer
)
using json 
options (path "/mnt/maxdev00storage/raw/pit_stops.json", multiLine true);

-- COMMAND ----------

drop table if exists f1_raw.lap_times;
create table if not exists f1_raw.lap_times(
raceId integer,
driverId integer,
lap integer,
position integer,
time string,
milliseconds integer
)
using csv
options (path "/mnt/maxdev00storage/raw/lap_times/", pathGlobFilter "*.csv");

-- COMMAND ----------

drop table if exists f1_raw.qualifying;
create table if not exists f1_raw.qualifying(
qualifyId integer,
raceId integer,
driverId integer,
constructorId integer,
number integer,
position integer,
q1 string,
q2 string,
q3 string
)
using json
options (path "/mnt/maxdev00storage/raw/qualifying/", pathGlobFilter "*.json", multiLine True);

-- COMMAND ----------

show tables in f1_raw;
