-- Databricks notebook source
create database if not exists f1_processed
location "/mnt/maxdev00storage/processed/"

-- Here we are specifying a location because we are creating managed tables and we want the location to be picked by the database itself. Since if we provide the location at the time of loading then the metadata(database) and the actual data will reside in two different locations. 
-- For managed tables both metadata and table data reside in the same location
-- while for an external table both are at the different location.

-- COMMAND ----------

desc database extended f1_processed;
