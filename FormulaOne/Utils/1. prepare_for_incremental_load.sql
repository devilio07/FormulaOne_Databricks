-- Databricks notebook source
drop database if exists f1_processed cascade;
create database if not exists f1_processed 
location "/mnt/maxdev00storage/processed/";

-- COMMAND ----------

drop database if exists f1_presentation cascade;
create database if not exists f1_presentation 
location "/mnt/maxdev00storage/presentation/";

-- COMMAND ----------


