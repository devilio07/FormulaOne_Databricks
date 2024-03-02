-- Databricks notebook source
select 
driver_name,
count(*) as total_races,
sum(calculated_points*1.00) as total_points,
avg(calculated_points*1.00) as avg_points
from f1_presentation.calculated_race_results 
group by driver_name
having total_races >=50
order by avg_points desc;

-- COMMAND ----------

select 
driver_name,
count(*) as total_races,
sum(calculated_points*1.00) as total_points,
avg(calculated_points*1.00) as avg_points
from f1_presentation.calculated_race_results 
where race_year between 2010 and 2023
group by driver_name
having total_races >=50
order by avg_points desc;

-- COMMAND ----------


