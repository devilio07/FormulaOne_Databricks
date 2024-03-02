-- Databricks notebook source
select 
team,
count(*) as total_races,
sum(calculated_points) as total_points,
avg(calculated_points) as avg_points
from f1_presentation.calculated_race_results
group by team
having total_races >=100
order by avg_points desc;

-- COMMAND ----------

select 
team,
count(*) as total_races,
sum(calculated_points) as total_points,
avg(calculated_points) as avg_points
from f1_presentation.calculated_race_results
where race_year between 2001 and 2011
group by team
having total_races >=100
order by avg_points desc;

-- COMMAND ----------


