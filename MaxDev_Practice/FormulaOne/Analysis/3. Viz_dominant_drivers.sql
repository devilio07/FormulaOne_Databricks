-- Databricks notebook source
-- MAGIC %python 
-- MAGIC
-- MAGIC dashboard_title = "<h1 style=\"color:Black;text-align:center;font-family:Calibri\"> Report on Dominant Formula One Drivers </h1>"
-- MAGIC displayHTML(dashboard_title)

-- COMMAND ----------

select 
driver_name,
count(*) as total_races,
sum(calculated_points*1.00) as total_points,
avg(calculated_points*1.00) as avg_points,
rank() over(order by avg(calculated_points*1.00) desc) as driver_rank
from f1_presentation.calculated_race_results 
group by driver_name
having total_races >=50
order by avg_points desc;

-- COMMAND ----------

select 
race_year,
driver_name,
count(1) as total_races,
sum(calculated_points) as total_points,
avg(calculated_points) as avg_points
from f1_presentation.calculated_race_results 
group by race_year,driver_name
order by race_year, avg_points desc;

-- COMMAND ----------

with cte as (
  select 
driver_name,
count(*) as total_races,
sum(calculated_points*1.00) as total_points,
avg(calculated_points*1.00) as avg_points,
rank() over(order by avg(calculated_points*1.00) desc) as driver_rank
from f1_presentation.calculated_race_results 
group by driver_name
having total_races >=50
)select 
a.race_year,
a.driver_name,
count(1) as total_races,
sum(a.calculated_points) as total_points,
avg(a.calculated_points) as avg_points
from f1_presentation.calculated_race_results as a
inner join cte on a.driver_name = cte.driver_name 
where cte.driver_rank <=10
group by a.race_year,a.driver_name
order by a.race_year, avg_points desc

-- COMMAND ----------


