-- Databricks notebook source
-- MAGIC %python 
-- MAGIC
-- MAGIC dashboard_title = "<h1 style=\"color:Black;text-align:center;font-family:Calibri\"> Report on Dominant Formula One Teams </h1>"
-- MAGIC displayHTML(dashboard_title)

-- COMMAND ----------

select 
team,
count(*) as total_races,
sum(calculated_points*1.00) as total_points,
avg(calculated_points*1.00) as avg_points,
rank() over(order by avg(calculated_points*1.00) desc) as team_rank
from f1_presentation.calculated_race_results 
group by team
having total_races >=100
order by avg_points desc;

-- COMMAND ----------

select 
race_year,
team,
count(1) as total_races,
sum(calculated_points) as total_points,
avg(calculated_points) as avg_points
from f1_presentation.calculated_race_results 
group by race_year,team
order by race_year, avg_points desc;

-- COMMAND ----------

with cte as (
select 
team,
count(*) as total_races,
sum(calculated_points*1.00) as total_points,
avg(calculated_points*1.00) as avg_points,
rank() over(order by avg(calculated_points*1.00) desc) as team_rank
from f1_presentation.calculated_race_results 
group by team
having total_races >=100
)select 
a.race_year,
a.team,
count(1) as total_races,
sum(a.calculated_points) as total_points,
avg(a.calculated_points) as avg_points
from f1_presentation.calculated_race_results as a
inner join cte on a.team = cte.team 
where cte.team_rank <=5
group by a.race_year,a.team
order by a.race_year, avg_points desc

-- COMMAND ----------


