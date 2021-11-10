-- Databricks notebook source
use retailer_db

-- COMMAND ----------

-- DBTITLE 1,Business Question 5
-- MAGIC %md
-- MAGIC For web sales, create a report showing the counts of orders shipped within 30 days, from 31 to 60 days, from 61 to 90 days, from 91 to 120 days and over 120 days within a given year, grouped by warehouse, shipping mode and web site.
-- MAGIC 
-- MAGIC - Month Sequence = 1200

-- COMMAND ----------

select 
w_warehouse_name as warehouse_name,
sm_type as shipping_type,
web_name as website_name,
sum(case when (ws_ship_date_sk - ws_sold_date_sk <= 30 ) then 1 else 0 end)  as 30_days

  ,sum(case when (ws_ship_date_sk - ws_sold_date_sk > 30) and 
                 (ws_ship_date_sk - ws_sold_date_sk <= 60) then 1 else 0 end )  as 31_to_60_days
  
  ,sum(case when (ws_ship_date_sk - ws_sold_date_sk > 60) and 
                 (ws_ship_date_sk - ws_sold_date_sk <= 90) then 1 else 0 end)  as 61_90_days 
  
  ,sum(case when (ws_ship_date_sk - ws_sold_date_sk > 90) and
                 (ws_ship_date_sk - ws_sold_date_sk <= 120) then 1 else 0 end)  as 91_to_120_days 
  
  ,sum(case when (ws_ship_date_sk - ws_sold_date_sk  > 120) then 1 else 0 end)  as over_120_days
from web_sales
inner join date_dim on ws_ship_date_sk = d_date_sk
inner join warehouse on ws_warehouse_sk = w_warehouse_sk
inner join ship_mode on ws_ship_mode_sk = sm_ship_mode_sk
inner join web_site on ws_web_site_sk = web_site_sk
where d_month_seq between 1200 and (1200+11) 
and w_warehouse_name is not null
group by
   warehouse_name
  ,shipping_type
  ,website_name
order by warehouse_name
         ,shipping_type
         ,website_name

-- COMMAND ----------

-- MAGIC 
-- MAGIC %md
-- MAGIC 
-- MAGIC To answer the business question we need the following tables:
-- MAGIC 
-- MAGIC - web_sales
-- MAGIC - warehouse
-- MAGIC - web_site
-- MAGIC - ship_mode
-- MAGIC - date_dim
-- MAGIC 
-- MAGIC ![5_Query](files/tables/retailer/images/5_Query.PNG)

-- COMMAND ----------

use retailer_db
