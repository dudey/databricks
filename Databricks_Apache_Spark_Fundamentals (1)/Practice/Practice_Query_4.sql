-- Databricks notebook source
-- DBTITLE 1,Business Question 4
-- MAGIC %md
-- MAGIC Report the total web sales for customers in specific zip codes, cities, counties or states, or specific items for a given year and quarter.
-- MAGIC 
-- MAGIC - Year = 2001
-- MAGIC - Quater Of Year = 2
-- MAGIC - Zip Codes = '85669', '86197','88274','83405','86475', '85392', '85460', '80348', '81792'
-- MAGIC - Item Keys = 2, 3, 5, 7, 11, 13, 17, 19, 23, 29

-- COMMAND ----------

-- MAGIC 
-- MAGIC %md
-- MAGIC 
-- MAGIC To answer the business question we need the following tables:
-- MAGIC 
-- MAGIC - web_sales
-- MAGIC - customer
-- MAGIC - customer_address
-- MAGIC - item
-- MAGIC - date_dim
-- MAGIC 
-- MAGIC ![4_Query](files/tables/retailer/images/4_Query.PNG)

-- COMMAND ----------

-- MAGIC %run ./Create_Retailer_DB

-- COMMAND ----------

use retailer_db

-- COMMAND ----------

select 
ca.ca_zip,
ca.ca_city,
ca.ca_county,
ca.ca_state,
sum(ws.ws_sales_price) total_web_sales_price
from web_sales ws
inner join customer c on c.c_customer_sk = ws.ws_bill_customer_sk
inner join customer_address ca on ca.ca_address_sk = c.c_current_addr_sk
inner join item it on it.i_item_sk = ws.ws_item_sk
inner join date_dim dd on dd.d_date_sk = ws.ws_sold_date_sk

where dd.d_year = 2001
and dd.d_qoy = 2
and
(substr(ca.ca_zip,1,5) in ('85669', '86197','88274','83405','86475', '85392', '85460', '80348', '81792')
or
it.i_item_id in (select i_item_id from item where i_item_sk in (2, 3, 5, 7, 11, 13, 17, 19, 23, 29)))

group by ca.ca_zip, ca.ca_city,ca.ca_county, ca.ca_state
order by total_web_sales_price desc
