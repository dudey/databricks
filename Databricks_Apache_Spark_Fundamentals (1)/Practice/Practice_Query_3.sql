-- Databricks notebook source
-- DBTITLE 1,Business Question 3
-- MAGIC %md
-- MAGIC Compute the total discounted amount for a particular manufacturer in a particular 90 day period for catalog sales whose discounts exceeded the average discount by at least 30%.
-- MAGIC 
-- MAGIC - Year = 2000
-- MAGIC - Date = 2000-01-27
-- MAGIC - Item Manufacturer Id = 977

-- COMMAND ----------

-- MAGIC 
-- MAGIC %md
-- MAGIC 
-- MAGIC To answer the business question we need the following tables:
-- MAGIC 
-- MAGIC - item
-- MAGIC - catalog_sales
-- MAGIC - date_dim
-- MAGIC 
-- MAGIC ![3_Query](files/tables/retailer/images/3_Query.PNG)

-- COMMAND ----------

-- MAGIC %run ./Create_Retailer_DB

-- COMMAND ----------

use retailer_db

-- COMMAND ----------

select 
it.i_manufact_id manufactId,
sum(cs_ext_discount_amt) excess_discount_amount
from catalog_sales cs
inner join item it on it.i_item_sk = cs.cs_item_sk
inner join date_dim dt on dt.d_date_sk = cs.cs_sold_date_sk
--where it.i_manufact_id = 977
where dt.d_date between date("2000-01-27") and date_add("2000-01-27",90)
and cs.cs_ext_discount_amt > (
  select 1.3 * avg(cs_ext_discount_amt)
  from catalog_sales
  inner join date_dim on d_date_sk = cs_sold_date_sk
  where cs_item_sk = it.i_item_sk
  and d_date between date("2000-01-27") and date_add("2000-01-27",90)
)
group by it.i_manufact_id

-- COMMAND ----------

select avg(cs_ext_discount_amt) from catalog_sales
