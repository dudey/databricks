-- Databricks notebook source
-- MAGIC %md
-- MAGIC ![rdms_view](files/tables/retailer/images/RDBMS_View.PNG)
-- MAGIC image via: https://www.postgresqltutorial.com/managing-postgresql-views/

-- COMMAND ----------

-- MAGIC %scala
-- MAGIC val customerAddress = spark.read.format("csv")
-- MAGIC .options(Map("header" -> "true","sep" -> "|","inferSchema" -> "true"))
-- MAGIC .load("/FileStore/tables/retailer/customer_address.dat")

-- COMMAND ----------

-- MAGIC %scala 
-- MAGIC customerAddress.show

-- COMMAND ----------

-- MAGIC %scala
-- MAGIC customerAddress.createTempView("vCustomerAddress")

-- COMMAND ----------

-- MAGIC %scala
-- MAGIC customerAddress.select("*")

-- COMMAND ----------

-- MAGIC %scala
-- MAGIC spark.sql("select * from vCustomerAddress").show

-- COMMAND ----------

-- MAGIC %scala
-- MAGIC customerAddress.
-- MAGIC select("ca_address_sk","ca_country","ca_state","ca_city","ca_street_name")
-- MAGIC .where($"ca_state".contains("AK"))
-- MAGIC .createTempView("vAK_Addresses")
-- MAGIC        

-- COMMAND ----------

select * from vAK_Addresses

-- COMMAND ----------

select * from vCustomerAddress

-- COMMAND ----------

create table if not exists demotbl as select * from vCustomerAddress

-- COMMAND ----------

show databases

-- COMMAND ----------

select * from demotbl limit 10

-- COMMAND ----------

select current_database()

-- COMMAND ----------

-- MAGIC %run ./Practice/Create_Retailer_DB

-- COMMAND ----------

use retailer_db

-- COMMAND ----------

select * from demotbl limit 10

-- COMMAND ----------

show tables

-- COMMAND ----------

use default

-- COMMAND ----------

create database dbname

-- COMMAND ----------

show databases

-- COMMAND ----------

drop database dbname

-- COMMAND ----------

use default

-- COMMAND ----------

select current_database()

-- COMMAND ----------

create table if not exists retailer_db.customer(c_customer_sk LONG comment "this is the primary key",c_customer_id STRING,c_current_cdemo_sk LONG,c_current_hdemo_sk LONG,c_current_addr_sk LONG,c_first_shipto_date_sk LONG,c_first_sales_date_sk LONG,c_salutation STRING,c_first_name STRING,c_last_name STRING,c_preferred_cust_flag STRING,c_birth_day INT,c_birth_month INT,c_birth_year INT,c_birth_country STRING,c_login STRING,c_email_address STRING,c_last_review_date LONG)
using csv
options(
path '/FileStore/tables/retailer/customer.dat',
sep '|',
header true
)

-- COMMAND ----------

show tables in retailer_db

-- COMMAND ----------

describe formatted  retailer_db.customer

-- COMMAND ----------

select current_database()

-- COMMAND ----------

drop table retailer_db.customer

-- COMMAND ----------

show tables in retailer_db

-- COMMAND ----------

-- MAGIC %fs ls dbfs:/FileStore/tables/retailer/customer.dat

-- COMMAND ----------

-- MAGIC %scala
-- MAGIC customerAddress.write.saveAsTable("retailer_db.customerAddress")

-- COMMAND ----------

create table retailer_db.customerWithAddress(customer_id bigint, first_name string, last_name string,
country string, city string, state string, street_name string)

-- COMMAND ----------

insert into retailer_db.customerWithAddress
select c_customer_sk, c_first_name, c_last_name, ca_country, 
ca_city, ca_state, ca_street_name
from retailer_db.customer c
inner join customerAddress ca on ca.ca_address_sk = c.c_current_addr_sk

-- COMMAND ----------

select * from retailer_db.customerWithAddress

-- COMMAND ----------

show tables in retailer_db

-- COMMAND ----------

describe formatted customer

-- COMMAND ----------

describe formatted  retailer_db.customeraddress

-- COMMAND ----------

drop table retailer_db.customeraddress

-- COMMAND ----------

-- MAGIC %fs ls dbfs:/user/hive/warehouse/retailer_db.db/customeraddress

-- COMMAND ----------

use retailer_db;

-- COMMAND ----------

describe customer

-- COMMAND ----------

select 2 * 2 

-- COMMAND ----------

select
2*2,
c_customer_sk,
c_customer_sk * 2,
c_salutation, 
upper(c_first_name),
c_first_name,
c_last_name
from customer

-- COMMAND ----------

-- MAGIC %scala
-- MAGIC import  org.apache.spark.sql.functions.{expr, col}

-- COMMAND ----------

-- MAGIC %scala
-- MAGIC customerAddress.select(col("ca_address_sk").as("id1"),expr("ca_address_sk as id")).show

-- COMMAND ----------

-- MAGIC %scala
-- MAGIC customerAddress

-- COMMAND ----------

-- MAGIC %scala
-- MAGIC customerAddress

-- COMMAND ----------

-- MAGIC %scala
-- MAGIC customerAddress.selectExpr(
-- MAGIC "ca_address_sk",
-- MAGIC "upperca_street_name"),
-- MAGIC "ca_street_number",
-- MAGIC "ca_street_number is not null and length(ca_street_number) > 0  as isValidAddress"
-- MAGIC ).show

-- COMMAND ----------

-- MAGIC %scala
-- MAGIC val validAddress = customerAddress.select(
-- MAGIC $"ca_address_sk",
-- MAGIC $"ca_street_name",
-- MAGIC $"ca_street_number",
-- MAGIC expr("ca_street_number is not null and length(ca_street_number) > 0  as isValidAddress")).show

-- COMMAND ----------

use retailer_db

-- COMMAND ----------

select length(c_salutation) from customer

-- COMMAND ----------

select * from customer
where c_birth_month = 1
and lower(c_birth_country) = "canada"
and c_birth_year <> 1980
and (trim(lower(c_salutation)) = "miss" or trim(lower(c_salutation)) = "mrs.")


-- COMMAND ----------

select c_customer_sk, c_birth_country, c_first_name, c_last_name
from customer
where c_birth_country is not null --> unknown <> null, unknown = null

-- COMMAND ----------

select count(*) from customer

-- COMMAND ----------

select count(*) from customer where c_first_name is null

-- COMMAND ----------

select count(c_first_name) from customer

-- COMMAND ----------

create table if not exists retailer_db.store_sales
using csv
options(
path '/FileStore/tables/retailer/store_sales.dat',
sep '|',
header true,
inferSchema true
)

-- COMMAND ----------

describe store_sales

-- COMMAND ----------

select sum(ss_quantity) from store_sales

-- COMMAND ----------

select sum(ss_net_profit) from store_sales

-- COMMAND ----------

select avg(ss_net_paid) from store_sales

-- COMMAND ----------

select mean(ss_net_paid) from store_sales

-- COMMAND ----------

select min(ss_net_paid) from store_sales

-- COMMAND ----------

select * from store_sales where ss_net_paid = 0

-- COMMAND ----------

select * from store_sales where ss_net_paid = 19562.4

-- COMMAND ----------

select max(ss_net_paid) from store_sales

-- COMMAND ----------

select ss_store_sk,
sum(ss_net_profit)
from store_sales
where ss_store_sk is not null
group by ss_store_sk


-- COMMAND ----------

select sum(ss_net_profit) from store_sales where ss_store_sk = 1

-- COMMAND ----------

select ss_store_sk,
ss_item_sk,
count(ss_quantity),
sum(ss_net_profit),
sum(ss_net_paid)
from store_sales
where ss_store_sk is not null and ss_item_sk is not null
group by ss_store_sk,ss_item_sk


-- COMMAND ----------

select count(*) from customer where c_birth_year = 1943 and c_birth_country = "GREECE"

-- COMMAND ----------

select  * from customer where c_birth_year = 1982 and c_birth_country = "MALDIVES" 

-- COMMAND ----------

select c_birth_year, c_birth_country, count(*) as c_count
from customer
where c_birth_country is not null and c_birth_year is not null
group by c_birth_year, c_birth_country
having max(c_birth_month) = 2
order by  c_birth_year desc, c_count desc, c_birth_country desc
-- Having c_birth_year > 1990 and c_birth_country like "A%" and count(*) > 10

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ![customer_address](files/tables/retailer/images/ER_customer_and_Addresses.PNG)

-- COMMAND ----------

create table if not exists retailer_db.customer_address(ca_address_sk LONG,ca_address_id STRING,ca_street_number STRING,ca_street_name STRING,ca_street_type STRING,ca_suite_number STRING,ca_city STRING,ca_county STRING,ca_state STRING,ca_zip STRING,ca_country STRING,ca_gmt_offset decimal(5,2),ca_location_type STRING)
using csv
options(
path '/FileStore/tables/retailer/customer_address.dat',
sep '|',
header true
)

-- COMMAND ----------

select 
-- c_customer_sk,
-- ca_address_sk,
-- c_first_name,
-- c_last_name,
-- ca_country,
-- ca_state,
ca_city,
count(*)
-- ca_street_name,
-- ca_street_number,
-- ca_county
from retailer_db.customer c
inner join retailer_db.customer_address ca
on c.c_current_addr_sk = ca.ca_address_sk
where ca_city is not null
group by ca_city order by count(*) asc

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ![ER_Customer_StoreSales](files/tables/retailer/images/ER_Customer_StoreSales.PNG)

-- COMMAND ----------

select count(distinct ss_customer_sk) from store_sales

-- COMMAND ----------

select 100000 - 90858

-- COMMAND ----------

select count(distinct c_customer_sk) from customer

-- COMMAND ----------


select count(*)
-- c.c_customer_sk customerId,
-- concat(c.c_first_name, c.c_last_name) customerName,
-- s.ss_customer_sk customerId_from_store,
-- s.ss_item_sk itemid
from customer c
left outer join store_sales s
on c.c_customer_sk = s.ss_customer_sk
where s.ss_customer_sk  is null

-- COMMAND ----------

describe store_sales

-- COMMAND ----------


select 
c.c_customer_sk customerId,
concat(c.c_first_name, c.c_last_name) customerName,
s.ss_customer_sk customerId_from_store,
s.ss_store_sk,
s.ss_ticket_number,
s.ss_item_sk itemid
from customer c
right outer join store_sales s
on c.c_customer_sk = s.ss_customer_sk

where s.ss_customer_sk  is null

-- COMMAND ----------

select c_customer_sk,
c_first_name,
c_last_name,
c_birth_year,
c_birth_country
from customer
where c_birth_country in ("SURINAME","FIJI","TOGO")
and c_birth_year between 1966 and 1980
and c_last_name like "M%"
and c_first_name like "_e%"


-- COMMAND ----------

select 
min(ss_sales_price), 
avg(ss_sales_price),
max(ss_sales_price) 
from store_sales

-- COMMAND ----------

select 
ss_item_sk,
ss_sales_price,
ss_customer_sk,
case 
when ss_sales_price between 0 and 37.892353 then "belowAvg"
when ss_sales_price between 37.892353 and 199.56 then "aboveAvg"
else "unknown" end as priceCategory
from store_sales
