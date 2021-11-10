// Databricks notebook source
// MAGIC %md
// MAGIC You can find more about the Apache Spark types in the [Spark Documention](https://spark.apache.org/docs/latest/api/java/index.html?org/apache/spark/sql/types/DataTypes.html)
// MAGIC https://spark.apache.org/docs/latest/api/java/index.html?org/apache/spark/sql/types/DataTypes.html

// COMMAND ----------

val household_demographicsDf = spark.read
.schema(
  "hd_demo_sk LONG,hd_income_band_sk LONG,hd_buy_potential STRING,hd_dep_count INT,hd_vehicle_count INT")
.options(
  Map("header" -> "true","sep" -> "|"))
.csv("/FileStore/tables/retailer/household_demographics.dat")

// COMMAND ----------

household_demographicsDf.schema

// COMMAND ----------

import org.apache.spark.sql.types._

// COMMAND ----------

val hds = StructType(
  Array(
    StructField("hd_demo_sk",LongType,true),
    StructField("hd_income_band_sk",LongType, true),
    StructField("hd_buy_potential",StringType,true),
    StructField("hd_dep_count",IntegerType,true),
    StructField("hd_vehicle_count",IntegerType)
  )
)

// COMMAND ----------

val household_demographicsDf2 = spark.read
.schema(hds)
.options(
  Map("header" -> "true","sep" -> "|"))
.csv("/FileStore/tables/retailer/household_demographics.dat")

// COMMAND ----------

household_demographicsDf2.printSchema

// COMMAND ----------

household_demographicsDf.printSchema

// COMMAND ----------

import org.apache.spark.sql.functions.lit
//  https://spark.apache.org/docs/latest/api/scala/index.html?org/apache/spark#org.apache.spark.package

// COMMAND ----------

lit("emptyColumn")

// COMMAND ----------

lit(3)

// COMMAND ----------

household_demographicsDf.select($"*",lit("house").name("theNewName"),lit(3).name("three")).show

// COMMAND ----------

household_demographicsDf.show(5)

// COMMAND ----------

household_demographicsDf
.where("hd_income_band_sk <> 2")
.where($"hd_vehicle_count" > 3 or $"hd_vehicle_count" === 1)
.orderBy($"hd_vehicle_count".asc)
.show

// COMMAND ----------

household_demographicsDf.select("hd_buy_potential").distinct.show

// COMMAND ----------

import org.apache.spark.sql.functions.trim

// COMMAND ----------

val isBuyPotentialHigh = trim($"hd_buy_potential").equalTo("5001-10000")

// COMMAND ----------

val Df = household_demographicsDf
.withColumn("high_buying_potential",isBuyPotentialHigh)

// COMMAND ----------

Df.select($"*").orderBy($"high_buying_potential".desc).show

// COMMAND ----------

Df.where($"high_buying_potential"
         .or($"hd_vehicle_count" > 3))
.orderBy("hd_vehicle_count")
.show

// COMMAND ----------

val webSalesDf = spark.read.table("retailer_db.web_sales")

// COMMAND ----------

val salesStatDf = webSalesDf.select(
"ws_order_number",
"ws_item_sk",
  "ws_quantity",
  "ws_net_paid",
  "ws_net_profit",
  "ws_wholesale_cost"
)

// COMMAND ----------

salesStatDf.show

// COMMAND ----------

import org.apache.spark.sql.functions.{expr,round,bround}

// COMMAND ----------

val salesPerfDf = salesStatDf.
withColumn("expected_net_paid", $"ws_quantity"*$"ws_wholesale_cost")
.withColumn("calculated_profit",$"ws_net_paid" - $"expected_net_paid")
.withColumn("unit_price",expr("ws_wholesale_cost / ws_quantity"))
.withColumn("rounded_unitPrice",round($"unit_price",2))
.withColumn("brounded_unitPrice",bround($"unit_price",2))

// COMMAND ----------

display(salesPerfDf)

// COMMAND ----------

salesPerfDf.describe()

// COMMAND ----------

salesPerfDf.summary()

// COMMAND ----------

//https://spark.apache.org/docs/latest/api/scala/index.html#org.apache.spark.sql.DataFrameStatFunctions

// COMMAND ----------

val itemDf = spark.read.table("retailer_db.item")

// COMMAND ----------

val Df1 = itemDf.where($"i_item_desc".contains("young"))

// COMMAND ----------

Df1.count

// COMMAND ----------

// MAGIC %sql
// MAGIC select count(i_item_sk) from retailer_db.item where i_item_desc like "%young%"

// COMMAND ----------

display(Df1)

// COMMAND ----------

display(itemDf)

// COMMAND ----------

itemDf

// COMMAND ----------

import org.apache.spark.sql.functions.{length}

// COMMAND ----------

itemDf.select(length($"i_color")).show

// COMMAND ----------

// MAGIC %sql
// MAGIC select length(i_color) from retailer_db.item

// COMMAND ----------

// MAGIC %sql
// MAGIC select distinct(i_color) from retailer_db.item

// COMMAND ----------

import org.apache.spark.sql.functions.{trim,ltrim,rtrim,lower,upper,initcap,lpad,rpad}

// COMMAND ----------

itemDf.where($"i_color" === rpad(lit("pink"),20," ")).count

// COMMAND ----------

itemDf.where(trim($"i_color") === "pink").count

// COMMAND ----------

display(itemDf.select(
lower($"i_item_desc"),
  initcap($"i_item_desc"),
upper($"i_item_desc"),
upper($"i_item_desc")
))

// COMMAND ----------

// MAGIC %sql
// MAGIC select i_item_desc,
// MAGIC initcap(i_item_desc),
// MAGIC lower(i_item_desc),
// MAGIC upper(i_item_desc)
// MAGIC from retailer_db.item

// COMMAND ----------

// MAGIC %sql
// MAGIC select count(*) from retailer_db.item where trim(i_color) = "pink"

// COMMAND ----------

val singleItemAndSoldDate = spark.sql("""
select ws_order_number, ws_item_sk item_id,
d_date sold_date
from retailer_db.web_sales 
inner join retailer_db.date_dim
      on ws_sold_date_sk = d_date_sk 
where ws_item_sk = 4591 and ws_order_number = 1
      """)

// COMMAND ----------

singleItemAndSoldDate.show

// COMMAND ----------

// MAGIC %md
// MAGIC Date Format are specified using  [Java SimpleDateFormat](https://docs.oracle.com/javase/10/docs/api/java/text/SimpleDateFormat.html)
// MAGIC https://docs.oracle.com/javase/10/docs/api/java/text/SimpleDateFormat.html

// COMMAND ----------

import org.apache.spark.sql.functions.{to_date,lit,unix_timestamp,from_unixtime}

// COMMAND ----------

import org.apache.spark.sql.types.{TimestampType}

// COMMAND ----------

val dateDf = singleItemAndSoldDate
.withColumn("sold_date_1",to_date(lit("1999-07-23")))
.withColumn("sold_date_2",to_date(lit("1999/07/23"),"yyyy/MM/dd"))
.withColumn("sold_date_3",to_date(lit("23.07.1999"),"dd.MM.yyyy"))
.withColumn("sold_date_4",to_date(lit("1999.07.23"),"yyyy.MM.dd"))
.withColumn("sold_date_5",to_date(lit("1999.07.23 20:21:53"),"yyyy.MM.dd HH:mm:ss"))
.withColumn("sold_date_6",to_date(lit("1999.07.23 20:21:53 PM"),"yyyy.MM.dd HH:mm:ss a"))
.withColumn("sold_date_7",unix_timestamp(lit("1999.07.23 20:21:53"),"yyyy.MM.dd HH:mm:ss"))
.withColumn("sold_date_8",from_unixtime($"sold_date_7","dd.MM.yyyy"))
.withColumn("sold_date_9",
            unix_timestamp(lit("1999.07.23 20:21:53 PM"),"yyyy.MM.dd HH:mm:ss a")
            .cast(TimestampType))

// COMMAND ----------

display(dateDf)

// COMMAND ----------

val itemWithDate = spark.sql("""
select ws_order_number, ws_item_sk item_id,
d.d_date sold_date,
dd.d_date ship_date
from retailer_db.web_sales 
inner join retailer_db.date_dim d
      on ws_sold_date_sk = d.d_date_sk 
inner join retailer_db.date_dim dd
      on ws_ship_date_sk = dd.d_date_sk 
      """)

// COMMAND ----------

import org.apache.spark.sql.functions.{expr,date_sub, date_add,datediff,months_between}

// COMMAND ----------

val shipPerfDf = itemWithDate
.withColumn("ideal_ship_date",date_add($"sold_date",7))
.withColumn("7_days_before_ship",date_sub($"ship_date",7))
.withColumn("sold_to_ship",datediff($"sold_date",$"ship_date"))
.withColumn("month_to_ship",months_between($"ship_date",$"sold_date"))
.withColumn("diff", $"ship_date" > $"ideal_ship_date")


// COMMAND ----------

shipPerfDf.select("*").where(!$"diff").show

// COMMAND ----------

val web_salesDf = spark.read.table("retailer_db.web_sales")

// COMMAND ----------

import org.apache.spark.sql.functions.{struct,expr}

// COMMAND ----------

val itemQuantityDf = web_salesDf.selectExpr("ws_bill_customer_sk customer_id",
                                           "(ws_item_sk item_id, ws_quantity quantity) as item_quantity")

// COMMAND ----------

itemQuantityDf.select($"customer_id",$"item_quantity".getField("quantity").as("quantity"))
.show

// COMMAND ----------

itemQuantityDf.select("customer_id","item_quantity.*")
.show

// COMMAND ----------

itemQuantityDf.select("customer_id","item_quantity.item_id","item_quantity.quantity")
.show

// COMMAND ----------

web_salesDf.select($"ws_bill_customer_sk".as("customer_id"), struct($"ws_item_sk".as("item_id"),$"ws_quantity".as("quantity")).as("item_quantity"))

// COMMAND ----------

itemQuantityDf.createOrReplaceTempView("tempDf")

// COMMAND ----------

// MAGIC %sql
// MAGIC select customer_id,  item_quantity.item_id from tempDf limit 10

// COMMAND ----------

val customerItemsDf = web_salesDf
.groupBy("ws_bill_customer_sk")
.agg(
collect_set("ws_item_sk").as("itemList")
)

// COMMAND ----------

display(customerItemsDf)

// COMMAND ----------

customerItemsDf
.select($"*"
       ,size($"itemList"))
.show

// COMMAND ----------

customerItemsDf.select(explode($"itemList")).show

// COMMAND ----------

customerItemsDf
.select($"*"
       ,size($"itemList")
       ,explode($"itemList"))
.show

// COMMAND ----------

customerItemsDf
.select($"*"
       ,size($"itemList")
       ,explode($"itemList")
       ,array_contains($"itemList",13910).as("hasItem"))
.where(!$"hasItem")
.show

// COMMAND ----------

customerItemsDf
.select($"*"
       ,size($"itemList")
       ,$"itemList"(0).as("firstItem")
        ,$"itemList".getItem(1).as("secondItem")
       ,explode($"itemList")
       ,array_contains($"itemList",13910).as("hasItem"))
.where(!$"hasItem")
.show

// COMMAND ----------

web_salesDf.select($"ws_item_sk"
                  ,array($"ws_sold_date_sk",$"ws_ship_date_sk"))
.show

// COMMAND ----------

val itemInfoDf = spark.read.table("retailer_db.item")
.select($"i_item_sk"
       ,map(trim($"i_category"),trim($"i_product_name")).as("category"))

// COMMAND ----------

itemInfoDf.printSchema

// COMMAND ----------

// MAGIC %sql
// MAGIC select * from retailer_db.item where i_item_sk = 2

// COMMAND ----------

itemInfoDf.select($"i_item_sk"
                 ,$"category.Music")
.show(false)

// COMMAND ----------

itemInfoDf.select($"i_item_sk"
                 ,$"category".getItem("Music").as("product_name")
                 ,lit("Music").as("item_category"))
.show(false)

// COMMAND ----------

itemInfoDf.select($"*"
                 ,explode($"category")
                 )
.withColumnRenamed("key","category_desc")
.withColumnRenamed("value","product_name")
.show

// COMMAND ----------

import org.apache.spark.sql.functions.{map,trim,explode}

// COMMAND ----------

// MAGIC %md
// MAGIC https://spark.apache.org/docs/latest/api/scala/index.html#org.apache.spark.sql.DataFrameNaFunctions

// COMMAND ----------

val customer_emailDf = spark.sql("""
select c_salutation
, c_first_name
, c_last_name
, c_email_address
,c_birth_year
,c_birth_month
,c_preferred_cust_flag
from retailer_db.customer
""")

// COMMAND ----------

display(customer_emailDf)

// COMMAND ----------

val allRowsWithValuesDf = customer_emailDf.na.drop("all")

// COMMAND ----------

customer_emailDf.count - allRowsWithValuesDf.count

// COMMAND ----------

display(allRowsWithValuesDf)

// COMMAND ----------

val cleanDf = allRowsWithValuesDf.na.drop("any")

// COMMAND ----------

cleanDf.count - allRowsWithValuesDf.count

// COMMAND ----------

display(cleanDf)

// COMMAND ----------

val min4ValidColumnsDf = allRowsWithValuesDf.na.drop(5)

// COMMAND ----------

display(min4ValidColumnsDf)

// COMMAND ----------

allRowsWithValuesDf.na.drop("all",Seq("c_first_name","c_last_name"))

// COMMAND ----------

display(allRowsWithValuesDf)

// COMMAND ----------

val nullFilledDf = allRowsWithValuesDf.na.fill("A Wonderful World")
.na.fill(Map("c_birth_year" -> 29999, "c_birth_month" -> 13))

// COMMAND ----------

display(nullFilledDf)
