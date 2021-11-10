// Databricks notebook source
// MAGIC %md
// MAGIC ![income_band_schema](files/tables/retailer/images/income_band.PNG)

// COMMAND ----------

val incomeBandDf = spark.read
.schema("ib_lower_band_sk long,ib_lower_bound int, ib_upper_bound int")
.option("sep","|")
.csv("/FileStore/tables/retailer/income_band.dat")

// COMMAND ----------

incomeBandDf.show

// COMMAND ----------

import org.apache.spark.sql.functions.lit

// COMMAND ----------

val incomeBandWithGroups = incomeBandDf
.withColumn("isFirstIncomeGroup",incomeBandDf.col("ib_upper_bound") <= 60000)
.withColumn("isSecondIncomeGroup",incomeBandDf.col("ib_upper_bound") >= 60000 and incomeBandDf.col("ib_upper_bound") < 120001)
.withColumn("isThirdIncomeGroup",incomeBandDf.col("ib_upper_bound") > 120001 && incomeBandDf.col("ib_upper_bound") <=200000)
.withColumn("demo",lit(1))

// COMMAND ----------

incomeBandWithGroups.show

// COMMAND ----------

incomeBandWithGroups.printSchema

// COMMAND ----------

val incomeClasses = incomeBandWithGroups
.withColumnRenamed("isThirdIncomeGroup","isHighIncomeClass")
.withColumnRenamed("isFirstIncomeGroup","isStandardIncomeClass")
.withColumnRenamed("isSecondIncomeGroup","isMediumIncomeClass")

// COMMAND ----------

incomeClasses.printSchema

// COMMAND ----------

incomeClasses.drop("demo","isHighIncomeClass","isMediumIncomeClass")

// COMMAND ----------

// MAGIC %md
// MAGIC ![customer_schema](files/tables/retailer/images/customer.PNG)

// COMMAND ----------

val customerDf = spark.read
.option("inferSchema",true)
.option("header",true)
.option("sep","|")
.csv("/FileStore/tables/retailer/customer.dat")

// COMMAND ----------

display(customerDf)

// COMMAND ----------

val customerWithValidBirthInfo = customerDf
.filter($"c_birth_day" > 0)
.where($"c_birth_day" <= 31)
.filter($"c_birth_month" > 0)
.filter($"c_birth_month" <= 12)
.where('c_birth_year.isNotNull)
.filter('c_birth_year > 0)

// COMMAND ----------

// MAGIC %md
// MAGIC **Inner Join**
// MAGIC 
// MAGIC An inner Join keeps rows with keys in the left and right datasets.
// MAGIC 
// MAGIC To perform an inner join, we specify a join expression, and only the rows that evaluate to true will show in the resulting DataFrame.
// MAGIC 
// MAGIC Let's join the customer DataFrame with the customer Address DataFrame to create a new DataFrame.

// COMMAND ----------

// MAGIC %md
// MAGIC ![customerAddress_schema](files/tables/retailer/images/customer_addresses.PNG)

// COMMAND ----------

val customerAddressDf = spark.read
.option("inferSchema",true)
.option("header",true)
.option("sep","|")
.csv("/FileStore/tables/retailer/customer_address.dat")

// COMMAND ----------

// MAGIC %md
// MAGIC ![ER_customer_and_Addresses](files/tables/retailer/images/ER_customer_and_Addresses.PNG)

// COMMAND ----------

val joinExpression = customerDf.col("c_current_addr_sk") === customerAddressDf.col("ca_address_sk")

// COMMAND ----------

val customerWithAddressesDf = customerDf
.join(customerAddressDf,joinExpression)
.select("c_customer_id",
        "ca_address_sk",
       "c_salutation",
       "c_first_name",
       "c_last_name",
       "ca_country",
       "ca_city",
       "ca_street_name",
       "ca_zip",
       "ca_street_number")

// COMMAND ----------

display(customerWithAddressesDf)

// COMMAND ----------

import org.apache.spark.sql.functions.count

// COMMAND ----------

customerDf.count()

// COMMAND ----------

customerDf.select(count("*")).show

// COMMAND ----------

customerDf.select(count("c_first_name")).show

// COMMAND ----------

customerDf.filter($"c_first_name".isNotNull).count

// COMMAND ----------

import org.apache.spark.sql.functions.countDistinct

// COMMAND ----------

customerDf.select(count("c_first_name")).show

// COMMAND ----------

customerDf.select(countDistinct("c_first_name")).show

// COMMAND ----------

// MAGIC %md
// MAGIC ![item.png](files/tables/retailer/images/item.PNG)

// COMMAND ----------

val itemDf = spark.read
.format("csv")
.options(Map("header" -> "true"
             ,"delimiter" -> "|"
             ,"inferSchema" -> "true"))
.load("/FileStore/tables/retailer/item.dat")

// COMMAND ----------

import org.apache.spark.sql.functions.min

// COMMAND ----------

val  minWSCostDf= itemDf.select(min("i_wholesale_cost")).withColumnRenamed("min(i_wholesale_cost)","minWSCost")

// COMMAND ----------

minWSCostDf.show

// COMMAND ----------

display(itemDf.filter($"i_wholesale_cost" === 0.02))

// COMMAND ----------

val cheapestItemDf = itemDf.join(minWSCostDf,
                                 itemDf.col("i_wholesale_cost") === minWSCostDf.col("minWSCost"),
                                 "inner")

// COMMAND ----------

display(cheapestItemDf)

// COMMAND ----------

import org.apache.spark.sql.functions.max

// COMMAND ----------

itemDf.select(max("i_current_price")).withColumnRenamed("max(i_current_price)","maxCurrentPrice").show

// COMMAND ----------

itemDf

// COMMAND ----------

// MAGIC %md
// MAGIC ![item.png](files/tables/retailer/images/store_sales.PNG)

// COMMAND ----------

val storeSalesDf = spark.read
.format("csv")
.options(Map("header" -> "true"
             ,"delimiter" -> "|"
             ,"inferSchema" -> "true"))
.load("/FileStore/tables/retailer/store_sales.dat")

// COMMAND ----------

import org.apache.spark.sql.functions.{sum,sumDistinct}

// COMMAND ----------

storeSalesDf.select(sum("ss_net_paid_inc_tax"),sum("ss_net_profit")).show

// COMMAND ----------

storeSalesDf.select(sumDistinct("ss_quantity"),sum("ss_quantity")).show

// COMMAND ----------

import org.apache.spark.sql.functions.{avg, mean}

// COMMAND ----------

storeSalesDf.select(
  avg("ss_quantity").as("average_purchases"),
  mean("ss_quantity").as("mean_purchases"),
  sum("ss_quantity") / count("ss_quantity"),
  min("ss_quantity").as("min_purchases"),
  max("ss_quantity").alias("max_purchases")
).show

// COMMAND ----------

storeSalesDf

// COMMAND ----------

// MAGIC %md
// MAGIC 
// MAGIC **Answer the following question about a customer**
// MAGIC 
// MAGIC - How many different items a customer bought
// MAGIC - The total quantity of all the item a customer bought
// MAGIC - How much the customer spend
// MAGIC - The Maximum price a customer paid for an item
// MAGIC - The Minimum price a customer paid for an item
// MAGIC - The Average price a customer paid for an item

// COMMAND ----------

storeSalesDf.groupBy("ss_customer_sk").agg(
  countDistinct("ss_item_sk").as("ItemCount"),
  sum("ss_quantity").as("TotalQuantity"),
  sum("ss_net_paid").as("TotalNetPaid"),
  max("ss_net_paid").as("MaxPaidPerItem"),
  min("ss_net_paid").as("MinPaidPerItem"),
  avg("ss_net_paid").as("AveragePaidPerItem")
).withColumnRenamed("ss_customer_sk","CustomerId").show

// COMMAND ----------

// MAGIC %run ./Practice/Schema_Declarations

// COMMAND ----------

val customerDf = spark.read
.schema(customerSchema)
.options(datFileReadOptions)
.csv("/FileStore/tables/retailer/customer.dat")

// COMMAND ----------

def getCustomerBirthDate(year:Int,month:Int, day:Int) : String = {
  return java.time.LocalDate.of(year,day,month).toString
}

// COMMAND ----------

// MAGIC %md
// MAGIC Register the function as DataFrame function

// COMMAND ----------

val getCustomerBirthDate_udf = 
udf(getCustomerBirthDate(_:Int,_:Int,_:Int):String)

// COMMAND ----------

import org.apache.spark.sql.functions._

// COMMAND ----------

customerDf.select(

$"c_customer_sk"
,concat($"c_first_name",$"c_last_name").as("c_name")
    ,getCustomerBirthDate_udf($"c_birth_year",$"c_birth_day",$"c_birth_month").as("c_birth_date")
,$"c_birth_year"
,$"c_birth_day"
,$"c_birth_month"
).show(false)


// COMMAND ----------

// MAGIC %md
// MAGIC register the function as SQL function

// COMMAND ----------

spark.udf.register("getCustomerBirthDate_udf",getCustomerBirthDate(_:Int,_:Int,_:Int):String)

// COMMAND ----------

customerDf.selectExpr("c_customer_sk"
                      ,"concat(c_first_name,c_last_name) as c_name"
                      ,"getCustomerBirthDate_udf(c_birth_year, c_birth_day,c_birth_month) as c_birth_date"
                      ,"c_birth_year"
                      ,"c_birth_month"
                      ,"c_birth_day"
                      )
.show(false)

// COMMAND ----------

customerDf.createOrReplaceTempView("vCustomer")

// COMMAND ----------

// MAGIC %sql
// MAGIC select 
// MAGIC   c_customer_sk,
// MAGIC   concat(c_first_name,c_last_name) as c_name,
// MAGIC   c_birth_year,
// MAGIC   c_birth_month,
// MAGIC   c_birth_day,
// MAGIC   getCustomerBirthDate_udf(c_birth_year,c_birth_day,c_birth_month) as c_birth_date 
// MAGIC from vCustomer
