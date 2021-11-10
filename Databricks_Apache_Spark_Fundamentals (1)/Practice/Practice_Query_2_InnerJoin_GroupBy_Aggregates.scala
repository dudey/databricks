// Databricks notebook source
// DBTITLE 1,Business Question 2
// MAGIC %md
// MAGIC Report the total extended sales price per item brand of a specific manufacturer for all sales in a specific month of the year.
// MAGIC 
// MAGIC - Month = 11
// MAGIC - ManufacturerId = 128

// COMMAND ----------

// MAGIC %md
// MAGIC 
// MAGIC To answer the business question we need the following tables:
// MAGIC 
// MAGIC - item
// MAGIC - store_sales
// MAGIC - date_dim
// MAGIC 
// MAGIC ![2_Query](files/tables/retailer/images/2_Query.PNG)

// COMMAND ----------

// MAGIC %run ./Schema_Declarations

// COMMAND ----------

val itemDf =  spark.read
.schema(itemSchema)
.options(datFileReadOptions)
.csv("/FileStore/tables/retailer/item.dat")

val storeSalesDf = spark.read
.schema(storeSalesSchema)
.options(datFileReadOptions)
.csv("/FileStore/tables/retailer/store_sales.dat")

val dateDimDf = spark.read
.schema(dateDimSchema)
.options(datFileReadOptions)
.csv("/FileStore/tables/retailer/date_dim.dat")

// COMMAND ----------

val itemManufacturerId = 128
val saleMonth = 11

// COMMAND ----------

import org.apache.spark.sql.functions.sum

// COMMAND ----------

val result = dateDimDf.join(storeSalesDf,
              storeSalesDf.col("ss_sold_date_sk") === dateDimDf.col("d_date_sk"))
.join(itemDf,
     itemDf.col("i_item_sk") === storeSalesDf.col("ss_item_sk"))

.filter($"i_manufact_id" === itemManufacturerId)
.where('d_moy === saleMonth)

.groupBy("d_year","d_moy","i_brand","i_brand_id","i_item_sk")
.agg(sum("ss_ext_sales_price").as("TotalExtSalePrice"))
.withColumnRenamed("d_year","Year")
.withColumnRenamed("d_moy","Month")
.withColumnRenamed("i_item_sk", "ItemKey")
.withColumnRenamed("i_brand","BrandName")
.withColumnRenamed("i_brand_id","brandId")

// COMMAND ----------

display(result)
