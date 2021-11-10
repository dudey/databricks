// Databricks notebook source
// DBTITLE 1,Business Question 1
// MAGIC %md
// MAGIC List all customers living in a specified city, with an income between 2 values.
// MAGIC 
// MAGIC - City: Edgewood
// MAGIC - Income Lower range = 38128

// COMMAND ----------

// MAGIC %md
// MAGIC 
// MAGIC To answer the question we need the following tables:
// MAGIC 
// MAGIC - customer
// MAGIC - customer_address
// MAGIC - household_demographics
// MAGIC - income_band
// MAGIC 
// MAGIC 
// MAGIC ![1_Query](files/tables/retailer/images/1_Query.PNG)

// COMMAND ----------

// MAGIC %run ./Schema_Declarations

// COMMAND ----------

val customerDf = spark.read
.schema(customerSchema)
.options(datFileReadOptions)
.csv("/FileStore/tables/retailer/customer.dat")

val customerAddressDf = spark.read
.schema(customerAddressSchema)
.options(datFileReadOptions)
.csv("/FileStore/tables/retailer/customer_address.dat")

val householdDemograhicsDf = spark.read
.schema(householdDemographicsSchema)
.options(datFileReadOptions)
.csv("/FileStore/tables/retailer/household_demographics.dat")

val incomeBandDf = spark.read
.schema(incomeBandSchema)
.options(datFileReadOptions)
.csv("/FileStore/tables/retailer/income_band.dat")

// COMMAND ----------

import org.apache.spark.sql.functions.concat

// COMMAND ----------

val result = customerDf.join(householdDemograhicsDf,
               customerDf.col("c_current_hdemo_sk") === householdDemograhicsDf.col("hd_demo_sk"))

.join(incomeBandDf,
     incomeBandDf.col("ib_income_band_sk") === householdDemograhicsDf.col("hd_income_band_sk"))

.where($"ib_lower_bound" >= 38128)
.where($"ib_upper_bound" <= 88128)

.join(customerAddressDf,
     customerDf.col("c_current_addr_sk") === customerAddressDf.col("ca_address_sk"))
.where($"ca_city" === "Edgewood")
.select($"c_customer_id",
       $"c_customer_sk",
        concat($"c_first_name",$"c_last_name").as("customerName"),
        $"ca_city",
        $"ib_lower_bound",
        $"ib_upper_bound"
       )

// COMMAND ----------

display(result)
