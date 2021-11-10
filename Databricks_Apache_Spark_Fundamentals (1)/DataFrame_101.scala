// Databricks notebook source
// MAGIC %fs ls /FileStore/tables/retailer

// COMMAND ----------

val customerDf = spark.read.csv("dbfs:/FileStore/tables/retailer/customer.csv")

// COMMAND ----------

val customerDf1 = spark.read.format("csv").load("dbfs:/FileStore/tables/retailer/customer.csv")

// COMMAND ----------

display(customerDf)

// COMMAND ----------

val customerDfWithHeader = spark.read
.option("header",true)
.csv("dbfs:/FileStore/tables/retailer/customer.csv")

// COMMAND ----------

display(customerDfWithHeader)

// COMMAND ----------

// MAGIC %fs head dbfs:/FileStore/tables/retailer/customer.dat

// COMMAND ----------

val customerDfWithHeader1 = spark.read
.option("header",true)
.option("sep","|")
.csv("dbfs:/FileStore/tables/retailer/customer.dat")

// COMMAND ----------

display(customerDfWithHeader1)

// COMMAND ----------

customerDfWithHeader.columns.size

// COMMAND ----------

val customerWithBirthDay = customerDfWithHeader.select("c_customer_id",
                                                      "c_first_name",
                                                      "c_last_name",
                                                      "c_birth_year",
                                                      "c_birth_month",
                                                      "c_birth_day")

// COMMAND ----------

customerWithBirthDay.show

// COMMAND ----------

import org.apache.spark.sql.functions.{col,column}

// COMMAND ----------

val customerWithBirtDate = customerDfWithHeader.select(col("c_customer_id"),
                                                      col("c_first_name"),
                                                      column("c_last_name"),
                                                      $"c_birth_year",
                                                      'c_birth_month,
                                                      $"c_birth_day")

// COMMAND ----------

display(customerWithBirtDate)

// COMMAND ----------

customerWithBirtDate.printSchema()

// COMMAND ----------

val customerWithBirthDate = customerWithBirtDate

// COMMAND ----------

customerWithBirthDate.printSchema()

// COMMAND ----------

customerWithBirtDate.schema

// COMMAND ----------

val customerDfWithHeader = spark.read
.option("header",true)
.csv("dbfs:/FileStore/tables/retailer/customer.csv")

// COMMAND ----------

customerDfWithHeader.printSchema

// COMMAND ----------

val customerDf = spark.read
.option("header",true)
.option("inferSchema",true)
.csv("dbfs:/FileStore/tables/retailer/customer.csv")

// COMMAND ----------

customerDf.printSchema

// COMMAND ----------

// MAGIC %fs ls /FileStore/tables/retailer

// COMMAND ----------

// MAGIC %fs head dbfs:/FileStore/tables/retailer/customer_address.dat

// COMMAND ----------

val customerAddressDf = spark.read
.option("header",true)
.option("sep","|")
.option("inferSchema",true)
.csv("dbfs:/FileStore/tables/retailer/customer_address.dat")

// COMMAND ----------

customerAddressDf.printSchema

// COMMAND ----------

// MAGIC %fs ls /FileStore/tables/retailer

// COMMAND ----------

// MAGIC %fs head /FileStore/tables/retailer/income_band.dat

// COMMAND ----------


