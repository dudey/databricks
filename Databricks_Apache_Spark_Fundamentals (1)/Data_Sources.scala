// Databricks notebook source
// MAGIC %md
// MAGIC # [The DataFrameReader API](https://spark.apache.org/docs/latest/api/scala/index.html#org.apache.spark.sql.DataFrameReader)

// COMMAND ----------

// MAGIC %md
// MAGIC 
// MAGIC To read data, we use the DataFrameReader API.
// MAGIC 
// MAGIC The [read](https://spark.apache.org/docs/latest/api/scala/index.html#org.apache.spark.sql.SparkSession@read:org.apache.spark.sql.DataFrameReader) function of the SparkSession gives us access to a DataFrameReader that can be used to read non-streaming data in as a DataFrame.
// MAGIC 
// MAGIC The structure for reading data with the DataFrameReader is as follows:
// MAGIC 
// MAGIC SparkSession.read
// MAGIC 
// MAGIC __.format__ : Parquet, CSV, JSON , ODBC, ORC or text. The default format is Parquet
// MAGIC 
// MAGIC __.option__ : (key, value) pair to configure the parsing of the file
// MAGIC 
// MAGIC __.schema__: In case the file type supports schema, i.e., name and data types of the fields
// MAGIC 
// MAGIC __.load__

// COMMAND ----------

// MAGIC %md
// MAGIC ## [Reading CSV Files](https://spark.apache.org/docs/latest/api/java/org/apache/spark/sql/DataFrameReader.html#csv-scala.collection.Seq-)

// COMMAND ----------

val customerDf = spark.read
.format("csv")
.option("path","/FileStore/tables/retailer/customer.csv")
.option("header",true)
.option("inferSchema",true)
.schema(customerSchema)
.load



// COMMAND ----------

display(customerDf)

// COMMAND ----------

val customerSchema = "c_customer_sk LONG,c_customer_id STRING,c_current_cdemo_sk LONG,c_current_hdemo_sk LONG,"+
"c_current_addr_sk LONG,c_first_shipto_date_sk LONG,c_first_sales_date_sk LONG,c_salutation STRING,c_first_name STRING,"+
"c_last_name STRING,c_preferred_cust_flag STRING,c_birth_day INT,c_birth_month INT,c_birth_year INT,c_birth_country STRING,"+
"c_login STRING,c_email_address STRING,c_last_review_date LONG"

// COMMAND ----------

import org.apache.spark.sql.types._

// COMMAND ----------

val hd_schema = StructType(
  Array(
    StructField("hd_demo_sk",LongType,true),
    StructField("hd_income_band_sk",LongType, true),
    StructField("hd_buy_potential",StringType,true),
    StructField("hd_dep_count",IntegerType,true),
    StructField("hd_vehicle_count",IntegerType)
  )
)

// COMMAND ----------

val household_demographicsDf = 
spark.read
.schema(hd_schema)
.options(
  Map("header" -> "true",
      "sep" -> "|"))
.csv("/FileStore/tables/retailer/household_demographics.dat")

// COMMAND ----------

household_demographicsDf.show

// COMMAND ----------

// DBTITLE 0,Specify the parsing mode
// MAGIC %md
// MAGIC 
// MAGIC #### Specify the parsing mode
// MAGIC 
// MAGIC PERMISSIVE: try to parse all lines: nulls are inserted for missing tokens and extra tokens are ignored. __This is the Default__
// MAGIC 
// MAGIC DROPMALFORMED: drop lines that have fewer or more tokens than expected or tokens which do not match the schema.
// MAGIC 
// MAGIC FAILFAST: abort with a RuntimeException if any malformed line is encountered.

// COMMAND ----------

// MAGIC %fs head /FileStore/tables/retailer/household_demographics_m.dat

// COMMAND ----------

// MAGIC %fs head /FileStore/tables/retailer/household_demographics.dat

// COMMAND ----------

val hd_schema_m = StructType(
  Array(
    StructField("hd_demo_sk",LongType,true),
    StructField("hd_income_band_sk",LongType, true),
    StructField("hd_buy_potential",LongType,true), //Change StringType to Long
    StructField("hd_dep_count",IntegerType,true),
    StructField("hd_vehicle_count",IntegerType)
  )
)

// COMMAND ----------

val household_demographicsDf_m = 
spark.read
.schema(hd_schema_m)
.options(
  Map("header" -> "true",
      "sep" -> "|",
      "mode" -> "DROPMALFORMED"
      ))
.csv("/FileStore/tables/retailer/household_demographics_m.dat")

// COMMAND ----------

display(household_demographicsDf_m)

// COMMAND ----------

// MAGIC %md
// MAGIC 
// MAGIC #### [Save corrupt records](https://docs.databricks.com/spark/latest/spark-sql/handling-bad-records.html)

// COMMAND ----------

val household_demographicsDf_m = 
spark.read
.schema(hd_schema_m)
.options(
  Map("header" -> "true",
      "sep" -> "|",
      "badRecordsPath" -> "/tmp/badRecordsPath"
      ))
.csv("/FileStore/tables/retailer/household_demographics_m.dat")

// COMMAND ----------

household_demographicsDf_m.show(20)

// COMMAND ----------

household_demographicsDf.count

// COMMAND ----------

// MAGIC %fs head /tmp/badRecordsPath/bad_records/part-00000-97e1ed5f-88df-428e-84f1-bcc6cb4cefa5

// COMMAND ----------

// MAGIC %md 
// MAGIC 
// MAGIC ## [Reading JSON files](https://spark.apache.org/docs/latest/api/java/org/apache/spark/sql/DataFrameReader.html#json-scala.collection.Seq-)

// COMMAND ----------

// MAGIC %fs head /FileStore/tables/retailer/single_line.json

// COMMAND ----------

val userDf = spark.read
.schema(userDfSchema)
.format("json")
.option("dateformat","dd.MM.yyyy")
.option("path","/FileStore/tables/retailer/single_line.json")
.load

// COMMAND ----------

display(userDf)

// COMMAND ----------

val userDfSchema = """address STRUCT<city: STRING, country: STRING, state: STRING>,
birthday date,
email STRING,
first_name STRING,
id BIGINT,
last_name STRING,
skills ARRAY<STRING>"""

// COMMAND ----------

val multiLineDf = spark.read
.schema(userDfSchema)
.format("json")
.option("dateformat","dd.MM.yyyy")
.option("multiLine",true)
.option("path","/FileStore/tables/retailer/multi_line.json")
.load

// COMMAND ----------

val  df = multiLineDf.select($"id",$"first_name",$"address.city",$"address",$"skills"(0),$"skills")

// COMMAND ----------

display(df)

// COMMAND ----------

display(multiLineDf)

// COMMAND ----------

// MAGIC %fs head /FileStore/tables/retailer/multi_line.json

// COMMAND ----------

// MAGIC %md
// MAGIC # [The DataFrameWriter API](https://spark.apache.org/docs/latest/api/scala/index.html#org.apache.spark.sql.DataFrameWriter)

// COMMAND ----------

DataFrameWriter
.format
.option 
.partitionBy 
.save

// COMMAND ----------

val customerDf = spark.read
.option("header",true)
.option("inferSchema",true)
.csv("/FileStore/tables/retailer/customer.csv")


// COMMAND ----------

val df = customerDf.selectExpr("c_customer_id as id","c_first_name as first_name","c_last_name as last_name",
                               "c_birth_year as birth_year","c_birth_month as birth_month")

// COMMAND ----------

df.write
.format("csv")
.mode(SaveMode.Overwrite)
.partitionBy("birth_year","birth_month")
.option("path","/tmp/output_csv")
.option("sep","|")
.option("header",true)
.option("compression","snappy")

.save

// COMMAND ----------

display(spark.read.option("header",true).option("sep","|").csv("dbfs:/tmp/output_csv/birth_year=1931/"))

// COMMAND ----------

// MAGIC %fs ls /tmp/output_csv

// COMMAND ----------

df.write
.mode(SaveMode.Overwrite)
.option("path","/tmp/output")
.save

// COMMAND ----------

// MAGIC %fs ls dbfs:/tmp/output

// COMMAND ----------

// MAGIC %fs ls dbfs:/tmp/output_json

// COMMAND ----------

df.rdd.getNumPartitions

// COMMAND ----------

display(spark.read.option("header",true).option("sep","|").csv("/tmp/output_csv"))

// COMMAND ----------

// MAGIC %fs ls /tmp/output_csv

// COMMAND ----------

// MAGIC %fs head /tmp/output_csv/part-00004-tid-4029778548566458525-721d7b6d-2aef-4485-a6fb-4a242dc2a50a-26-1-c000.csv.snappy

// COMMAND ----------

// MAGIC %fs ls /tmp/output_csv

// COMMAND ----------

// MAGIC %fs head tmp/output_csv/part-00000-tid-4157908056781906578-2b4eda2b-d8cf-493a-ba65-74b0b4e327db-17-1-c000.csv

// COMMAND ----------

display(df)

// COMMAND ----------

df.write.saveAsTable("default.customer_tbl")

// COMMAND ----------

// MAGIC %sql
// MAGIC describe formatted default.customer_tbl

// COMMAND ----------

// MAGIC %sql
// MAGIC drop table default.customer_tbl

// COMMAND ----------

df.show

// COMMAND ----------

// MAGIC %fs ls dbfs:/user/hive/warehouse/customer_tbl

// COMMAND ----------

// MAGIC %sql
// MAGIC select * from default.customer_tbl

// COMMAND ----------

// MAGIC %md
// MAGIC # Manually create DataFrame

// COMMAND ----------

// MAGIC %md
// MAGIC #### Convert a scala collection to DataFrame

// COMMAND ----------

val personSeq = 
Seq(
  (0,"Javier","Lewis"),
  (1,"Amy","Moses"))

// COMMAND ----------

val personDf = personSeq.toDF("id","first_name","last_name")

// COMMAND ----------

spark.createDataFrame(personSeq).toDF("id","first_name","last_name")

// COMMAND ----------

res13.show

// COMMAND ----------

import spark.implicits._

// COMMAND ----------

personDf.printSchema

// COMMAND ----------

// MAGIC %md
// MAGIC #### Convert a Row Array to DataFrame

// COMMAND ----------

val personRows = Seq(
  Row(0L,"Javier","Lewis"),
  Row(1L,"Amy","Moses"))

// COMMAND ----------

import org.apache.spark.sql.types._

// COMMAND ----------

val personSchema = new StructType(Array(
new StructField("person_id",LongType,false),
new StructField("first_name",StringType,true),
new StructField("last_name",StringType,true)
))

// COMMAND ----------

spark.createDataFrame(spark.sparkContext.parallelize(personRows),personSchema)

// COMMAND ----------

res3.repartition(1)

// COMMAND ----------

res7.rdd.getNumPartitions
