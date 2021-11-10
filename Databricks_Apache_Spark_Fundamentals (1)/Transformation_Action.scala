// Databricks notebook source
// MAGIC %md
// MAGIC ![Spark_Architecture](files/tables/retailer/images/Spark_Architecture.png)

// COMMAND ----------

// MAGIC %md
// MAGIC In Apache Spark, DataFrames are immutable. 
// MAGIC 
// MAGIC That means that, once a DataFrame is created, it can not be modified. 
// MAGIC 
// MAGIC Once we create a DataFrame, we cannot change its structure, such as adding/removing  rows/columns or removing or change the values of the columns
// MAGIC 
// MAGIC To change the structure or the content of a DataFrame,  you need to instruct Apache Spark how you would like to modify it to do what you want. 
// MAGIC 
// MAGIC These instructions are called transformations.
// MAGIC 
// MAGIC Transformations are a series of step that’s must be applied to a DataFrame to create another one

// COMMAND ----------

// MAGIC %md
// MAGIC ![Spark_Transformation](files/tables/retailer/images/Spark_Transformation.png)

// COMMAND ----------

// MAGIC %md
// MAGIC __Narrow Transformation__
// MAGIC 
// MAGIC One partition contributes to only one output partition. 
// MAGIC 
// MAGIC For example, when we filter a dataFrame, one partition contribute to only one resulting partitions

// COMMAND ----------

// MAGIC %md
// MAGIC ![Spark_Narrow_Transformatio](files/tables/retailer/images/Spark_Narrow_Transformation.png)

// COMMAND ----------

// MAGIC %md
// MAGIC __Wide Transformations__
// MAGIC 
// MAGIC One input partition contributes to more than one output partitions
// MAGIC 
// MAGIC For example, when we are grouping data.
// MAGIC 
// MAGIC With a wide transformation, data must be exchanged across executors and nodes.
// MAGIC 
// MAGIC A wide transformation is also known as shuffle.

// COMMAND ----------

// MAGIC %md
// MAGIC ![Spark_Wide_Transformation](files/tables/retailer/images/Spark_Wide_Transformation.png)

// COMMAND ----------

// MAGIC %run ./Practice/Schema_Declarations

// COMMAND ----------

val customerSrc =  spark.read
.schema(customerSchema)
.options(datFileReadOptions)
.csv("/FileStore/tables/retailer/customer.dat")

// COMMAND ----------

import org.apache.spark.sql.functions.{count,avg}

// COMMAND ----------

val df1 = customerSrc
.filter($"c_birth_year" > 1980)
.withColumnRenamed("c_birth_year","birth_year")
.withColumnRenamed("c_birth_month","birth_month")
.select("birth_year","birth_month")
// .groupBy("birth_year")
// .agg(count("*"),avg("birth_month"))


// COMMAND ----------

df.show

// COMMAND ----------

val df = customerSrc
.filter($"c_birth_year" > 1980)
.withColumnRenamed("c_birth_year","birth_year")
.withColumnRenamed("c_birth_month","birth_month")
.select("birth_year","birth_month")
.groupBy("birth_year")
.agg(count("*"),avg("birth_month"))


// COMMAND ----------

// MAGIC %md
// MAGIC __Lazy Evaluation__
// MAGIC 
// MAGIC Apache Spark lazily executes transformations.
// MAGIC 
// MAGIC Apache Spark does not modify the data immediately after we specify some transformation.
// MAGIC 
// MAGIC Instead, all the transformations build up a logical transformation plan that is then compiled and optimized to run as efficiently as possible across the cluster
// MAGIC 
// MAGIC We can take a look at the optimized plan - the physical plan - that Spark executes using the __explain__ method

// COMMAND ----------

df.explain

// COMMAND ----------

// MAGIC %md
// MAGIC __Actions__
// MAGIC 
// MAGIC Transformations allow us to build up a logical transformation plan.
// MAGIC 
// MAGIC The transformtion plan come in forms of a Directed Acylic Graph, the __DAG__
// MAGIC 
// MAGIC A Directed Acyclic Graph, is a graph that never goes back and has no cycle.
// MAGIC 
// MAGIC To execute that plan, we run an action.
// MAGIC 
// MAGIC An Action instructs Spark to compute a result from a series of transformations.
// MAGIC 
// MAGIC Actions always returns results.
// MAGIC 
// MAGIC There are three types of actions:
// MAGIC 1.	Actions to view data in console such as, i.e. **df.show**
// MAGIC 2.	Action to collect data to native objects in the respective language, i.e. __df.collect__
// MAGIC 3.	Action to write to output data sources, i.e. df.write.__save__

// COMMAND ----------

df.show // runs the shows action and returns the results. where this actions is run, apache executes the physical plan across the cluster.

// COMMAND ----------

// MAGIC %md
// MAGIC __Spark Jobs__
// MAGIC 
// MAGIC A Spark Job is the highest element of Spark's execution hierarchy.
// MAGIC 
// MAGIC When an action is triggered, Apache Spark starts a Job. 
// MAGIC 
// MAGIC Each Spark job corresponds to one action, and an action may include one or several transformations.
// MAGIC 
// MAGIC A Job has Stages, and the number of stages depends on how many shuffle operations need to take place.
// MAGIC 
// MAGIC A shuffle represents a physical repartitioning of the data – this happens when a wide transformation is performed – for example, when sorting or grouping data.

// COMMAND ----------

df.collect

// COMMAND ----------

// MAGIC %md
// MAGIC __Spark Stages__
// MAGIC 
// MAGIC Each stage corresponds to a shuffle (Wide Transformations) in a Spark programm.
// MAGIC 
// MAGIC Apache Spark starts a stage after each shuffle operation.
// MAGIC 
// MAGIC The number of stages depends on how many shuffle operations need to take place.
// MAGIC 
// MAGIC Stages in Spark consists of tasks.

// COMMAND ----------

df.explain

// COMMAND ----------

// MAGIC %md
// MAGIC __Apache Spark Task__
// MAGIC 
// MAGIC A Task is a combination of data and a set of transformation that run on a single executor.
// MAGIC 
// MAGIC A Task is just a unit of computation applied to a unit of data (the partition).
// MAGIC 
// MAGIC If we have 1 partition, we will have one task.
// MAGIC 
// MAGIC If there are 200 partitions we will have 200 tasks that can be executed in parallel

// COMMAND ----------

df.show
