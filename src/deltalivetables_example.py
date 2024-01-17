# Databricks notebook source
# Example: Defining a Delta Live Table
# Python code
from delta.tables import *

# Define source, transformations, and output
# [Write your code here]


# COMMAND ----------

# Python code in Databricks notebook
df = spark.read.csv("/path/to/sales_data.csv", header=True, inferSchema=True)


# COMMAND ----------

from pyspark.sql.functions import sum

# Transform the data: Sum Amount by ProductID
transformed_df = df.groupBy("ProductID").agg(sum("Amount").alias("TotalAmount"))

# Write the transformed data to a Delta table
transformed_df.write.format("delta").save("/path/to/output/delta_table")

