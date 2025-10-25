# Databricks notebook source
dbutils.fs.ls('dbfs:/mnt/predictiveanalytics/2024-01-16_19-46-31/')

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE STREAMING LIVE TABLE customers
# MAGIC COMMENT "The customers buying finished products, ingested from adls mount folder."
# MAGIC TBLPROPERTIES ("myCompanyPipeline.quality" = "mapping")
# MAGIC AS SELECT * FROM cloud_files("dbfs:/mnt/predictiveanalytics/2024-01-16_19-46-31/", "csv");
# MAGIC

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC select * from LIVE.customers

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC select * from LIVE.customers
# MAGIC

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE STREAMING LIVE TABLE sales_orders_raw
# MAGIC COMMENT "The raw sales orders, ingested from /databricks-datasets."
# MAGIC TBLPROPERTIES ("myCompanyPipeline.quality" = "bronze")
# MAGIC AS
# MAGIC SELECT * FROM cloud_files("/databricks-datasets/retail-org/sales_orders/", "json", map("cloudFiles.inferColumnTypes", "true"))

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE STREAMING LIVE TABLE sales_orders_cleaned(
# MAGIC   CONSTRAINT valid_order_number EXPECT (order_number IS NOT NULL) ON VIOLATION DROP ROW
# MAGIC )
# MAGIC PARTITIONED BY (order_date)
# MAGIC COMMENT "The cleaned sales orders with valid order_number(s) and partitioned by order_datetime."
# MAGIC TBLPROPERTIES ("myCompanyPipeline.quality" = "silver")
# MAGIC AS
# MAGIC SELECT f.customer_id, f.customer_name, f.number_of_line_items, 
# MAGIC   TIMESTAMP(from_unixtime((cast(f.order_datetime as long)))) as order_datetime, 
# MAGIC   DATE(from_unixtime((cast(f.order_datetime as long)))) as order_date, 
# MAGIC   f.order_number, f.ordered_products, c.state, c.city, c.lon, c.lat, c.units_purchased, c.loyalty_segment
# MAGIC   FROM STREAM(LIVE.sales_orders_raw) f
# MAGIC   LEFT JOIN LIVE.customers c
# MAGIC       ON c.customer_id = f.customer_id
# MAGIC      AND c.customer_name = f.customer_name

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE LIVE TABLE sales_order_in_la
# MAGIC COMMENT "Sales orders in LA."
# MAGIC TBLPROPERTIES ("myCompanyPipeline.quality" = "gold")
# MAGIC AS
# MAGIC SELECT city, order_date, customer_id, customer_name, ordered_products_explode.curr, SUM(ordered_products_explode.price) as sales, SUM(ordered_products_explode.qty) as quantity, COUNT(ordered_products_explode.id) as product_count
# MAGIC FROM (
# MAGIC   SELECT city, order_date, customer_id, customer_name, EXPLODE(ordered_products) as ordered_products_explode
# MAGIC   FROM LIVE.sales_orders_cleaned 
# MAGIC   WHERE city = 'Los Angeles'
# MAGIC   )
# MAGIC GROUP BY order_date, city, customer_id, customer_name, ordered_products_explode.curr

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE LIVE TABLE sales_order_in_chicago
# MAGIC COMMENT "Sales orders in Chicago."
# MAGIC TBLPROPERTIES ("myCompanyPipeline.quality" = "gold")
# MAGIC AS
# MAGIC SELECT city, order_date, customer_id, customer_name, ordered_products_explode.curr, SUM(ordered_products_explode.price) as sales, SUM(ordered_products_explode.qty) as quantity, COUNT(ordered_products_explode.id) as product_count
# MAGIC FROM (
# MAGIC   SELECT city, order_date, customer_id, customer_name, EXPLODE(ordered_products) as ordered_products_explode
# MAGIC   FROM LIVE.sales_orders_cleaned 
# MAGIC   WHERE city = 'Chicago'
# MAGIC   )
# MAGIC GROUP BY order_date, city, customer_id, customer_name, ordered_products_explode.curr
