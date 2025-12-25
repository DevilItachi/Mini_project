# Databricks notebook source
# MAGIC %sql
# MAGIC use catalog mini_project;

# COMMAND ----------

from pyspark.sql.functions import current_timestamp

base_path = "/Volumes/mini_project/cmn_schema/ext_volume"


# COMMAND ----------

from pyspark.sql.functions import current_timestamp

df = (
    spark.read
    .schema(spark.table("bronze.customers").schema)
    .option("header", True)
    .csv(f"{base_path}/Customers.csv")
    .withColumn("ingest_ts", current_timestamp())
)

df.write.format("delta") \
  .mode("append") \
  .saveAsTable("bronze.customers")


# COMMAND ----------

df = (
    spark.read
    .schema(spark.table("bronze.products").schema)
    .option("header", True)
    .csv(f"{base_path}/Products.csv")
    .withColumn("ingest_ts", current_timestamp())
)

df.write.format("delta") \
  .mode("append") \
  .saveAsTable("bronze.products")


# COMMAND ----------

df = (
    spark.read
    .schema(spark.table("bronze.orders").schema)
    .option("header", True)
    .csv(f"{base_path}/Orders.csv")
    .withColumn("ingest_ts", current_timestamp())
)

df.write.format("delta") \
  .mode("append") \
  .saveAsTable("bronze.orders")


# COMMAND ----------

df = (
    spark.read
    .schema(spark.table("bronze.order_items").schema)
    .option("header", True)
    .csv(f"{base_path}/Order_Items.csv")
    .withColumn("ingest_ts", current_timestamp())
)

df.write.format("delta") \
  .mode("append") \
  .saveAsTable("bronze.order_items")

