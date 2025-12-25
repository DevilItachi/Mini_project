# Databricks notebook source
# MAGIC %sql
# MAGIC use catalog mini_project

# COMMAND ----------

# ----------------------------------------
# Silver Orders
# Bronze → Silver
# ----------------------------------------

from pyspark.sql.functions import (
    col,
    current_timestamp,
    to_date,
    upper,
    trim
)

# ----------------------------------------
# Helper: align DF to target table schema
# ----------------------------------------
def align_to_table_schema(df, table_name):
    target_schema = spark.table(table_name).schema
    return df.select([col(f.name).cast(f.dataType) for f in target_schema])

# ----------------------------------------
# Step 1: Read Bronze
# ----------------------------------------
src_df = spark.table("bronze.orders")

# ----------------------------------------
# Step 2: Rename + clean
# ----------------------------------------
silver_df = (
    src_df
    .withColumnRenamed("OrderID", "order_id")
    .withColumnRenamed("CustomerID", "customer_id")
    .withColumnRenamed("OrderDate", "order_date")
    .withColumnRenamed("OrderStatus", "order_status")

    # clean text
    .withColumn("order_status", upper(trim(col("order_status"))))

    # parse date safely
    .withColumn("order_date", to_date(col("order_date")))

    .withColumn("updated_ts", current_timestamp())
    .drop("ingest_ts")
)

# ----------------------------------------
# Step 3: Business rule
# Remove CANCELLED orders
# ----------------------------------------
silver_df = silver_df.filter(col("order_status") != "CANCELLED")

# ----------------------------------------
# Step 4: Deduplicate (safety)
# ----------------------------------------
silver_df = silver_df.dropDuplicates(["order_id"])

# 🔍 Optional debug (remove later)
silver_df.printSchema()

# ----------------------------------------
# Step 5: Align & write
# ----------------------------------------
silver_df_aligned = align_to_table_schema(
    silver_df,
    "silver.orders"
)

(
    silver_df_aligned
    .write
    .format("delta")
    .mode("overwrite")
    .saveAsTable("silver.orders")
)

print("Silver orders load completed successfully")

