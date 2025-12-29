# Databricks notebook source
# MAGIC %md
# MAGIC BRONZE → SILVER (with business logic)
# MAGIC 🔑 Silver layer principles (VERY IMPORTANT)
# MAGIC
# MAGIC Clean & standardized data
# MAGIC
# MAGIC Correct data types
# MAGIC
# MAGIC Deduplication
# MAGIC
# MAGIC Business rules applied
# MAGIC
# MAGIC Join only where needed
# MAGIC
# MAGIC Idempotent (re-runnable)
# MAGIC
# MAGIC 👉 This is why PySpark is preferred here.

# COMMAND ----------

# MAGIC %md
# MAGIC ✅ STEP 1: Silver Customers (Dedup + Cleanup)
# MAGIC
# MAGIC 📘 Business logic
# MAGIC
# MAGIC Remove duplicate customers
# MAGIC
# MAGIC Latest record wins (based on ingest_ts)
# MAGIC
# MAGIC Cast IDs
# MAGIC
# MAGIC Add updated timestamp

# COMMAND ----------

# MAGIC %sql
# MAGIC use catalog mini_project

# COMMAND ----------

# ----------------------------------------
# Silver Customers
# Bronze → Silver
# ----------------------------------------

from pyspark.sql.functions import (
    col,
    row_number,
    current_timestamp,
    to_date
)
from pyspark.sql.window import Window

# ----------------------------------------
# Helper: align DF to target table schema
# ----------------------------------------
# It does three things at once:

# Selects columns in the target table’s order
# Casts each column to the target data type
# Drops any extra columns from the source

def align_to_table_schema(df, table_name):
    target_schema = spark.table(table_name).schema
    return df.select([
        col(field.name).cast(field.dataType)
        for field in target_schema
    ])

# ----------------------------------------
# Step 1: Read Bronze (RAW column names)
# ----------------------------------------
src_df = spark.table("bronze.customers")

# ----------------------------------------
# Step 2: Rename + transform columns
# ----------------------------------------
transformed_df = (
    src_df
    .withColumnRenamed("CustomerID", "customer_id")
    .withColumnRenamed("FullName", "full_name")
    .withColumnRenamed("SignUpDate", "signup_date")
    .withColumn("signup_date", to_date(col("signup_date")))
    .withColumn("updated_ts", current_timestamp())
    .drop("ingest_ts")
)

# ----------------------------------------
# Step 3: Deduplicate (latest record wins)
# ----------------------------------------
window_spec = (
    Window
    .partitionBy("customer_id")
    .orderBy(col("updated_ts").desc())
)

silver_df = (
    transformed_df
    .withColumn("rn", row_number().over(window_spec))
    .filter(col("rn") == 1)
    .drop("rn")
)

# ----------------------------------------
# Step 4: Align schema to Silver table
# ----------------------------------------
silver_df_aligned = align_to_table_schema(
    silver_df,
    "silver.customers"
)

# ----------------------------------------
# Step 5: Write Silver
# ----------------------------------------
(
    silver_df_aligned
    .write
    .format("delta")
    .mode("overwrite")
    .saveAsTable("silver.customers")
)

print("Silver customers load completed successfully")

