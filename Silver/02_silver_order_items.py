# Databricks notebook source
# MAGIC %sql
# MAGIC use catalog mini_project

# COMMAND ----------

# ----------------------------------------
# Silver Order Items
# Bronze → Silver
# ----------------------------------------

from pyspark.sql.functions import (
    col,
    current_timestamp,
    when
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
src_df = spark.table("bronze.order_items")

# ----------------------------------------
# Step 2: Rename columns FIRST
# ----------------------------------------
df = (
    src_df
    .withColumnRenamed("OrderItemID", "order_item_id")
    .withColumnRenamed("OrderID", "order_id")
    .withColumnRenamed("ProductID", "product_id")
    .withColumnRenamed("Quantity", "quantity")
    .withColumnRenamed("PricePerUnit", "price_per_unit")
)

# ----------------------------------------
# Step 3: Clean & cast safely
# ----------------------------------------
silver_df = (
    df
    .withColumn(
        "quantity",
        when(col("quantity").rlike("^[0-9]+$"), col("quantity").cast("int"))
        .otherwise(None)
    )
    .withColumn(
        "price_per_unit",
        when(
            col("price_per_unit").rlike("^[0-9]+(\\.[0-9]+)?$"),
            col("price_per_unit").cast("double")
        ).otherwise(None)
    )
    .withColumn("updated_ts", current_timestamp())
    .drop("ingest_ts")
)

# ----------------------------------------
# Step 4: Deduplicate (safety)
# ----------------------------------------
silver_df = silver_df.dropDuplicates(["order_item_id"])

# 🔍 Optional debug (remove later)
silver_df.printSchema()

# ----------------------------------------
# Step 5: Align & write
# ----------------------------------------
silver_df_aligned = align_to_table_schema(
    silver_df,
    "silver.order_items"
)

(
    silver_df_aligned
    .write
    .format("delta")
    .mode("overwrite")
    .saveAsTable("silver.order_items")
)

print("Silver order_items load completed successfully")

