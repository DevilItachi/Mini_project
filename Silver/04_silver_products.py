# Databricks notebook source
# MAGIC %sql
# MAGIC use catalog mini_project

# COMMAND ----------

from pyspark.sql.functions import (
    col,
    current_timestamp,
    trim,
    upper,
    when
)

# ----------------------------------------
# Helper
# ----------------------------------------
def align_to_table_schema(df, table_name):
    target_schema = spark.table(table_name).schema
    return df.select([col(f.name).cast(f.dataType) for f in target_schema])

# ----------------------------------------
# Read Bronze
# ----------------------------------------
src_df = spark.table("bronze.products")

# ----------------------------------------
# Rename FIRST (critical)
# ----------------------------------------
df = (
    src_df
    .withColumnRenamed("ProductID", "product_id")
    .withColumnRenamed("ProductName", "product_name")
    .withColumnRenamed("Category", "category")
    .withColumnRenamed("Brand", "brand")
    .withColumnRenamed("Price", "price")   # 🔑 KEY LINE
)

# ----------------------------------------
# Clean & cast
# ----------------------------------------
silver_df = (
    df
    .withColumn("category", upper(trim(col("category"))))
    .withColumn("brand", trim(col("brand")))
    .withColumn(
        "price",
        when(col("price").rlike("^[0-9]+(\\.[0-9]+)?$"), col("price").cast("double"))
        .otherwise(None)
    )
    .withColumn("updated_ts", current_timestamp())
    .drop("ingest_ts")
)

# ----------------------------------------
# Deduplicate
# ----------------------------------------
silver_df = silver_df.dropDuplicates(["product_id"])

# 🔍 DEBUG — KEEP THIS TEMPORARILY
silver_df.printSchema()

# ----------------------------------------
# Align & write
# ----------------------------------------
silver_df_aligned = align_to_table_schema(silver_df, "silver.products")

(
    silver_df_aligned
    .write
    .format("delta")
    .mode("overwrite")
    .saveAsTable("silver.products")
)

print("Silver products load completed successfully")

