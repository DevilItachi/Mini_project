# Databricks notebook source
# MAGIC %sql
# MAGIC use catalog mini_project

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE OR REPLACE TEMP VIEW top_products_incr AS
# MAGIC SELECT
# MAGIC     p.product_id,
# MAGIC     p.product_name,
# MAGIC     SUM(oi.quantity)                         AS total_quantity,
# MAGIC     SUM(oi.quantity * oi.price_per_unit)     AS total_revenue
# MAGIC FROM silver.products p
# MAGIC JOIN silver.order_items oi
# MAGIC   ON p.product_id = oi.product_id
# MAGIC GROUP BY p.product_id, p.product_name;
# MAGIC

# COMMAND ----------

# MAGIC %sql
# MAGIC MERGE INTO gold.top_products tgt
# MAGIC USING top_products_incr src
# MAGIC ON tgt.product_id = src.product_id
# MAGIC
# MAGIC WHEN MATCHED THEN
# MAGIC   UPDATE SET
# MAGIC     tgt.product_name   = src.product_name,
# MAGIC     tgt.total_quantity = src.total_quantity,
# MAGIC     tgt.total_revenue  = src.total_revenue
# MAGIC
# MAGIC WHEN NOT MATCHED THEN
# MAGIC   INSERT (
# MAGIC     product_id,
# MAGIC     product_name,
# MAGIC     total_quantity,
# MAGIC     total_revenue
# MAGIC   )
# MAGIC   VALUES (
# MAGIC     src.product_id,
# MAGIC     src.product_name,
# MAGIC     src.total_quantity,
# MAGIC     src.total_revenue
# MAGIC   );
# MAGIC
