# Databricks notebook source
# MAGIC %sql
# MAGIC use catalog mini_project

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE OR REPLACE TEMP VIEW customer_ltv_incr AS
# MAGIC SELECT
# MAGIC     c.customer_id,
# MAGIC     c.full_name,
# MAGIC     COUNT(DISTINCT o.order_id)           AS total_orders,
# MAGIC     SUM(oi.quantity * oi.price_per_unit) AS lifetime_value
# MAGIC FROM silver.customers c
# MAGIC JOIN silver.orders o
# MAGIC   ON c.customer_id = o.customer_id
# MAGIC JOIN silver.order_items oi
# MAGIC   ON o.order_id = oi.order_id
# MAGIC GROUP BY c.customer_id, c.full_name;
# MAGIC

# COMMAND ----------

# MAGIC %sql
# MAGIC MERGE INTO gold.customer_lifetime_value tgt
# MAGIC USING customer_ltv_incr src
# MAGIC ON tgt.customer_id = src.customer_id
# MAGIC
# MAGIC WHEN MATCHED THEN
# MAGIC   UPDATE SET
# MAGIC     tgt.full_name      = src.full_name,
# MAGIC     tgt.total_orders   = src.total_orders,
# MAGIC     tgt.lifetime_value = src.lifetime_value
# MAGIC
# MAGIC WHEN NOT MATCHED THEN
# MAGIC   INSERT (
# MAGIC     customer_id,
# MAGIC     full_name,
# MAGIC     total_orders,
# MAGIC     lifetime_value
# MAGIC   )
# MAGIC   VALUES (
# MAGIC     src.customer_id,
# MAGIC     src.full_name,
# MAGIC     src.total_orders,
# MAGIC     src.lifetime_value
# MAGIC   );
# MAGIC
