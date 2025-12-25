# Databricks notebook source
# MAGIC %md
# MAGIC From here on:
# MAGIC
# MAGIC Only SQL
# MAGIC
# MAGIC Only aggregates
# MAGIC
# MAGIC Only business questions

# COMMAND ----------

# MAGIC %md
# MAGIC 🔑 Gold principles (lock this in)
# MAGIC
# MAGIC Built only from Silver
# MAGIC
# MAGIC Aggregated, BI-ready
# MAGIC
# MAGIC No raw columns
# MAGIC
# MAGIC No schema drama
# MAGIC
# MAGIC Rerunnable (CREATE OR REPLACE)

# COMMAND ----------

# MAGIC %md
# MAGIC ✅ STEP 1: gold.daily_sales
# MAGIC 📊 Business question
# MAGIC
# MAGIC How much did we sell per day?

# COMMAND ----------

# MAGIC %sql
# MAGIC use catalog mini_project

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE OR REPLACE TEMP VIEW daily_sales_incr AS
# MAGIC SELECT
# MAGIC     o.order_date,
# MAGIC     COUNT(DISTINCT o.order_id)              AS total_orders,
# MAGIC     SUM(oi.quantity * oi.price_per_unit)    AS total_sales
# MAGIC FROM silver.orders o
# MAGIC JOIN silver.order_items oi
# MAGIC   ON o.order_id = oi.order_id
# MAGIC WHERE o.order_date >= date_sub(current_date(), 1)   -- incremental window
# MAGIC GROUP BY o.order_date;
# MAGIC

# COMMAND ----------

# MAGIC %sql
# MAGIC MERGE INTO gold.daily_sales tgt
# MAGIC USING daily_sales_incr src
# MAGIC ON tgt.order_date = src.order_date
# MAGIC
# MAGIC WHEN MATCHED THEN
# MAGIC   UPDATE SET
# MAGIC     tgt.total_orders = src.total_orders,
# MAGIC     tgt.total_sales  = src.total_sales
# MAGIC
# MAGIC WHEN NOT MATCHED THEN
# MAGIC   INSERT (
# MAGIC     order_date,
# MAGIC     total_orders,
# MAGIC     total_sales
# MAGIC   )
# MAGIC   VALUES (
# MAGIC     src.order_date,
# MAGIC     src.total_orders,
# MAGIC     src.total_sales
# MAGIC   );
# MAGIC

# COMMAND ----------

# MAGIC %md
# MAGIC ✅ Incremental
# MAGIC ✅ Idempotent
# MAGIC ✅ Production-safe

# COMMAND ----------

# MAGIC %md
# MAGIC Why MERGE fits Databricks perfectly
# MAGIC
# MAGIC Delta Lake ACID guarantees
# MAGIC
# MAGIC Idempotent reruns
# MAGIC
# MAGIC Handles late-arriving data
# MAGIC
# MAGIC No locking issues
# MAGIC
# MAGIC Time travel friendly
