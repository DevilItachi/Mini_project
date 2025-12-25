# Databricks notebook source
# MAGIC %sql
# MAGIC --Create catalog mini_project

# COMMAND ----------

# MAGIC %sql
# MAGIC use catalog mini_project

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE SCHEMA IF NOT EXISTS bronze;
# MAGIC CREATE SCHEMA IF NOT EXISTS silver;
# MAGIC CREATE SCHEMA IF NOT EXISTS gold;
# MAGIC

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE TABLE IF NOT EXISTS bronze.customers (
# MAGIC     CustomerID    STRING,
# MAGIC     FullName      STRING,
# MAGIC     Email         STRING,
# MAGIC     SignUpDate    STRING,     -- kept STRING in Bronze
# MAGIC     City          STRING,
# MAGIC     Country       STRING,
# MAGIC     ingest_ts     TIMESTAMP
# MAGIC )
# MAGIC USING DELTA;
# MAGIC
# MAGIC
# MAGIC CREATE TABLE IF NOT EXISTS bronze.products (
# MAGIC     ProductID     STRING,
# MAGIC     ProductName   STRING,
# MAGIC     Category      STRING,
# MAGIC     Brand         STRING,
# MAGIC     Price         STRING,     -- STRING because malformed values exist
# MAGIC     ingest_ts     TIMESTAMP
# MAGIC )
# MAGIC USING DELTA;
# MAGIC
# MAGIC
# MAGIC
# MAGIC CREATE TABLE IF NOT EXISTS bronze.orders (
# MAGIC     OrderID       STRING,
# MAGIC     CustomerID    STRING,
# MAGIC     OrderDate     STRING,
# MAGIC     OrderStatus   STRING,
# MAGIC     ingest_ts     TIMESTAMP
# MAGIC )
# MAGIC USING DELTA;
# MAGIC
# MAGIC
# MAGIC
# MAGIC CREATE TABLE IF NOT EXISTS bronze.order_items (
# MAGIC     OrderItemID    STRING,
# MAGIC     OrderID        STRING,
# MAGIC     ProductID      STRING,
# MAGIC     Quantity       STRING,    -- STRING in Bronze
# MAGIC     PricePerUnit   STRING,    -- STRING in Bronze
# MAGIC     ingest_ts      TIMESTAMP
# MAGIC )
# MAGIC USING DELTA;
# MAGIC
# MAGIC

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC
# MAGIC CREATE TABLE IF NOT EXISTS silver.customers (
# MAGIC     customer_id     STRING,     -- CUST-1001
# MAGIC     full_name       STRING,
# MAGIC     email           STRING,
# MAGIC     signup_date     DATE,
# MAGIC     city            STRING,
# MAGIC     country         STRING,
# MAGIC     updated_ts      TIMESTAMP
# MAGIC )
# MAGIC USING DELTA;
# MAGIC
# MAGIC
# MAGIC
# MAGIC CREATE TABLE IF NOT EXISTS silver.products (
# MAGIC     product_id      STRING,     -- PROD-101
# MAGIC     product_name    STRING,
# MAGIC     category         STRING,
# MAGIC     brand            STRING,
# MAGIC     price            DOUBLE,    -- cleaned using try_cast
# MAGIC     updated_ts       TIMESTAMP
# MAGIC )
# MAGIC USING DELTA;
# MAGIC
# MAGIC
# MAGIC
# MAGIC CREATE TABLE IF NOT EXISTS silver.orders (
# MAGIC     order_id        STRING,     -- ORD-50001
# MAGIC     customer_id     STRING,     -- CUST-1001
# MAGIC     order_date      DATE,
# MAGIC     order_status    STRING,
# MAGIC     updated_ts      TIMESTAMP
# MAGIC )
# MAGIC USING DELTA;
# MAGIC
# MAGIC
# MAGIC CREATE TABLE IF NOT EXISTS silver.order_items (
# MAGIC     order_item_id   STRING,
# MAGIC     order_id        STRING,
# MAGIC     product_id      STRING,
# MAGIC     quantity        INT,
# MAGIC     price_per_unit  DOUBLE,
# MAGIC     updated_ts      TIMESTAMP
# MAGIC )
# MAGIC USING DELTA;
# MAGIC

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE TABLE IF NOT EXISTS gold.daily_sales (
# MAGIC     order_date     DATE,
# MAGIC     total_orders   INT,
# MAGIC     total_sales    DOUBLE
# MAGIC )
# MAGIC USING DELTA;
# MAGIC
# MAGIC
# MAGIC
# MAGIC CREATE TABLE IF NOT EXISTS gold.customer_lifetime_value (
# MAGIC     customer_id     STRING,
# MAGIC     full_name       STRING,
# MAGIC     total_orders    INT,
# MAGIC     lifetime_value  DOUBLE
# MAGIC )
# MAGIC USING DELTA;
# MAGIC
# MAGIC
# MAGIC CREATE TABLE IF NOT EXISTS gold.top_products (
# MAGIC     product_id      STRING,
# MAGIC     product_name    STRING,
# MAGIC     total_quantity  INT,
# MAGIC     total_revenue   DOUBLE
# MAGIC )
# MAGIC USING DELTA;
# MAGIC
# MAGIC CREATE TABLE IF NOT EXISTS gold.category_sales (
# MAGIC     category        STRING,
# MAGIC     total_quantity  INT,
# MAGIC     total_revenue   DOUBLE
# MAGIC )
# MAGIC USING DELTA;
# MAGIC

# COMMAND ----------

# MAGIC %sql
# MAGIC create schema cmn_schema

# COMMAND ----------

# MAGIC %sql
# MAGIC create volume if not exists cmn_schema.ext_volume
