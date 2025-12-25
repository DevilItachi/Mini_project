# Databricks notebook source
# MAGIC %sql
# MAGIC Create catalog mini_project

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
# MAGIC     customer_id        STRING,
# MAGIC     customer_name      STRING,
# MAGIC     email              STRING,
# MAGIC     phone              STRING,
# MAGIC     city               STRING,
# MAGIC     state              STRING,
# MAGIC     country            STRING,
# MAGIC     ingest_ts          TIMESTAMP
# MAGIC )
# MAGIC USING DELTA;
# MAGIC
# MAGIC
# MAGIC CREATE TABLE IF NOT EXISTS bronze.products (
# MAGIC     product_id         STRING,
# MAGIC     product_name       STRING,
# MAGIC     category           STRING,
# MAGIC     price              STRING,
# MAGIC     ingest_ts          TIMESTAMP
# MAGIC )
# MAGIC USING DELTA;
# MAGIC
# MAGIC
# MAGIC CREATE TABLE IF NOT EXISTS bronze.orders (
# MAGIC     order_id           STRING,
# MAGIC     customer_id        STRING,
# MAGIC     order_date         STRING,
# MAGIC     order_status       STRING,
# MAGIC     ingest_ts          TIMESTAMP
# MAGIC )
# MAGIC USING DELTA;
# MAGIC
# MAGIC
# MAGIC CREATE TABLE IF NOT EXISTS bronze.order_items (
# MAGIC     order_item_id      STRING,
# MAGIC     order_id           STRING,
# MAGIC     product_id         STRING,
# MAGIC     quantity           STRING,
# MAGIC     price              STRING,
# MAGIC     ingest_ts          TIMESTAMP
# MAGIC )
# MAGIC USING DELTA;
# MAGIC

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE TABLE IF NOT EXISTS silver.customers (
# MAGIC     customer_id        INT,
# MAGIC     customer_name      STRING,
# MAGIC     email              STRING,
# MAGIC     phone              STRING,
# MAGIC     city               STRING,
# MAGIC     state              STRING,
# MAGIC     country            STRING,
# MAGIC     updated_ts         TIMESTAMP
# MAGIC )
# MAGIC USING DELTA;
# MAGIC
# MAGIC
# MAGIC CREATE TABLE IF NOT EXISTS silver.products (
# MAGIC     product_id         INT,
# MAGIC     product_name       STRING,
# MAGIC     category           STRING,
# MAGIC     price              DOUBLE,
# MAGIC     updated_ts         TIMESTAMP
# MAGIC )
# MAGIC USING DELTA;
# MAGIC
# MAGIC
# MAGIC CREATE TABLE IF NOT EXISTS silver.orders (
# MAGIC     order_id           INT,
# MAGIC     customer_id        INT,
# MAGIC     order_date         DATE,
# MAGIC     order_status       STRING,
# MAGIC     updated_ts         TIMESTAMP
# MAGIC )
# MAGIC USING DELTA;
# MAGIC
# MAGIC CREATE TABLE IF NOT EXISTS silver.order_details (
# MAGIC     order_id           INT,
# MAGIC     order_date         DATE,
# MAGIC     customer_id        INT,
# MAGIC     customer_name      STRING,
# MAGIC     product_id         INT,
# MAGIC     product_name       STRING,
# MAGIC     category           STRING,
# MAGIC     quantity           INT,
# MAGIC     price              DOUBLE,
# MAGIC     line_amount        DOUBLE,
# MAGIC     updated_ts         TIMESTAMP
# MAGIC )
# MAGIC USING DELTA;
# MAGIC
# MAGIC

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE TABLE IF NOT EXISTS gold.daily_sales (
# MAGIC     order_date         DATE,
# MAGIC     total_orders       INT,
# MAGIC     total_sales        DOUBLE
# MAGIC )
# MAGIC USING DELTA;
# MAGIC
# MAGIC
# MAGIC CREATE TABLE IF NOT EXISTS gold.customer_ltv (
# MAGIC     customer_id        INT,
# MAGIC     customer_name      STRING,
# MAGIC     total_orders       INT,
# MAGIC     lifetime_value     DOUBLE
# MAGIC )
# MAGIC USING DELTA;
# MAGIC
# MAGIC
# MAGIC CREATE TABLE IF NOT EXISTS gold.top_products (
# MAGIC     product_id         INT,
# MAGIC     product_name       STRING,
# MAGIC     total_quantity     INT,
# MAGIC     revenue            DOUBLE
# MAGIC )
# MAGIC USING DELTA;
# MAGIC

# COMMAND ----------

# MAGIC %sql
# MAGIC create schema cmn_schema

# COMMAND ----------

# MAGIC %sql
# MAGIC create volume if not exists cmn_schema.ext_volume
