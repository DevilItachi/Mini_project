# Databricks notebook source
# MAGIC %sql
# MAGIC use catalog mini_project;
# MAGIC use schema cmn_dev_schema;
# MAGIC
# MAGIC CREATE TABLE IF NOT EXISTS tbl_metadata_config_details (
# MAGIC   application STRING,
# MAGIC   job_name STRING,
# MAGIC   task_name STRING,
# MAGIC   grouping_id STRING,
# MAGIC   transformation_type STRING,
# MAGIC   execute_child_notebook STRING,
# MAGIC   load_type STRING,
# MAGIC   source_adls_path STRING,
# MAGIC   source_file_extension STRING,
# MAGIC   source_file_header STRING,
# MAGIC   source_file_delimiter STRING,
# MAGIC   source_file_escape_quote STRING,
# MAGIC   source_schema STRING,
# MAGIC   source_table STRING,
# MAGIC   target_notebook STRING,
# MAGIC   target_notebook_parameters STRING,
# MAGIC   target_schema STRING,
# MAGIC   target_table STRING,
# MAGIC   merge_cols STRING,
# MAGIC   load_query STRING,
# MAGIC   archival_adls_path STRING,
# MAGIC   is_active STRING,
# MAGIC   created_user_id STRING,
# MAGIC   created_timestamp STRING,
# MAGIC   last_updated_user_id STRING,
# MAGIC   last_updated_timestamp STRING,
# MAGIC   ingest_all_files_from_adls CHAR(1),
# MAGIC   ingest_latest_files_from_adls CHAR(1),
# MAGIC   ingest_oldest_files_from_adls CHAR(1),
# MAGIC   run_only_if_prev_job_success CHAR(1),
# MAGIC   skip_rows INT,
# MAGIC   file_encoding STRING)
# MAGIC USING delta
# MAGIC CLUSTER BY (application,job_name)
# MAGIC TBLPROPERTIES (
# MAGIC   'delta.checkpointPolicy' = 'v2',
# MAGIC   'delta.enableDeletionVectors' = 'true',
# MAGIC   'delta.enableRowTracking' = 'true',
# MAGIC   'delta.feature.appendOnly' = 'supported',
# MAGIC   'delta.feature.deletionVectors' = 'supported',
# MAGIC   'delta.feature.invariants' = 'supported',
# MAGIC   'delta.feature.rowTracking' = 'supported',
# MAGIC   'delta.feature.v2Checkpoint' = 'supported');
# MAGIC
# MAGIC CREATE TABLE IF NOT EXISTS tbl_task_control (
# MAGIC   job_name STRING,
# MAGIC   job_id STRING,
# MAGIC   parent_run_id STRING,
# MAGIC   task_name STRING,
# MAGIC   task_run_id STRING,
# MAGIC   filter_start_ts TIMESTAMP,
# MAGIC   filter_end_ts TIMESTAMP,
# MAGIC   execution_start_ts TIMESTAMP,
# MAGIC   execution_end_ts TIMESTAMP,
# MAGIC   source_count INT,
# MAGIC   target_count INT,
# MAGIC   status STRING,
# MAGIC   last_processed_file STRING)
# MAGIC USING delta
# MAGIC CLUSTER BY (job_id,parent_run_id)
# MAGIC TBLPROPERTIES (
# MAGIC   'delta.checkpointPolicy' = 'v2',
# MAGIC   'delta.enableDeletionVectors' = 'true',
# MAGIC   'delta.enableRowTracking' = 'true',
# MAGIC   'delta.feature.appendOnly' = 'supported',
# MAGIC   'delta.feature.deletionVectors' = 'supported',
# MAGIC   'delta.feature.invariants' = 'supported',
# MAGIC   'delta.feature.rowTracking' = 'supported',
# MAGIC   'delta.feature.v2Checkpoint' = 'supported')
# MAGIC   ;
# MAGIC
# MAGIC CREATE TABLE IF NOT EXISTS tbl_task_run_log (
# MAGIC   job_id STRING,
# MAGIC   parent_run_id STRING,
# MAGIC   task_run_id STRING,
# MAGIC   procedure_name STRING,
# MAGIC   create_dt TIMESTAMP,
# MAGIC   message_type STRING,
# MAGIC   priority DECIMAL(38,0),
# MAGIC   message_cd STRING,
# MAGIC   primary_message STRING,
# MAGIC   secondary_message STRING,
# MAGIC   login_id STRING,
# MAGIC   data_key STRING,
# MAGIC   records_in DECIMAL(38,0),
# MAGIC   records_out DECIMAL(38,0),
# MAGIC   procedure_runtime_stamp STRING,
# MAGIC   records_updated DECIMAL(38,0))
# MAGIC USING delta
# MAGIC CLUSTER BY (job_id,parent_run_id)
# MAGIC TBLPROPERTIES (
# MAGIC   'delta.checkpointPolicy' = 'v2',
# MAGIC   'delta.enableDeletionVectors' = 'true',
# MAGIC   'delta.enableRowTracking' = 'true',
# MAGIC   'delta.feature.appendOnly' = 'supported',
# MAGIC   'delta.feature.deletionVectors' = 'supported',
# MAGIC   'delta.feature.invariants' = 'supported',
# MAGIC   'delta.feature.rowTracking' = 'supported',
# MAGIC   'delta.feature.v2Checkpoint' = 'supported');
# MAGIC
# MAGIC   CREATE TABLE IF NOT EXISTS tbl_email_recipients (
# MAGIC   recipient_email_id STRING,
# MAGIC   notify_start BOOLEAN,
# MAGIC   notify_success BOOLEAN,
# MAGIC   notify_warning BOOLEAN,
# MAGIC   notify_failed BOOLEAN,
# MAGIC   notify_to_application_group STRING,
# MAGIC   send_in_seq INT,
# MAGIC   is_active BOOLEAN,
# MAGIC   crt_by STRING,
# MAGIC   crt_ts TIMESTAMP,
# MAGIC   upd_by STRING,
# MAGIC   upd_ts TIMESTAMP)
# MAGIC USING delta;
# MAGIC
# MAGIC
# MAGIC  
