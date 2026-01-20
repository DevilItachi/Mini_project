# Databricks notebook source
# Required Imports
import sys
import shutil
import os
import datetime
import json
import pyspark
from pyspark.sql.types import *
from pyspark.sql.functions import *
from datetime import datetime
from pyspark.errors import PySparkException

# COMMAND ----------

# Common Variables

dbutils.widgets.text("ENV","")
dbutils.widgets.text("SUB_APP","")
dbutils.widgets.text("WORKSPACE_INSTANCE_URL","")
var_str_env_nm = dbutils.widgets.get("ENV").lower()
var_str_subapp_nm = dbutils.widgets.get("SUB_APP").lower()
var_workspace_instance_url = dbutils.widgets.get("WORKSPACE_INSTANCE_URL")

if var_str_env_nm == 'prod':
        var_catalog_param = 'eu_cog_multiple_prod'
        var_storage_account = ''  
        
        
else :
        var_catalog_param = 'mini_project'
        var_storage_account = '/Volumes/mini_project/cmn_dev_schema/ext_volume'
        
print(f'var_catalog_param = {var_catalog_param}')
print(f'var_storage_account = {var_storage_account}')
#var_source_adls_path = 'inbound'

#Control Schema
var_schema_nm_taskctrl = f'cmn_{var_str_env_nm}_schema'
print(f'var_schema_nm_taskctrl = {var_schema_nm_taskctrl}')

# Raw(Bronze) Schema
var_schema_nm_raw = 'bronze'
print(f'var_schema_nm_raw = {var_schema_nm_raw}')


#Refined(Silver) Schema
var_schema_nm_rfnd = 'silver'
print(f'var_schema_nm_rfnd = {var_schema_nm_rfnd}')

#Consumed(Gold) Schema
var_schema_nm_consd = 'gold'
print(f'var_schema_nm_consd = {var_schema_nm_consd}')

# Framework Table Names 
var_metadata_tbl = 'TBL_METADATA_CONFIG_DETAILS'
var_task_control = 'TBL_TASK_CONTROL'
var_task_run_log = 'TBL_TASK_RUN_LOG'
print(f'var_metadata_tbl = {var_metadata_tbl}')
print(f'var_task_control = {var_task_control}')
print(f'var_task_run_log = {var_task_run_log}')



# COMMAND ----------

if var_str_subapp_nm == 'meridian':
        # var_json_schema = '/Workspace/Shared/PRIMA/src/config/mrdn_file_schema.json' 
        var_json_schema = f'{var_storage_account}/inbound/mrdn_file_schema.json'      
        with open(var_json_schema, 'r') as f:
                data = f.read()
        var_text_files_schema = json.loads(data)
