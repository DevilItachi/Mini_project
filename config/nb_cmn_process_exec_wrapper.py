# Databricks notebook source
# MAGIC %run ./nb_cmn_env_config

# COMMAND ----------

# MAGIC %run ./nb_cmn_utils

# COMMAND ----------

dbutils.widgets.text("ENV", "")
dbutils.widgets.text("schema_nm_raw", "")
dbutils.widgets.text("storage_account", "raw")

dbutils.widgets.text("RUN_DT", "")
dbutils.widgets.text("JOB_ID", "1")
dbutils.widgets.text("JOB_RUN_ID", "1234")
dbutils.widgets.text("TASK_RUN_ID", "123456")


dbutils.widgets.text("JOB_NM", "")

dbutils.widgets.text("TASK_NM", "")

dbutils.widgets.text("GROUPING_ID", "history_gd_grp1")
dbutils.widgets.text("JOB_STRT_TMS","")

# COMMAND ----------

var_env_nm = dbutils.widgets.get("ENV")
var_job_name = dbutils.widgets.get("JOB_NM")
var_workspace_instance_url =  dbutils.widgets.get("WORKSPACE_INSTANCE_URL")
try:
  var_grouping_id = dbutils.widgets.get("GROUPING_ID")
except Exception as e:
  var_grouping_id=None
print(f'var_grouping_id : {var_grouping_id}')

# COMMAND ----------

var_job_id = dbutils.widgets.get("JOB_ID")
var_parent_run_id = dbutils.widgets.get("JOB_RUN_ID")
var_task_run_id = dbutils.widgets.get("TASK_RUN_ID") 

var_task_name = dbutils.widgets.get("TASK_NM")
var_job_strt_tms = dbutils.widgets.get("JOB_STRT_TMS")

# COMMAND ----------

var_job_status=1
var_recon_status=1


# COMMAND ----------

import datetime
from pyspark.sql.types import *
import sys
import re
from pyspark.errors import PySparkException
import time
import json

# COMMAND ----------

#print('Grouping id is : ', var_grouping_id)
load_status = 'COMPLETED'
# df_ins_ts=spark.sql("select current_date() as ins_tms")
# var_ins_tms= df_ins_ts.select('ins_tms').collect()[0][0]

var_ins_tms = datetime.datetime.now().strftime("%Y-%m-%d")
execution_start_time = datetime.datetime.now().replace(microsecond = 0)


filter_start_time = get_filter_start_ts(var_job_name, var_task_name)
filter_end_time   = execution_start_time
execution_start_time = datetime.datetime.now().replace(microsecond=0)
execution_end_time = datetime.datetime.now().replace(microsecond=0)

#for back date file processing
var_is_backdated = 0

#assign 0 if running a failure task
var_pre_job_succes_status = 0



# COMMAND ----------

# DBTITLE 1,Cell 9
try:
        #check if process is triggered for restartability, to start from last failure point, default 0
        #print("Verify previous failed jobs!")
        history_df = spark.sql(f'DESCRIBE HISTORY {var_catalog_param}.{var_schema_nm_taskctrl}.{var_task_control} LIMIT 1')
        version_no = history_df.collect()[0][0]
        print(f'version_no is : {version_no}')
        #get recordshaving status other then success(2) or force completed(4)
        v_sql_qry = f"""SELECT * FROM (
                        SELECT job_name, job_id, parent_run_id, task_name, task_run_id,status,
                        row_number() over(order by execution_start_ts desc) as rn
                        FROM {var_catalog_param}.{var_schema_nm_taskctrl}.{var_task_control} version as of {version_no}
                        WHERE job_name = '{var_job_name}' AND parent_run_id = '{var_parent_run_id}'
                        ) tmp 
                        """
        print(f'v_sql_qry : {v_sql_qry}')
        df_result = spark.sql(v_sql_qry)
        df_result.createOrReplaceTempView(var_task_control+'_'+var_job_name+var_task_run_id+'_tmp')
        print(df_result.count())
        # if df_result.count() > 0 : # BB Performance tuning - 02/12/24
        if df_result.isEmpty() == False:
                var_task_and_grouping_cond = ''
                print("Job will be triggered from failure point! ")
                #var_pre_run_id = df_result.select('parent_run_id').collect()[0][0]
                if len(str(var_grouping_id)) > 0 :
                        print("1=============")
                        var_task_and_grouping_cond = f" and  task_name in (select task_name from   {var_catalog_param}.{var_schema_nm_taskctrl}.{var_metadata_tbl} where grouping_id like '{var_grouping_id}' and is_active ='Y' )" 
                else:
                        print("2=============")
                        var_task_and_grouping_cond = f" and  task_name like '{var_task_name}'" 
                print(f'var_task_and_grouping_cond : {var_task_and_grouping_cond}')

                sql_query = f"select * from  {var_task_control}_{var_job_name}{var_task_run_id}_tmp  where parent_run_id like '{var_parent_run_id}' {var_task_and_grouping_cond} and status = 'FAILED' "
                print(f'sql_query : {sql_query}')
                df_result_qry = spark.sql(sql_query)
                if df_result_qry.isEmpty() == False:
                        #var_last_run_status =  df_result.select('status').collect()[0][0]
                        #if var_last_run_status =='FAILED':
                        if 'HISTORY' in var_job_name or len(str(var_grouping_id).strip()) > 0:
                                fltr_condition = f"""  job_name ='{var_job_name}' AND UPPER(is_active) = 'Y' 
                                AND grouping_id = '{var_grouping_id}' AND (job_name,TASK_NAME) in (select job_name,TASK_NAME from  """+var_task_control+"""_"""+var_job_name+var_task_run_id+"""_tmp  as lg where parent_run_id = '""" + var_parent_run_id +"""' and task_name not in (select task_name from  """+var_task_control+"""_"""+var_job_name+var_task_run_id+"""_tmp where status in ('INITIATED','NOT STARTED','COMPLETED')))"""
                        else:
                                fltr_condition ="""  job_name = '"""+ var_job_name+ """'  AND  UPPER         (is_active)   ='Y' and (job_name,TASK_NAME) in (select job_name,TASK_NAME from   """+var_task_control+"""_"""+var_job_name+var_task_run_id+"""_tmp as lg where parent_run_id = '""" + var_parent_run_id +"""' and task_name not in (select task_name from  """+var_task_control+"""_"""+var_job_name+var_task_run_id+"""_tmp where status in ('INITIATED','NOT STARTED','COMPLETED')))"""
                        print(f'fltr_condition : {fltr_condition}') 

                        #Insert new log for failed job with NOT STARTED status
                        var_sql_qry = """INSERT INTO  """ + var_catalog_param + """."""+ var_schema_nm_taskctrl +"""."""+var_task_control +""" (job_name ,job_id ,parent_run_id ,task_name ,status)
                                        SELECT         DISTINCT        
                                                job_name ,'"""+var_job_id+"""' ,'"""+var_parent_run_id+"""' ,task_name ,'NOT STARTED' 
                                FROM """ + var_catalog_param + """."""+ var_schema_nm_taskctrl +"""."""+ var_metadata_tbl +"""  WHERE """+  fltr_condition + var_task_and_grouping_cond

                        #assign 0 if running a failure task
                        var_pre_job_succes_status = 0
                        print(f'var_sql_qry : {var_sql_qry}')
                else:
                        #print("If previous batch completed without failures1, so job will start from first step")
                        if 'HISTORY' in var_job_name  or len(str(var_grouping_id).strip()) > 0:
                                fltr_condition = f"""  job_name = '{var_job_name}' AND UPPER(is_active) = 'Y' 
                                AND grouping_id = '{var_grouping_id}'"""
                                #Insert  logs for all task with Not started status
                                var_sql_qry ="""INSERT INTO  """ + var_catalog_param + """."""+ var_schema_nm_taskctrl +"""."""+var_task_control +""" (job_name ,job_id ,parent_run_id ,task_name ,status)
                                                SELECT   distinct              
                                                        job_name ,'"""+var_job_id+"""' ,'"""+var_parent_run_id+"""' ,task_name ,'NOT STARTED' 
                                        FROM """ + var_catalog_param + """."""+ var_schema_nm_taskctrl +"""."""+ var_metadata_tbl +"""  WHERE """+  fltr_condition  
                                
                        else:
                                fltr_condition = """  job_name ='"""+ var_job_name+ """'  AND  UPPER(is_active) = 'Y'"""
                        
                                var_sql_qry = "select 1 "
        else:
                #print("If previous batch completed without failures, so job will start from first step")
                if 'HISTORY' in var_job_name or len(str(var_grouping_id).strip()) > 0:
                        fltr_condition = f"""  job_name ='{var_job_name}' AND UPPER(is_active) = 'Y' 
                        AND grouping_id = '{var_grouping_id}'"""
                else:
                        fltr_condition = """  job_name = '"""+ var_job_name+ """' AND    UPPER(is_active) = 'Y'  """
                print(f'fltr_condition : {fltr_condition}')
        
                #Insert  logs for all task with Not started status
                var_sql_qry = """INSERT INTO  """ + var_catalog_param + """."""+ var_schema_nm_taskctrl +"""."""+var_task_control +""" (job_name ,job_id ,parent_run_id ,task_name ,status)
                                SELECT   distinct              
                                        job_name ,'"""+var_job_id+"""' ,'"""+var_parent_run_id+"""' ,task_name ,'NOT STARTED' 
                        FROM """ + var_catalog_param + """."""+ var_schema_nm_taskctrl +"""."""+ var_metadata_tbl +"""  WHERE """+  fltr_condition  
                print(f'var_sql_qry : {var_sql_qry}')
                taskk = spark.sql(var_sql_qry) 
        
        if len(str(var_grouping_id).strip()) > 0:
                fnl_fltr_condition =  fltr_condition + """  AND  grouping_id = '"""+ var_grouping_id+ """' """       
        else:
                fnl_fltr_condition =  fltr_condition + """  AND  task_name = '"""+ var_task_name+ """' """
        print(f'fnl_fltr_condition : {fnl_fltr_condition}')      
        var_sql_qry_1 = """ SELECT  distinct
                         'NOT STARTED' running_status_tracker,*
                                FROM """ + var_catalog_param + """."""+ var_schema_nm_taskctrl +"""."""+ var_metadata_tbl +""" 
                                where """+ fnl_fltr_condition 
        print(f'var_sql_qry_1 : {var_sql_qry_1}')
        df_result = spark.sql( var_sql_qry_1 )
        df_result.createOrReplaceTempView(var_metadata_tbl+"""_"""+var_job_name+ str(var_task_run_id)+"""_tmp""")
        spark.sql("""create or replace  table """ + var_catalog_param + """."""+ var_schema_nm_rfnd +""".""" +var_metadata_tbl+"""_"""+var_job_name+ str(var_task_run_id)+"""_tmp as select * from  """ +var_metadata_tbl+"""_"""+var_job_name+ str(var_task_run_id)+"""_tmp """)
        
        df_result = spark.sql("""select * from   """ + var_catalog_param + """."""+ var_schema_nm_rfnd +""".""" +var_metadata_tbl+"""_"""+var_job_name+ str(var_task_run_id)+"""_tmp  """)
        #df_result.createOrReplaceTempView(var_metadata_tbl+'_'+var_job_name+'_tmp')
        print('number of process in tmp : ',df_result.count())
        df_result_cnt = df_result.count()
        print("Number of task to execute :", df_result_cnt)
        #print('-------Add entry in task control------------------',var_sql_qry)  
        print('-------Added entry in task control------------------')    

        var_sql_qry = ''
        
        if df_result_cnt > 0 :
                print("inside task execution ")
                #df_result = spark.sql("""select * from  """ +var_metadata_tbl+"""_"""+var_job_name+ str(var_task_run_id)+"""_tmp""")
                #print(df_result.count())
                var_transformation_type = df_result.select('transformation_type').collect()[0][0]

                i=0
                print("Number of process to execute : ", df_result.count())

                #src load logic inside for loop to handle  multiple jobs 
                #sys.exit()
                result_lst = df_result.toPandas().to_json(orient='records')
                for metadata in json.loads(result_lst):
                        #print(f'loading started for task - {i+1}')                              
                        var_src_cnt='0' 
                        var_tgt_tbl_cnt='0'
                        # Assign column values to variables  
                        var_application = metadata['application']
                        var_job_nm = metadata['job_name']
                        var_task_name = metadata['task_name']
                        var_grouping_id = metadata['grouping_id']
                        var_transformation_type =metadata['transformation_type']
                        var_execute_child_notebook = metadata['execute_child_notebook']
                        var_load_type = metadata['load_type']
                        var_source_adls_path = metadata['source_adls_path']
                        var_source_file_extension = metadata['source_file_extension']
                        var_source_file_header = metadata['source_file_header']
                        var_source_file_delimiter = metadata['source_file_delimiter']
                        var_source_file_escape_quote = metadata['source_file_escape_quote']
                        var_source_schema = metadata['source_schema']
                        var_source_table = metadata['source_table']
                        var_target_notebook = metadata['target_notebook']
                        var_target_notebook_parameters = metadata['target_notebook_parameters']
                        var_target_schema = metadata['target_schema']
                        var_target_table = metadata['target_table']
                        var_merge_cols = metadata['merge_cols']
                        var_load_query = metadata['load_query']
                        var_archival_adls_path = metadata['archival_adls_path']
                        var_is_active = metadata['is_active']
                        var_ingest_all_files_from_adls = metadata['ingest_all_files_from_adls']
                        var_ingest_latest_files_from_adls = metadata['ingest_latest_files_from_adls']
                        var_ingest_oldest_files_from_adls = metadata['ingest_oldest_files_from_adls']
                        var_run_only_if_prev_job_success = metadata['run_only_if_prev_job_success']
                        var_skip_rows = metadata['skip_rows']
                        var_file_encoding = metadata['file_encoding']

                        if var_transformation_type is None:
                                #print("Transformation type is None:")
                                var_transformation_type = "None"

                        if var_load_type is None:
                                #print("Load type is None:")
                                var_load_type = "None"
                        
                        try:
                                if var_execute_child_notebook.strip().upper() == 'Y':
                                        var_execute_child_notebook='Y'
                                else:
                                        var_execute_child_notebook='N'
                        except:
                                var_execute_child_notebook='N'      

                        print("Transformation type is : ",var_transformation_type)
                        err_msg=''
                        var_is_archived=1 

                        if var_transformation_type.upper() in ('ADLS_TO_RAW','ADLS_TO_REFINED','ADLS_TO_CONSUMED') and var_execute_child_notebook !='Y':                
                                # update as initiated in  task  cntrl 
                                load_status = 'INITIATED' 
                                print(f'load_status : {load_status}')
                                execution_start_time = datetime.datetime.now().replace(microsecond=0)
                                update_task_control(var_job_name,var_job_id,var_parent_run_id,var_task_name,var_task_run_id,str(filter_start_time),str(filter_end_time),execution_start_time, execution_end_time, 'NULL', 'NULL', load_status)
                                #Get full path of file
                                var_file_path = var_storage_account+"""/"""+var_source_adls_path 
                                print(f'var_file_path : {var_file_path}')
                                #File header parameter 1 for True and others as False  
                                if var_source_file_header == 'Y':
                                        var_header='True'
                                else:
                                        var_header='False'
                                try:
                                        filter_prev_end_ts = get_filter_start_ts(var_job_name,var_task_name)
                                        print(f'filter_prev_end_ts : {filter_prev_end_ts}')
                                        try:
                                                var_ingest_all_files_from_adls.strip().upper() == 'Y'
                                        except:
                                                var_ingest_all_files_from_adls='N'
                                        print(f'var_ingest_all_files_from_adls : {var_ingest_all_files_from_adls}')
                                        if var_ingest_all_files_from_adls.strip().upper() == 'Y':
                                                print('Ingesting all files from ADLS')
                                                var_src_cnt, var_tgt_cnt,execution_end_time ,last_processed_file, files_processed = fn_ingest_all_files_from_adls(var_file_path, var_load_type, var_catalog_param, var_schema_nm_taskctrl ,var_target_schema, var_target_table, var_task_name, var_job_name, var_task_run_id,filter_prev_end_ts,var_skip_rows,var_file_encoding,var_source_file_extension)     
                                                print(f'var_src_cnt : {var_src_cnt}')
                                                print(f'var_tgt_cnt : {var_tgt_cnt}')
                                                print(f'execution_end_time : {execution_end_time}')
                                                print(f'last_processed_file : {last_processed_file}')
                                                print(f'files_processed : {files_processed}')                                           
                                               
                                                
                                        #calling of function for file ingestion non fixed files
                                        else:
                                                var_file_path = get_latest_filename(var_file_path, '') if var_ingest_latest_files_from_adls == 'Y' else var_file_path
                                                print(f'var_file_path : {var_file_path}')
                                                var_src_cnt, var_tgt_cnt,execution_end_time = fn_src_tgt_ingestion_raw(var_load_type,var_file_path,var_source_file_extension,var_source_file_delimiter,var_header,var_target_schema,var_target_table)
                                                print(f'var_src_cnt : {var_src_cnt}')
                                                print(f'var_tgt_cnt : {var_tgt_cnt}')
                                                print(f'execution_end_time : {execution_end_time}')
                                        
                                        err_msg =''
                                        if str(var_src_cnt) != str(var_tgt_cnt) :
                                                #print("source and target counts are not matching! some issue with load process")
                                                load_status  = 'FAILED'
                                                err_msg = f'{var_task_name} - {load_status} source and target counts are not matching!'
                                                var_recon_status = 0
                                                var_send_email_nb_run_details = dbutils.notebook.run(var_email_notification_nb_path, 0, {"SUB_APP":var_str_subapp_nm.upper(), "NOTIFICATION_TYPE": "failed", "JOB_RUN_ID": var_parent_run_id,"ENV":var_str_env_nm, "WORKSPACE_INSTANCE_URL":var_workspace_instance_url, "CATALOG_NAME":var_catalog_param, "TASK_CONTROL_TBL": var_schema_nm_taskctrl, "ERROR_MESSAGE":"src vs trgt count not matching","TASK_NAME":var_task_name}) #limiting error to 30 characters for sending in mail
                                        else: 
                                                load_status = 'COMPLETED'
                                                err_msg = f'{var_task_name} - {load_status}' 
                                                
                                        # update status in task cntrl 
                                        execution_end_time = datetime.datetime.now().replace(microsecond=0)
                                        
                                        if var_ingest_all_files_from_adls.strip().upper() == 'Y':
                                                update_task_control_restart(var_job_name, var_job_id, var_parent_run_id, var_task_name, var_task_run_id, str(filter_prev_end_ts), str(execution_start_time), execution_start_time, execution_end_time, var_src_cnt, var_tgt_cnt, load_status,last_processed_file)
                                                err_msg = f'{err_msg},  {files_processed}'
                                        else:
                                                update_task_control(var_job_name, var_job_id, var_parent_run_id, var_task_name, var_task_run_id, str(filter_prev_end_ts), str(execution_start_time), execution_start_time, execution_end_time, var_src_cnt, var_tgt_cnt, load_status)
                                        # log success aduit in  run log insert
                                        task_run_logging(var_job_id, var_parent_run_id, 'NULL', var_task_run_id, 'NULL', 'INFO:'+err_msg, var_src_cnt, var_tgt_cnt)
                                        err_msg =''

                                except Exception as e:
                                        load_status = 'FAILED'
                                        execution_end_time = datetime.datetime.now().replace(microsecond=0)
                                        #  log Failure task cntrl update 
                                        if var_ingest_all_files_from_adls.strip().upper() == 'Y':
                                                last_processed_file =  str(e).split('_err_msg_')[0]
                                                # e = f'{var_task_name} - {load_status} {str(e).split("_err_msg_")[1]}'
                                                split_error = str(e).split("_err_msg_")
                                                print("last_processed_file:",last_processed_file,"\nsplit_error",split_error)
                                                if len(split_error) >= 2:
                                                        e = f'{var_task_name} - {load_status} {split_error[1]}'
                                                else:
                                                        e = f'{var_task_name} - {load_status} {split_error[0]}'
                                                update_task_control_restart(var_job_name, var_job_id, var_parent_run_id, var_task_name, var_task_run_id, '', '', execution_start_time, execution_end_time, 'NULL', 'NULL', load_status,f'{last_processed_file}')
                                        else:
                                                update_task_control(var_job_name, var_job_id, var_parent_run_id, var_task_name, var_task_run_id, '', '', execution_start_time, execution_end_time, 'NULL', 'NULL', load_status)
                                        # log failed aduit in  run log insert
                                        task_run_logging(var_job_id, var_parent_run_id, 'NULL', var_task_run_id, 'NULL', 'CRITICAL:'+str(e).replace('"','').replace("'",""), 'NULL', 'NULL')
                                        print("Error Class       : " + type(e).__name__)
                                        #print("Message parameters: " + str,'NULL', 'NULL'(e.args))
                                        print("SQLSTATE          : " + e.__class__.__name__)
                                        var_job_status=0
                                     
                                        if var_grouping_id != '' or len(var_grouping_id)>0:
                                                sys.exit()
                                        continue   

except Exception as e:
        #print(e)
        #drop temp table if exception occured!
        spark.sql("""drop table if exists    """ + var_catalog_param + """."""+ var_schema_nm_rfnd +""".""" +var_metadata_tbl+"""_"""+var_job_name+ str(var_task_run_id)+"""_tmp """)
        #logging for job failure 
        load_status = 'FAILED'
        execution_end_time = datetime.datetime.now().replace(microsecond=0)
        # log Failure task cntrl update 
        time.sleep(10)

       
        update_task_control(var_job_name, var_job_id, var_parent_run_id, var_task_name, var_task_run_id, str(filter_start_time),str(filter_end_time), execution_start_time, execution_end_time, 'NULL', 'NULL', load_status)
        # log Failure aduit in run log insert
        task_run_logging(var_job_id, var_parent_run_id, 'NULL', var_task_run_id, 'NULL', 'CRITICAL:'+str(e).replace('"',''),'NULL', 'NULL')
        
finally:
        print('Finally Block, dropping unncessary tables/views')
        spark.sql("""drop table if exists    """ + var_catalog_param + """."""+ var_schema_nm_rfnd +""".""" +var_metadata_tbl+"""_"""+var_job_name+ str(var_task_run_id)+"""_tmp """)        

