# Databricks notebook source
import pyspark
from pyspark.sql.types import *
from pyspark.sql.functions import *
from datetime import datetime  
import sys
from pyspark.errors import PySparkException
import shutil
import os
import time
import re
from delta.exceptions import ConcurrentAppendException
import random

# COMMAND ----------

dbutils.widgets.text("ENV","")
dbutils.widgets.text("JOB_NM","")
dbutils.widgets.text("TASK_NM","")

# COMMAND ----------

var_env_name=dbutils.widgets.get("ENV")
var_source_param=dbutils.widgets.get("JOB_NM")
var_task_name=dbutils.widgets.get("TASK_NM")

current_session_user = spark.sql(f'select current_user()').collect()[0][0]
print(f'current_session_user : {current_session_user}')



# COMMAND ----------

def get_latest_filename(source_adls_full_path, run_dt):
       
        files_df = spark.read.format("binaryFile").load(source_adls_full_path)
        print(f'files_df : {files_df}')

        # Extract the file name and modification time
        files_df = files_df.select(col("path"), col("modificationTime"))
        print(f'files_df : {files_df}')

        # Identify the latest file
        if run_dt != '':
                files_df = files_df.filter(files_df.path.like("%{}%".format(run_dt)))


        latest_file_df = files_df.orderBy(col("modificationTime").desc()).limit(1)
        if latest_file_df.count() == 0:
                var_latest_filename = "No File Found"
                print(f'var_latest_filename : {var_latest_filename}')
        else:
                var_latest_filename = latest_file_df.collect()[0][0]
                print(f'var_latest_filename : {var_latest_filename}')

        return var_latest_filename

# COMMAND ----------

def fn_src_tgt_ingestion_raw(v_load_type,v_src_adls_path,v_src_extn,v_delim,v_is_hdr,v_tgt_schema,v_tgt_tbl):
        df_src = spark.read.format(f"{v_src_extn}")\
                                .option("header" ,f"{v_is_hdr}")\
                                .option("inferSchema", "False")\
                                .load(v_src_adls_path,sep=f"{v_delim}")
       
        var_src_cnt=df_src.count()
        df_src.createOrReplaceTempView(v_tgt_tbl+'_tmp')
        var_tgt_tbl_cnt=df_src.count()
        execution_end_time = datetime.datetime.now().replace(microsecond=0)
        
        if v_load_type.upper() == 'UPSERT':
                v_sql_qry=""" INSERT INTO  """+var_catalog_param+"""."""+v_tgt_schema+"""."""+v_tgt_tbl+""" 
                                SELECT  *,  '"""+ var_task_name + """' CRT_BY,
                          '"""+ str(execution_end_time) + """' CRT_TS,
                          '"""+ var_task_name + """' UPD_BY,
                        '"""+ str(execution_end_time) + """' UPD_TS
                                FROM """+ v_tgt_tbl+"""_tmp""" 
        elif v_load_type.upper() == 'OVERWRITE_ONLY_LOAD_BY':
                v_sql_qry= f"""INSERT OVERWRITE   {var_catalog_param}.{v_tgt_schema}.{v_tgt_tbl} 
                                SELECT  *,'{current_session_user}' AS LOAD_BY
                                FROM {v_tgt_tbl}_tmp""" 

        elif v_load_type.upper() == 'APPEND':
                v_sql_qry= f"""INSERT INTO   {var_catalog_param}.{v_tgt_schema}.{v_tgt_tbl} 
                                SELECT  *,'{current_session_user}' AS LOAD_BY, CURRENT_TIMESTAMP() AS LOAD_TS
                                FROM {v_tgt_tbl}_tmp"""                                

        else:
                if v_load_type.upper() == 'OVERWRITE':
                        v_sql_qry = f"""INSERT OVERWRITE {var_catalog_param}.{v_tgt_schema}.{v_tgt_tbl} 
                                SELECT  *,'{current_session_user}' AS LOAD_BY, CURRENT_TIMESTAMP() AS LOAD_TS 
                                FROM {v_tgt_tbl}_tmp""" 
        #print(v_sql_qry)
        spark.sql(v_sql_qry)
        df_tgt_cnt = spark.sql(f"describe history {var_catalog_param}.{v_tgt_schema}.{v_tgt_tbl} limit 1")
        var_tgt_cnt = df_tgt_cnt.select('operationMetrics').collect()[0][0]['numOutputRows']
        #df_tgt_cnt_qry = f"SELECT * FROM {var_catalog_param}.{v_tgt_schema}.{v_tgt_tbl}"
        #var_tgt_cnt = spark.sql(df_tgt_cnt_qry).count()
        #print("File loading to table is successful!")

        return var_src_cnt, var_tgt_cnt,execution_end_time

# COMMAND ----------

def fn_append_ts_todate(input_dt): 
        ###############################################################
        # Append the current timestamp to the given date.

        # Parameters:
        # input_dt (str): The input date.

        # Returns:
        # str: The input date with the current timestamp appended.
        ###############################################################
        df_end_ts=spark.sql("select current_timestamp() as end_ts")
        var_upd_tms= df_end_ts.select('end_ts').collect()[0][0]
        return input_dt[:10]+ str(var_upd_tms)[10:]

# COMMAND ----------

def task_control_logging(job_name, job_id, parent_run_id, task_name, task_run_id, filter_start_ts,filter_end_ts,execution_start_ts,execution_end_ts, source_count, target_count,status):
        ##################################################
        # Log task control information using parameter values..

        # Parameters:
        # job_name (str): The job name.
        # job_id (str): The job ID.
        # parent_run_id (str): The parent run ID.
        # task_name (str): The task name.
        # task_run_id (str): The task run ID.
        # filter_start_ts (str): The filter start timestamp.
        # filter_end_ts (str): The filter end timestamp.
        # execution_start_ts (str): The execution start timestamp.
        # execution_end_ts (str): The execution end timestamp.
        # source_count (int): The source count.
        # target_count (int): The target count.
        # status (str): The status.
        ##################################################
        #print(f"--------Logging started in {var_task_control} -----------")
        query = f'''INSERT INTO {var_catalog_param}.{var_schema_nm_taskctrl}.{var_task_control}
                (job_name, job_id, parent_run_id, task_name, task_run_id, filter_start_ts, filter_end_ts, execution_start_ts, execution_end_ts, source_count, target_count, status)
                VALUES ('{job_name}', '{job_id}', '{parent_run_id}', '{task_name}', '{task_run_id}', {filter_start_ts}, {filter_end_ts}, {execution_start_ts}, {execution_end_ts}, {source_count}, {target_count}, '{status}')'''
        print(f'query : {query}')
        spark.sql(query)
        print(f"--------Logging ended in {var_task_control} -----------")

# COMMAND ----------

def task_run_logging(job_id, parent_run_id, procedure_name, task_run_id, procedure_runtime_stamp, err_msg, src_cnt, tgt_cnt):
        ##################################################################################
        # Log task run information using parameter values.

        # Parameters:
        # job_id (str): The job ID.
        # parent_run_id (str): The parent run ID.
        # task_run_id (str): The task run ID.
        # procedure_name (str): The procedure name.
        # procedure_runtime_stamp (str): The procedure runtime timestamp.
        # err_msg (str): The error message.
        # src_cnt (int): The source count.
        # tgt_cnt (int): The target count.
        ##################################################################################
        #print(f"--------Logging started in {var_task_run_log} -----------")
        query = f'''INSERT INTO {var_catalog_param}.{var_schema_nm_taskctrl}.{var_task_run_log}
                (job_id, parent_run_id, task_run_id, procedure_name, create_dt, message_type, priority, message_cd, primary_message, secondary_message, login_id, data_key, records_in, records_out, procedure_runtime_stamp, records_updated)
                VALUES ({job_id}, {parent_run_id}, {task_run_id},{procedure_name}, current_timestamp(),NULL,NULL,NULL, "{err_msg}",NULL,NULL,NULL,{src_cnt},{tgt_cnt},{procedure_runtime_stamp},NULL)'''
        print(f'query : {query}')
        spark.sql(query)
        print(f"--------Logging ended in {var_task_run_log} -----------")

# COMMAND ----------

def fn_arch_file(v_file_path,v_archival_adls_path):
        #################################################################
        # Archive a file from the source path to the archival path.

        # Parameters:
        # v_file_path (str): The source file path.
        # v_archival_adls_path (str): The archival ADLS path.

        # Returns:
        # tuple: Source count and target count.
        #################################################################
        #print(v_file_path ,'  ',v_archival_adls_path)
        file_list = [file.path for file in dbutils.fs.ls(v_file_path)]
        var_src_cnt=len(file_list)
        var_tgt_cnt=var_src_cnt
        # Move the file to the archival path
        dbutils.fs.mv(v_file_path, v_archival_adls_path) 
        return var_src_cnt, var_tgt_cnt

# COMMAND ----------

def sp_exec_minutes(s_time):
        e_time = datetime.now()
        time_diff = e_time-s_time 
        #print('Difference: ', time_diff)
        min_diff =  "{:.2f}".format(time_diff.total_seconds() / 60.00)
        return min_diff

# COMMAND ----------

def db_list_files(file_path, file_prefix):
        #################################################################
        # List files in a given directory that start with a specified prefix.

        # Args:
        # file_path (str): The path to the directory.
        # file_prefix (str): The prefix to filter files.

        # Returns:
        # list: A list of file paths that match the prefix.
        #################################################################
        file_list = [file.path for file in dbutils.fs.ls(file_path) if os.path.basename(file.path).startswith(file_prefix)]
        print(f'file_list : {file_list}')
        return file_list

# COMMAND ----------


def sp_exec_minutes(s_time):
    ##########################################################################
    # Calculate the difference in minutes between the current time and the provided datetime.

    # Parameters:
    # s_time (datetime): The datetime.

    # Returns:
    # str: The difference in minutes formatted to two decimal places.
    ##########################################################################
    e_time = datetime.now()
    time_diff = e_time-s_time    
    min_diff =  "{:.2f}".format(time_diff.total_seconds() / 60.00)
    return min_diff

# COMMAND ----------

def convertMillis(millisecond:int):
    ##########################################################################
    # Convert milliseconds to hours, minutes, and seconds.

    # Parameters:
    # millisecond (int): The time in milliseconds.

    # Returns:
    # tuple: A tuple containing hours, minutes, and seconds.
    ##########################################################################
    hours=int(millisecond/(1000 * 60 * 60))
    minutes=int(millisecond / (1000 * 60)) % 60
    seconds = int(millisecond / 1000) % 60
    return hours, minutes, seconds

# COMMAND ----------


# Get last ingested file from the task control table for job task
def fn_get_adls_last_processed_file(v_catalog_param, v_schema_nm_taskctrl, v_job_name, v_task_name):
    ##########################################################################
    # Retrieve the last processed file for a given job task from the task control table.
    #
    # Parameters:
    # v_catalog_param (str): The catalog parameter.
    # v_schema_nm_taskctrl (str): The schema name of the task control table.
    # v_job_name (str): The job name.
    # v_task_name (str): The task name.
    #
    # Returns:
    # str: The name of the last processed file. Returns 'NULL' if exception.
    ##########################################################################
    
    try:
        return  spark.sql(f"""  select 
                                    last_processed_file --, *                                    
                                from
                                (
                                    select
                                        *,
                                        row_number() over (
                                        partition by task_name
                                        order by execution_start_ts desc
                                        ) as row_no
                                    from
                                        {v_catalog_param}.{v_schema_nm_taskctrl}.{var_task_control}
                                        where trim(job_name) = trim('{v_job_name}') and trim(task_name) = trim('{v_task_name}')
                                        and status = 'COMPLETED'
                                )
                                where
                                    row_no = 1
                        """).collect()[0][0]
    except:
        return 'NULL'

# COMMAND ----------

def fn_get_file_info_from_adls(adls_path):
    ##########################################################################
    # Retrieve the list of files available in the ADLS container along with the last modified time.
    #
    # Parameters:
    # adls_path (str): The path to the ADLS container.
    #
    # Returns:
    # DataFrame: A DataFrame containing the list of files with their details,
    #            including a timestamp column converted from modificationTime.
    ##########################################################################
    file_list = dbutils.fs.ls(adls_path)
    print(f'file_list : {file_list}')
    file_list_df = spark.createDataFrame(file_list)
    file_list_df = file_list_df.withColumn("timestamp",to_timestamp(file_list_df['modificationTime']/1000))
    # file_list_df1 = file_list_df.filter(file_list_df.size>0) #/* size > 0 order by modificationTime */
    final_file_list_df = file_list_df.sort('modificationTime')
    print(f'file_list_df : {file_list_df}')
    return final_file_list_df

# COMMAND ----------

def fn_ingest_all_files_from_adls(var_file_path,var_text_files_schema, var_load_type, var_catalog_param, var_schema_nm_taskctrl ,var_target_schema, var_target_table, var_task_name, var_job_name, var_task_run_id,filter_prev_end_ts,var_skip_rows,var_file_encoding,var_source_file_extension):
    ##########################################################################
    # Ingest all files from ADLS into target delta table based on the file extension. If txt it get processed as fixed width else as csv
    #
    # Parameters:
    # var_file_path (str): The path to the files in ADLS.
    # var_text_files_schema (dict): The schema for fixed-width text files.
    # var_load_type (str): The type of load operation (e.g., 'APPEND').
    # var_catalog_param (str): The catalog parameter.
    # var_schema_nm_taskctrl (str): The schema name of the task control table.
    # var_target_schema (str): The target schema name.
    # var_target_table (str): The target table name.
    # var_task_name (str): The task name.
    # var_job_name (str): The job name.
    # var_task_run_id (str): The task run ID.
    # filter_prev_end_ts (str): The previous end timestamp for filtering files.
    # var_skip_rows (int): The number of rows to skip in the file.
    # var_file_encoding (str): The file encoding type.
    # var_source_file_extension (str): The source file extension (e.g., 'txt', 'csv').
    #
    # Returns:
    # tuple: A tuple containing the source count, target count, execution end time,
    #        last processed file, and a message indicating the files processed.
    ##########################################################################
    if var_source_file_extension == 'txt':
        var_src_cnt, var_tgt_cnt,execution_end_time ,last_processed_file, files_processed = fn_fixed_width_file_ingestion(var_file_path,var_text_files_schema, var_load_type, var_catalog_param, var_schema_nm_taskctrl ,var_target_schema, var_target_table, var_task_name, var_job_name, var_task_run_id,filter_prev_end_ts,var_skip_rows,var_file_encoding)
    elif var_source_file_extension == 'csv':
        var_src_cnt, var_tgt_cnt,execution_end_time ,last_processed_file, files_processed = fn_csv_file_ingestion(var_load_type,var_file_path,var_source_file_extension,var_source_file_delimiter,var_header,var_target_schema,var_target_table,var_catalog_param, var_schema_nm_taskctrl,var_task_name, var_job_name, var_task_run_id,filter_prev_end_ts,var_skip_rows)
    return var_src_cnt, var_tgt_cnt,execution_end_time ,last_processed_file, files_processed

# COMMAND ----------


def fn_csv_file_ingestion(v_load_type,v_src_adls_path,v_src_extn,v_delim,v_is_hdr,v_tgt_schema,v_tgt_tbl,v_catalog_param, v_schema_nm_taskctrl, v_task_name, v_job_name, v_task_run_id,filter_prev_end_ts,var_skip_rows):
    files_info = fn_get_file_info_from_adls(v_src_adls_path)
    print(f'files_info : {files_info}')
    files_info.createOrReplaceTempView("files_info_tmp")
    last_processed_file = fn_get_adls_last_processed_file(v_catalog_param, v_schema_nm_taskctrl,v_job_name,v_task_name)
    print(f'last_processed_file : {last_processed_file}')
    if last_processed_file is None or last_processed_file=='NULL':
        #last_processed_file = re.sub("[',\],\[,']","",str(v_src_adls_path.split('/')[-1:]))+'_19000101000000.csv'
        last_processed_file = (re.sub(r"[,\[\]']", "", str(v_src_adls_path.split('/')[-1:]))+ "_19000101000000.csv")
        print(f'last_processed_file : {last_processed_file}')

    if 'monthly' in v_src_adls_path:
        monthly_files_df = files_info.filter(col("path").contains("monthly"))
        # Get the latest file based on modification time
        latest_file = monthly_files_df.orderBy(col("modificationTime").desc()).limit(1).collect()[0].name
        print(f'monthly_files_df : {monthly_files_df}')
        print(f'latest_file : {latest_file}')
    try:
        files_modified_time = spark.sql(f"select  unix_timestamp(to_timestamp('{filter_prev_end_ts}'))").collect()[0][0]
        print(f'files_modified_time : {files_modified_time}')
    except:
        files_modified_time = '-2208988800' #spark.sql("select  unix_timestamp(to_timestamp('1900-01-01 00:00:00'))").collect()[0][0] Default timestamp value
    files_to_process = []
    files_to_process1 = spark.sql(f""" select collect_list(name) from files_info_tmp where trim(modificationTime) > trim({files_modified_time}) """).collect()[0][0]
    print(f'files_to_process1 : {files_to_process1}')
    
    files_to_process.extend(files_to_process1)
    files_processed = []
    
    if len(files_to_process) > 0:
        last_inserted_file = []
        for file in files_to_process:
            v_src_adls_path_1 = f"{v_src_adls_path}/{file}"
            print(f'v_src_adls_path_1 : {v_src_adls_path_1}')
            prefix = file.split('_')[0]
            # file_fixed_width_schema = fixed_width_schema["file_schema"][f"{prefix.lower()}"]
            process_path = lambda x: ('/'.join(x.split('/')[-4:]), x)
            print(f'process_path : {process_path}')
            file_name = process_path(v_src_adls_path_1)[0]
            print(f'file_name : {file_name}'')
            file_ts = str(str(v_src_adls_path_1.split('/')[-1:]).split('.')[-2]).split('_')[-1]
            print(f'file_ts : {file_ts}'')
            
            df_src = spark.read.format(f"{v_src_extn}")\
                                .option("header" ,f"{v_is_hdr}")\
                                .option("inferSchema", "False")\
                                .option("escape",'"')\
                                .option("ignoreLeadingWhiteSpace", "False")\
                                .option("ignoreTrailingWhiteSpace", "False")\
                                .load(v_src_adls_path,sep=f"{v_delim}")      
            display(df_src)
            if (df_src.count() > 0) and var_skip_rows is not None:
                df_src = df_src.withColumn('index', monotonically_increasing_id())
                rows_to_remove = list(range(0,int(var_skip_rows))) 
                df_src = df_src.filter(~df_src.index.isin(rows_to_remove))
                df_src = df_src.drop('index')
                

            # df_src.display()
            if df_src.count() > 0:
                var_src_cnt=df_src.count()
                
                # Load file data into  temp view as a place holder 
                df_src.createOrReplaceTempView(v_tgt_tbl+'_tmp')
                var_tgt_tbl_cnt=df_src.count()
                if v_load_type.upper() == 'APPEND':
                    try:
                        v_sql_qry = f"""INSERT INTO {v_catalog_param}.{v_tgt_schema}.{v_tgt_tbl} 
                                SELECT  *,'{current_session_user}' AS LOAD_BY, CURRENT_TIMESTAMP() AS LOAD_TS, '{file_name}' AS FILE_NAME, cast('{file_ts}' as string) AS FILE_TS
                                FROM {v_tgt_tbl}_tmp"""
                        spark.sql(v_sql_qry)
                        execution_end_time = datetime.datetime.now().replace(microsecond=0)
                        df_tgt_cnt = spark.sql(f"describe history {v_catalog_param}.{v_tgt_schema}.{v_tgt_tbl} limit 1")
                        var_tgt_cnt = df_tgt_cnt.select('operationMetrics').collect()[0][0]['numOutputRows']
                        files_processed.append(file)
                        last_processed_file = file
                        spark.sql(f"UNCACHE TABLE IF EXISTS {v_tgt_tbl}_tmp")
                        spark.sql(f"DROP TABLE IF EXISTS {v_tgt_tbl}_tmp")
                    except Exception as e:
                        spark.sql(f"DROP TABLE IF EXISTS {v_tgt_tbl}_tmp") 
                        # spark.sql(f"DROP TABLE IF EXISTS files_list_tmp") 
                        var_src_cnt, var_tgt_cnt, execution_end_time = 'NULL', 'NULL', datetime.datetime.now().replace(microsecond=0)
                        # last_processed_file = file
                        err_msgg = f'{last_processed_file}_err_msg_{str(e)}'
                        raise Exception(err_msgg)
            else:
                var_src_cnt, var_tgt_cnt, execution_end_time = 0, 0, datetime.datetime.now().replace(microsecond=0)
                last_processed_file = file
                files_processed.append(file)
    else:
        var_src_cnt, var_tgt_cnt, execution_end_time = 'NULL', 'NULL', datetime.datetime.now().replace(microsecond=0)

    spark.sql(f"DROP TABLE IF EXISTS files_info_tmp")
    files_processed_1 =  f"{', '.join(files_processed)} files are processed" if len(files_processed) > 0 else 'No files available to process for the day'

    return var_src_cnt, var_tgt_cnt, execution_end_time , last_processed_file, files_processed_1

# COMMAND ----------


def update_task_control_restart(job_name, job_id, parent_run_id, task_name, task_run_id, filter_start_time, filter_end_time, execution_start_time, execution_end_time, src_cnt, tgt_cnt, load_status,last_processed_file='NULL'):
    ##########################################################################
    # Update the task control table with the restart information using parameter values. Raise exception if update fails with exception or reached the max retries incase of cuncurrent execution exception
    #
    # Parameters:
    # job_name (str): The name of the job.
    # job_id (str): The ID of the job.
    # parent_run_id (str): The parent run ID.
    # task_name (str): The name of the task.
    # task_run_id (str): The run ID of the task.
    # filter_start_time (str): The start timestamp for filtering.
    # filter_end_time (str): The end timestamp for filtering.
    # execution_start_time (str): The start timestamp for execution.
    # execution_end_time (str): The end timestamp for execution.
    # src_cnt (int): The source count.
    # tgt_cnt (int): The target count.
    # load_status (str): The status of the load operation.
    # last_processed_file (str): The last processed file (default is 'NULL').
    #
    # Returns:
    # None
    ##########################################################################
    query = f'''UPDATE {var_catalog_param}.{var_schema_nm_taskctrl}.{var_task_control}
            SET task_run_id = "{task_run_id}", 
            filter_start_ts = to_timestamp("{filter_start_time}", "yyyy-MM-dd HH:mm:ss"),
            filter_end_ts = to_timestamp("{filter_end_time}", "yyyy-MM-dd HH:mm:ss"),
            execution_start_ts = to_timestamp("{execution_start_time}", "yyyy-MM-dd HH:mm:ss"),
            execution_end_ts = to_timestamp("{execution_end_time}", "yyyy-MM-dd HH:mm:ss"),
            source_count = {src_cnt},
            target_count = {tgt_cnt},
            status = "{load_status}",
            last_processed_file = "{last_processed_file}"
            WHERE task_name = "{task_name}"
            AND job_id = "{job_id}"
            AND parent_run_id = "{parent_run_id}"
            AND job_name = "{job_name}"
            AND upper(status) != "FAILED"  '''
    # spark.sql(query)
    retries = 0
    max_retries = 30
    initial_wait_time = 1
    backoff_factor = 2
    while retries < max_retries:
            try:
                    spark.sql(query)
                    break
            except ConcurrentAppendException as e:
                    retries += 1
                #     wait_time = initial_wait_time * (backoff_factor ** retries)
                    wait_time = random.randint(20, 45)
                    time.sleep(wait_time)
                    
    if retries == max_retries:
            raise Exception(f"Failed to update {var_task_control} after {max_retries} retries")

# COMMAND ----------


def update_task_control(job_name, job_id, parent_run_id, task_name, task_run_id, filter_start_time, filter_end_time, execution_start_time, execution_end_time, src_cnt, tgt_cnt, load_status):
        ###########################################################################
        # Update task control information using parameter values with last process file parameter. Raise exception if update fails with exception or reached the max retries incase of cuncurrent execution exception

        # Parameters:
        # job_name (str): The job name.
        # job_id (str): The job ID.
        # parent_run_id (str): The parent run ID.
        # task_name (str): The task name.
        # task_run_id (str): The task run ID.
        # filter_start_time (str): The filter start time.
        # filter_end_time (str): The filter end time.
        # execution_start_time (str): The execution start time.
        # execution_end_time (str): The execution end time.
        # src_cnt (int): The source count.
        # tgt_cnt (int): The target count.
        # load_status (str): The load status.
        ##########################################################################
        update_task_control_restart(job_name, job_id, parent_run_id, task_name, task_run_id, filter_start_time, filter_end_time, execution_start_time, execution_end_time, src_cnt, tgt_cnt, load_status)


# COMMAND ----------


def get_filter_start_ts(var_job_name,var_task_name):    
    ##########################################################################
    # Get the max filter end timestamp for a given job and task to use as filter start time for new run.

    # Parameters:
    # var_job_name (str): The name of the job.
    # var_task_name (str): The name of the task.
    #
    # Returns:
    # str: The filter end timestamp of the last completed task run, or a default
    #      timestamp ('1900-01-01 00:00:00') if no completed task run is found.
    ##########################################################################
    var_sql_query= """ select max(filter_end_ts) filter_end_ts from  """ + var_catalog_param + """."""+ var_schema_nm_taskctrl +"""."""+var_task_control+""" where job_name = '""" + var_job_name +"""' and task_name = '"""+var_task_name  +"""' and status ='COMPLETED'""" 
    #print(var_sql_query)    
    df_result1=spark.sql(var_sql_query)
    filter_prev_end_ts = df_result1.select('filter_end_ts').collect()[0][0]

    if filter_prev_end_ts is None:
            filter_prev_end_ts='1900-01-01 00:00:00'
    return filter_prev_end_ts
