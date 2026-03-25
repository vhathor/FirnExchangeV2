/*==============================================================================
  FirnExchange - Stored Procedures for Task-Based Execution
  
  Purpose: Python stored procedures that execute data migration operations
           using async queries (collect_nowait) for parallel processing
  
  Version: 2.4 (ADD_FILES_COPY → FULL_INGEST fallback for import)
  
  Components:
    1. FIRN_TASK_REGISTRY table - Master table for tracking tasks
    2. FIRN_EXPORT_ASYNC_PROC - Export procedure (processes all PENDING partitions)
    3. FIRN_IMPORT_ASYNC_PROC - Import procedure (processes all PENDING files)
  
  Usage:
    Run this script BEFORE deploying the FirnExchange Streamlit application
    
  Fixes in v2.4:
    - Import: ADD_FILES_COPY auto-fallback to FULL_INGEST on failure
    - Import log table: added LOAD_MODE_USED column to track actual mode used
    
  Fixes in v2.2:
    - Fixed DATE/TIMESTAMP partition values (now properly quoted in WHERE clause)
    - Added format_partition_where_clause helper for all data types
    - Handles str, date, datetime, numeric, and NULL values correctly
==============================================================================*/

-- NOTE: These will use the context from the deployment script
-- USE ROLE ACCOUNTADMIN;
-- USE WAREHOUSE XSMALL;
-- USE DATABASE FT_DB;
-- USE SCHEMA FT_SCH;

--------------------------------------------------------------------------------
-- STEP 1: Create Task Registry Table
--------------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS FIRN_TASK_REGISTRY (
    TASK_NAME STRING PRIMARY KEY,
    OPERATION_TYPE STRING NOT NULL,  -- 'EXPORT' or 'IMPORT'
    SOURCE_DATABASE STRING,
    SOURCE_SCHEMA STRING,
    SOURCE_TABLE STRING,
    TARGET_DATABASE STRING,
    TARGET_SCHEMA STRING,
    TARGET_TABLE STRING,
    TRACKING_TABLE STRING NOT NULL,
    STAGE STRING,
    STAGE_PATH STRING,
    WAREHOUSE STRING,
    MAX_WORKERS INT,
    CREATED_BY STRING DEFAULT CURRENT_USER(),
    CREATED_AT TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP(),
    TASK_STATUS STRING DEFAULT 'CREATED',  -- 'CREATED', 'RUNNING', 'COMPLETED', 'FAILED', 'SUSPENDED'
    START_TIME TIMESTAMP_NTZ,
    END_TIME TIMESTAMP_NTZ,
    TOTAL_ITEMS INT,
    COMPLETED_ITEMS INT DEFAULT 0,
    FAILED_ITEMS INT DEFAULT 0,
    ERROR_MESSAGE STRING,
    PARAMETERS VARIANT
)
COMMENT = 'Registry table for tracking FirnExchange task executions';

SELECT 'Task registry table created' AS STATUS;

--------------------------------------------------------------------------------
-- STEP 2: Create Export Async Procedure
--------------------------------------------------------------------------------
CREATE OR REPLACE PROCEDURE FIRN_EXPORT_ASYNC_PROC(
    TASK_NAME STRING,
    TRACKING_TABLE STRING,
    SOURCE_DATABASE STRING,
    SOURCE_SCHEMA STRING,
    SOURCE_TABLE STRING,
    PARTITION_COLUMN STRING,
    STAGE STRING,
    STAGE_PATH STRING,
    PARQUET_COLUMNS STRING,
    WAREHOUSE STRING,
    MAX_WORKERS INT,
    OVERWRITE BOOLEAN,
    SINGLE_FILE BOOLEAN,
    MAX_FILE_SIZE NUMBER
)
RETURNS STRING
LANGUAGE PYTHON
RUNTIME_VERSION = '3.11'
PACKAGES = ('snowflake-snowpark-python')
HANDLER = 'export_handler'
AS
$$
import time
import re
from datetime import datetime, date

def format_partition_where_clause(partition_column, partition_value):
    """Format WHERE clause for partition value handling all data types correctly"""
    safe_partition = str(partition_value).replace("'", "''")
    if isinstance(partition_value, (str, date, datetime)):
        return f'"{partition_column}" = \'{safe_partition}\''
    elif partition_value is None:
        return f'"{partition_column}" IS NULL'
    else:
        return f'"{partition_column}" = {partition_value}'

def export_handler(session, task_name, tracking_table, source_database, source_schema, source_table,
                   partition_column, stage, stage_path, parquet_columns, warehouse, max_workers,
                   overwrite, single_file, max_file_size):
    """Export all PENDING partitions using async queries with concurrency control"""
    try:
        # Update task registry - set to RUNNING
        session.sql(f"UPDATE FIRN_TASK_REGISTRY SET TASK_STATUS = 'RUNNING', START_TIME = CURRENT_TIMESTAMP() WHERE TASK_NAME = '{task_name}'").collect()
        
        # Get all PENDING partitions
        pending = session.sql(f"SELECT * FROM {tracking_table} WHERE PARTITION_MIGRATED_STATUS = 'PENDING' ORDER BY TOTAL_ROWS ASC").collect()
        
        if not pending:
            session.sql(f"UPDATE FIRN_TASK_REGISTRY SET TASK_STATUS = 'COMPLETED', END_TIME = CURRENT_TIMESTAMP(), COMPLETED_ITEMS = 0, FAILED_ITEMS = 0 WHERE TASK_NAME = '{task_name}'").collect()
            return "No PENDING partitions to export"
        
        total_partitions = len(pending)
        active_jobs = {}
        completed_count = 0
        failed_count = 0
        partition_index = 0
        
        while partition_index < total_partitions or active_jobs:
            # Poll existing jobs
            for partition_value, (job, start_time) in list(active_jobs.items()):
                if job.is_done():
                    try:
                        copy_result = job.result()
                        rows_unloaded = copy_result[0].as_dict().get('rows_unloaded', 0) if copy_result else 0
                        where_clause = format_partition_where_clause(partition_column, partition_value)
                        session.sql(f"UPDATE {tracking_table} SET PARTITION_MIGRATED_STATUS = 'COMPLETED', PARTITION_MIGRATED_END_AT = CURRENT_TIMESTAMP(), ROWS_UNLOADED = {rows_unloaded} WHERE {where_clause}").collect()
                        completed_count += 1
                    except Exception as e:
                        error_msg = str(e).replace("'", "''")[:500]
                        where_clause = format_partition_where_clause(partition_column, partition_value)
                        session.sql(f"UPDATE {tracking_table} SET PARTITION_MIGRATED_STATUS = 'FAILED', ERROR_MESSAGE = '{error_msg}' WHERE {where_clause}").collect()
                        failed_count += 1
                    del active_jobs[partition_value]
            
            # Submit new jobs if slots available
            while len(active_jobs) < max_workers and partition_index < total_partitions:
                partition_row = pending[partition_index]
                partition_value = partition_row[partition_column]
                where_clause = format_partition_where_clause(partition_column, partition_value)
                
                # Sanitize partition key for use in path: replace spaces and special chars with underscore
                partition_key_path = re.sub(r'[^a-zA-Z0-9_-]', '_', str(partition_value))
                
                # Update to IN_PROGRESS
                session.sql(f"UPDATE {tracking_table} SET PARTITION_MIGRATED_STATUS = 'IN_PROGRESS' WHERE {where_clause}").collect()
                
                # Submit async COPY INTO
                copy_sql = f"COPY INTO @{stage}/{stage_path}/{partition_key_path}/ FROM (SELECT {parquet_columns} FROM \"{source_database}\".\"{source_schema}\".\"{source_table}\" WHERE {where_clause}) FILE_FORMAT = (TYPE = PARQUET) MAX_FILE_SIZE = {max_file_size} OVERWRITE = {'TRUE' if overwrite else 'FALSE'} SINGLE = {'TRUE' if single_file else 'FALSE'} HEADER = TRUE"
                async_job = session.sql(copy_sql).collect_nowait()
                active_jobs[partition_value] = (async_job, datetime.now())
                partition_index += 1
            
            if active_jobs:
                time.sleep(2)
        
        # Update task registry - COMPLETED
        session.sql(f"UPDATE FIRN_TASK_REGISTRY SET TASK_STATUS = 'COMPLETED', END_TIME = CURRENT_TIMESTAMP(), COMPLETED_ITEMS = {completed_count}, FAILED_ITEMS = {failed_count} WHERE TASK_NAME = '{task_name}'").collect()
        return f"Export completed: {completed_count} succeeded, {failed_count} failed out of {total_partitions}"
        
    except Exception as e:
        error_msg = str(e).replace("'", "''")[:500]
        session.sql(f"UPDATE FIRN_TASK_REGISTRY SET TASK_STATUS = 'FAILED', ERROR_MESSAGE = '{error_msg}' WHERE TASK_NAME = '{task_name}'").collect()
        return f"Export failed: {error_msg}"
$$;

SELECT 'Export procedure created' AS STATUS;

--------------------------------------------------------------------------------
-- STEP 3: Create Import Async Procedure
-- v2.4: When load_mode is ADD_FILES_COPY and a file fails, automatically
--        retries with FULL_INGEST before marking as FAILED.
--------------------------------------------------------------------------------
CREATE OR REPLACE PROCEDURE FIRN_IMPORT_ASYNC_PROC(
    TASK_NAME STRING,
    IMPORT_LOG_TABLE STRING,
    TARGET_DATABASE STRING,
    TARGET_SCHEMA STRING,
    TARGET_TABLE STRING,
    STAGE STRING,
    WAREHOUSE STRING,
    LOAD_MODE STRING,
    MAX_WORKERS INT
)
RETURNS STRING
LANGUAGE PYTHON
RUNTIME_VERSION = '3.11'
PACKAGES = ('snowflake-snowpark-python')
HANDLER = 'import_handler'
AS
$$
import time
from datetime import datetime

def build_copy_sql(fully_qualified_table, stage, file_path, mode):
    """Build COPY INTO SQL for the given load mode"""
    return (
        f"COPY INTO {fully_qualified_table} FROM @{stage}/{file_path} "
        f"FILE_FORMAT = (TYPE = PARQUET USE_VECTORIZED_SCANNER = TRUE) "
        f"MATCH_BY_COLUMN_NAME = CASE_SENSITIVE ON_ERROR = ABORT_STATEMENT "
        f"LOAD_MODE = {mode}"
    )

def import_handler(session, task_name, import_log_table, target_database, target_schema, target_table,
                   stage, warehouse, load_mode, max_workers):
    """Import all PENDING files using async queries with concurrency control.
    
    When load_mode is ADD_FILES_COPY: if a file fails, automatically retries 
    with FULL_INGEST. Only marks as FAILED if both modes fail.
    """
    try:
        session.sql(f"UPDATE FIRN_TASK_REGISTRY SET TASK_STATUS = 'RUNNING', START_TIME = CURRENT_TIMESTAMP() WHERE TASK_NAME = '{task_name}'").collect()
        
        try:
            session.sql(f"ALTER TABLE {import_log_table} ADD COLUMN IF NOT EXISTS LOAD_MODE_USED STRING").collect()
        except Exception:
            pass
        
        pending_files = session.sql(f"SELECT FILE_PATH FROM {import_log_table} WHERE FILE_STATUS = 'PENDING'").collect()
        
        if not pending_files:
            session.sql(f"UPDATE FIRN_TASK_REGISTRY SET TASK_STATUS = 'COMPLETED', END_TIME = CURRENT_TIMESTAMP(), COMPLETED_ITEMS = 0, FAILED_ITEMS = 0 WHERE TASK_NAME = '{task_name}'").collect()
            return "No PENDING files to import"
        
        total_files = len(pending_files)
        active_jobs = {}
        completed_count = 0
        failed_count = 0
        fallback_count = 0
        file_index = 0
        fully_qualified_table = f'"{target_database}"."{target_schema}"."{target_table}"'
        fallback_enabled = load_mode.upper() == 'ADD_FILES_COPY'
        fallback_queue = []
        
        while file_index < total_files or active_jobs:
            for file_path, (job, start_time, current_mode) in list(active_jobs.items()):
                if job.is_done():
                    safe_file_path = file_path.replace("'", "''")
                    try:
                        import_result = job.result()
                        rows_loaded = import_result[0].as_dict().get('rows_loaded', 0) if import_result else 0
                        session.sql(f"UPDATE {import_log_table} SET FILE_STATUS = 'SUCCESS', IMPORT_END_AT = CURRENT_TIMESTAMP(), ROWS_LOADED = {rows_loaded}, LOAD_MODE_USED = '{current_mode}' WHERE FILE_PATH = '{safe_file_path}'").collect()
                        completed_count += 1
                    except Exception as e:
                        if fallback_enabled and current_mode == 'ADD_FILES_COPY':
                            fallback_error = str(e).replace("'", "''")[:500]
                            session.sql(f"UPDATE {import_log_table} SET FILE_STATUS = 'RETRYING_FULL_INGEST', ERROR_MESSAGE = 'ADD_FILES_COPY failed: {fallback_error}. Retrying with FULL_INGEST...' WHERE FILE_PATH = '{safe_file_path}'").collect()
                            fallback_queue.append(file_path)
                        else:
                            error_msg = str(e).replace("'", "''")[:500]
                            mode_note = f' (after ADD_FILES_COPY fallback)' if current_mode == 'FULL_INGEST' and fallback_enabled else ''
                            session.sql(f"UPDATE {import_log_table} SET FILE_STATUS = 'FAILED', ERROR_MESSAGE = '{error_msg}{mode_note}', LOAD_MODE_USED = '{current_mode}' WHERE FILE_PATH = '{safe_file_path}'").collect()
                            failed_count += 1
                    del active_jobs[file_path]
            
            while len(active_jobs) < max_workers and file_index < total_files:
                file_path = pending_files[file_index]['FILE_PATH']
                safe_file_path = file_path.replace("'", "''")
                session.sql(f"UPDATE {import_log_table} SET FILE_STATUS = 'IN_PROGRESS' WHERE FILE_PATH = '{safe_file_path}'").collect()
                copy_sql = build_copy_sql(fully_qualified_table, stage, file_path, load_mode)
                async_job = session.sql(copy_sql).collect_nowait()
                active_jobs[file_path] = (async_job, datetime.now(), load_mode.upper())
                file_index += 1
            
            while len(active_jobs) < max_workers and fallback_queue:
                file_path = fallback_queue.pop(0)
                safe_file_path = file_path.replace("'", "''")
                session.sql(f"UPDATE {import_log_table} SET FILE_STATUS = 'IN_PROGRESS' WHERE FILE_PATH = '{safe_file_path}'").collect()
                copy_sql = build_copy_sql(fully_qualified_table, stage, file_path, 'FULL_INGEST')
                async_job = session.sql(copy_sql).collect_nowait()
                active_jobs[file_path] = (async_job, datetime.now(), 'FULL_INGEST')
                fallback_count += 1
            
            if active_jobs:
                time.sleep(2)
        
        fallback_msg = f", {fallback_count} used FULL_INGEST fallback" if fallback_count > 0 else ""
        session.sql(f"UPDATE FIRN_TASK_REGISTRY SET TASK_STATUS = 'COMPLETED', END_TIME = CURRENT_TIMESTAMP(), COMPLETED_ITEMS = {completed_count}, FAILED_ITEMS = {failed_count} WHERE TASK_NAME = '{task_name}'").collect()
        return f"Import completed: {completed_count} succeeded, {failed_count} failed out of {total_files}{fallback_msg}"
        
    except Exception as e:
        error_msg = str(e).replace("'", "''")[:500]
        session.sql(f"UPDATE FIRN_TASK_REGISTRY SET TASK_STATUS = 'FAILED', ERROR_MESSAGE = '{error_msg}' WHERE TASK_NAME = '{task_name}'").collect()
        return f"Import failed: {error_msg}"
$$;

SELECT 'Import procedure created' AS STATUS;

--------------------------------------------------------------------------------
-- STEP 4: Grant Permissions
--------------------------------------------------------------------------------
-- Update with your actual role name
GRANT SELECT, INSERT, UPDATE ON TABLE FIRN_TASK_REGISTRY TO ROLE SYSADMIN;
GRANT USAGE ON PROCEDURE FIRN_EXPORT_ASYNC_PROC(STRING, STRING, STRING, STRING, STRING, STRING, STRING, STRING, STRING, STRING, INT, BOOLEAN, BOOLEAN, NUMBER) TO ROLE SYSADMIN;
GRANT USAGE ON PROCEDURE FIRN_IMPORT_ASYNC_PROC(STRING, STRING, STRING, STRING, STRING, STRING, STRING, STRING, INT) TO ROLE SYSADMIN;

SELECT 'Permissions granted' AS STATUS;

/*==============================================================================
  Deployment Complete!
  
  Created Objects:
    ✓ FIRN_TASK_REGISTRY table
    ✓ FIRN_EXPORT_ASYNC_PROC procedure
    ✓ FIRN_IMPORT_ASYNC_PROC procedure
  
  Next Steps:
    1. Deploy/update the FirnExchange Streamlit application
    2. The app will create tasks that call these procedures
    3. Monitor tasks from the "Monitor Tasks" tab
==============================================================================*/
