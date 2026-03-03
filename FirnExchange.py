"""
FirnExchange - High-Performance Data Migration Tool for Snowflake
Version: 2.0
Copyright (c) 2024-2026 Snowflake Inc.

DISCLAIMER: This software is provided by Snowflake Inc. on an "AS IS" basis without 
warranties or conditions of any kind, either express or implied. By using this software, 
you acknowledge and agree that you are solely responsible for determining the appropriateness 
of using this tool and assume all risks associated with its use, including but not limited 
to data integrity, performance impacts, and operational outcomes. Snowflake Inc. shall not 
be liable for any direct, indirect, incidental, special, or consequential damages arising 
from the use of this software. You accept full responsibility for all benefits and risks 
associated with deploying and operating FirnExchange. Use at your own risk.

For complete license terms, see LICENSE.txt
cd /Users/vverma/sprojects/snowflake/apps/FirnExchange_V2
conda activate firnexchange
streamlit run FirnExchange.py

FIRN_LOCAL_TEST=true SNOWFLAKE_CONNECTION_NAME=sf-usb97494-vverma-kp_une streamlit run FirnExchange.py

# SNOWFLAKE_CONNECTION_NAME=sf-usb97494-vverma-kp_une streamlit run /Users/vverma/sprojects/snowflake/apps/Notebook_analyzer/app.py --server.port 8502

All 3 test using sql files are successful.
test_copy_into_add_files_copy.sql
test_copy_into_add_files_reference.sql
test_copy_into_full_ingest.sql


However when I am testing through streamlit app, 
add_files_reference and full_ingest are working fine 
but add_files_copy does not showing rows in iceberg table after process completes.

Test load add_files_copy import in streamlit app and confirm if if you data in final iceberg table.

"""

import streamlit as st
import snowflake.snowpark as snowpark
from snowflake.snowpark import Session
import pandas as pd
import datetime
from datetime import datetime as dt
import time
import json
from typing import List, Dict, Any
import traceback

# Page configuration
st.set_page_config(
    page_title="FirnTransfer",
    page_icon="❄️",
    layout="wide",
    initial_sidebar_state="expanded"
)

# Snowflake theme styling
st.markdown("""
<style>
    .stApp {
        background-color: #f8f9fa;
    }
    .stButton>button {
        background-color: #29B5E8;
        color: white;
        font-weight: bold;
        border-radius: 5px;
        border: none;
        padding: 0.5rem 1rem;
    }
    .stButton>button:hover {
        background-color: #1a8fc1;
    }
    .stSelectbox, .stMultiselect {
        color: #0E1117;
    }
    h1, h2, h3 {
        color: #29B5E8;
    }
    .sidebar .sidebar-content {
        background-color: #e8f4f8;
    }
</style>
""", unsafe_allow_html=True)

# Initialize session state
if 'session' not in st.session_state:
    st.session_state.session = None
if 'connected' not in st.session_state:
    st.session_state.connected = False
if 'auto_connected' not in st.session_state:
    st.session_state.auto_connected = False
if 'connection_params' not in st.session_state:
    st.session_state.connection_params = {}
if 'running_environment' not in st.session_state:
    st.session_state.running_environment = None
if 'databases' not in st.session_state:
    st.session_state.databases = []
if 'schemas' not in st.session_state:
    st.session_state.schemas = []
if 'tables' not in st.session_state:
    st.session_state.tables = []
if 'columns' not in st.session_state:
    st.session_state.columns = []
if 'stages' not in st.session_state:
    st.session_state.stages = []
if 'migration_running' not in st.session_state:
    st.session_state.migration_running = False
if 'migration_stats' not in st.session_state:
    st.session_state.migration_stats = {'pending': 0, 'in_progress': 0, 'completed': 0, 'failed': 0}
if 'tracking_table_name' not in st.session_state:
    st.session_state.tracking_table_name = None
if 'selected_database' not in st.session_state:
    st.session_state.selected_database = None
if 'selected_schema' not in st.session_state:
    st.session_state.selected_schema = None
if 'selected_table' not in st.session_state:
    st.session_state.selected_table = None

def validate_sis_environment():
    """
    Validate that the application is running in Streamlit in Snowflake (SiS).
    Returns True if in SiS, False otherwise.
    For local testing, set environment variable FIRN_LOCAL_TEST=true
    """
    import os
    if os.environ.get('FIRN_LOCAL_TEST', '').lower() == 'true':
        return True  # Allow local testing
    try:
        from snowflake.snowpark.context import get_active_session
        get_active_session()
        return True
    except:
        return False

def get_snowpark_session():
    """
    Get Snowpark session from Streamlit in Snowflake environment.
    For local testing, uses connection name from SNOWFLAKE_CONNECTION_NAME env var.
    Returns: session object
    """
    import os
    
    # Check for local testing mode
    if os.environ.get('FIRN_LOCAL_TEST', '').lower() == 'true':
        try:
            import snowflake.connector
            from snowflake.snowpark import Session
            connection_name = os.environ.get('SNOWFLAKE_CONNECTION_NAME', 'sf-usb97494-vverma-kp_une')
            conn = snowflake.connector.connect(connection_name=connection_name)
            session = Session.builder.configs({"connection": conn}).create()
            st.session_state.running_environment = "Local (Testing)"
            print(f"[Environment] Running in Local Testing Mode with connection: {connection_name}")
            return session
        except Exception as e:
            st.session_state.running_environment = "Invalid"
            print(f"[Environment] Error creating local session: {str(e)}")
            return None
    
    # SiS mode
    try:
        from snowflake.snowpark.context import get_active_session
        session = get_active_session()
        st.session_state.running_environment = "Streamlit in Snowflake"
        print("[Environment] Running in Streamlit in Snowflake")
        return session
    except Exception as e:
        st.session_state.running_environment = "Invalid"
        print(f"[Environment] Error: Cannot get active session - {str(e)}")
        return None

def get_databases(session):
    """Get list of databases"""
    try:
        result = session.sql("SHOW DATABASES").collect()
        return [row['name'] for row in result]
    except Exception as e:
        st.error(f"Error fetching databases: {str(e)}")
        return []

def get_schemas(session, database):
    """Get list of schemas in a database"""
    try:
        result = session.sql(f'SHOW SCHEMAS IN DATABASE "{database}"').collect()
        return [row['name'] for row in result]
    except Exception as e:
        st.error(f"Error fetching schemas: {str(e)}")
        return []

def get_tables(session, database, schema):
    """Get list of tables in a schema"""
    try:
        result = session.sql(f'SHOW TABLES IN SCHEMA "{database}"."{schema}"').collect()
        return [row['name'] for row in result]
    except Exception as e:
        st.error(f"Error fetching tables: {str(e)}")
        return []

def get_columns(session, database, schema, table):
    """Get list of columns in a table"""
    try:
        result = session.sql(f'DESCRIBE TABLE "{database}"."{schema}"."{table}"').collect()
        return [(row['name'], row['type']) for row in result]
    except Exception as e:
        st.error(f"Error fetching columns: {str(e)}")
        return []

def get_external_stages(session):
    """Get list of external stages"""
    try:
        result = session.sql("""SHOW STAGES IN ACCOUNT ->> SELECT "database_name" ||'.' || "schema_name" || '.' || "name" "name" from $1 where "type" = 'EXTERNAL'""").collect()
        return [row['name'] for row in result]
    except Exception as e:
        st.error(f"Error fetching stages: {str(e)}")
        return []

def check_log_table_exists(session, database, schema, table_name, operation_type='export'):
    """Check if log table exists for the given table
    
    Args:
        operation_type: 'export' or 'import'
    """
    log_table_name = f"{table_name}_{operation_type.upper()}_FELOG"
    try:
        check_sql = f"""
        SELECT COUNT(*) as cnt 
        FROM "{database}".INFORMATION_SCHEMA.TABLES 
        WHERE TABLE_SCHEMA = '{schema}' 
        AND TABLE_NAME = '{log_table_name}'
        """
        result = session.sql(check_sql).collect()
        exists = result[0]['CNT'] > 0
        return exists, log_table_name
    except Exception as e:
        print(f"[Log Table Check] Error: {str(e)}")
        return False, log_table_name

def create_import_log_table(session, database, schema, table_name):
    """Create log table for import operations"""
    log_table_name = f"{table_name}_IMPORT_FELOG"
    log_table = f'"{database}"."{schema}"."{log_table_name}"'
    
    try:
        # Check if table exists
        check_sql = f"""
        SELECT COUNT(*) as cnt 
        FROM "{database}".INFORMATION_SCHEMA.TABLES 
        WHERE TABLE_SCHEMA = '{schema}' 
        AND TABLE_NAME = '{log_table_name}'
        """
        result = session.sql(check_sql).collect()
        table_exists = result[0]['CNT'] > 0
        
        if table_exists:
            print(f"[Import Log Table] Reusing existing log table: {log_table}")
            return log_table
        else:
            print(f"[Import Log Table] Creating new log table: {log_table}")
            create_sql = f"""
            CREATE TABLE IF NOT EXISTS {log_table} (
                FILE_PATH STRING,
                FILE_STATUS STRING DEFAULT 'PENDING',
                IMPORT_START_AT TIMESTAMP DEFAULT CURRENT_TIMESTAMP(),
                IMPORT_END_AT TIMESTAMP,
                ERROR_MESSAGE STRING,
                ROWS_LOADED BIGINT,
                ROWS_PARSED BIGINT
            )
            """
            session.sql(create_sql).collect()
            return log_table
    except Exception as e:
        st.error(f"Error creating import log table: {str(e)}")
        return None

def create_tracking_table(session, database, schema, table_name, partition_columns, column_types_dict):
    """Create tracking table for migration - reuses existing if present"""
    # Create consistent tracking table name based on source table (for export)
    tracking_table_name = f"{table_name}_EXPORT_FELOG"
    tracking_table = f'"{database}"."{schema}"."{tracking_table_name}"'
    
    # Build column definitions for partition keys
    partition_col_defs = []
    for col in partition_columns:
        col_type = column_types_dict.get(col, 'STRING')
        partition_col_defs.append(f'"{col}" {col_type}')
    
    partition_cols_sql = ', '.join(partition_col_defs)
    
    # Check if table exists
    try:
        check_sql = f"""
        SELECT COUNT(*) as cnt 
        FROM "{database}".INFORMATION_SCHEMA.TABLES 
        WHERE TABLE_SCHEMA = '{schema}' 
        AND TABLE_NAME = '{tracking_table_name}'
        """
        result = session.sql(check_sql).collect()
        table_exists = result[0]['CNT'] > 0
        
        if table_exists:
            print(f"[Tracking Table] Reusing existing tracking table: {tracking_table}")
            st.info(f"Reusing existing tracking table: {tracking_table_name}")
            return tracking_table
        else:
            print(f"[Tracking Table] Creating new tracking table: {tracking_table}")
            create_sql = f"""
            -- CREATE HYBRID TABLE IF NOT EXISTS {tracking_table} (
            CREATE TABLE IF NOT EXISTS {tracking_table} (
                --ID INT AUTOINCREMENT PRIMARY KEY,
                {partition_cols_sql} PRIMARY KEY,
                TOTAL_ROWS BIGINT,
                PARTITION_MIGRATED_STATUS STRING DEFAULT 'NOT_SELECTED',
                PARTITION_MIGRATED_START_AT TIMESTAMP DEFAULT CURRENT_TIMESTAMP(),
                PARTITION_MIGRATED_END_AT TIMESTAMP DEFAULT CURRENT_TIMESTAMP(),
                ERROR_MESSAGE STRING,
                RETRY_COUNT INT DEFAULT 0,
                ROWS_UNLOADED BIGINT,
                INPUT_BYTES BIGINT,
                OUTPUT_BYTES BIGINT
            )
            """
            
            session.sql(create_sql).collect()
            return tracking_table
    except Exception as e:
        st.error(f"Error creating tracking table: {str(e)}")
        return None

def populate_tracking_table(session, tracking_table, source_database, source_schema, source_table, partition_columns):
    """Populate tracking table with distinct partition values - only adds new partitions"""
    try:
        partition_cols = ', '.join([f'"{col}"' for col in partition_columns])
        
        # Check if tracking table already has data
        count_result = session.sql(f"SELECT COUNT(*) as cnt FROM {tracking_table}").collect()
        existing_count = count_result[0]['CNT']
        
        if existing_count > 0:
            print(f"[Tracking Table] Found {existing_count} existing partitions in tracking table")
            st.info(f"Found {existing_count} existing partitions in tracking table. Only new partitions will be added.")
            
            # Insert only new partitions that don't exist in tracking table
            # Build the join condition
            join_conditions = []
            for col in partition_columns:
                join_conditions.append(f't."{col}" = s."{col}"')
            join_clause = ' AND '.join(join_conditions)
            
            insert_sql = f"""
            INSERT INTO {tracking_table} ({partition_cols}, TOTAL_ROWS, PARTITION_MIGRATED_STATUS)
            SELECT s.{partition_cols}, COUNT(*) as TOTAL_ROWS, 'NOT_SELECTED' as PARTITION_MIGRATED_STATUS
            FROM "{source_database}"."{source_schema}"."{source_table}" s
            LEFT JOIN {tracking_table} t ON {join_clause}
            WHERE t."{partition_columns[0]}" IS NULL
            GROUP BY s.{partition_cols}
            """
        else:
            print(f"[Tracking Table] No existing partitions, inserting all")
            # Insert all partitions
            insert_sql = f"""
            INSERT INTO {tracking_table} ({partition_cols}, TOTAL_ROWS)
            SELECT {partition_cols}, COUNT(*) as TOTAL_ROWS
            FROM "{source_database}"."{source_schema}"."{source_table}"
            GROUP BY {partition_cols}
            """
        
        session.sql(insert_sql).collect()
        
        # Get final count of all partitions (not just PENDING)
        final_count_result = session.sql(f"SELECT COUNT(*) as cnt FROM {tracking_table}").collect()
        total_count = final_count_result[0]['CNT']
        print(f"[Tracking Table] {total_count} total partitions discovered")
        st.success(f"{total_count} partitions discovered. Select partitions below to export.")
        
        return True
    except Exception as e:
        st.error(f"Error populating tracking table: {str(e)}")
        print(f"[Tracking Table] Error: {str(e)}")
        import traceback
        traceback.print_exc()
        return False

def load_log_table_data_as_list(session, log_table_name, database, schema):
    """Load data from export log table and return as list of simple dicts (no pandas)"""
    try:
        log_table = f'"{database}"."{schema}"."{log_table_name}"'
        
        # First, get column names using DESCRIBE TABLE
        desc_result = session.sql(f"DESCRIBE TABLE {log_table}").collect()
        col_names = []
        for row in desc_result:
            try:
                # Extract column name - try different ways
                if hasattr(row, 'name'):
                    col_names.append(str(row.name))
                elif 'name' in row:
                    col_names.append(str(row['name']))
                else:
                    # Try accessing by index 0
                    col_names.append(str(row[0]))
            except Exception as e:
                print(f"[Load Log Table] Error extracting column name: {e}")
        
        print(f"[Load Log Table] Column names: {col_names}")
        
        # Now query the data
        query = f"""
            SELECT * FROM {log_table} 
            ORDER BY PARTITION_MIGRATED_STATUS, PARTITION_MIGRATED_START_AT
        """
        
        # Collect results
        result = session.sql(query).collect()
        
        if not result:
            return []
        
        # Convert to simple list of dicts using column names
        data = []
        for i, row in enumerate(result):
            row_dict = {'_index': i}  # Add index for selection tracking
            
            # Use column names we got from DESCRIBE TABLE
            for col_idx, col_name in enumerate(col_names):
                try:
                    val = row[col_idx]
                    row_dict[col_name] = str(val) if val is not None else ''
                except Exception as e:
                    print(f"[Load Log Table] Error accessing row[{col_idx}]: {e}")
                    row_dict[col_name] = ''
            
            data.append(row_dict)
        
        print(f"[Load Log Table] Successfully loaded {len(data)} rows")
        return data
    except Exception as e:
        print(f"[Load Log Table] Error loading log table data: {str(e)}")
        import traceback
        traceback.print_exc()
        return []

def get_parquet_compatible_columns(session, database, schema, table):
    """
    Get column list with Parquet-compatible type casting for export operations.
    
    Transforms incompatible Snowflake data types to Parquet-supported formats:
    - TIMESTAMP_TZ/LTZ -> TIMESTAMP_NTZ(6)
    - VARIANT -> STRING
    - GEOGRAPHY/GEOMETRY -> ST_ASTEXT()
    - TIME -> TIME(6)
    
    Returns:
        tuple: (column_selection_string, transformation_info_dict)
    """
    try:
        print(f"[Parquet Columns] Analyzing table schema: {database}.{schema}.{table}")
        
        # Query to build Parquet-compatible column list with transformations
        # Note: Column names are double-quoted for case sensitivity and special characters
        query = f"""
        SELECT LISTAGG(
            CASE
                WHEN data_type = 'TIME'          THEN '"' || column_name || '"::TIME(6) "' || column_name || '"'
                WHEN data_type = 'TIMESTAMP_TZ'  THEN '"' || column_name || '"::TIMESTAMP_NTZ(6) "' || column_name || '"'
                WHEN data_type = 'TIMESTAMP_LTZ' THEN '"' || column_name || '"::TIMESTAMP_NTZ(6) "' || column_name || '"'
                WHEN data_type = 'TIMESTAMP_NTZ' THEN '"' || column_name || '"::TIMESTAMP_NTZ(6) "' || column_name || '"'
                WHEN data_type = 'VARIANT'       THEN '"' || column_name || '"::STRING "' || column_name || '"'
                WHEN data_type = 'GEOGRAPHY'     THEN 'ST_ASTEXT("' || column_name || '") "' || column_name || '"'
                WHEN data_type = 'GEOMETRY'      THEN 'ST_ASTEXT("' || column_name || '") "' || column_name || '"'
                ELSE '"' || column_name || '"'
            END, ', '
        ) WITHIN GROUP (ORDER BY ordinal_position) AS column_list
        FROM "{database}".INFORMATION_SCHEMA.COLUMNS
        WHERE table_catalog = '{database}'
          AND table_schema = '{schema}'
          AND table_name = '{table}'
        """
        
        result = session.sql(query).collect()
        
        if not result or not result[0]['COLUMN_LIST']:
            raise Exception(f"No columns found for table {database}.{schema}.{table}")
        
        column_list = result[0]['COLUMN_LIST']
        print(f"[Parquet Columns] Generated column list with {column_list.count(',') + 1} columns")
        
        # Get transformation details for UI display
        detail_query = f"""
        SELECT 
            data_type,
            COUNT(*) as count
        FROM "{database}".INFORMATION_SCHEMA.COLUMNS
        WHERE table_catalog = '{database}'
          AND table_schema = '{schema}'
          AND table_name = '{table}'
          AND data_type IN ('TIMESTAMP_TZ', 'TIMESTAMP_LTZ', 'TIMESTAMP_NTZ', 'TIME', 'VARIANT', 'GEOGRAPHY', 'GEOMETRY')
        GROUP BY data_type
        ORDER BY data_type
        """
        
        detail_result = session.sql(detail_query).collect()
        
        # Build transformation info
        transformations = {}
        for row in detail_result:
            data_type = row['DATA_TYPE']
            count = row['COUNT']
            transformations[data_type] = count
        
        print(f"[Parquet Columns] Transformations: {transformations}")
        
        return column_list, transformations
        
    except Exception as e:
        error_msg = f"Failed to generate Parquet-compatible columns: {str(e)}"
        print(f"[Parquet Columns] ERROR: {error_msg}")
        raise Exception(error_msg)

# ============================================================================
# TASK MANAGEMENT FUNCTIONS (v2.0 - Task-based Architecture)
# ============================================================================

def create_task_name(operation_type, database, schema, table):
    """Generate unique task name with timestamp"""
    timestamp = dt.now().strftime('%Y%m%d_%H%M%S')
    return f"FIRN_{operation_type}_{database}_{schema}_{table}_{timestamp}".upper()

def create_export_task(session, task_name, tracking_table, source_db, source_schema,
                       source_table, partition_col, stage, stage_path, parquet_cols,
                       warehouse, max_workers, overwrite, single, max_size):
    """Create and start Snowflake task for export operation"""
    try:
        # Get total pending partitions
        pending_count = session.sql(f"""
            SELECT COUNT(*) as cnt FROM {tracking_table}
            WHERE PARTITION_MIGRATED_STATUS = 'PENDING'
        """).collect()[0]['CNT']
        
        # Register in task registry
        params_json = json.dumps({
            'source_db': source_db,
            'source_schema': source_schema,
            'source_table': source_table,
            'partition_col': partition_col,
            'stage': stage,
            'stage_path': stage_path,
            'warehouse': warehouse,
            'max_workers': max_workers,
            'overwrite': overwrite,
            'single': single,
            'max_size': max_size
        }).replace("'", "''")
        
        session.sql(f"""
            INSERT INTO FT_DB.FT_SCH.FIRN_TASK_REGISTRY (
                TASK_NAME, OPERATION_TYPE, SOURCE_DATABASE, SOURCE_SCHEMA,
                SOURCE_TABLE, TRACKING_TABLE, STAGE, STAGE_PATH,
                WAREHOUSE, MAX_WORKERS, TOTAL_ITEMS, TASK_STATUS, PARAMETERS
            ) SELECT
                '{task_name}', 'EXPORT', '{source_db}', '{source_schema}',
                '{source_table}', '{tracking_table}', '{stage}', '{stage_path}',
                '{warehouse}', {max_workers}, {pending_count}, 'CREATED', PARSE_JSON('{params_json}')
        """).collect()
        
        # Create Snowflake task
        task_sql = f"""
        CREATE TASK {task_name}
            WAREHOUSE = {warehouse}
            SCHEDULE = 'USING CRON 0 0 1 1 * UTC'
        AS
        CALL FIRN_EXPORT_ASYNC_PROC(
            '{task_name}',
            '{tracking_table}',
            '{source_db}',
            '{source_schema}',
            '{source_table}',
            '{partition_col}',
            '{stage}',
            '{stage_path}',
            '{parquet_cols}',
            '{warehouse}',
            {max_workers},
            {str(overwrite).upper()},
            {str(single).upper()},
            {max_size}
        )
        """
        
        session.sql(task_sql).collect()
        
        # Resume task to run immediately
        session.sql(f"ALTER TASK {task_name} RESUME").collect()
        session.sql(f"EXECUTE TASK {task_name}").collect()
        
        return task_name, None
        
    except Exception as e:
        return None, str(e)

def create_import_task(session, task_name, import_log_table, target_db, target_schema,
                      target_table, stage, warehouse, load_mode, max_workers):
    """Create and start Snowflake task for import operation"""
    try:
        # Get total pending files
        pending_count = session.sql(f"""
            SELECT COUNT(*) as cnt FROM {import_log_table}
            WHERE FILE_STATUS = 'PENDING'
        """).collect()[0]['CNT']
        
        # Register in task registry
        params_json = json.dumps({
            'target_db': target_db,
            'target_schema': target_schema,
            'target_table': target_table,
            'stage': stage,
            'load_mode': load_mode,
            'warehouse': warehouse,
            'max_workers': max_workers
        }).replace("'", "''")
        
        session.sql(f"""
            INSERT INTO FT_DB.FT_SCH.FIRN_TASK_REGISTRY (
                TASK_NAME, OPERATION_TYPE, TARGET_DATABASE, TARGET_SCHEMA,
                TARGET_TABLE, TRACKING_TABLE, STAGE, WAREHOUSE,
                MAX_WORKERS, TOTAL_ITEMS, TASK_STATUS, PARAMETERS
            ) SELECT
                '{task_name}', 'IMPORT', '{target_db}', '{target_schema}',
                '{target_table}', '{import_log_table}', '{stage}', '{warehouse}',
                {max_workers}, {pending_count}, 'CREATED', PARSE_JSON('{params_json}')
        """).collect()
        
        # Create Snowflake task
        task_sql = f"""
        CREATE TASK {task_name}
            WAREHOUSE = {warehouse}
            SCHEDULE = 'USING CRON 0 0 1 1 * UTC'
        AS
        CALL FIRN_IMPORT_ASYNC_PROC(
            '{task_name}',
            '{import_log_table}',
            '{target_db}',
            '{target_schema}',
            '{target_table}',
            '{stage}',
            '{warehouse}',
            '{load_mode}',
            {max_workers}
        )
        """
        
        session.sql(task_sql).collect()
        
        # Resume and execute task
        session.sql(f"ALTER TASK {task_name} RESUME").collect()
        session.sql(f"EXECUTE TASK {task_name}").collect()
        
        return task_name, None
        
    except Exception as e:
        return None, str(e)

def get_active_tasks(session):
    """Get active FIRN tasks from TASK_HISTORY"""
    try:
        result = session.sql("""
            SELECT 
                NAME as TASK_NAME,
                STATE,
                SCHEDULED_TIME,
                QUERY_START_TIME,
                COMPLETED_TIME,
                DATABASE_NAME,
                SCHEMA_NAME
            FROM TABLE(INFORMATION_SCHEMA.TASK_HISTORY())
            WHERE NAME LIKE 'FIRN_%'
            AND STATE IN ('SCHEDULED', 'EXECUTING')
            ORDER BY SCHEDULED_TIME DESC
            LIMIT 100
        """).collect()
        return result
    except Exception as e:
        print(f"[Task Monitor] Error getting active tasks: {str(e)}")
        return []

def get_task_registry_info(session, task_name=None):
    """Get task info from FIRN_TASK_REGISTRY"""
    try:
        if task_name:
            where_clause = f"WHERE TASK_NAME = '{task_name}'"
        else:
            where_clause = ""
        
        result = session.sql(f"""
            SELECT * FROM FT_DB.FT_SCH.FIRN_TASK_REGISTRY
            {where_clause}
            ORDER BY CREATED_AT DESC
            LIMIT 100
        """).collect()
        return result
    except Exception as e:
        print(f"[Task Monitor] Error getting task registry info: {str(e)}")
        return []

def suspend_task(session, task_name):
    """Suspend a running task"""
    try:
        session.sql(f"ALTER TASK {task_name} SUSPEND").collect()
        session.sql(f"""
            UPDATE FT_DB.FT_SCH.FIRN_TASK_REGISTRY
            SET TASK_STATUS = 'SUSPENDED'
            WHERE TASK_NAME = '{task_name}'
        """).collect()
        return True, None
    except Exception as e:
        return False, str(e)

def resume_task(session, task_name):
    """Resume a suspended task"""
    try:
        session.sql(f"ALTER TASK {task_name} RESUME").collect()
        session.sql(f"""
            UPDATE FT_DB.FT_SCH.FIRN_TASK_REGISTRY
            SET TASK_STATUS = 'RUNNING'
            WHERE TASK_NAME = '{task_name}'
        """).collect()
        return True, None
    except Exception as e:
        return False, str(e)

def drop_task(session, task_name):
    """Drop a task and remove from registry"""
    try:
        session.sql(f"DROP TASK IF EXISTS {task_name}").collect()
        session.sql(f"""
            DELETE FROM FT_DB.FT_SCH.FIRN_TASK_REGISTRY
            WHERE TASK_NAME = '{task_name}'
        """).collect()
        return True, None
    except Exception as e:
        return False, str(e)

# ============================================================================

def build_where_clause(partition_columns, partition_values):
    """Build WHERE clause for partition"""
    conditions = []
    for col, val in zip(partition_columns, partition_values):
        if val is None:
            conditions.append(f'"{col}" IS NULL')
        elif isinstance(val, (str, datetime.date, datetime.datetime)):
            # Quote strings, dates, and timestamps
            conditions.append(f'"{col}" = \'{val}\'')
        else:
            # Numeric types don't need quotes
            conditions.append(f'"{col}" = {val}')
    return ' AND '.join(conditions)

def format_elapsed_time(elapsed_seconds):
    """Format elapsed time as days, hours, minutes, seconds"""
    days = int(elapsed_seconds // 86400)
    hours = int((elapsed_seconds % 86400) // 3600)
    minutes = int((elapsed_seconds % 3600) // 60)
    seconds = int(elapsed_seconds % 60)
    
    parts = []
    if days > 0:
        parts.append(f"{days} day{'s' if days != 1 else ''}")
    if hours > 0:
        parts.append(f"{hours} hour{'s' if hours != 1 else ''}")
    if minutes > 0:
        parts.append(f"{minutes} minute{'s' if minutes != 1 else ''}")
    parts.append(f"{seconds} second{'s' if seconds != 1 else ''}")
    
    return ", ".join(parts)

def get_migration_stats(session, tracking_table):
    """Get migration statistics - excludes NOT_SELECTED partitions"""
    try:
        result = session.sql(f"""
        SELECT 
            PARTITION_MIGRATED_STATUS,
            COUNT(*) as COUNT
        FROM {tracking_table}
        GROUP BY PARTITION_MIGRATED_STATUS
        """).collect()
        
        stats = {'PENDING': 0, 'IN_PROGRESS': 0, 'COMPLETED': 0, 'FAILED': 0}
        for row in result:
            status = row['PARTITION_MIGRATED_STATUS']
            # Only count partitions that are part of the migration (exclude NOT_SELECTED)
            if status in stats:
                stats[status] = row['COUNT']
        
        return stats
    except Exception as e:
        return {'PENDING': 0, 'IN_PROGRESS': 0, 'COMPLETED': 0, 'FAILED': 0}

def get_tracking_data(session, tracking_table):
    """Get tracking table data for display - returns list of dicts using simple types"""
    try:
        # Simple query without complex type conversions
        query = f"""
            SELECT * 
            FROM {tracking_table} 
            ORDER BY PARTITION_MIGRATED_START_AT DESC
        """
        result = session.sql(query).collect()
        
        if not result:
            return []
        
        # Get column names from first row as pure Python strings
        first_row_dict = result[0].asDict()
        column_names = [str(k) for k in first_row_dict.keys()]
        
        # Build data as list of dicts with pure Python types
        data = []
        for row in result:
            row_dict = {}
            for i, col in enumerate(column_names):
                try:
                    # Access by index to avoid complex key types
                    val = row[i]
                    # Convert to simple Python string
                    row_dict[col] = str(val) if val is not None else None
                except Exception as e:
                    print(f"[Tracking Data] Error accessing row[{i}]: {str(e)}")
                    row_dict[col] = None
            data.append(row_dict)
        
        return data
    except Exception as e:
        st.error(f"Error fetching tracking data: {str(e)}")
        print(f"[Tracking Data] Error: {str(e)}")
        import traceback
        traceback.print_exc()
        return []

# Main UI
# Environment badge in top right corner
if st.session_state.running_environment == "Streamlit in Snowflake":
    st.markdown('<div style="position: fixed; top: 10px; right: 60px; z-index: 999; background: #f0f2f6; padding: 5px 10px; border-radius: 5px; font-weight: bold;">🏔️ SiS</div>', unsafe_allow_html=True)
elif st.session_state.running_environment == "Local":
    st.markdown('<div style="position: fixed; top: 10px; right: 60px; z-index: 999; background: #f0f2f6; padding: 5px 10px; border-radius: 5px; font-weight: bold;">💻 Local</div>', unsafe_allow_html=True)

# Centered title
st.markdown("""
<div style="text-align: center; margin-bottom: 2rem;">
    <h1 style="color: #29B5E8; margin-bottom: 0;">❄️ FirnExchange</h1>
    <h3 style="color: #666; margin-top: 0.5rem;">High-Performance Data Exchange Iceberg Migration</h3>
</div>
""", unsafe_allow_html=True)

# Create tabs
# Tab 1: Configure & Launch, Tab 2: Monitor Tasks
tab1, tab2 = st.tabs(["📋 Configure & Launch", "📊 Monitor Tasks"])

# Validate environment before proceeding
if not validate_sis_environment():
    st.error("⚠️ **Invalid Environment**")
    st.markdown("""
    ### Environment Not Detected
    
    **FirnExchange** is designed for **Streamlit in Snowflake (SiS)** or **Local Testing** mode.
    
    **Option 1: Run in Snowflake (Production)**
    1. Deploy using the `firnexchange_sis.sql` deployment script
    2. Access via: **Data Products > Streamlit > FirnExchange**
    
    **Option 2: Run Locally (Testing)**
    ```bash
    FIRN_LOCAL_TEST=true SNOWFLAKE_CONNECTION_NAME=sf-usb97494-vverma-kp_une streamlit run FirnExchange.py
    ```
    
    **Required Environment Variables for Local Testing:**
    - `FIRN_LOCAL_TEST=true` - Enables local testing mode
    - `SNOWFLAKE_CONNECTION_NAME` - Connection name from ~/.snowflake/connections.toml
    """)
    st.stop()

# Sidebar - Connection Management (SiS only)
with st.sidebar:
    st.header("Connection")
    
    # Auto-connect to Snowflake session
    if not st.session_state.connected:
        session = get_snowpark_session()
        
        if session:
            st.session_state.session = session
            st.session_state.connected = True
            st.session_state.auto_connected = True
            # running_environment is already set by get_snowpark_session()
            
            # Get connection info from active session
            try:
                current_db = session.get_current_database()
                current_schema = session.get_current_schema()
                current_warehouse = session.get_current_warehouse()
                current_role = session.get_current_role()
                current_user = session.get_current_user()
                
                # Get account name using SQL
                current_account = 'N/A'
                try:
                    account_result = session.sql("SELECT current_account() as account").collect()
                    if account_result:
                        current_account = account_result[0]['ACCOUNT']
                except:
                    pass
                
                st.session_state.connection_params = {
                    'account': current_account,
                    'database': current_db,
                    'schema': current_schema,
                    'warehouse': current_warehouse,
                    'role': current_role,
                    'user': current_user
                }
            except:
                st.session_state.connection_params = {}
            
            # Load metadata
            st.session_state.stages = get_external_stages(session)
            st.session_state.databases = get_databases(session)
            
            st.success("🏔️ Connected to Snowflake")
            st.rerun()
        else:
            st.error("❌ Failed to connect to Snowflake")
            st.stop()
    
    # Show environment indicator
    env = st.session_state.get('running_environment', 'Unknown')
    if 'Local' in env:
        st.warning(f"💻 **{env}**")
    else:
        st.info(f"🏔️ **{env}**")
    
    # Show connection details
    if st.session_state.connected:
        st.divider()
        st.subheader("Connection Details")
        connection_params = st.session_state.connection_params
        
        # Get account name from SQL if not in params
        account_name = connection_params.get('account', 'N/A')
        if account_name == 'N/A' and st.session_state.session:
            try:
                account_result = st.session_state.session.sql("SELECT current_account() as account").collect()
                if account_result:
                    account_name = account_result[0]['ACCOUNT']
            except:
                account_name = 'N/A'
        
        st.write(f"**Account:** {account_name}")
        st.write(f"**User:** {connection_params.get('user', 'N/A')}")
        st.write(f"**Role:** {connection_params.get('role', 'N/A')}")
        st.write(f"**Warehouse:** {connection_params.get('warehouse', 'N/A')}")
        
    # Environment info
    if st.session_state.connected:
        st.divider()
        env = st.session_state.get('running_environment', 'Unknown')
        with st.expander("ℹ️ Environment Info", expanded=False):
            if 'Local' in env:
                st.markdown("""
                **Local Testing Mode**
                - 💻 Running on local machine
                - 🔗 Using connection from connections.toml
                - ⚠️ For testing/development only
                - ✅ All features supported
                """)
            else:
                st.markdown("""
                **Streamlit in Snowflake**
                - ✅ Native Snowflake authentication
                - ✅ Secure execution context
                - ✅ No external configuration required
                - ✅ Centralized access control
                - ✅ All features supported
                """)

# Tab 1: FirnExchange - Combined Export and Import
with tab1:
    st.subheader("Configure & Launch Operations")
    
    if st.session_state.connected:
        session = st.session_state.session
        
        # Initialize session state for FirnExchange
        if 'exchange_operation' not in st.session_state:
            st.session_state.exchange_operation = "FDN Table Data Export"
        if 'exchange_selected_warehouse' not in st.session_state:
            st.session_state.exchange_selected_warehouse = None
        if 'exchange_selected_stage' not in st.session_state:
            st.session_state.exchange_selected_stage = None
        if 'exchange_tracking_table_name' not in st.session_state:
            st.session_state.exchange_tracking_table_name = None
        if 'exchange_calculated_max_workers' not in st.session_state:
            st.session_state.exchange_calculated_max_workers = 366
        if 'exchange_default_workers' not in st.session_state:
            st.session_state.exchange_default_workers = 366
        if 'exchange_selected_partitions' not in st.session_state:
            st.session_state.exchange_selected_partitions = []
        if 'exchange_partitions_list' not in st.session_state:
            st.session_state.exchange_partitions_list = []
        if 'exchange_log_table_loaded' not in st.session_state:
            st.session_state.exchange_log_table_loaded = False
        if 'exchange_parquet_columns' not in st.session_state:
            st.session_state.exchange_parquet_columns = None
        if 'exchange_parquet_transformations' not in st.session_state:
            st.session_state.exchange_parquet_transformations = {}
        
        # Warehouse and Worker Configuration Section
        with st.expander("🏭 Warehouse & Worker Configuration", expanded=True):
            col1, col2 = st.columns(2)
            
            with col1:
                # Load warehouses if not already loaded
                if 'warehouses' not in st.session_state or not st.session_state.warehouses:
                    st.session_state.warehouses = []
                    st.session_state.warehouse_details = {}
                    try:
                        result = session.sql("SHOW WAREHOUSES").collect()
                        for row in result:
                            warehouse_name = row['name']
                            st.session_state.warehouses.append(warehouse_name)
                            row_dict = row.asDict()
                            st.session_state.warehouse_details[warehouse_name] = {
                                'type': row_dict.get('type', 'N/A'),
                                'size': row_dict.get('size', 'N/A'),
                                'min_cluster_count': row_dict.get('min_cluster_count', 'N/A'),
                                'max_cluster_count': row_dict.get('max_cluster_count', 'N/A'),
                                'enable_query_acceleration': row_dict.get('enable_query_acceleration', 'N/A'),
                                'query_acceleration_max_scale_factor': row_dict.get('query_acceleration_max_scale_factor', 'N/A'),
                                'scaling_policy': row_dict.get('scaling_policy', 'N/A'),
                                'warehouse_credit_limit': row_dict.get('warehouse_credit_limit', 'N/A')
                            }
                    except Exception as e:
                        st.error(f"Error loading warehouses: {str(e)}")
                
                if st.session_state.warehouses:
                    exchange_warehouse = st.selectbox(
                        "Warehouse",
                        st.session_state.warehouses,
                        index=0,
                        help="Select the warehouse to use for the operation",
                        key="exchange_warehouse_select"
                    )
                    st.session_state.exchange_selected_warehouse = exchange_warehouse
                    
                    # Display warehouse properties and calculate worker limits
                    if exchange_warehouse and exchange_warehouse in st.session_state.warehouse_details:
                        st.caption("**Warehouse Properties:**")
                        wh_details = st.session_state.warehouse_details[exchange_warehouse]
                        st.caption(f"🔹 **Type:** {wh_details['type']}")
                        st.caption(f"🔹 **Size:** {wh_details['size']}")
                        st.caption(f"🔹 **Cluster Count:** {wh_details['min_cluster_count']} - {wh_details['max_cluster_count']}")
                        st.caption(f"🔹 **Scaling Policy:** {wh_details['scaling_policy']}")
                        
                        # Get max_cluster_count
                        max_cluster_count = wh_details['max_cluster_count']
                        
                        # Fetch and display warehouse parameters
                        max_concurrency_level = 8  # Default value if not found
                        try:
                            params_result = session.sql(f"SHOW PARAMETERS FOR WAREHOUSE {exchange_warehouse}").collect()
                            if params_result:
                                st.caption("**Warehouse Parameters:**")
                                for row in params_result:
                                    row_dict = row.asDict()
                                    param_key = row_dict.get('key', '')
                                    param_value = row_dict.get('value', '')
                                    if param_key and param_value:
                                        st.caption(f"🔹 **{param_key}:** {param_value}")
                                        # Capture MAX_CONCURRENCY_LEVEL
                                        if param_key == 'MAX_CONCURRENCY_LEVEL':
                                            try:
                                                max_concurrency_level = int(param_value)
                                            except (ValueError, TypeError):
                                                max_concurrency_level = 8
                        except Exception as e:
                            print(f"[FirnExchange] Error fetching warehouse parameters: {str(e)}")
                        
                        # Calculate dynamic max workers
                        calculated_max_workers = max_cluster_count * max_concurrency_level
                        
                        # Set default value to the maximum calculated value
                        default_workers = calculated_max_workers
                        
                        # Store in session state for use in slider
                        st.session_state.exchange_calculated_max_workers = calculated_max_workers
                        st.session_state.exchange_default_workers = default_workers
                        
                        # print(f"[FirnExchange] Warehouse: {exchange_warehouse}, Max Cluster: {max_cluster_count}, Max Concurrency: {max_concurrency_level}")
                        # print(f"[FirnExchange] Calculated Max Workers: {calculated_max_workers}, Default: {default_workers}")
                else:
                    st.warning("No warehouses available")
                    exchange_warehouse = None
                    # Set defaults if no warehouse selected
                    st.session_state.exchange_calculated_max_workers = 366
                    st.session_state.exchange_default_workers = 366
            
            with col2:
                # Get dynamic values from session state
                slider_max_value = st.session_state.get('exchange_calculated_max_workers', 366)
                slider_default_value = st.session_state.get('exchange_default_workers', 366)
                
                # Ensure default doesn't exceed max
                slider_default_value = min(slider_default_value, slider_max_value)
                
                exchange_max_workers = st.slider(
                    "Max Concurrent Workers",
                    min_value=1,
                    max_value=slider_max_value,
                    value=slider_default_value,
                    step=1,
                    help=f"Maximum number of concurrent threads (calculated: {slider_max_value} = max_cluster_count × MAX_CONCURRENCY_LEVEL)",
                    key="exchange_max_workers_slider"
                )
                current_operation = st.session_state.get('exchange_operation', 'FDN Table Data Export')
                st.caption(f"Using up to **{exchange_max_workers}** parallel threads for data {current_operation.split()[-1].lower()}")
                
                # Show calculation info if warehouse is selected
                if exchange_warehouse and exchange_warehouse in st.session_state.warehouse_details:
                    wh_details = st.session_state.warehouse_details[exchange_warehouse]
                    max_cluster = wh_details['max_cluster_count']
                    st.caption(f"ℹ️ Max value calculated: {slider_max_value} (Max Clusters: {max_cluster} × Max Concurrency: {slider_max_value // max_cluster})")
        
        st.divider()
        
        # Stage Configuration Section
        with st.expander("📦 Stage Configuration", expanded=True):
            col1, col2 = st.columns(2)
            
            with col1:
                # Load stages if not already loaded
                if 'stages' not in st.session_state or not st.session_state.stages:
                    st.session_state.stages = get_external_stages(session)
                
                if st.session_state.stages:
                    exchange_stage = st.selectbox(
                        "External Stage",
                        st.session_state.stages,
                        key="exchange_stage_select"
                    )
                    st.session_state.exchange_selected_stage = exchange_stage
                    
                    # Display stage properties
                    if exchange_stage:
                        try:
                            # Parse stage name to get database and schema
                            stage_parts = exchange_stage.split('.')
                            if len(stage_parts) == 3:
                                stage_db = stage_parts[0]
                                stage_sch = stage_parts[1]
                                stage_name = stage_parts[2]
                                show_stage_cmd = f"SHOW STAGES LIKE '{stage_name}' IN {stage_db}.{stage_sch}"
                            else:
                                show_stage_cmd = f"SHOW STAGES LIKE '{exchange_stage}'"
                            
                            stage_result = session.sql(show_stage_cmd).collect()
                            if stage_result:
                                stage_info = stage_result[0].asDict()
                                
                                # Display stage properties
                                st.caption("**Stage Properties:**")
                                
                                # Extract and display the requested properties
                                url = stage_info.get('url', 'N/A')
                                region = stage_info.get('region', 'N/A')
                                stage_type = stage_info.get('type', 'N/A')
                                storage_integration = stage_info.get('storage_integration', 'N/A')
                                cloud = stage_info.get('cloud', 'N/A')
                                
                                # Create a compact display
                                prop_col1, prop_col2 = st.columns(2)
                                with prop_col1:
                                    st.caption(f"🔹 **Type:** {stage_type}")
                                    st.caption(f"🔹 **Cloud:** {cloud}")
                                    st.caption(f"🔹 **Region:** {region}")
                                with prop_col2:
                                    st.caption(f"🔹 **URL:** {url}")
                                    st.caption(f"🔹 **Storage Integration:** {storage_integration}")
                        except Exception as e:
                            print(f"[Stage Properties] Error fetching stage properties: {str(e)}")
                else:
                    st.warning("No external stages available")
                    exchange_stage = None
            
            with col2:
                exchange_stage_path = st.text_input(
                    "Relative Path in Stage",
                    value="",
                    placeholder="path/to/data",
                    key="exchange_stage_path_input"
                )
            
            # File listing for stage (useful for both export and import)
            if exchange_stage:
                if st.button("List Files in Stage", key="exchange_list_files"):
                    try:
                        if exchange_stage_path:
                            list_command = f"LIST @{exchange_stage}/{exchange_stage_path}"
                        else:
                            list_command = f"LIST @{exchange_stage}"
                        
                        print(f"[FirnExchange] Executing: {list_command}")
                        # Collect and convert to avoid pandas conversion issues with complex types
                        result = session.sql(list_command).collect()
                        
                        if result:
                            # Convert to list of dicts with simple types using JSON serialization
                            import json
                            import pandas as pd
                            data = []
                            for row in result:
                                # Get row as dict first
                                row_dict_raw = row.asDict()
                                row_dict = {}
                                for field_name, value in row_dict_raw.items():
                                    # Ensure field name is a simple string
                                    field_name_str = str(field_name)
                                    
                                    # Convert value to JSON-safe type
                                    try:
                                        json.dumps(value)
                                        row_dict[field_name_str] = value
                                    except (TypeError, ValueError):
                                        row_dict[field_name_str] = str(value)
                                data.append(row_dict)
                            
                            # Use JSON round-trip to ensure all data is JSON-safe
                            try:
                                json_str = json.dumps(data)
                                clean_data = json.loads(json_str)
                                files_df = pd.DataFrame(clean_data)
                            except Exception as e:
                                print(f"[List Files] JSON conversion failed: {str(e)}, falling back to string conversion")
                                string_data = []
                                for row_dict in data:
                                    string_dict = {str(k): str(v) if v is not None else None for k, v in row_dict.items()}
                                    string_data.append(string_dict)
                                files_df = pd.DataFrame(string_data)
                            st.session_state.exchange_files_df = files_df
                            st.session_state.exchange_selected_files = []
                            st.success(f"Found {len(files_df)} file(s)")
                        else:
                            st.warning("No files found in the specified path")
                            st.session_state.exchange_files_df = None
                    except Exception as e:
                        st.error(f"Error listing files: {str(e)}")
                        st.session_state.exchange_files_df = None
                
                # Display files table with checkboxes
                if 'exchange_files_df' in st.session_state and st.session_state.exchange_files_df is not None:
                    st.subheader("Files in Stage")
                    
                    # Filter option
                    col_filter1, col_filter2 = st.columns([2, 4])
                    with col_filter1:
                        file_filter_option = st.radio(
                            "Show:",
                            ["Files and Folders", "Files Only"],
                            index=0,
                            horizontal=True,
                            key="exchange_file_filter"
                        )
                    
                    files_df = st.session_state.exchange_files_df.copy()
                    
                    # Apply filter if "Files Only" is selected
                    # Keep track of original indices before filtering
                    if file_filter_option == "Files Only":
                        # Filter to show only files with size > 0, but keep original index
                        files_df = files_df[files_df['size'] > 0]
                        # Store original indices before reset
                        original_indices = files_df.index.tolist()
                        # Reset index for display but keep the mapping
                        files_df = files_df.reset_index(drop=True)
                        st.caption(f"📄 Showing files only (size > 0): {len(files_df)} file(s)")
                    else:
                        original_indices = list(range(len(files_df)))
                        st.caption(f"📁 Showing files and folders: {len(files_df)} item(s)")
                    
                    if 'exchange_selected_files' not in st.session_state:
                        st.session_state.exchange_selected_files = []
                    
                    # Store original dataframe for operations
                    original_files_df = st.session_state.exchange_files_df
                    
                    # Use the original_indices we saved above for operations
                    filtered_indices = original_indices
                    
                    # Select All / Select None / Delete buttons
                    col_btn1, col_btn2, col_btn3, col_btn4, col_btn5 = st.columns([1, 1, 1, 1, 2])
                    with col_btn1:
                        if st.button("Select All", key="exchange_select_all_files"):
                            # Select all items in the filtered view
                            st.session_state.exchange_selected_files = filtered_indices
                            st.rerun()
                    with col_btn2:
                        if st.button("Select None", key="exchange_select_none_files"):
                            st.session_state.exchange_selected_files = []
                            st.rerun()
                    with col_btn3:
                        if st.button("Delete Selected", key="exchange_delete_selected_files", type="secondary"):
                            if st.session_state.exchange_selected_files:
                                try:
                                    deleted_count = 0
                                    failed_count = 0
                                    for idx in st.session_state.exchange_selected_files:
                                        file_name = original_files_df.loc[idx, 'name']
                                        # Extract relative path from cloud storage URL if needed
                                        relative_file_path = None
                                        
                                        if '://' in file_name and exchange_stage_path:
                                            # Cloud storage URL (s3://, azure://, gs://, etc.)
                                            search_pattern = f'/{exchange_stage_path}/'
                                            if search_pattern in file_name:
                                                split_pos = file_name.index(search_pattern) + len(search_pattern)
                                                relative_file_path = file_name[split_pos:]
                                            else:
                                                # Pattern not found, use the last segment of stage_path to find position
                                                parts = file_name.split('/')
                                                stage_path_segments = exchange_stage_path.rstrip('/').split('/')
                                                last_stage_segment = stage_path_segments[-1]
                                                
                                                stage_idx = -1
                                                for i, part in enumerate(parts):
                                                    if part == last_stage_segment:
                                                        stage_idx = i
                                                
                                                if stage_idx >= 0:
                                                    remaining_parts = parts[stage_idx + 1:]
                                                    if remaining_parts:
                                                        relative_file_path = '/'.join(remaining_parts)
                                                    else:
                                                        relative_file_path = parts[-1]
                                                else:
                                                    relative_file_path = parts[-1]
                                        else:
                                            relative_file_path = file_name.split('/')[-1]
                                        
                                        if exchange_stage_path:
                                            rm_command = f"REMOVE @{exchange_stage}/{exchange_stage_path}/{relative_file_path}"
                                        else:
                                            rm_command = f"REMOVE @{exchange_stage}/{relative_file_path}"
                                        
                                        try:
                                            print(f"[FirnExchange] Executing: {rm_command}")
                                            session.sql(rm_command).collect()
                                            deleted_count += 1
                                        except Exception as e:
                                            print(f"[FirnExchange] Error deleting {file_name}: {str(e)}")
                                            failed_count += 1
                                    
                                    if failed_count == 0:
                                        st.success(f"✅ Deleted {deleted_count} file(s) successfully")
                                    else:
                                        st.warning(f"⚠️ Deleted {deleted_count} file(s), failed to delete {failed_count} file(s)")
                                    
                                    # Clear selection and refresh file list
                                    st.session_state.exchange_selected_files = []
                                    st.session_state.exchange_files_df = None
                                    st.rerun()
                                except Exception as e:
                                    st.error(f"Error deleting files: {str(e)}")
                            else:
                                st.warning("No files selected")
                    with col_btn4:
                        # Initialize confirmation state
                        if 'exchange_delete_all_confirm' not in st.session_state:
                            st.session_state.exchange_delete_all_confirm = False
                        
                        if not st.session_state.exchange_delete_all_confirm:
                            if st.button("Delete All", key="exchange_delete_all_files", type="secondary"):
                                st.session_state.exchange_delete_all_confirm = True
                                st.rerun()
                        else:
                            if st.button("⚠️ Confirm Delete All", key="exchange_confirm_delete_all", type="primary"):
                                try:
                                    if exchange_stage_path:
                                        rm_command = f"REMOVE @{exchange_stage}/{exchange_stage_path}"
                                    else:
                                        rm_command = f"REMOVE @{exchange_stage}"
                                    
                                    print(f"[FirnExchange] Executing: {rm_command}")
                                    result = session.sql(rm_command).collect()
                                    st.success(f"✅ Deleted all files from stage path")
                                    
                                    # Clear selection and refresh file list
                                    st.session_state.exchange_selected_files = []
                                    st.session_state.exchange_files_df = None
                                    st.session_state.exchange_delete_all_confirm = False
                                    st.rerun()
                                except Exception as e:
                                    st.error(f"Error deleting all files: {str(e)}")
                                    st.session_state.exchange_delete_all_confirm = False
                    with col_btn5:
                        if st.session_state.exchange_delete_all_confirm:
                            if st.button("Cancel", key="exchange_cancel_delete_all"):
                                st.session_state.exchange_delete_all_confirm = False
                                st.rerun()
                    
                    # Create mapping from filtered to original indices for checkbox handling
                    # Map display index (0, 1, 2...) to original dataframe index
                    filtered_to_original_map = {i: orig_idx for i, orig_idx in enumerate(original_indices)}
                    # print(f"[FirnExchange] Filter: {file_filter_option}, Mapping: {filtered_to_original_map}")
                    
                    st.write(f"**Selected: {len(st.session_state.exchange_selected_files)} of {len(original_files_df)} total files**")
                    
                    # Create a dataframe with checkbox column
                    display_df = files_df.copy()
                    display_df.insert(0, 'Select', False)
                    
                    # Mark selected files in the filtered view
                    for display_idx in range(len(display_df)):
                        orig_idx = filtered_to_original_map[display_idx]
                        if orig_idx in st.session_state.exchange_selected_files:
                            display_df.at[display_idx, 'Select'] = True
                    
                    # Display the interactive dataframe with sortable columns
                    edited_df = st.data_editor(
                        display_df,
                        hide_index=True,
                        use_container_width=True,
                        height=400,
                        disabled=[col for col in display_df.columns if col != 'Select'],
                        column_config={
                            "Select": st.column_config.CheckboxColumn("Select", help="Select files", default=False),
                            "name": st.column_config.TextColumn("File Name", width="large"),
                            "size": st.column_config.NumberColumn("Size (bytes)", format="%d"),
                            "md5": st.column_config.TextColumn("MD5", width="medium"),
                            "last_modified": st.column_config.TextColumn("Last Modified", width="medium"),
                        },
                        key="exchange_file_editor"
                    )
                    
                    # Update selection mapping back to original indices
                    new_selected = []
                    for display_idx in edited_df[edited_df['Select']].index.tolist():
                        orig_idx = filtered_to_original_map[display_idx]
                        new_selected.append(orig_idx)
                    st.session_state.exchange_selected_files = new_selected
        
        st.divider()
        
        # Operation Configuration Section
        with st.expander("⚙️ Operation Configuration", expanded=True):
            operation = st.selectbox(
                "Select Operation",
                ["FDN Table Data Export", "Iceberg Table Data Import"],
                key="exchange_operation_select"
            )
            st.session_state.exchange_operation = operation
            st.info(f"**Selected Operation:** {operation}")
        
            # Export-specific configuration
            if operation == "FDN Table Data Export":
                st.divider()
                st.subheader("Export Options")
                
                col1, col2, col3 = st.columns(3)
                
                with col1:
                    # Initialize session state for export options
                    if 'exchange_overwrite' not in st.session_state:
                        st.session_state.exchange_overwrite = True
                    
                    exchange_overwrite = st.radio(
                        "OVERWRITE",
                        [True, False],
                        index=0 if st.session_state.exchange_overwrite else 1,
                        key="exchange_overwrite_radio",
                        help="Overwrite existing files in the stage"
                    )
                    st.session_state.exchange_overwrite = exchange_overwrite
                
                with col2:
                    # Initialize session state for single file option
                    if 'exchange_single_file' not in st.session_state:
                        st.session_state.exchange_single_file = True
                    
                    exchange_single_file = st.radio(
                        "SINGLE FILE",
                        [True, False],
                        index=0 if st.session_state.exchange_single_file else 1,
                        key="exchange_single_file_radio",
                        help="Export to a single file per partition"
                    )
                    st.session_state.exchange_single_file = exchange_single_file
                
                with col3:
                    # Initialize session state for max file size
                    if 'exchange_max_file_size' not in st.session_state:
                        st.session_state.exchange_max_file_size = 5368709120
                    
                    exchange_max_file_size = st.number_input(
                        "MAX FILE SIZE (bytes)",
                        min_value=1,
                        max_value=5368709120,
                        value=st.session_state.exchange_max_file_size,
                        step=1000000,
                        key="exchange_max_file_size_input",
                        help="Maximum file size in bytes (max: 5368709120 = 5GB)"
                    )
                    st.session_state.exchange_max_file_size = exchange_max_file_size
                
                st.caption(f"**Export Settings:** OVERWRITE={exchange_overwrite}, SINGLE={exchange_single_file}, MAX_FILE_SIZE={exchange_max_file_size:,}")
        
        st.divider()
        
        # Operation-Specific Configuration
        if operation == "FDN Table Data Export":
            st.header("Source Table Configuration")
            
            col1, col2, col3 = st.columns(3)
            
            with col1:
                exchange_src_database = st.selectbox(
                    "Source Database",
                    st.session_state.databases if st.session_state.databases else ["Select database"],
                    key="exchange_src_db"
                )
            
            with col2:
                if exchange_src_database and exchange_src_database != "Select database":
                    exchange_src_schemas = get_schemas(session, exchange_src_database)
                    exchange_src_schema = st.selectbox(
                        "Source Schema",
                        exchange_src_schemas if exchange_src_schemas else ["Select schema"],
                        key="exchange_src_schema"
                    )
                else:
                    exchange_src_schema = st.selectbox("Source Schema", ["Select schema"], key="exchange_src_schema_disabled")
                    exchange_src_schemas = []
            
            with col3:
                if exchange_src_schema and exchange_src_schema != "Select schema":
                    exchange_src_tables = get_tables(session, exchange_src_database, exchange_src_schema)
                    
                    # Preserve table selection across reruns
                    if 'exchange_src_table_selection' not in st.session_state:
                        st.session_state.exchange_src_table_selection = None
                    
                    # Find index of previously selected table
                    table_index = 0
                    if st.session_state.exchange_src_table_selection in exchange_src_tables:
                        table_index = exchange_src_tables.index(st.session_state.exchange_src_table_selection)
                    
                    exchange_src_table = st.selectbox(
                        "Source Table",
                        exchange_src_tables if exchange_src_tables else ["Select table"],
                        index=table_index,
                        key="exchange_src_table"
                    )
                    
                    # Store selection in session state
                    st.session_state.exchange_src_table_selection = exchange_src_table
                else:
                    exchange_src_table = st.selectbox("Source Table", ["Select table"], key="exchange_src_table_disabled")
                    exchange_src_tables = []
                    st.session_state.exchange_src_table_selection = None
            
            # Partition Key Selection for Export
            if exchange_src_table and exchange_src_table != "Select table":
                exchange_columns = get_columns(session, exchange_src_database, exchange_src_schema, exchange_src_table)
                
                if exchange_columns:
                    column_names = [col[0] for col in exchange_columns]
                    column_types_dict = {col[0]: col[1] for col in exchange_columns}
                    
                    st.subheader("Partition Key Column")
                    default_partition = column_names[0] if column_names else None
                    exchange_partition_column = st.selectbox(
                        "Select partition key column (single column only)",
                        column_names,
                        index=0 if column_names else 0,
                        key="exchange_partition_cols"
                    )
                    # Convert to list for compatibility with existing code
                    exchange_partition_columns = [exchange_partition_column] if exchange_partition_column else []
                    
                    # Check if log table exists for export
                    log_exists, log_table_name = check_log_table_exists(
                        session, exchange_src_database, exchange_src_schema, exchange_src_table, 'export'
                    )
                    
                    st.divider()
                    st.subheader("Log Table Configuration")
                    
                    if log_exists:
                        st.info(f"📋 Log table exists: `{log_table_name}`")
                        
                        # Initialize session state for truncate option
                        if 'exchange_truncate_log' not in st.session_state:
                            st.session_state.exchange_truncate_log = False
                        
                        # Action buttons for existing log table
                        col_log_action1, col_log_action2 = st.columns(2)
                        
                        with col_log_action1:
                            exchange_truncate_log = st.radio(
                                "Log Table Action",
                                ["Reuse existing log table", "Truncate log table before export"],
                                index=0 if not st.session_state.exchange_truncate_log else 1,
                                key="exchange_truncate_log_radio"
                            )
                            st.session_state.exchange_truncate_log = (exchange_truncate_log == "Truncate log table before export")
                        
                        with col_log_action2:
                            st.write("")  # Spacing
                            st.write("")  # Spacing
                            
                            # Drop log table button
                            if st.button("🗑️ Drop Log Table", key="export_drop_log_table", type="secondary"):
                                try:
                                    # Use fully qualified path for drop table
                                    fully_qualified_log_table = f'"{exchange_src_database}"."{exchange_src_schema}"."{log_table_name}"'
                                    with st.spinner(f"Dropping log table `{log_table_name}`..."):
                                        session.sql(f"DROP TABLE IF EXISTS {fully_qualified_log_table}").collect()
                                        st.success(f"✅ Log table `{log_table_name}` dropped successfully")
                                        # Clear all partition-related session state
                                        st.session_state.exchange_log_table_loaded = False
                                        st.session_state.exchange_selected_partitions = []
                                        st.session_state.exchange_partitions_list = []
                                        st.session_state.exchange_tracking_table_name = None
                                        st.rerun()
                                except Exception as e:
                                    st.error(f"Error dropping log table: {str(e)}")
                            
                            # Re-analyze partitions button
                            if st.button("🔄 Re-Analyze Partitions", key="export_reanalyze_partitions", type="secondary"):
                                if not exchange_partition_columns:
                                    st.error("Please select a partition key column")
                                else:
                                    with st.spinner("Re-analyzing partitions and refreshing Parquet schema..."):
                                        try:
                                            # Get Parquet-compatible column transformations
                                            parquet_columns, transformations = get_parquet_compatible_columns(
                                                session, exchange_src_database, exchange_src_schema, exchange_src_table
                                            )
                                            
                                            # Store in session state
                                            st.session_state.exchange_parquet_columns = parquet_columns
                                            st.session_state.exchange_parquet_transformations = transformations
                                            
                                            # Drop existing log table
                                            fully_qualified_log_table = f'"{exchange_src_database}"."{exchange_src_schema}"."{log_table_name}"'
                                            session.sql(f"DROP TABLE IF EXISTS {fully_qualified_log_table}").collect()
                                            
                                            # Create tracking table
                                            tracking_table = create_tracking_table(
                                                session, exchange_src_database, exchange_src_schema, exchange_src_table,
                                                exchange_partition_columns, column_types_dict
                                            )
                                            
                                            if tracking_table:
                                                # Populate tracking table
                                                if populate_tracking_table(
                                                    session, tracking_table, exchange_src_database,
                                                    exchange_src_schema, exchange_src_table, exchange_partition_columns
                                                ):
                                                    st.success(f"✅ Partition re-analysis complete! Log table recreated: {log_table_name}")
                                                    
                                                    # Show transformation info
                                                    if transformations:
                                                        st.info(f"✓ Parquet compatibility: {sum(transformations.values())} column(s) will be transformed")
                                                    
                                                    st.session_state.exchange_tracking_table_name = tracking_table
                                                    st.session_state.exchange_log_table_loaded = False
                                                    st.session_state.exchange_selected_partitions = []
                                                    st.session_state.exchange_partitions_list = []
                                                    st.rerun()
                                                else:
                                                    st.error("Failed to populate tracking table")
                                            else:
                                                st.error("Failed to create tracking table")
                                        except Exception as e:
                                            st.error(f"Error during re-analysis: {str(e)}")
                        
                        # Load and display partition data with selection
                        st.divider()
                        
                        # Load log table data as list of dicts (avoiding pandas)
                        # Skip reloading if we already have the data cached
                        if st.session_state.get('exchange_partitions_list'):
                            partitions_list = st.session_state.exchange_partitions_list
                        else:
                            partitions_list = load_log_table_data_as_list(session, log_table_name, exchange_src_database, exchange_src_schema)
                        
                        if partitions_list:
                            st.session_state.exchange_partitions_list = partitions_list
                            st.session_state.exchange_log_table_loaded = True
                            
                            # Get column names from first partition (excluding internal fields)
                            col_names = [k for k in partitions_list[0].keys() if not k.startswith('_')]
                            
                            # Initialize checkbox render version (used to force checkbox recreation)
                            if 'exchange_checkbox_version' not in st.session_state:
                                st.session_state.exchange_checkbox_version = 0
                            
                            # Display Parquet transformation information if available
                            if st.session_state.exchange_parquet_transformations:
                                transformations = st.session_state.exchange_parquet_transformations
                                total_transformed = sum(transformations.values())
                                
                                if total_transformed > 0:
                                    st.divider()
                                    st.subheader("📊 Parquet Export Configuration")
                                    
                                    with st.expander("ℹ️ Data Type Transformations (for Parquet compatibility)", expanded=False):
                                        st.markdown("**The following column type transformations will be applied during export:**")
                                        
                                        transformation_details = []
                                        if 'TIMESTAMP_TZ' in transformations:
                                            transformation_details.append(f"• **TIMESTAMP_TZ** → TIMESTAMP_NTZ(6): {transformations['TIMESTAMP_TZ']} column(s)")
                                        if 'TIMESTAMP_LTZ' in transformations:
                                            transformation_details.append(f"• **TIMESTAMP_LTZ** → TIMESTAMP_NTZ(6): {transformations['TIMESTAMP_LTZ']} column(s)")
                                        if 'TIMESTAMP_NTZ' in transformations:
                                            transformation_details.append(f"• **TIMESTAMP_NTZ** → TIMESTAMP_NTZ(6): {transformations['TIMESTAMP_NTZ']} column(s)")
                                        if 'TIME' in transformations:
                                            transformation_details.append(f"• **TIME** → TIME(6): {transformations['TIME']} column(s)")
                                        if 'VARIANT' in transformations:
                                            transformation_details.append(f"• **VARIANT** → STRING: {transformations['VARIANT']} column(s)")
                                        if 'GEOGRAPHY' in transformations:
                                            transformation_details.append(f"• **GEOGRAPHY** → ST_ASTEXT(): {transformations['GEOGRAPHY']} column(s)")
                                        if 'GEOMETRY' in transformations:
                                            transformation_details.append(f"• **GEOMETRY** → ST_ASTEXT(): {transformations['GEOMETRY']} column(s)")
                                        
                                        for detail in transformation_details:
                                            st.markdown(detail)
                                        
                                        st.caption(f"**Total columns transformed:** {total_transformed}")
                                        st.caption("ℹ️ These transformations ensure compatibility with Parquet file format")
                            
                            # Wrap partition selection table in collapsible expander (collapsed by default)
                            with st.expander("🔍 Partition Selection Table", expanded=False):
                                # Filter buttons
                                col_filter1, col_filter2, col_filter3 = st.columns(3)
                                
                                with col_filter1:
                                    if st.button("✅ Select All", key="export_select_all"):
                                        st.session_state.exchange_selected_partitions = list(range(len(partitions_list)))
                                        # Increment version to force checkbox recreation
                                        st.session_state.exchange_checkbox_version += 1
                                        st.rerun()
                                
                                with col_filter2:
                                    if st.button("🔄 Select Non-Completed", key="export_select_non_completed"):
                                        # Select rows where status is not 'COMPLETED'
                                        non_completed_indices = []
                                        for i, partition in enumerate(partitions_list):
                                            if partition.get('PARTITION_MIGRATED_STATUS', '') != 'COMPLETED':
                                                non_completed_indices.append(i)
                                        st.session_state.exchange_selected_partitions = non_completed_indices
                                        # Increment version to force checkbox recreation
                                        st.session_state.exchange_checkbox_version += 1
                                        st.rerun()
                                
                                with col_filter3:
                                    if st.button("❌ Unselect All", key="export_unselect_all"):
                                        st.session_state.exchange_selected_partitions = []
                                        # Increment version to force checkbox recreation
                                        st.session_state.exchange_checkbox_version += 1
                                        st.rerun()
                                
                                # Display partitions with interactive checkboxes in table-like format
                                # Track checkbox states and sync with session state
                                new_selected_partitions = []
                                
                                # Create table-like display with inline checkboxes
                                # Limit columns to display (first 6 columns to keep it manageable)
                                display_cols = col_names[:6] if len(col_names) > 6 else col_names
                                
                                # Add custom CSS for table-like appearance
                                st.markdown("""
                                <style>
                                .partition-row {
                                    border-bottom: 1px solid #e0e0e0;
                                    padding: 5px 0;
                                }
                                .partition-header {
                                    background: #f0f2f6;
                                    font-weight: bold;
                                    padding: 10px 5px;
                                    border-bottom: 2px solid #29B5E8;
                                    margin-bottom: 5px;
                                }
                                </style>
                                """, unsafe_allow_html=True)
                                
                                # Create header row
                                header_html = '<div class="partition-header" style="display: grid; grid-template-columns: 50px '
                                header_html += ' '.join(['1fr'] * len(display_cols))
                                header_html += '; gap: 5px; align-items: center;">'
                                header_html += '<div style="text-align: center;">Select</div>'
                                for col in display_cols:
                                    # Shorten column names if too long
                                    display_name = col if len(col) <= 20 else col[:17] + '...'
                                    header_html += f'<div>{display_name}</div>'
                                header_html += '</div>'
                                st.markdown(header_html, unsafe_allow_html=True)
                                
                                # Container for scrollable content
                                with st.container():
                                    # Display each row with checkbox and data
                                    for i, partition in enumerate(partitions_list):
                                        is_selected = i in st.session_state.exchange_selected_partitions
                                        
                                        # Determine status for color coding
                                        status = partition.get('PARTITION_MIGRATED_STATUS', '')
                                        if status == 'COMPLETED':
                                            status_icon = '✅'
                                            bg_style = 'background: #d4edda;'
                                        elif status == 'FAILED':
                                            status_icon = '❌'
                                            bg_style = 'background: #f8d7da;'
                                        elif status == 'IN_PROGRESS':
                                            status_icon = '⏳'
                                            bg_style = 'background: #fff3cd;'
                                        elif is_selected:
                                            status_icon = '📋'
                                            bg_style = 'background: #d1ecf1;'
                                        else:
                                            status_icon = ''
                                            bg_style = ''
                                        
                                        # Create row with columns
                                        cols = st.columns([0.5] + [1] * len(display_cols))
                                        
                                        # Checkbox column
                                        with cols[0]:
                                            # Include version in key to force recreation when buttons are clicked
                                            checkbox_key = f"export_partition_checkbox_{i}_v{st.session_state.exchange_checkbox_version}"
                                            checkbox_value = st.checkbox(
                                                "",
                                                value=is_selected,
                                                key=checkbox_key,
                                                label_visibility="collapsed"
                                            )
                                            if checkbox_value:
                                                new_selected_partitions.append(i)
                                        
                                        # Data columns
                                        for idx, col in enumerate(display_cols, start=1):
                                            with cols[idx]:
                                                value = partition.get(col, '')
                                                # Format value for display
                                                if value is None:
                                                    display_value = ''
                                                elif isinstance(value, (int, float)):
                                                    if isinstance(value, float):
                                                        display_value = f'{value:,.2f}'
                                                    else:
                                                        display_value = f'{value:,}'
                                                else:
                                                    display_value = str(value)
                                                    # Truncate very long values
                                                    if len(display_value) > 30:
                                                        display_value = display_value[:27] + '...'
                                                
                                                # Display with status indicator
                                                if idx == 1 and status_icon:  # Add icon to first data column
                                                    st.markdown(f"{status_icon} {display_value}")
                                                else:
                                                    st.markdown(f"{display_value}")
                                        
                                        # Add separator line
                                        if i < len(partitions_list) - 1:
                                            st.markdown('<hr style="margin: 2px 0; border: none; border-top: 1px solid #e0e0e0;">', unsafe_allow_html=True)
                                
                                # Update session state with new selection from checkboxes
                                st.session_state.exchange_selected_partitions = new_selected_partitions
                            
                            # Show selection summary AFTER expander (after checkboxes are processed)
                            total_partitions = len(partitions_list)
                            selected_count = len(st.session_state.exchange_selected_partitions)
                            st.info(f"📊 **Selected: {selected_count} of {total_partitions} partitions** (expand above to modify selection)")
                                
                        else:
                            st.warning("⚠️ Cannot load partition data for selection (technical limitation with complex data types). The export will process all PENDING partitions in the log table when you click 'Start Export'.")
                            st.caption(f"You can manually query the log table to see partitions: `SELECT * FROM {log_table_name}`")
                            st.session_state.exchange_log_table_loaded = False
                    else:
                        st.info(f"📋 Log table will be created: `{log_table_name}`")
                        st.session_state.exchange_truncate_log = False  # New table, no need to truncate
                        st.session_state.exchange_log_table_loaded = False
                        
                        # Show "Analyze Partitions" button
                        st.divider()
                        st.subheader("Partition Analysis")
                        
                        if st.button("🔍 Analyze Partitions", key="export_analyze_partitions", type="primary"):
                            if not exchange_partition_columns:
                                st.error("Please select a partition key column")
                            else:
                                with st.spinner("Analyzing partitions and preparing Parquet-compatible schema..."):
                                    try:
                                        # Get Parquet-compatible column transformations
                                        parquet_columns, transformations = get_parquet_compatible_columns(
                                            session, exchange_src_database, exchange_src_schema, exchange_src_table
                                        )
                                        
                                        # Store in session state
                                        st.session_state.exchange_parquet_columns = parquet_columns
                                        st.session_state.exchange_parquet_transformations = transformations
                                        
                                        # Show transformation info
                                        if transformations:
                                            st.info(f"✓ Parquet compatibility: {sum(transformations.values())} column(s) will be transformed for export")
                                        
                                        # Create tracking table
                                        tracking_table = create_tracking_table(
                                            session, exchange_src_database, exchange_src_schema, exchange_src_table,
                                            exchange_partition_columns, column_types_dict
                                        )
                                        
                                        if tracking_table:
                                            # Populate tracking table
                                            if populate_tracking_table(
                                                session, tracking_table, exchange_src_database,
                                                exchange_src_schema, exchange_src_table, exchange_partition_columns
                                            ):
                                                st.success(f"Partition analysis complete! Log table created: {log_table_name}")
                                                st.session_state.exchange_tracking_table_name = tracking_table
                                                st.rerun()
                                            else:
                                                st.error("Failed to populate tracking table")
                                        else:
                                            st.error("Failed to create tracking table")
                                    
                                    except Exception as e:
                                        st.error(f"Failed to analyze table schema: {str(e)}")
                                        st.stop()
            
            # Start/Stop Export Buttons
            col_btn1, col_btn2 = st.columns([1, 1])
            
            # Determine if Start Export should be disabled
            # If log table wasn't loaded successfully, allow export without partition selection
            log_table_loaded = st.session_state.get('exchange_log_table_loaded', False)
            export_disabled = (log_table_loaded and len(st.session_state.exchange_selected_partitions) == 0)
            
            with col_btn1:
                if st.button("Start Export", disabled=export_disabled, key="exchange_start_export", type="primary"):
                    print("=" * 80)
                    print("[FirnExchange Export] ★★★ START EXPORT BUTTON CLICKED ★★★")
                    print("=" * 80)
                    
                    print(f"[FirnExchange Export] Validation starting...")
                    
                    if not exchange_partition_columns:
                        print("[FirnExchange Export] ERROR: No partition column selected")
                        st.error("Please select a partition key column")
                    elif log_table_loaded and len(st.session_state.exchange_selected_partitions) == 0:
                        print("[FirnExchange Export] ERROR: No partitions selected")
                        st.error("Please select at least one partition for export")
                    elif not exchange_stage:
                        print("[FirnExchange Export] ERROR: No stage selected")
                        st.error("Please select an external stage")
                    elif not locals().get('exchange_stage_path'):
                        print("[FirnExchange Export] ERROR: No stage path entered")
                        st.error("Please enter a relative path in stage")
                    elif not exchange_warehouse:
                        print("[FirnExchange Export] ERROR: No warehouse selected")
                        st.error("Please select a warehouse")
                    else:
                        print("[FirnExchange Export] ✓ All validations passed")
                        print(f"[FirnExchange Export] Partition columns: {exchange_partition_columns}")
                        print(f"[FirnExchange Export] Stage: {exchange_stage}")
                        print(f"[FirnExchange Export] Stage path: {exchange_stage_path}")
                        print(f"[FirnExchange Export] Warehouse: {exchange_warehouse}")
                        print(f"[FirnExchange Export] Selected partitions: {len(st.session_state.exchange_selected_partitions)}")
                        
                        # Set warehouse
                        try:
                            session.sql(f"USE WAREHOUSE {exchange_warehouse}").collect()
                            st.info(f"Using warehouse: {exchange_warehouse}")
                        except Exception as e:
                            st.error(f"Error setting warehouse: {str(e)}")
                            st.stop()
                        
                        # Get tracking table name
                        log_table_name = f"{exchange_src_table}_EXPORT_FELOG"
                        tracking_table = f'"{exchange_src_database}"."{exchange_src_schema}"."{log_table_name}"'
                        st.session_state.exchange_tracking_table_name = tracking_table
                        
                        # Get selected partitions from list (not dataframe)
                        partitions_list = st.session_state.get('exchange_partitions_list', [])
                        selected_indices = st.session_state.exchange_selected_partitions
                        
                        # Extract selected partitions from the list
                        selected_partitions = [partitions_list[i] for i in selected_indices if i < len(partitions_list)]
                        
                        print(f"[FirnExchange Export] Processing {len(selected_partitions)} selected partitions")
                        
                        # Mark selected partitions as PENDING in the tracking table
                        # Note: All partitions start as 'NOT_SELECTED' after analysis
                        # Only selected partitions are changed to 'PENDING' here
                        # This ensures run_migration only processes selected partitions
                        try:
                            # Mark all selected partitions as PENDING
                            for partition_dict in selected_partitions:
                                # Build WHERE clause for this partition
                                where_conditions = []
                                for col in exchange_partition_columns:
                                    val = partition_dict.get(col, None)
                                    if val is None or val == '':
                                        where_conditions.append(f'"{col}" IS NULL')
                                    elif isinstance(val, str):
                                        where_conditions.append(f'"{col}" = \'{val}\'')
                                    else:
                                        where_conditions.append(f'"{col}" = {val}')
                                where_clause = ' AND '.join(where_conditions)
                                
                                # Update status to PENDING for selected partition
                                update_sql = f"""
                                UPDATE {tracking_table}
                                SET PARTITION_MIGRATED_STATUS = 'PENDING',
                                    RETRY_COUNT = 0,
                                    ERROR_MESSAGE = NULL
                                WHERE {where_clause}
                                """
                                session.sql(update_sql).collect()
                            
                            print(f"[FirnExchange Export] Marked {len(selected_partitions)} partitions as PENDING")
                            st.success(f"Ready to export {len(selected_partitions)} selected partitions")
                        
                        except Exception as e:
                            st.error(f"Error preparing partitions for export: {str(e)}")
                            print(f"[FirnExchange Export] Error: {str(e)}")
                            import traceback
                            traceback.print_exc()
                            st.stop()
                        
                        # Create Snowflake Task for export
                        print(f"[FirnExchange Export] Creating Snowflake task for export")
                        print(f"[FirnExchange Export] Warehouse: {exchange_warehouse}, Max workers: {exchange_max_workers}")
                        
                        # Get export configuration
                        export_overwrite = st.session_state.get('exchange_overwrite', True)
                        export_single_file = st.session_state.get('exchange_single_file', True)
                        export_max_file_size = st.session_state.get('exchange_max_file_size', 5368709120)
                        
                        # Create task name
                        task_name = create_task_name(
                            'EXPORT', 
                            exchange_src_database, 
                            exchange_src_schema, 
                            exchange_src_table
                        )
                        
                        print(f"[FirnExchange Export] Task name: {task_name}")
                        
                        # Create and start export task
                        result, error = create_export_task(
                            session,
                            task_name,
                            tracking_table,
                            exchange_src_database,
                            exchange_src_schema,
                            exchange_src_table,
                            exchange_partition_columns[0],  # First partition column
                            exchange_stage,
                            exchange_stage_path,
                            st.session_state.exchange_parquet_columns,
                            exchange_warehouse,
                            exchange_max_workers,
                            export_overwrite,
                            export_single_file,
                            export_max_file_size
                        )
                        
                        if result:
                            st.success(f"✅ Export task created successfully: `{task_name}`")
                            st.info("📊 Monitor progress in the **'Monitor Tasks'** tab above")
                            st.info("💡 You can close this app - the task will continue running in Snowflake")
                            print(f"[FirnExchange Export] Task created successfully: {task_name}")
                        else:
                            st.error(f"❌ Failed to create export task: {error}")
                            print(f"[FirnExchange Export] ERROR: {error}")
                        
                        time.sleep(2)  # Brief pause to show messages
                        st.rerun()
        
        elif operation == "Iceberg Table Data Import":
            st.header("Target Table Configuration")
            
            col1, col2, col3 = st.columns(3)
            
            with col1:
                if not st.session_state.databases:
                    st.session_state.databases = get_databases(session)
                
                exchange_tgt_database = st.selectbox(
                    "Target Database",
                    st.session_state.databases if st.session_state.databases else ["Select database"],
                    key="exchange_tgt_db"
                )
            
            with col2:
                if exchange_tgt_database and exchange_tgt_database != "Select database":
                    exchange_tgt_schemas = get_schemas(session, exchange_tgt_database)
                    exchange_tgt_schema = st.selectbox(
                        "Target Schema",
                        exchange_tgt_schemas if exchange_tgt_schemas else ["Select schema"],
                        key="exchange_tgt_schema"
                    )
                else:
                    exchange_tgt_schema = st.selectbox("Target Schema", ["Select schema"], key="exchange_tgt_schema_disabled")
            
            with col3:
                if exchange_tgt_schema and exchange_tgt_schema != "Select schema":
                    try:
                        result = session.sql(f"SHOW ICEBERG TABLES IN {exchange_tgt_database}.{exchange_tgt_schema}").collect()
                        exchange_iceberg_tables = ["Select Iceberg table"] + [row['name'] for row in result]
                    except Exception as e:
                        st.error(f"Error loading Iceberg tables: {str(e)}")
                        exchange_iceberg_tables = ["Select Iceberg table"]
                    
                    exchange_tgt_table = st.selectbox(
                        "Target Iceberg Table",
                        exchange_iceberg_tables,
                        key="exchange_tgt_table"
                    )
                    
                    # Display Iceberg table properties
                    if exchange_tgt_table and exchange_tgt_table != "Select Iceberg table":
                        try:
                            show_iceberg_cmd = f"SHOW ICEBERG TABLES LIKE '{exchange_tgt_table}' IN {exchange_tgt_database}.{exchange_tgt_schema}"
                            print(f"[Iceberg Table Properties] Executing: {show_iceberg_cmd}")
                            iceberg_result = session.sql(show_iceberg_cmd).collect()
                            
                            if iceberg_result:
                                iceberg_info = iceberg_result[0].asDict()
                                
                                # Display iceberg table properties
                                st.caption("**Iceberg Table Properties:**")
                                
                                # Extract and display the requested properties (case-sensitive lowercase)
                                catalog_name = iceberg_info.get('catalog_name', 'N/A')
                                iceberg_table_type = iceberg_info.get('iceberg_table_type', 'N/A')
                                external_volume_name = iceberg_info.get('external_volume_name', 'N/A')
                                base_location = iceberg_info.get('base_location', 'N/A')
                                
                                # Create a compact display
                                ice_col1, ice_col2 = st.columns(2)
                                with ice_col1:
                                    st.caption(f"🔹 **Catalog Name:** {catalog_name}")
                                    st.caption(f"🔹 **Table Type:** {iceberg_table_type}")
                                with ice_col2:
                                    st.caption(f"🔹 **External Volume:** {external_volume_name}")
                                    st.caption(f"🔹 **Base Location:** {base_location}")
                        except Exception as e:
                            print(f"[Iceberg Table Properties] Error fetching properties: {str(e)}")
                else:
                    exchange_tgt_table = st.selectbox("Target Iceberg Table", ["Select schema first"], key="exchange_tgt_table_disabled")
            
            # Data Loading Parameters
            st.subheader("Data Loading Parameters")
            col1, col2 = st.columns(2)
            
            with col1:
                exchange_load_mode = st.selectbox(
                    "LOAD_MODE",
                    ["ADD_FILES_COPY", "ADD_FILES_REFERENCE", "FULL_INGEST"],
                    index=0,
                    help="""
**ADD_FILES_COPY**: Copy parquet files from external stage to Iceberg table's base location, then register in metadata. Best for files in a different location.

**ADD_FILES_REFERENCE**: Register existing parquet files already in Iceberg table's base location without copying (Private Preview). Fastest option when files are already in table location.

**FULL_INGEST**: Traditional COPY with full data parsing and transformation. Use when data transformation is needed.
                    """,
                    key="exchange_load_mode_select"
                )
            
            with col2:
                if exchange_load_mode == "ADD_FILES_REFERENCE":
                    st.warning("**ADD_FILES_REFERENCE Requirements:**\n\n"
                              "• Files must be in Iceberg table's BASE_LOCATION\n\n"
                              "• Parquet columns must match table schema exactly\n\n"
                              "• INTEGER exports as NUMBER(38,0) - ensure table uses NUMBER(38,0)")
                else:
                    st.info("**Fixed Parameters:**\n\n"
                           "• FILE_FORMAT: PARQUET (USE_VECTORIZED_SCANNER = TRUE)\n\n"
                           "• MATCH_BY_COLUMN_NAME: CASE_SENSITIVE\n\n"
                           "• ON_ERROR: ABORT_STATEMENT")
            
            st.caption(f"**Load Configuration:** LOAD_MODE={exchange_load_mode}, FILE_FORMAT=PARQUET (USE_VECTORIZED_SCANNER=TRUE), MATCH_BY_COLUMN_NAME=CASE_SENSITIVE, ON_ERROR=ABORT_STATEMENT")
            
            # Check if log table exists for import
            if exchange_tgt_table and exchange_tgt_table != "Select Iceberg table":
                log_exists_import, log_table_name_import = check_log_table_exists(
                    session, exchange_tgt_database, exchange_tgt_schema, exchange_tgt_table, 'import'
                )
                
                st.divider()
                st.subheader("Log Table Configuration")
                
                if log_exists_import:
                    st.info(f"📋 Log table exists: `{log_table_name_import}`")
                    
                    # Initialize session state for import truncate option
                    if 'exchange_import_truncate_log' not in st.session_state:
                        st.session_state.exchange_import_truncate_log = False
                    
                    # Action buttons for existing log table
                    col_import_log_action1, col_import_log_action2 = st.columns([2, 1])
                    
                    with col_import_log_action1:
                        exchange_import_truncate_log = st.radio(
                            "Log Table Action",
                            ["Reuse existing log table", "Truncate log table before import"],
                            index=0 if not st.session_state.exchange_import_truncate_log else 1,
                            key="exchange_import_truncate_log_radio"
                        )
                        st.session_state.exchange_import_truncate_log = (exchange_import_truncate_log == "Truncate log table before import")
                    
                    with col_import_log_action2:
                        st.write("")  # Spacing
                        st.write("")  # Spacing
                        
                        # Drop log table button
                        if st.button("🗑️ Drop Log Table", key="import_drop_log_table", type="secondary"):
                            try:
                                # Use fully qualified path for drop table
                                fully_qualified_log_table_import = f'"{exchange_tgt_database}"."{exchange_tgt_schema}"."{log_table_name_import}"'
                                with st.spinner(f"Dropping log table `{log_table_name_import}`..."):
                                    session.sql(f"DROP TABLE IF EXISTS {fully_qualified_log_table_import}").collect()
                                    st.success(f"✅ Log table `{log_table_name_import}` dropped successfully")
                                    st.rerun()
                            except Exception as e:
                                st.error(f"Error dropping log table: {str(e)}")
                else:
                    st.info(f"📋 Log table will be created: `{log_table_name_import}`")
                    st.session_state.exchange_import_truncate_log = False  # New table, no need to truncate
            
            # Start/Stop Import Buttons
            col_btn1, col_btn2 = st.columns([1, 1])
            
            with col_btn1:
                if st.button("Start Import", key="exchange_start_import", type="primary"):
                    print("=" * 80)
                    print("[FirnExchange Import] ★★★ START IMPORT BUTTON CLICKED ★★★")
                    print("=" * 80)
                    
                    print(f"[FirnExchange Import] Validation starting...")
                    
                    if not exchange_tgt_table or exchange_tgt_table == "Select Iceberg table":
                        print("[FirnExchange Import] ERROR: No target table selected")
                        st.error("Please select an Iceberg table")
                    elif not exchange_stage:
                        st.error("Please select a stage")
                    elif 'exchange_selected_files' not in st.session_state or not st.session_state.exchange_selected_files:
                        st.error("Please select at least one file to import")
                    elif not exchange_warehouse:
                        st.error("Please select a warehouse")
                    else:
                        # Set warehouse
                        try:
                            session.sql(f"USE WAREHOUSE {exchange_warehouse}").collect()
                            st.info(f"Using warehouse: {exchange_warehouse}")
                        except Exception as e:
                            st.error(f"Error setting warehouse: {str(e)}")
                            st.stop()
                        
                        # Prepare files list for import
                        if 'exchange_files_df' in st.session_state and st.session_state.exchange_files_df is not None:
                            files_df = st.session_state.exchange_files_df
                            selected_files = []
                            skipped_count = 0
                            
                            print(f"[FirnExchange Import] Selected indices: {st.session_state.exchange_selected_files}")
                            print(f"[FirnExchange Import] Total files in dataframe: {len(files_df)}")
                            
                            for idx in st.session_state.exchange_selected_files:
                                print(f"[FirnExchange Import] Processing index: {idx}")
                                file_name = files_df.loc[idx, 'name']
                                file_size = files_df.loc[idx, 'size']
                                
                                print(f"[FirnExchange Import] File at index {idx}: {file_name}, size: {file_size}")
                                
                                # Skip files with size 0 (directories and empty files)
                                if file_size == 0:
                                    print(f"[FirnExchange Import] Skipping file with size 0: {file_name}")
                                    skipped_count += 1
                                    continue
                                
                                # Extract relative path from cloud storage URL
                                # The file_name from LIST command is the full cloud URL
                                # We need to extract the path relative to the stage base location
                                relative_file_path = None
                                
                                if '://' in file_name:
                                    # Cloud storage URL (s3://, azure://, gs://, etc.)
                                    if exchange_stage_path:
                                        # Pattern to search: /{exchange_stage_path}/
                                        search_pattern = f'/{exchange_stage_path}/'
                                        print(f"[FirnExchange Import] Looking for pattern '{search_pattern}' in {file_name}")
                                        
                                        if search_pattern in file_name:
                                            # Extract everything after the stage relative path
                                            split_pos = file_name.index(search_pattern) + len(search_pattern)
                                            path_after_stage = file_name[split_pos:]
                                            # Construct full path: stage_path/remaining_path
                                            relative_file_path = f"{exchange_stage_path}/{path_after_stage}"
                                            print(f"[FirnExchange Import] Extracted relative path: {relative_file_path}")
                                        else:
                                            # Pattern not found, extract path using the last segment of stage_path
                                            print(f"[FirnExchange Import] Pattern not found, trying alternative extraction")
                                            # Split by '/' and find the last segment of stage_path in parts
                                            parts = file_name.split('/')
                                            stage_path_segments = exchange_stage_path.rstrip('/').split('/')
                                            last_stage_segment = stage_path_segments[-1]
                                            print(f"[FirnExchange Import] Looking for last segment '{last_stage_segment}' in URL parts")
                                            
                                            try:
                                                # Find the index of the last segment of stage_path
                                                # Use rfind logic to find the last occurrence
                                                stage_idx = -1
                                                for i, part in enumerate(parts):
                                                    if part == last_stage_segment:
                                                        stage_idx = i
                                                
                                                if stage_idx >= 0:
                                                    # Get all parts after the last stage segment
                                                    remaining_parts = parts[stage_idx + 1:]
                                                    if remaining_parts:
                                                        path_after_stage = '/'.join(remaining_parts)
                                                        relative_file_path = f"{exchange_stage_path}/{path_after_stage}"
                                                    else:
                                                        # No remaining parts, just use the stage path
                                                        relative_file_path = exchange_stage_path
                                                    print(f"[FirnExchange Import] Alternative extraction result: {relative_file_path}")
                                                else:
                                                    # Last segment not found, just use filename
                                                    relative_file_path = f"{exchange_stage_path}/{parts[-1]}"
                                                    print(f"[FirnExchange Import] Last stage segment not found in URL parts, using: {relative_file_path}")
                                            except Exception as e:
                                                # Fallback: just use filename
                                                relative_file_path = f"{exchange_stage_path}/{parts[-1]}"
                                                print(f"[FirnExchange Import] Error in alternative extraction: {e}, using: {relative_file_path}")
                                    else:
                                        # No stage path specified, just get the filename
                                        relative_file_path = file_name.split('/')[-1]
                                        print(f"[FirnExchange Import] No stage path, using filename: {relative_file_path}")
                                else:
                                    # Not a cloud URL, treat as local path
                                    if exchange_stage_path:
                                        relative_file_path = f"{exchange_stage_path}/{file_name.split('/')[-1]}"
                                    else:
                                        relative_file_path = file_name.split('/')[-1]
                                    print(f"[FirnExchange Import] Local path, using: {relative_file_path}")
                                
                                if relative_file_path:
                                    # Normalize path: remove double slashes that can occur when
                                    # stage_path ends with / and extracted path starts with /
                                    # This is critical for ADD_FILES_COPY which doesn't tolerate //
                                    while '//' in relative_file_path:
                                        relative_file_path = relative_file_path.replace('//', '/')
                                    selected_files.append(relative_file_path)
                                    print(f"[FirnExchange Import] Final file path for COPY: {relative_file_path}")
                                else:
                                    print(f"[FirnExchange Import] ERROR: Could not extract relative path for: {file_name}")
                            
                            if len(selected_files) == 0:
                                st.error(f"No valid files to import. All {skipped_count} selected file(s) have size 0 (directories or empty files).")
                                st.stop()
                            
                            if skipped_count > 0:
                                st.info(f"Skipped {skipped_count} file(s) with size 0 (directories or empty files)")
                            
                            print(f"[FirnExchange Import] Starting import of {len(selected_files)} file(s)")
                            
                            # Create import log table
                            import_log_table = create_import_log_table(
                                session, exchange_tgt_database, exchange_tgt_schema, exchange_tgt_table
                            )
                            
                            if import_log_table:
                                # Truncate log table if user selected that option
                                if st.session_state.get('exchange_import_truncate_log', False):
                                    try:
                                        print(f"[FirnExchange Import] Truncating import log table: {import_log_table}")
                                        session.sql(f"TRUNCATE TABLE {import_log_table}").collect()
                                        print(f"[FirnExchange Import] Truncated log table")
                                    except Exception as e:
                                        st.error(f"Error truncating import log table: {str(e)}")
                                        st.stop()
                                else:
                                    print(f"[FirnExchange Import] Reusing existing import log table: {import_log_table}")
                                
                                # Insert pending files into log table (bulk insert for efficiency)
                                try:
                                    # Build bulk insert statement
                                    values_list = []
                                    for file_path in selected_files:
                                        safe_file_path = file_path.replace("'", "''")
                                        values_list.append(f"('{safe_file_path}', 'PENDING')")
                                    
                                    if values_list:
                                        # Use INSERT with ON CONFLICT handling if supported, or merge
                                        insert_sql = f"""
                                        INSERT INTO {import_log_table} (FILE_PATH, FILE_STATUS)
                                        SELECT column1, column2 FROM (VALUES {', '.join(values_list)})
                                        WHERE column1 NOT IN (SELECT FILE_PATH FROM {import_log_table})
                                        """
                                        session.sql(insert_sql).collect()
                                        print(f"[FirnExchange Import] Added files to import log table")
                                except Exception as e:
                                    st.error(f"Error populating import log table: {str(e)}")
                                    print(f"[FirnExchange Import] Error: {str(e)}")
                                    st.stop()
                            else:
                                st.error("Failed to create import log table")
                                st.stop()
                            
                            # Create Snowflake Task for import
                            print(f"[FirnExchange Import] Creating Snowflake task for import")
                            print(f"[FirnExchange Import] Warehouse: {exchange_warehouse}, Max workers: {exchange_max_workers}")
                            print(f"[FirnExchange Import] Files: {len(selected_files)}, Load mode: {exchange_load_mode}")
                            
                            # Create task name
                            task_name = create_task_name(
                                'IMPORT',
                                exchange_tgt_database,
                                exchange_tgt_schema,
                                exchange_tgt_table
                            )
                            
                            print(f"[FirnExchange Import] Task name: {task_name}")
                            
                            # Create and start import task
                            result, error = create_import_task(
                                session,
                                task_name,
                                import_log_table,
                                exchange_tgt_database,
                                exchange_tgt_schema,
                                exchange_tgt_table,
                                exchange_stage,
                                exchange_warehouse,
                                exchange_load_mode,
                                exchange_max_workers
                            )
                            
                            if result:
                                st.success(f"✅ Import task created successfully: `{task_name}`")
                                st.info("📊 Monitor progress in the **'Monitor Tasks'** tab above")
                                st.info("💡 You can close this app - the task will continue running in Snowflake")
                                print(f"[FirnExchange Import] Task created successfully: {task_name}")
                            else:
                                st.error(f"❌ Failed to create import task: {error}")
                                print(f"[FirnExchange Import] ERROR: {error}")
                            
                            time.sleep(2)  # Brief pause to show messages
                            st.rerun()
                        else:
                            st.error("Files data not available. Please refresh the file list.")
                            st.stop()
    
    # TAB 2: Task Monitor
    with tab2:
        st.header("📊 Task Monitor")
        st.caption("Monitor all FirnExchange tasks running in Snowflake")
        
        if st.session_state.connected:
            session = st.session_state.session
            
            # Refresh button
            col_refresh, col_spacer = st.columns([1, 4])
            with col_refresh:
                if st.button("🔄 Refresh", key="refresh_tasks", type="primary"):
                    st.rerun()
            
            st.divider()
            
            # Section 1: Active Tasks
            st.subheader("🔄 Active Tasks")
            st.caption("Tasks currently scheduled or executing")
            
            active_tasks = get_active_tasks(session)
            
            if active_tasks:
                # Display active tasks in dataframe
                try:
                    active_data = []
                    for t in active_tasks:
                        active_data.append({
                            'Task Name': t['TASK_NAME'],
                            'State': t['STATE'],
                            'Scheduled': str(t['SCHEDULED_TIME']) if t['SCHEDULED_TIME'] else 'N/A',
                            'Started': str(t['QUERY_START_TIME']) if t['QUERY_START_TIME'] else 'N/A',
                            'Database': t['DATABASE_NAME'],
                            'Schema': t['SCHEMA_NAME']
                        })
                    
                    if active_data:
                        active_df = pd.DataFrame(active_data)
                        st.dataframe(active_df, use_container_width=True, height=min(len(active_data) * 35 + 38, 400))
                    
                    # Task selection for details
                    st.divider()
                    st.subheader("📋 Task Details")
                    
                    task_names = [t['TASK_NAME'] for t in active_tasks]
                    selected_task = st.selectbox(
                        "Select task to view details and control",
                        task_names,
                        key="selected_task"
                    )
                    
                    if selected_task:
                        # Get task registry info
                        task_info = get_task_registry_info(session, selected_task)
                        
                        if task_info and len(task_info) > 0:
                            info = task_info[0]
                            
                            # Display task details
                            with st.expander(f"📋 Details: {selected_task}", expanded=True):
                                col1, col2, col3 = st.columns(3)
                                
                                with col1:
                                    st.metric("Operation", info['OPERATION_TYPE'])
                                    st.metric("Status", info['TASK_STATUS'])
                                
                                with col2:
                                    st.metric("Total Items", info['TOTAL_ITEMS'] if info['TOTAL_ITEMS'] else 0)
                                    st.metric("Completed", info['COMPLETED_ITEMS'] if info['COMPLETED_ITEMS'] else 0)
                                
                                with col3:
                                    st.metric("Failed", info['FAILED_ITEMS'] if info['FAILED_ITEMS'] else 0)
                                    st.metric("Warehouse", info['WAREHOUSE'])
                                
                                # Show tracking table data
                                tracking_table = info['TRACKING_TABLE']
                                
                                if tracking_table:
                                    st.divider()
                                    st.subheader("Detailed Status")
                                    
                                    # Query tracking table based on operation type
                                    if info['OPERATION_TYPE'] == 'EXPORT':
                                        try:
                                            stats = get_migration_stats(session, tracking_table)
                                            
                                            col1, col2, col3, col4 = st.columns(4)
                                            with col1:
                                                st.metric("Completed", stats.get('COMPLETED', 0))
                                            with col2:
                                                st.metric("In Progress", stats.get('IN_PROGRESS', 0))
                                            with col3:
                                                st.metric("Pending", stats.get('PENDING', 0))
                                            with col4:
                                                st.metric("Failed", stats.get('FAILED', 0))
                                            
                                            # Calculate and show progress
                                            total = sum(stats.values())
                                            if total > 0:
                                                progress = stats.get('COMPLETED', 0) / total
                                                st.progress(progress, text=f"Progress: {progress*100:.1f}%")
                                            
                                            # Show tracking data in expander
                                            tracking_data = get_tracking_data(session, tracking_table)
                                            if tracking_data:
                                                with st.expander(f"View Tracking Table ({len(tracking_data)} rows)", expanded=False):
                                                    try:
                                                        # Convert to pandas DataFrame for display
                                                        df_data = []
                                                        for row in tracking_data:
                                                            df_data.append(dict(row.asDict()))
                                                        if df_data:
                                                            df = pd.DataFrame(df_data)
                                                            st.dataframe(df, use_container_width=True, height=400)
                                                    except Exception as e:
                                                        st.error(f"Error displaying tracking data: {str(e)}")
                                        except Exception as e:
                                            st.error(f"Error loading export stats: {str(e)}")
                                    
                                    elif info['OPERATION_TYPE'] == 'IMPORT':
                                        try:
                                            # Query import log table for stats
                                            stats_query = f"""
                                            SELECT 
                                                FILE_STATUS,
                                                COUNT(*) as COUNT
                                            FROM {tracking_table}
                                            GROUP BY FILE_STATUS
                                            """
                                            stats_result = session.sql(stats_query).collect()
                                            
                                            # Build stats dict
                                            import_stats = {'SUCCESS': 0, 'FAILED': 0, 'PENDING': 0, 'IN_PROGRESS': 0}
                                            for row in stats_result:
                                                status = row['FILE_STATUS']
                                                count = row['COUNT']
                                                if status in import_stats:
                                                    import_stats[status] = count
                                            
                                            col1, col2, col3, col4 = st.columns(4)
                                            with col1:
                                                st.metric("Success", import_stats['SUCCESS'])
                                            with col2:
                                                st.metric("In Progress", import_stats['IN_PROGRESS'])
                                            with col3:
                                                st.metric("Pending", import_stats['PENDING'])
                                            with col4:
                                                st.metric("Failed", import_stats['FAILED'])
                                            
                                            # Calculate and show progress
                                            total = sum(import_stats.values())
                                            if total > 0:
                                                progress = import_stats['SUCCESS'] / total
                                                st.progress(progress, text=f"Progress: {progress*100:.1f}%")
                                            
                                            # Show import log in expander
                                            with st.expander(f"View Import Log ({total} files)", expanded=False):
                                                try:
                                                    log_data = session.sql(f"SELECT * FROM {tracking_table} ORDER BY IMPORT_START_AT DESC LIMIT 100").collect()
                                                    if log_data:
                                                        df_data = []
                                                        for row in log_data:
                                                            df_data.append(dict(row.asDict()))
                                                        if df_data:
                                                            df = pd.DataFrame(df_data)
                                                            st.dataframe(df, use_container_width=True, height=400)
                                                except Exception as e:
                                                    st.error(f"Error displaying import log: {str(e)}")
                                        except Exception as e:
                                            st.error(f"Error loading import stats: {str(e)}")
                                
                                # Action buttons
                                st.divider()
                                st.subheader("Task Controls")
                                col_a1, col_a2, col_a3 = st.columns(3)
                                
                                with col_a1:
                                    if st.button("⏸️ Suspend Task", key=f"suspend_{selected_task}"):
                                        success, error = suspend_task(session, selected_task)
                                        if success:
                                            st.success("✅ Task suspended successfully")
                                            time.sleep(1)
                                            st.rerun()
                                        else:
                                            st.error(f"❌ Error suspending task: {error}")
                                
                                with col_a2:
                                    if st.button("▶️ Resume Task", key=f"resume_{selected_task}"):
                                        success, error = resume_task(session, selected_task)
                                        if success:
                                            st.success("✅ Task resumed successfully")
                                            time.sleep(1)
                                            st.rerun()
                                        else:
                                            st.error(f"❌ Error resuming task: {error}")
                                
                                with col_a3:
                                    if st.button("🗑️ Drop Task", key=f"drop_{selected_task}", type="secondary"):
                                        # Confirmation step
                                        st.warning("⚠️ This will permanently delete the task. Click again to confirm.")
                                        if st.button("✓ Confirm Drop", key=f"confirm_drop_{selected_task}", type="primary"):
                                            success, error = drop_task(session, selected_task)
                                            if success:
                                                st.success("✅ Task dropped successfully")
                                                time.sleep(1)
                                                st.rerun()
                                            else:
                                                st.error(f"❌ Error dropping task: {error}")
                        else:
                            st.info(f"No registry information found for task: {selected_task}")
                
                except Exception as e:
                    st.error(f"Error displaying active tasks: {str(e)}")
                    print(f"[Task Monitor] Error: {str(e)}")
            else:
                st.info("ℹ️ No active tasks found")
                st.caption("Tasks will appear here when you launch an export or import operation from the 'Configure & Launch' tab")
            
            st.divider()
            
            # Section 2: Recent Task History
            st.subheader("📜 Recent Task History")
            st.caption("Last 50 FirnExchange tasks")
            
            try:
                recent_tasks = get_task_registry_info(session)
                
                if recent_tasks:
                    history_data = []
                    for t in recent_tasks[:50]:  # Last 50 tasks
                        history_data.append({
                            'Task Name': t['TASK_NAME'],
                            'Operation': t['OPERATION_TYPE'],
                            'Status': t['TASK_STATUS'],
                            'Created': str(t['CREATED_AT']) if t['CREATED_AT'] else 'N/A',
                            'Started': str(t['START_TIME']) if t['START_TIME'] else 'N/A',
                            'Ended': str(t['END_TIME']) if t['END_TIME'] else 'N/A',
                            'Total': t['TOTAL_ITEMS'] if t['TOTAL_ITEMS'] else 0,
                            'Completed': t['COMPLETED_ITEMS'] if t['COMPLETED_ITEMS'] else 0,
                            'Failed': t['FAILED_ITEMS'] if t['FAILED_ITEMS'] else 0
                        })
                    
                    if history_data:
                        history_df = pd.DataFrame(history_data)
                        st.dataframe(history_df, use_container_width=True, height=400)
                else:
                    st.info("ℹ️ No task history found")
                    st.caption("Task history will appear here after you create and run tasks")
            except Exception as e:
                st.error(f"Error loading task history: {str(e)}")
                print(f"[Task Monitor] History error: {str(e)}")
            
            # Auto-refresh notice
            st.divider()
            st.caption("💡 **Tip:** Click the 🔄 Refresh button above to update task status")
        else:
            st.info("Please connect to Snowflake using the sidebar to view task monitoring")
    
    # Show connection message if not connected (outside both tabs)
    if not st.session_state.connected:
        st.info("Please connect to Snowflake using the sidebar")
