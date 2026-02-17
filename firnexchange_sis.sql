/*==============================================================================
  FirnExchange - Streamlit in Snowflake (SiS) Deployment Script
  
  Purpose: Deploy FirnExchange data migration tool as a Streamlit app in Snowflake
  Version: 1.0
  
  Usage:
    cd /Users/vverma/sprojects/FirnExchange_V2
    
    # Step 1: Deploy stored procedures first
    snow sql --connection sf-usb97494-vverma-kp_une --filename firn_procedures.sql
    
    # Step 2: Deploy Streamlit app
    snow sql --connection sf-usb97494-vverma-kp_une --filename firnexchange_sis.sql
  
  Prerequisites:
    - ACCOUNTADMIN role or equivalent privileges
    - FirnExchange.py, environment.yml, and pyproject.toml files in the directory
    - Snowflake CLI configured with appropriate connection
==============================================================================*/

-- Set execution context
USE ROLE ACCOUNTADMIN;
USE WAREHOUSE XSMALL;

--------------------------------------------------------------------------------
-- STEP 1: Create Database and Schema
--------------------------------------------------------------------------------
CREATE DATABASE IF NOT EXISTS FT_DB
    COMMENT = 'Database for FirnExchange application';

CREATE SCHEMA IF NOT EXISTS FT_DB.FT_SCH
    COMMENT = 'Schema for FirnExchange application objects';

USE SCHEMA FT_DB.FT_SCH;

--------------------------------------------------------------------------------
-- STEP 2: Create Network Rule for PyPI Access
--------------------------------------------------------------------------------
-- Network rule allows Streamlit app to download Python packages from PyPI
CREATE NETWORK RULE IF NOT EXISTS pypi_network_rule
    MODE = EGRESS
    TYPE = HOST_PORT
    VALUE_LIST = (
        'pypi.org', 
        'pypi.python.org', 
        'pythonhosted.org', 
        'files.pythonhosted.org'
    )
    COMMENT = 'Allow access to PyPI for Python package installation';

--------------------------------------------------------------------------------
-- STEP 3: Create External Access Integration
--------------------------------------------------------------------------------
-- External access integration enables Streamlit to use the network rule
CREATE EXTERNAL ACCESS INTEGRATION IF NOT EXISTS pypi_access_integration
    ALLOWED_NETWORK_RULES = (pypi_network_rule)
    ENABLED = TRUE
    COMMENT = 'External access integration for PyPI package downloads';

-- Grant usage to SYSADMIN for operational management
GRANT USAGE ON INTEGRATION pypi_access_integration TO ROLE SYSADMIN;

--------------------------------------------------------------------------------
-- STEP 4: Create Stage for Application Files
--------------------------------------------------------------------------------
CREATE STAGE IF NOT EXISTS FT_DB.FT_SCH.FIRNEXCHANGE_STAGE
    COMMENT = 'Stage for FirnExchange application files';

--------------------------------------------------------------------------------
-- STEP 5: Upload Application Files to Stage
--------------------------------------------------------------------------------
-- Upload main application file
PUT file:///Users/vverma/sprojects/FirnExchange_V2/FirnExchange.py 
    @FT_DB.FT_SCH.FIRNEXCHANGE_STAGE 
    AUTO_COMPRESS = FALSE 
    OVERWRITE = TRUE;

-- Upload Python dependencies files (both formats for compatibility)
PUT file:///Users/vverma/sprojects/FirnExchange_V2/environment.yml 
    @FT_DB.FT_SCH.FIRNEXCHANGE_STAGE 
    AUTO_COMPRESS = FALSE 
    OVERWRITE = TRUE;

PUT file:///Users/vverma/sprojects/FirnExchange_V2/pyproject.toml 
    @FT_DB.FT_SCH.FIRNEXCHANGE_STAGE 
    AUTO_COMPRESS = FALSE 
    OVERWRITE = TRUE;

-- Verify uploaded files
LIST @FT_DB.FT_SCH.FIRNEXCHANGE_STAGE;

--------------------------------------------------------------------------------
-- STEP 6: Create Compute Pool (for Streamlit Container Runtime)
--------------------------------------------------------------------------------
CREATE COMPUTE POOL IF NOT EXISTS COMPUTE_POOL_CPU_X64_S_3VCPU_13GB
    MIN_NODES = 1
    MAX_NODES = 1
    INSTANCE_FAMILY = CPU_X64_S
    AUTO_RESUME = FALSE
    INITIALLY_SUSPENDED = TRUE
    AUTO_SUSPEND_SECS = 600
    COMMENT = 'Compute pool for FirnExchange Streamlit app';

-- Verify compute pool status
SHOW COMPUTE POOLS LIKE 'COMPUTE_POOL_CPU_X64_S_3VCPU_13GB';

-- Grant permissions
GRANT MONITOR, OPERATE, USAGE ON COMPUTE POOL COMPUTE_POOL_CPU_X64_S_3VCPU_13GB TO ROLE SYSADMIN;
GRANT MODIFY ON COMPUTE POOL COMPUTE_POOL_CPU_X64_S_3VCPU_13GB TO ROLE SYSADMIN;



--------------------------------------------------------------------------------
-- STEP 7: Create Warehouse (for Query Execution)
--------------------------------------------------------------------------------
CREATE WAREHOUSE IF NOT EXISTS XSMALL
WITH
    WAREHOUSE_TYPE = 'STANDARD'
    WAREHOUSE_SIZE = 'X-SMALL'
    MAX_CLUSTER_COUNT = 1
    MIN_CLUSTER_COUNT = 1
    SCALING_POLICY = 'ECONOMY'
    AUTO_SUSPEND = 180
    AUTO_RESUME = TRUE
    INITIALLY_SUSPENDED = TRUE
    ENABLE_QUERY_ACCELERATION = FALSE
    QUERY_ACCELERATION_MAX_SCALE_FACTOR = 2
    MAX_CONCURRENCY_LEVEL = 3
    STATEMENT_QUEUED_TIMEOUT_IN_SECONDS = 0
    STATEMENT_TIMEOUT_IN_SECONDS = 172800
    COMMENT = 'Warehouse for FirnExchange query execution';

--------------------------------------------------------------------------------
-- STEP 7A: Deploy Task Infrastructure (v2.0 - Task-Based Architecture)
--------------------------------------------------------------------------------
-- Deploy stored procedures and task registry table for v2.0 architecture
-- This must be done BEFORE creating the Streamlit app

-- NOTE: Deploy stored procedures BEFORE running this script:
--   snow sql --connection sf-usb97494-vverma-kp_une --filename firn_procedures.sql
--
-- This creates:
--   1. FIRN_TASK_REGISTRY table
--   2. FIRN_EXPORT_ASYNC_PROC stored procedure  
--   3. FIRN_IMPORT_ASYNC_PROC stored procedure

-- Verify deployment
SELECT 'Task infrastructure deployed successfully' AS STATUS;
SHOW TABLES LIKE 'FIRN_TASK_REGISTRY' IN SCHEMA FT_DB.FT_SCH;
SHOW PROCEDURES LIKE 'FIRN_%' IN SCHEMA FT_DB.FT_SCH;

--------------------------------------------------------------------------------
-- STEP 8: Create Streamlit Application
--------------------------------------------------------------------------------
-- OPTION A: Native Runtime (Recommended - faster startup, simpler)
-- Uses environment.yml for dependencies from Snowflake conda channel
/*
CREATE OR REPLACE STREAMLIT FT_DB.FT_SCH.FirnExchange
    FROM '@FT_DB.FT_SCH.FIRNEXCHANGE_STAGE'
    MAIN_FILE = 'FirnExchange.py'
    QUERY_WAREHOUSE = 'XSMALL'
    COMMENT = 'FirnExchange - High-Performance Data Migration Tool for Snowflake'
    TITLE = 'FirnExchange';
*/

-- OPTION B: Container Runtime (requires compute pool, for PyPI packages)
-- Uncomment below and comment out Option A if you need PyPI packages

CREATE OR REPLACE STREAMLIT FT_DB.FT_SCH.FirnExchange
    FROM '@FT_DB.FT_SCH.FIRNEXCHANGE_STAGE'
    MAIN_FILE = 'FirnExchange.py'
    QUERY_WAREHOUSE = 'XSMALL'
    COMPUTE_POOL = 'COMPUTE_POOL_CPU_X64_S_3VCPU_13GB'
    RUNTIME_NAME = 'SYSTEM$ST_CONTAINER_RUNTIME_PY3_11'
    COMMENT = 'FirnExchange - High-Performance Data Migration Tool for Snowflake'
    TITLE = 'FirnExchange'
    EXTERNAL_ACCESS_INTEGRATIONS = (pypi_access_integration);


--------------------------------------------------------------------------------
-- STEP 9: Grant Permissions (Optional - Adjust roles as needed)
--------------------------------------------------------------------------------
-- Grant usage to data engineering team
-- GRANT USAGE ON STREAMLIT FT_DB.FT_SCH.FirnExchange TO ROLE DATA_ENGINEER;

-- Grant usage to sysadmin for operational management
-- GRANT USAGE ON STREAMLIT FT_DB.FT_SCH.FirnExchange TO ROLE SYSADMIN;

--------------------------------------------------------------------------------
-- STEP 10: Verification
--------------------------------------------------------------------------------
-- Show created Streamlit app details
SHOW STREAMLITS LIKE 'FirnExchange' IN SCHEMA FT_DB.FT_SCH;

-- Get Streamlit app URL
-- SELECT SYSTEM$GET_STREAMLIT_URL('FT_DB.FT_SCH.FirnExchange') AS STREAMLIT_URL;

/*==============================================================================
  Deployment Complete!
  
  Access the application:
    1. Navigate to: Data Products > Streamlit > FirnExchange
    2. Or use: SELECT SYSTEM$GET_STREAMLIT_URL('FT_DB.FT_SCH.FirnExchange');
  
  Notes:
    - Compute pool will auto-suspend after 600 seconds of inactivity
    - Warehouse will auto-suspend after 180 seconds of inactivity
    - Uncomment GRANT statements in Step 9 to provide access to other roles
    - PyPI access integration allows installation of Python packages at runtime
  
  Troubleshooting:
    - If app fails to start, check compute pool status: SHOW COMPUTE POOLS;
    - View app logs: DESCRIBE STREAMLIT FT_DB.FT_SCH.FirnExchange;
    - Check warehouse status: SHOW WAREHOUSES LIKE 'XSMALL';
==============================================================================*/

