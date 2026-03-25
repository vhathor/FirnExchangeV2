# FirnExchange User Guide

*A Simple Guide to High-Performance Data Migration Between Snowflake Tables*

**Version**: 2.4  
**Last Updated**: March 2026

---

## What is FirnExchange?

FirnExchange is a Streamlit application that enables high-performance migration of large datasets between Snowflake FDN tables and Iceberg tables using external cloud stages (Azure Blob Storage, AWS S3, or GCS). It uses parallel processing to handle millions or billions of rows efficiently by partitioning data into manageable chunks.

## Architecture Overview

FirnExchange v2.4 uses a **task-based architecture**:

```
┌─────────────────┐     ┌──────────────────┐     ┌─────────────────┐
│   Streamlit UI  │────▶│ Stored Procedure │────▶│ Snowflake Task  │
│  (Configuration)│     │  (FIRN_*_PROC)   │     │ (Background Job)│
└─────────────────┘     └──────────────────┘     └─────────────────┘
                                                         │
                        ┌──────────────────┐             │
                        │  Task Registry   │◀────────────┘
                        │ (Status Tracking)│
                        └──────────────────┘
```

**Key Components:**
- **FIRN_TASK_REGISTRY**: Central table tracking all export/import operations
- **FIRN_EXPORT_ASYNC_PROC**: Stored procedure for parallel partition export
- **FIRN_IMPORT_ASYNC_PROC**: Stored procedure for parallel file import (with ADD_FILES_COPY -> FULL_INGEST automatic fallback)
- **Tracking Tables**: Per-operation tables tracking individual partitions/files

## Prerequisites

Before you start, ensure you have:

- A Snowflake account with appropriate permissions
- An external stage configured (Azure Blob, AWS S3, or GCS)
- Source table (FDN or Iceberg) ready for migration
- Target Iceberg table created (or FirnExchange can create it for you)
- A warehouse for executing queries

---

## Deployment

### Quick Deployment

```bash
cd /path/to/FirnExchange_V2
snow sql --connection YOUR_CONNECTION --filename firnexchange_sis.sql
```

### Manual Deployment Steps

1. **Create Infrastructure**
   ```sql
   USE ROLE ACCOUNTADMIN;
   CREATE DATABASE IF NOT EXISTS FT_DB;
   CREATE SCHEMA IF NOT EXISTS FT_DB.FT_SCH;
   ```

2. **Deploy Stored Procedures**
   ```sql
   -- Execute firn_procedures.sql to create:
   --   - FIRN_TASK_REGISTRY table
   --   - FIRN_EXPORT_ASYNC_PROC stored procedure
   --   - FIRN_IMPORT_ASYNC_PROC stored procedure
   ```

3. **Create Streamlit App** (optional - for UI access)
   ```sql
   CREATE OR REPLACE STREAMLIT FT_DB.FT_SCH.FirnExchange
       FROM '@FT_DB.FT_SCH.FIRNEXCHANGE_STAGE'
       MAIN_FILE = 'FirnExchange.py'
       QUERY_WAREHOUSE = 'XSMALL';
   ```

---

## Export Operation

### Overview

Export transfers data from a source table to an external stage as Parquet files, organized by partition values.

### Export Procedure Parameters

```sql
CALL FIRN_EXPORT_ASYNC_PROC(
    TASK_NAME,           -- Unique identifier for this export task
    TRACKING_TABLE,      -- Fully qualified table name for partition tracking
    SOURCE_DATABASE,     -- Database containing source table
    SOURCE_SCHEMA,       -- Schema containing source table
    SOURCE_TABLE,        -- Name of source table
    PARTITION_COLUMN,    -- Column to partition data by
    STAGE,               -- Fully qualified stage name
    STAGE_PATH,          -- Path within stage for output files
    PARQUET_COLUMNS,     -- Comma-separated column list for output
    WAREHOUSE,           -- Warehouse for query execution
    MAX_WORKERS,         -- Number of parallel export threads
    OVERWRITE,           -- TRUE to overwrite existing files
    SINGLE_FILE,         -- TRUE for one file per partition
    MAX_FILE_SIZE        -- Maximum file size in bytes (default 5GB)
);
```

### Parameter Details

| Parameter | Type | Description | Example |
|-----------|------|-------------|---------|
| TASK_NAME | VARCHAR | Unique task identifier | `'EXPORT_LINEITEM_20260210'` |
| TRACKING_TABLE | VARCHAR | Fully qualified tracking table | `'"DB"."SCH"."TABLE_FELOG"'` |
| SOURCE_DATABASE | VARCHAR | Source database name | `'FT_DB'` |
| SOURCE_SCHEMA | VARCHAR | Source schema name | `'FT_SCH'` |
| SOURCE_TABLE | VARCHAR | Source table name | `'LINEITEM'` |
| PARTITION_COLUMN | VARCHAR | Column for partitioning | `'L_SHIPMODE'` |
| STAGE | VARCHAR | External stage (fully qualified) | `'DB.SCH.MY_STAGE'` |
| STAGE_PATH | VARCHAR | Output path in stage | `'exports/2026/'` |
| PARQUET_COLUMNS | VARCHAR | Columns to export | `'"COL1", "COL2", "COL3"'` |
| WAREHOUSE | VARCHAR | Warehouse name | `'XSMALL'` |
| MAX_WORKERS | NUMBER | Parallel threads (auto-calculated: `max_cluster_count x MAX_CONCURRENCY_LEVEL`) | `80` |
| OVERWRITE | BOOLEAN | Overwrite existing files | `TRUE` |
| SINGLE_FILE | BOOLEAN | One file per partition | `TRUE` |
| MAX_FILE_SIZE | NUMBER | Max file size (bytes) | `5368709120` (5GB) |

### Export Tracking Table Schema

```sql
CREATE TABLE <table_name>_EXPORT_FELOG (
    <PARTITION_COLUMN> STRING PRIMARY KEY,    -- Partition value
    TOTAL_ROWS BIGINT,                        -- Rows in partition
    PARTITION_MIGRATED_STATUS STRING,         -- PENDING/IN_PROGRESS/SUCCESS/FAILED
    PARTITION_MIGRATED_START_AT TIMESTAMP,    -- Export start time
    PARTITION_MIGRATED_END_AT TIMESTAMP,      -- Export end time
    ERROR_MESSAGE STRING,                     -- Error details if failed
    RETRY_COUNT INT,                          -- Number of retries
    ROWS_UNLOADED BIGINT,                     -- Actual rows exported
    INPUT_BYTES BIGINT,                       -- Source data size
    OUTPUT_BYTES BIGINT                       -- Output file size
);
```

### Stage Output Structure

Files are organized by sanitized partition value:
```
@STAGE/path/
├── AIR/
│   └── data                 # Parquet file
├── FOB/
│   └── data
├── REG_AIR/                 # "REG AIR" sanitized to "REG_AIR"
│   └── data
└── SHIP/
    └── data
```

**Note:** Partition values with spaces or special characters are automatically sanitized (e.g., "REG AIR" → "REG_AIR") in file paths, while the original value is preserved in the data.

---

## Import Operation

### Overview

Import loads Parquet files from an external stage into an Iceberg table using parallel file processing.

### Import Procedure Parameters

```sql
CALL FIRN_IMPORT_ASYNC_PROC(
    TASK_NAME,           -- Unique identifier for this import task
    IMPORT_LOG_TABLE,    -- Fully qualified table name for file tracking
    TARGET_DATABASE,     -- Database containing target table
    TARGET_SCHEMA,       -- Schema containing target table
    TARGET_TABLE,        -- Name of target Iceberg table
    STAGE,               -- Fully qualified stage name
    WAREHOUSE,           -- Warehouse for query execution
    LOAD_MODE,           -- FULL_INGEST, ADD_FILES_COPY, or ADD_FILES_REFERENCE
    MAX_WORKERS          -- Number of parallel import threads
);
```

### Parameter Details

| Parameter | Type | Description | Example |
|-----------|------|-------------|---------|
| TASK_NAME | VARCHAR | Unique task identifier | `'IMPORT_LINEITEM_20260210'` |
| IMPORT_LOG_TABLE | VARCHAR | Fully qualified log table | `'"DB"."SCH"."TABLE_IMPORT_FELOG"'` |
| TARGET_DATABASE | VARCHAR | Target database name | `'FT_DB'` |
| TARGET_SCHEMA | VARCHAR | Target schema name | `'FT_SCH'` |
| TARGET_TABLE | VARCHAR | Target Iceberg table name | `'LINEITEM_ICEBERG'` |
| STAGE | VARCHAR | External stage (fully qualified) | `'DB.SCH.MY_STAGE'` |
| WAREHOUSE | VARCHAR | Warehouse name | `'XSMALL'` |
| LOAD_MODE | VARCHAR | `'FULL_INGEST'`, `'ADD_FILES_COPY'`, or `'ADD_FILES_REFERENCE'` | `'ADD_FILES_COPY'` |
| MAX_WORKERS | NUMBER | Parallel threads (auto-calculated: `max_cluster_count x MAX_CONCURRENCY_LEVEL`) | `80` |

### Concurrency Calculation

Max Workers is dynamically calculated from the selected warehouse:

```
max_workers = max_cluster_count x MAX_CONCURRENCY_LEVEL
```

For example, an XSMALL warehouse with `MAX_CONCURRENCY_LEVEL=8` and `max_cluster_count=10` yields up to **80** parallel threads. The Streamlit UI auto-detects these values and sets the slider range accordingly.

### Load Modes

| Mode | Description | Use Case |
|------|-------------|----------|
| `FULL_INGEST` | Traditional COPY INTO with full data parsing | Standard data loading, data transformation needed |
| `ADD_FILES_COPY` | Copy parquet files to table base location, then register | Best for Iceberg tables when files are on external stage |
| `ADD_FILES_REFERENCE` | Register existing parquet files without copying (Private Preview) | Files already in Iceberg table's base location |

**Notes:**
- `ADD_FILES_COPY` and `ADD_FILES_REFERENCE` require `USE_VECTORIZED_SCANNER = TRUE` (automatically enabled)
- `ADD_FILES_REFERENCE` requires files to already exist under the Iceberg table's user-specified `BASE_LOCATION` path (not the internal Snowflake-managed path)
- `ADD_FILES_REFERENCE` does **not** allow files in reserved `data/` or `metadata/` subdirectories under the base location
- `ADD_FILES_REFERENCE` requires **exact** Parquet physical type matching (e.g., Snowflake exports FLOAT as DOUBLE, INT as NUMBER(38,0)) — the Iceberg table schema must use the exact Parquet types
- `ADD_FILES_REFERENCE` has no automatic fallback — if it fails, the file is marked FAILED (unlike ADD_FILES_COPY which falls back to FULL_INGEST)
- `ADD_FILES_COPY` may report `ROWS_LOADED = 0` on success (this is expected — it registers files without counting rows)
- For `ADD_FILES_REFERENCE`, export files directly to the table's base location subfolder

### ADD_FILES_COPY -> FULL_INGEST Automatic Fallback (v2.4)

When `ADD_FILES_COPY` is selected, the import procedure automatically handles failures by retrying with `FULL_INGEST`:

```
ADD_FILES_COPY attempt -> fails -> RETRYING_FULL_INGEST -> FULL_INGEST attempt -> SUCCESS or FAILED
```

- If `ADD_FILES_COPY` fails for a file (e.g., timestamp precision mismatch), the file status is set to `RETRYING_FULL_INGEST` and queued for `FULL_INGEST` retry
- The `LOAD_MODE_USED` column in the import log records which mode ultimately succeeded
- The `ERROR_MESSAGE` column preserves the original ADD_FILES_COPY failure reason even on successful fallback
- Only marks a file as `FAILED` if **both** modes fail
- Fallback is only triggered when the original load mode is `ADD_FILES_COPY`

### Known Limitations: Parquet Export and Iceberg Compatibility

| Limitation | Description | Workaround |
|------------|-------------|------------|
| **TIMESTAMP_LTZ / TIMESTAMP_TZ** | Cannot be exported to Parquet format. Snowflake returns: `TIMESTAMP_TZ and LTZ types are not supported for unloading to Parquet` | Cast to `TIMESTAMP_NTZ` in the export SELECT (FirnExchange handles this automatically) |
| **Timestamp Precision (MILLIS vs MICROS)** | Snowflake Parquet writer exports timestamps with MILLIS precision (scale 3), but Iceberg ADD_FILES_COPY and ADD_FILES_REFERENCE expect MICROS precision (scale 6). Error: `Parquet file time column has an unsupported time unit` | Use `FULL_INGEST` (re-parses data), or rely on ADD_FILES_COPY -> FULL_INGEST fallback (v2.4). ADD_FILES_REFERENCE has no fallback. |
| **INTEGER/SMALLINT/TINYINT/BIGINT** | Snowflake exports all integer types as NUMBER(38,0) in Parquet. SMALLINT, TINYINT, and BIGINT are not supported as Iceberg column types. | Use `NUMBER(38,0)` or `INT` in Iceberg DDL. For ADD_FILES_REFERENCE, must use `NUMBER(38,0)` to match Parquet exactly. |
| **FLOAT exports as DOUBLE** | Snowflake exports FLOAT columns as DOUBLE in Parquet. ADD_FILES_REFERENCE requires exact physical type match. | Use `DOUBLE` instead of `FLOAT` in Iceberg DDL when using ADD_FILES_REFERENCE |
| **VARCHAR(N) not supported** | Iceberg tables only support max-length VARCHAR (134,217,728) or STRING | Use `STRING` instead of `VARCHAR(N)` in Iceberg DDL |
| **TIMESTAMP_NTZ precision** | Iceberg max timestamp precision is microseconds (scale 6). TIMESTAMP_NTZ(9) and TIMESTAMP_NTZ(0) are not valid. | Use `TIMESTAMP_NTZ(6)` in Iceberg DDL |
| **COLLATE not supported** | Iceberg tables do not support collation specifications | Remove all COLLATE clauses when converting FDN DDL to Iceberg |

### Import Log Table Schema

```sql
CREATE TABLE <table_name>_IMPORT_FELOG (
    FILE_PATH STRING PRIMARY KEY,      -- Stage path to file
    FILE_STATUS STRING,                 -- PENDING/IN_PROGRESS/RETRYING_FULL_INGEST/SUCCESS/FAILED
    IMPORT_START_AT TIMESTAMP,          -- Import start time
    IMPORT_END_AT TIMESTAMP,            -- Import end time
    ERROR_MESSAGE STRING,               -- Error details if failed (preserved even on fallback success)
    ROWS_LOADED BIGINT,                 -- Rows loaded from file
    ROWS_PARSED BIGINT,                 -- Rows parsed from file
    LOAD_MODE_USED STRING              -- Actual load mode used (added automatically by procedure)
);
```

---

## Task Registry

### FIRN_TASK_REGISTRY Schema

Central tracking table for all FirnExchange operations:

| Column | Type | Description |
|--------|------|-------------|
| TASK_NAME | VARCHAR | Primary key, unique task identifier |
| OPERATION_TYPE | VARCHAR | `'EXPORT'` or `'IMPORT'` |
| SOURCE_DATABASE | VARCHAR | Source database (export only) |
| SOURCE_SCHEMA | VARCHAR | Source schema (export only) |
| SOURCE_TABLE | VARCHAR | Source table (export only) |
| TARGET_DATABASE | VARCHAR | Target database (import only) |
| TARGET_SCHEMA | VARCHAR | Target schema (import only) |
| TARGET_TABLE | VARCHAR | Target table (import only) |
| TRACKING_TABLE | VARCHAR | Associated tracking/log table |
| STAGE | VARCHAR | External stage name |
| STAGE_PATH | VARCHAR | Path within stage |
| WAREHOUSE | VARCHAR | Warehouse used |
| MAX_WORKERS | NUMBER | Parallel worker count |
| CREATED_BY | VARCHAR | User who created task |
| CREATED_AT | TIMESTAMP | Task creation time |
| TASK_STATUS | VARCHAR | CREATED/PENDING/RUNNING/COMPLETED/FAILED |
| START_TIME | TIMESTAMP | Execution start time |
| END_TIME | TIMESTAMP | Execution end time |
| TOTAL_ITEMS | NUMBER | Total partitions/files |
| COMPLETED_ITEMS | NUMBER | Successfully processed |
| FAILED_ITEMS | NUMBER | Failed items |
| ERROR_MESSAGE | VARCHAR | Error details if failed |
| PARAMETERS | VARIANT | Additional JSON parameters |

### Checking Task Status

```sql
-- View all tasks
SELECT TASK_NAME, OPERATION_TYPE, TASK_STATUS, COMPLETED_ITEMS, FAILED_ITEMS, ERROR_MESSAGE
FROM FIRN_TASK_REGISTRY
ORDER BY CREATED_AT DESC;

-- View running tasks
SELECT * FROM FIRN_TASK_REGISTRY WHERE TASK_STATUS = 'RUNNING';
```

---

## Complete Validation Example

This section demonstrates a complete end-to-end export and import workflow using SQL commands.

### Test Environment

- **Source Table**: `FT_SRC_TABLE_5K` (5,000 rows from TPCH LINEITEM)
- **Target Table**: `FT_TGT_TABLE_LINEITEM_AFC` (Iceberg table)
- **Partition Column**: `L_SHIPMODE` (7 unique values)
- **External Stage**: `FT_EXT_STAGE_AZURE` (Azure Blob Storage)

### Source Data Distribution

| L_SHIPMODE | Row Count |
|------------|-----------|
| TRUCK | 683 |
| RAIL | 698 |
| AIR | 709 |
| REG AIR | 710 |
| SHIP | 728 |
| FOB | 735 |
| MAIL | 737 |
| **Total** | **5,000** |

### Step 1: Create Export Tracking Table

```sql
-- Create tracking table for export operation
CREATE OR REPLACE TABLE FT_DB.FT_SCH.FT_SRC_TABLE_5K_EXPORT_FELOG (
    L_SHIPMODE STRING PRIMARY KEY,
    TOTAL_ROWS BIGINT,
    PARTITION_MIGRATED_STATUS STRING DEFAULT 'PENDING',
    PARTITION_MIGRATED_START_AT TIMESTAMP DEFAULT CURRENT_TIMESTAMP(),
    PARTITION_MIGRATED_END_AT TIMESTAMP,
    ERROR_MESSAGE STRING,
    RETRY_COUNT INT DEFAULT 0,
    ROWS_UNLOADED BIGINT,
    INPUT_BYTES BIGINT,
    OUTPUT_BYTES BIGINT
);

-- Populate with partition values
INSERT INTO FT_DB.FT_SCH.FT_SRC_TABLE_5K_EXPORT_FELOG (L_SHIPMODE, TOTAL_ROWS, PARTITION_MIGRATED_STATUS)
SELECT L_SHIPMODE, COUNT(*) as TOTAL_ROWS, 'PENDING' 
FROM FT_DB.FT_SCH.FT_SRC_TABLE_5K
GROUP BY L_SHIPMODE;

-- Verify
SELECT * FROM FT_DB.FT_SCH.FT_SRC_TABLE_5K_EXPORT_FELOG;
```

### Step 2: Register Export Task

```sql
-- Create task registry entry
INSERT INTO FT_DB.FT_SCH.FIRN_TASK_REGISTRY (
    TASK_NAME, OPERATION_TYPE, SOURCE_DATABASE, SOURCE_SCHEMA, SOURCE_TABLE,
    TRACKING_TABLE, STAGE, STAGE_PATH, WAREHOUSE, MAX_WORKERS, TOTAL_ITEMS, 
    TASK_STATUS, PARAMETERS
) VALUES (
    'VALIDATION_EXPORT_' || TO_VARCHAR(CURRENT_TIMESTAMP(), 'YYYYMMDD_HH24MISS'),
    'EXPORT', 'FT_DB', 'FT_SCH', 'FT_SRC_TABLE_5K',
    '"FT_DB"."FT_SCH"."FT_SRC_TABLE_5K_EXPORT_FELOG"',
    'FT_DB.FT_SCH.FT_EXT_STAGE_AZURE', 'validation_test/',
    'XSMALL', 4, 7, 'CREATED',
    PARSE_JSON('{"partition_col": "L_SHIPMODE", "max_workers": 4}')
);

-- Get the task name for next step
SELECT TASK_NAME FROM FT_DB.FT_SCH.FIRN_TASK_REGISTRY 
WHERE TASK_NAME LIKE 'VALIDATION_EXPORT_%' 
ORDER BY CREATED_AT DESC LIMIT 1;
```

### Step 3: Execute Export Procedure

```sql
-- Replace TASK_NAME with actual value from Step 2
CALL FT_DB.FT_SCH.FIRN_EXPORT_ASYNC_PROC(
    'VALIDATION_EXPORT_20260210_120000',                  -- TASK_NAME (replace with actual)
    '"FT_DB"."FT_SCH"."FT_SRC_TABLE_5K_EXPORT_FELOG"',   -- TRACKING_TABLE
    'FT_DB',                                              -- SOURCE_DATABASE
    'FT_SCH',                                             -- SOURCE_SCHEMA
    'FT_SRC_TABLE_5K',                                    -- SOURCE_TABLE
    'L_SHIPMODE',                                         -- PARTITION_COLUMN
    'FT_DB.FT_SCH.FT_EXT_STAGE_AZURE',                   -- STAGE
    'validation_test/',                                   -- STAGE_PATH
    '"L_PARTKEY", "L_SUPPKEY", "L_LINENUMBER", "L_QUANTITY", "L_EXTENDEDPRICE", "L_DISCOUNT", "L_TAX", "L_RETURNFLAG", "L_LINESTATUS", "L_SHIPDATE", "L_COMMITDATE", "L_RECEIPTDATE", "L_SHIPINSTRUCT", "L_SHIPMODE", "L_COMMENT"',
    'XSMALL',                                             -- WAREHOUSE
    4,                                                    -- MAX_WORKERS
    TRUE,                                                 -- OVERWRITE
    TRUE,                                                 -- SINGLE_FILE
    5368709120                                            -- MAX_FILE_SIZE (5GB)
);

-- Expected output: Export completed: 7 succeeded, 0 failed out of 7
```

### Step 4: Verify Export Results

```sql
-- Check tracking table status
SELECT L_SHIPMODE, TOTAL_ROWS, PARTITION_MIGRATED_STATUS, ROWS_UNLOADED, ERROR_MESSAGE 
FROM FT_DB.FT_SCH.FT_SRC_TABLE_5K_EXPORT_FELOG 
ORDER BY L_SHIPMODE;

-- Check files in stage (note: "REG AIR" becomes "REG_AIR")
LIST @FT_DB.FT_SCH.FT_EXT_STAGE_AZURE/validation_test/;

-- Expected: 7 folders with parquet files:
-- AIR/data, FOB/data, MAIL/data, RAIL/data, REG_AIR/data, SHIP/data, TRUCK/data

-- Check task registry status
SELECT TASK_NAME, TASK_STATUS, COMPLETED_ITEMS, FAILED_ITEMS 
FROM FT_DB.FT_SCH.FIRN_TASK_REGISTRY 
WHERE TASK_NAME LIKE 'VALIDATION_EXPORT_%';
```

### Step 5: Create Import Log Table

```sql
-- Create import log table
CREATE OR REPLACE TABLE FT_DB.FT_SCH.FT_TGT_TABLE_LINEITEM_AFC_IMPORT_FELOG (
    FILE_PATH STRING PRIMARY KEY,
    FILE_STATUS STRING DEFAULT 'PENDING',
    IMPORT_START_AT TIMESTAMP DEFAULT CURRENT_TIMESTAMP(),
    IMPORT_END_AT TIMESTAMP,
    ERROR_MESSAGE STRING,
    ROWS_LOADED BIGINT,
    ROWS_PARSED BIGINT
);

-- List files and populate import log (Azure example)
LIST @FT_DB.FT_SCH.FT_EXT_STAGE_AZURE/validation_test/;

-- Insert file paths (only data files, not empty folders)
INSERT INTO FT_DB.FT_SCH.FT_TGT_TABLE_LINEITEM_AFC_IMPORT_FELOG (FILE_PATH, FILE_STATUS)
SELECT 
    REPLACE("name", 'azure://vvermastorageeastus2.blob.core.windows.net/dbschema-app-container-1/', ''),
    'PENDING'
FROM TABLE(RESULT_SCAN(LAST_QUERY_ID()))
WHERE "size" > 0;

-- Verify
SELECT * FROM FT_DB.FT_SCH.FT_TGT_TABLE_LINEITEM_AFC_IMPORT_FELOG;
```

### Step 6: Register Import Task

```sql
-- Create task registry entry for import
INSERT INTO FT_DB.FT_SCH.FIRN_TASK_REGISTRY (
    TASK_NAME, OPERATION_TYPE, TARGET_DATABASE, TARGET_SCHEMA, TARGET_TABLE,
    TRACKING_TABLE, STAGE, WAREHOUSE, MAX_WORKERS, TOTAL_ITEMS, 
    TASK_STATUS, PARAMETERS
) VALUES (
    'VALIDATION_IMPORT_' || TO_VARCHAR(CURRENT_TIMESTAMP(), 'YYYYMMDD_HH24MISS'),
    'IMPORT', 'FT_DB', 'FT_SCH', 'FT_TGT_TABLE_LINEITEM_AFC',
    '"FT_DB"."FT_SCH"."FT_TGT_TABLE_LINEITEM_AFC_IMPORT_FELOG"',
    'FT_DB.FT_SCH.FT_EXT_STAGE_AZURE', 'XSMALL', 4, 7, 'CREATED',
    PARSE_JSON('{"load_mode": "ADD_FILES_COPY", "max_workers": 4}')
);

-- Get the task name for next step
SELECT TASK_NAME FROM FT_DB.FT_SCH.FIRN_TASK_REGISTRY 
WHERE TASK_NAME LIKE 'VALIDATION_IMPORT_%' 
ORDER BY CREATED_AT DESC LIMIT 1;
```

### Step 7: Execute Import Procedure

```sql
-- Replace TASK_NAME with actual value from Step 6
CALL FT_DB.FT_SCH.FIRN_IMPORT_ASYNC_PROC(
    'VALIDATION_IMPORT_20260210_120500',                  -- TASK_NAME (replace with actual)
    '"FT_DB"."FT_SCH"."FT_TGT_TABLE_LINEITEM_AFC_IMPORT_FELOG"',
    'FT_DB',                                              -- TARGET_DATABASE
    'FT_SCH',                                             -- TARGET_SCHEMA
    'FT_TGT_TABLE_LINEITEM_AFC',                         -- TARGET_TABLE
    'FT_DB.FT_SCH.FT_EXT_STAGE_AZURE',                   -- STAGE
    'XSMALL',                                             -- WAREHOUSE
    'ADD_FILES_COPY',                                     -- LOAD_MODE (or 'FULL_INGEST')
    4                                                     -- MAX_WORKERS
);

-- Expected output: Import completed: 7 succeeded, 0 failed out of 7
```

### Step 8: Verify Import Results

```sql
-- Check import log status
SELECT FILE_PATH, FILE_STATUS, ROWS_LOADED 
FROM FT_DB.FT_SCH.FT_TGT_TABLE_LINEITEM_AFC_IMPORT_FELOG
ORDER BY FILE_PATH;

-- Verify row counts by partition (note: L_SHIPMODE preserves original value "REG AIR")
SELECT L_SHIPMODE, COUNT(*) as ROW_COUNT 
FROM FT_DB.FT_SCH.FT_TGT_TABLE_LINEITEM_AFC 
GROUP BY L_SHIPMODE 
ORDER BY ROW_COUNT;

-- Verify total row count
SELECT COUNT(*) as TOTAL_ROWS FROM FT_DB.FT_SCH.FT_TGT_TABLE_LINEITEM_AFC;
-- Expected: 5000 rows

-- Check task registry status
SELECT TASK_NAME, TASK_STATUS, COMPLETED_ITEMS, FAILED_ITEMS 
FROM FT_DB.FT_SCH.FIRN_TASK_REGISTRY 
WHERE TASK_NAME LIKE 'VALIDATION_IMPORT_%';
```

### Cleanup (Optional)

```sql
-- Reset for re-testing
DELETE FROM FT_DB.FT_SCH.FIRN_TASK_REGISTRY WHERE TASK_NAME LIKE 'VALIDATION_%';
DROP TABLE IF EXISTS FT_DB.FT_SCH.FT_SRC_TABLE_5K_EXPORT_FELOG;
DROP TABLE IF EXISTS FT_DB.FT_SCH.FT_TGT_TABLE_LINEITEM_AFC_IMPORT_FELOG;
DELETE FROM FT_DB.FT_SCH.FT_TGT_TABLE_LINEITEM_AFC;
REMOVE @FT_DB.FT_SCH.FT_EXT_STAGE_AZURE/validation_test/;
```

---

## Best Practices

### Performance Tips

1. **Choose the Right Partition Key**: Use a column with good data distribution (e.g., date columns)
2. **Tune Thread Count**: Max workers auto-calculated as `max_cluster_count x MAX_CONCURRENCY_LEVEL`. Adjust via slider based on observed performance.
3. **Monitor Warehouse Load**: Ensure your warehouse can handle the concurrency level
4. **Incremental Migration**: Use "Select Non-Completed" for large tables to migrate in batches

### Important Notes

- **Tracking Tables**: FirnExchange creates log tables (`*_EXPORT_FELOG` and `*_IMPORT_FELOG`) to track progress
- **Resume Capability**: If a migration is interrupted, simply rerun and use "Select Non-Completed"
- **Drop Log Tables**: Use "Drop Log Table" buttons to start fresh analysis
- **Single Selection Only**: Partition key must be a single column
- **Partition Names with Spaces**: Handled automatically - "REG AIR" becomes "REG_AIR" in stage paths while preserving original value in data

### Monitoring Your Migration

FirnExchange v2.4 uses **Snowflake Tasks** for execution, providing robust monitoring:

#### Monitor Tasks Tab (Streamlit UI)

1. **Active Tasks Section**
   - Lists all currently running FirnExchange tasks
   - Shows task state (SCHEDULED, EXECUTING)
   - Displays task names, databases, and schemas

2. **Task Details Panel**
   - Select any task to view detailed information
   - See progress metrics (total, completed, failed items)
   - View tracking table data in real-time
   - Check export/import statistics

3. **Task Controls**
   - **Suspend**: Temporarily pause a running task
   - **Resume**: Restart a suspended task
   - **Drop**: Permanently delete a task

#### SQL Monitoring

```sql
-- View all recent tasks
SELECT TASK_NAME, OPERATION_TYPE, TASK_STATUS, 
       COMPLETED_ITEMS, FAILED_ITEMS, START_TIME, END_TIME
FROM FIRN_TASK_REGISTRY
ORDER BY CREATED_AT DESC
LIMIT 20;

-- Check detailed export progress
SELECT * FROM <your_table>_EXPORT_FELOG
WHERE PARTITION_MIGRATED_STATUS != 'SUCCESS';

-- Check detailed import progress
SELECT * FROM <your_table>_IMPORT_FELOG
WHERE FILE_STATUS != 'SUCCESS';
```

---

## Common Scenarios

### Scenario 1: Migrating a 10 Billion Row Table

1. Choose a date partition key with ~daily granularity
2. Analyze partitions (might show 365+ partitions)
3. Test with 10 partitions first
4. Once successful, use "Select Non-Completed" to process remaining partitions
5. Set max workers based on warehouse capacity (auto-calculated from cluster count and concurrency level)

### Scenario 2: Failed Partitions

1. Check the error in the tracking table:
   ```sql
   SELECT * FROM <table>_EXPORT_FELOG WHERE PARTITION_MIGRATED_STATUS = 'FAILED';
   ```
2. Common fixes:
   - Reduce file size if files are too large
   - Increase warehouse size if timeout occurs
   - Check stage permissions if access errors occur
3. Reset failed partitions and re-run:
   ```sql
   UPDATE <table>_EXPORT_FELOG 
   SET PARTITION_MIGRATED_STATUS = 'PENDING', ERROR_MESSAGE = NULL 
   WHERE PARTITION_MIGRATED_STATUS = 'FAILED';
   ```

### Scenario 3: Incremental Daily Updates

1. Keep the same log table
2. New partitions will automatically appear when you re-analyze
3. Select only new partition values
4. Export and import as usual

### Scenario 4: Partition Values with Special Characters

FirnExchange automatically handles partition values containing:
- Spaces: "REG AIR" → "REG_AIR" in paths
- Special characters: sanitized using regex `[^a-zA-Z0-9_-]` → `_`

The original value is preserved in the exported Parquet data; only the stage path is sanitized.

---

## Troubleshooting

### Common Errors

| Error | Cause | Solution |
|-------|-------|----------|
| `Unsupported statement type 'USE'` | Old procedure version | Redeploy `firn_procedures.sql` |
| `LOAD_MODE = ADD_FILES_COPY option is not supported` | Missing vectorized scanner | Ensure using latest procedures with `USE_VECTORIZED_SCANNER = TRUE` |
| `SQL execution internal error (370001)` | Double slashes in file path with ADD_FILES_COPY | Fixed in v2.1 - paths are now normalized to remove `//` |
| `syntax error ... unexpected 'AIR'` | Space in partition path | Ensure using latest procedures with path sanitization |
| `Access denied` | Stage permissions | Grant USAGE on stage to executing role |
| `Warehouse timeout` | Large partition | Reduce partition size or increase warehouse |
| `Parquet file time column has an unsupported time unit. Parquet Unit: 'MILLIS'...` | Snowflake exports timestamps as MILLIS; Iceberg ADD_FILES_COPY/REFERENCE expects MICROS | Automatic fallback to FULL_INGEST (v2.4) for ADD_FILES_COPY; use FULL_INGEST directly for ADD_FILES_REFERENCE |
| `TIMESTAMP_TZ and LTZ types are not supported for unloading to Parquet` | Snowflake cannot export TIMESTAMP_LTZ/TZ to Parquet | FirnExchange auto-casts to TIMESTAMP_NTZ during export |
| `Parquet file schema data type is incompatible with table column` | ADD_FILES_REFERENCE requires exact Parquet physical type match (FLOAT vs DOUBLE, INT vs DECIMAL) | Match Iceberg types to Parquet: use DOUBLE not FLOAT, NUMBER(38,0) not INT |
| `Invalid source location for ADD_FILES_REFERENCE ... data and metadata folders are restricted` | Files in reserved `data/` or `metadata/` subdirectories | Place files in custom subdirectories (e.g., `part_a/`, `export_001/`) |
| `copy allowed only under locations: ...` | ADD_FILES_REFERENCE files not under user-specified BASE_LOCATION | Ensure files are under the BASE_LOCATION from CREATE ICEBERG TABLE, not the internal Snowflake path |

### Debugging Steps

1. **Check task registry for errors:**
   ```sql
   SELECT TASK_NAME, TASK_STATUS, ERROR_MESSAGE 
   FROM FIRN_TASK_REGISTRY 
   WHERE TASK_STATUS = 'FAILED';
   ```

2. **Check tracking table for partition-level errors:**
   ```sql
   SELECT * FROM <table>_EXPORT_FELOG WHERE ERROR_MESSAGE IS NOT NULL;
   ```

3. **Verify stage accessibility:**
   ```sql
   LIST @<your_stage>/;
   ```

4. **Check warehouse status:**
   ```sql
   SHOW WAREHOUSES LIKE '<your_warehouse>';
   ```

---

## Local Testing Mode

FirnExchange supports local execution for development and testing:

### Running Locally

```bash
# Set environment variables and run
FIRN_LOCAL_TEST=true SNOWFLAKE_CONNECTION_NAME=your-connection-name streamlit run FirnExchange.py
```

### Environment Variables

| Variable | Description | Required |
|----------|-------------|----------|
| `FIRN_LOCAL_TEST` | Set to `true` to enable local testing mode | Yes |
| `SNOWFLAKE_CONNECTION_NAME` | Connection name from `~/.snowflake/connections.toml` | Yes |

### Local vs Snowflake Deployment

| Feature | Local Testing | Streamlit in Snowflake |
|---------|---------------|------------------------|
| Authentication | connections.toml | Native Snowflake auth |
| Environment indicator | "💻 Local (Testing)" | "🏔️ Streamlit in Snowflake" |
| All features | ✅ Supported | ✅ Supported |
| Production use | ❌ Not recommended | ✅ Recommended |

---

## Summary

FirnExchange makes large-scale data migration simple:

1. **Export**: Partition → Export to Stage (parallel)
2. **Import**: Load from Stage → Iceberg Table (parallel)
3. **Monitor**: Track progress in real-time via Task Registry
4. **Resume**: Handle failures gracefully with retry capability

With proper configuration, you can migrate billions of rows efficiently while maintaining full control over the process.
