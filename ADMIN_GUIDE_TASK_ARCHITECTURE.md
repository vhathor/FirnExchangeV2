# FirnExchange v2.4 - Task-Based Architecture Admin Guide

**Version**: 2.4  
**Architecture**: Snowflake Task-Based Execution  
**Last Updated**: March 2026

---

## Table of Contents

1. [Architecture Overview](#architecture-overview)
2. [How Tasks Work](#how-tasks-work)
3. [Task Lifecycle](#task-lifecycle)
4. [Import Fallback Logic](#import-fallback-logic)
5. [Monitoring Procedures](#monitoring-procedures)
6. [Troubleshooting Guide](#troubleshooting-guide)
7. [Privilege Requirements](#privilege-requirements)
8. [Known Limitations](#known-limitations)
9. [Best Practices](#best-practices)

---

## Architecture Overview

### From Threading to Tasks

FirnExchange v2.4 represents a major architectural shift from threading-based to Snowflake task-based execution:

**Previous Architecture (v1.x)**:
```
User clicks button → Streamlit creates threads → Threads run in app
└─ Problem: User must keep Streamlit open
```

**New Architecture (v2.0)**:
```
User clicks button → Streamlit creates Snowflake Task → Task runs independently
└─ Benefit: User can close Streamlit, task continues running
```

### Key Components

1. **FIRN_TASK_REGISTRY** - Central task registry table
   - Tracks all FirnExchange tasks
   - Stores task metadata, parameters, and status
   - Enables monitoring and management

2. **FIRN_EXPORT_ASYNC_PROC** - Export stored procedure
   - Called by export tasks
   - Uses `collect_nowait()` for async parallelism
   - Processes partitions independently

3. **FIRN_IMPORT_ASYNC_PROC** - Import stored procedure
   - Called by import tasks
   - Uses `collect_nowait()` for async parallelism
   - Processes files independently
   - Automatic ADD_FILES_COPY -> FULL_INGEST fallback (v2.4)
   - Tracks actual load mode used per file via `LOAD_MODE_USED` column

4. **Task Management Functions** (in FirnExchange.py)
   - `create_task_name()` - Generates unique task names
   - `create_export_task()` - Creates and starts export tasks
   - `create_import_task()` - Creates and starts import tasks
   - `get_active_tasks()` - Queries active FIRN tasks
   - `get_task_registry_info()` - Retrieves task details
   - `suspend_task()` - Suspends a running task
   - `resume_task()` - Resumes a suspended task
   - `drop_task()` - Drops a task permanently

---

## How Tasks Work

### Task Creation Flow

#### Export Task Creation:
```sql
1. User selects partitions and configuration in Streamlit
2. Streamlit marks partitions as PENDING in tracking table
3. Streamlit calls create_export_task():
   a. Insert record into FIRN_TASK_REGISTRY
   b. CREATE TASK with SQL stored procedure call
   c. ALTER TASK RESUME (enable task)
   d. EXECUTE TASK (run immediately)
4. Task calls FIRN_EXPORT_ASYNC_PROC
5. Procedure processes partitions asynchronously
6. User can monitor via "Monitor Tasks" tab
```

#### Import Task Creation:
```sql
1. User selects files and configuration in Streamlit
2. Streamlit populates import log table with PENDING files
3. Streamlit calls create_import_task():
   a. Insert record into FIRN_TASK_REGISTRY
   b. CREATE TASK with SQL stored procedure call
   c. ALTER TASK RESUME (enable task)
   d. EXECUTE TASK (run immediately)
4. Task calls FIRN_IMPORT_ASYNC_PROC
5. Procedure processes files asynchronously
6. User can monitor via "Monitor Tasks" tab
```

### Task Naming Convention

Tasks follow a predictable naming pattern:
```
FIRN_<OPERATION>_<DATABASE>_<SCHEMA>_<TABLE>_<TIMESTAMP>

Examples:
- FIRN_EXPORT_MYDB_MYSCHEMA_MYTABLE_20260127_143522
- FIRN_IMPORT_TARGETDB_TARGETSCHEMA_TARGETTABLE_20260127_150145
```

This ensures:
- ✅ Uniqueness (timestamp component)
- ✅ Easy identification (FIRN_ prefix)
- ✅ Context visibility (operation, database, schema, table)

---

## Task Lifecycle

### States

1. **CREATED** - Task registered in FIRN_TASK_REGISTRY
2. **SCHEDULED** - Task scheduled for execution
3. **EXECUTING** - Task currently running
4. **RUNNING** - Registry status during execution
5. **COMPLETED** - Task finished successfully
6. **FAILED** - Task encountered errors
7. **SUSPENDED** - Task manually paused
8. **RETRYING_FULL_INGEST** - Import file: ADD_FILES_COPY failed, retrying with FULL_INGEST (v2.4)

### State Transitions

```
CREATED -> SCHEDULED -> EXECUTING -> RUNNING -> COMPLETED
                                    |
                                  FAILED
           <->
      SUSPENDED

Import file-level states (v2.4):
PENDING -> IN_PROGRESS -> SUCCESS
                       -> RETRYING_FULL_INGEST -> IN_PROGRESS -> SUCCESS
                                                             -> FAILED (both modes failed)
```

### Automatic State Updates

The stored procedures automatically update:
- `START_TIME` when execution begins
- `END_TIME` when execution completes
- `COMPLETED_ITEMS` as items are processed
- `FAILED_ITEMS` when errors occur
- `TASK_STATUS` throughout lifecycle

---

## Import Fallback Logic

### ADD_FILES_COPY -> FULL_INGEST Automatic Fallback (v2.4)

When the import load mode is `ADD_FILES_COPY`, the procedure automatically handles failures by retrying with `FULL_INGEST`. This is particularly useful for Parquet files with timestamp columns, where Snowflake exports MILLIS precision but the Iceberg spec requires MICROS.

**Note:** ADD_FILES_REFERENCE does **not** have automatic fallback. If ADD_FILES_REFERENCE fails (e.g., type mismatch, MILLIS precision), the file is marked as FAILED immediately. This is by design — ADD_FILES_REFERENCE files are in the table's base location and cannot be re-ingested via FULL_INGEST from that location.

### How It Works

1. File is attempted with `ADD_FILES_COPY`
2. If the COPY INTO fails, the file status is set to `RETRYING_FULL_INGEST`
3. The original error message is preserved in `ERROR_MESSAGE`
4. The file is queued for retry with `FULL_INGEST`
5. If `FULL_INGEST` succeeds: `FILE_STATUS = 'SUCCESS'`, `LOAD_MODE_USED = 'FULL_INGEST'`
6. If `FULL_INGEST` also fails: `FILE_STATUS = 'FAILED'` with error noting "after ADD_FILES_COPY fallback"

### Import Log Table Columns (v2.4)

The procedure automatically adds a `LOAD_MODE_USED` column to the import log table via `ALTER TABLE ADD COLUMN IF NOT EXISTS`. This column records which load mode ultimately processed each file.

### Monitoring Fallback Activity

```sql
-- Check files that used fallback
SELECT FILE_PATH, FILE_STATUS, LOAD_MODE_USED, ERROR_MESSAGE
FROM <table>_IMPORT_FELOG
WHERE LOAD_MODE_USED = 'FULL_INGEST'
AND ERROR_MESSAGE LIKE 'ADD_FILES_COPY failed%';

-- Summary of load modes used
SELECT LOAD_MODE_USED, FILE_STATUS, COUNT(*) as FILE_COUNT
FROM <table>_IMPORT_FELOG
GROUP BY LOAD_MODE_USED, FILE_STATUS;
```

---

## Monitoring Procedures

### Via Streamlit (Recommended)

1. Navigate to **"Monitor Tasks"** tab
2. View **Active Tasks** section for running tasks
3. Select a task to view detailed status
4. Use **Refresh** button to update status

### Via SQL (Advanced)

#### Check Active Tasks:
```sql
SELECT 
    NAME as TASK_NAME,
    STATE,
    SCHEDULED_TIME,
    QUERY_START_TIME,
    COMPLETED_TIME
FROM TABLE(INFORMATION_SCHEMA.TASK_HISTORY())
WHERE NAME LIKE 'FIRN_%'
AND STATE IN ('SCHEDULED', 'EXECUTING')
ORDER BY SCHEDULED_TIME DESC;
```

#### Check Task Registry:
```sql
SELECT * 
FROM FIRN_TASK_REGISTRY 
ORDER BY CREATED_AT DESC 
LIMIT 50;
```

#### Check Export Progress:
```sql
-- Replace with your tracking table name
SELECT 
    PARTITION_MIGRATED_STATUS,
    COUNT(*) as COUNT
FROM "DATABASE"."SCHEMA"."TABLE_EXPORT_FELOG"
GROUP BY PARTITION_MIGRATED_STATUS;
```

#### Check Import Progress:
```sql
-- Replace with your import log table name
SELECT 
    FILE_STATUS,
    COUNT(*) as COUNT
FROM "DATABASE"."SCHEMA"."TABLE_IMPORT_LOG"
GROUP BY FILE_STATUS;
```

---

## Troubleshooting Guide

### Task Not Starting

**Symptoms**: Task created but never executes

**Possible Causes**:
1. Insufficient warehouse compute
2. Task not resumed
3. Privilege issues

**Solutions**:
```sql
-- Check task state
SHOW TASKS LIKE 'FIRN_%';

-- Resume task if suspended
ALTER TASK <task_name> RESUME;

-- Check warehouse status
SHOW WAREHOUSES;

-- Check grants
SHOW GRANTS TO ROLE <your_role>;
```

### Task Failing Repeatedly

**Symptoms**: Task starts but errors immediately

**Possible Causes**:
1. Invalid SQL in tracking table
2. Missing objects (stage, table, etc.)
3. Permission issues
4. Warehouse suspended

**Solutions**:
```sql
-- Check task execution history
SELECT * 
FROM TABLE(INFORMATION_SCHEMA.TASK_HISTORY())
WHERE NAME = '<task_name>'
ORDER BY SCHEDULED_TIME DESC;

-- Check error message in registry
SELECT ERROR_MESSAGE, TASK_STATUS
FROM FIRN_TASK_REGISTRY
WHERE TASK_NAME = '<task_name>';

-- Verify objects exist
SHOW STAGES LIKE '<stage_name>';
SHOW TABLES LIKE '<table_name>';
```

### Slow Performance

**Symptoms**: Task running but very slow

**Possible Causes**:
1. Warehouse too small
2. Max workers set too low
3. Large partition/file sizes
4. Network/storage latency

**Solutions**:
1. Increase warehouse size or cluster count
2. Increase max_workers (auto-calculated as `max_cluster_count x MAX_CONCURRENCY_LEVEL`)
3. Review partition strategy
4. Check stage performance

### Task Stuck

**Symptoms**: Task shows EXECUTING but no progress

**Possible Causes**:
1. Procedure hung on async operation
2. Warehouse auto-suspended
3. Deadlock condition

**Solutions**:
```sql
-- Check currently running queries for the task
SHOW QUERIES;

-- Suspend and resume task
ALTER TASK <task_name> SUSPEND;
ALTER TASK <task_name> RESUME;
EXECUTE TASK <task_name>;

-- As last resort, drop and recreate
-- (Will lose progress!)
DROP TASK <task_name>;
-- Recreate via Streamlit
```

---

## Privilege Requirements

### Required Privileges

Users need these privileges to use FirnExchange v2.4:

#### Object Privileges:
```sql
-- On database
GRANT USAGE ON DATABASE <database> TO ROLE <role>;
GRANT CREATE SCHEMA ON DATABASE <database> TO ROLE <role>;

-- On schema
GRANT USAGE ON SCHEMA <database>.<schema> TO ROLE <role>;
GRANT CREATE TABLE ON SCHEMA <database>.<schema> TO ROLE <role>;
GRANT CREATE TASK ON SCHEMA <database>.<schema> TO ROLE <role>;

-- On tables
GRANT SELECT, INSERT, UPDATE, DELETE ON TABLE <tracking_table> TO ROLE <role>;

-- On stages
GRANT READ, WRITE ON STAGE <stage> TO ROLE <role>;

-- On warehouse
GRANT USAGE ON WAREHOUSE <warehouse> TO ROLE <role>;
GRANT OPERATE ON WAREHOUSE <warehouse> TO ROLE <role>;
```

#### Account Privileges:
```sql
-- Task execution
GRANT EXECUTE TASK ON ACCOUNT TO ROLE <role>;

-- Task monitoring
GRANT MONITOR EXECUTION ON ACCOUNT TO ROLE <role>;
```

### Verification Script:
```sql
-- Run as ACCOUNTADMIN to verify grants
SHOW GRANTS TO ROLE <user_role>;
```

---

## Known Limitations

| Limitation | Description | Impact | Workaround |
|------------|-------------|--------|------------|
| **TIMESTAMP_LTZ / TIMESTAMP_TZ Parquet Export** | Snowflake cannot export these types to Parquet format | Export fails with error | FirnExchange auto-casts to TIMESTAMP_NTZ during export |
| **Timestamp MILLIS vs MICROS** | Snowflake Parquet writer exports timestamps with MILLIS precision (scale 3); Iceberg ADD_FILES_COPY and ADD_FILES_REFERENCE expect MICROS (scale 6) | ADD_FILES_COPY and ADD_FILES_REFERENCE fail for files with timestamp columns | Automatic fallback to FULL_INGEST (v2.4) for ADD_FILES_COPY only. ADD_FILES_REFERENCE has no fallback. |
| **INTEGER/SMALLINT/TINYINT/BIGINT** | Snowflake exports all integer types as NUMBER(38,0) in Parquet. SMALLINT, TINYINT, BIGINT not supported as Iceberg column types. | Schema mismatch with ADD_FILES_REFERENCE | Use NUMBER(38,0) or INT in Iceberg DDL. For ADD_FILES_REFERENCE, must use NUMBER(38,0). |
| **FLOAT exports as DOUBLE** | Snowflake exports FLOAT as DOUBLE in Parquet | ADD_FILES_REFERENCE fails with type mismatch | Use DOUBLE instead of FLOAT in Iceberg DDL for ADD_FILES_REFERENCE |
| **VARCHAR(N), TIMESTAMP_NTZ(9/0), COLLATE** | Iceberg does not support VARCHAR(N), TIMESTAMP_NTZ precision > 6, or COLLATE | DDL creation fails | Use STRING, TIMESTAMP_NTZ(6), remove COLLATE clauses |
| **ADD_FILES_REFERENCE location rules** | Files must be under user-specified BASE_LOCATION (not internal Snowflake path). Reserved `data/` and `metadata/` subdirectories are not allowed. | Import fails with location errors | Place files in custom subdirectories under BASE_LOCATION |
| **ADD_FILES_REFERENCE** | Currently in Private Preview | May not be available in all accounts | Use ADD_FILES_COPY or FULL_INGEST |

---

## Best Practices

### Task Management

1. **Always monitor tasks after creation**
   - Check "Monitor Tasks" tab within 5 minutes
   - Verify task is executing and making progress
   - Look for error messages

2. **Use descriptive names**
   - Default names include timestamp
   - Easy to identify purpose and context

3. **Clean up completed tasks**
   - Drop old successful tasks via UI
   - Prevents clutter in task list
   - Maintains registry table size

4. **Suspend before troubleshooting**
   - Suspend task before investigating issues
   - Prevents continuous failures
   - Resume when ready

### Performance Optimization

1. **Right-size warehouse**
   - Small: < 100 partitions/files, light workload
   - Medium: 100-1000 partitions/files
   - Large: > 1000 partitions/files, heavy workload

2. **Tune max_workers**
   - Auto-calculated: `max_cluster_count x MAX_CONCURRENCY_LEVEL`
   - Example: XSMALL (8 concurrency) x 10 clusters = 80 max workers
   - Lower if hitting warehouse limits
   - Higher if warehouse underutilized

3. **Partition strategy**
   - Aim for balanced partition sizes
   - Avoid too many small partitions
   - Avoid very large partitions (> 1GB)

4. **Monitor warehouse usage**
   - Check warehouse utilization
   - Adjust size if over/under utilized
   - Consider auto-suspend settings

### Security

1. **Use role-based access**
   - Grant minimum required privileges
   - Use separate roles for different environments
   - Review grants regularly

2. **Audit task activity**
   - Review FIRN_TASK_REGISTRY periodically
   - Check TASK_HISTORY for anomalies
   - Monitor warehouse costs

3. **Protect tracking tables**
   - Don't manually modify tracking tables
   - Use Streamlit UI for all operations
   - Backup tracking tables if critical

### Maintenance

1. **Regular cleanup**
   - Drop old completed tasks (> 7 days)
   - Archive old registry records
   - Truncate old tracking tables

2. **Monitor registry size**
   ```sql
   SELECT 
       COUNT(*) as TOTAL_TASKS,
       SUM(CASE WHEN TASK_STATUS = 'COMPLETED' THEN 1 ELSE 0 END) as COMPLETED,
       SUM(CASE WHEN TASK_STATUS = 'FAILED' THEN 1 ELSE 0 END) as FAILED
   FROM FIRN_TASK_REGISTRY;
   ```

3. **Backup important data**
   - Export registry periodically
   - Save tracking tables for audit
   - Document configuration

---

## Support and Contact

For issues not covered in this guide:

1. Check task error messages in registry
2. Review Snowflake task history
3. Verify all privileges are granted
4. Contact your Snowflake admin

---

**Document Version**: 2.4  
**Last Updated**: March 2026  
**Architecture Version**: FirnExchange v2.4
