# Pipeline Logging & Monitoring - Implementation Guide

## Overview
A comprehensive logging and monitoring system has been implemented to track all procedure executions, capture errors, and monitor pipeline health. This ensures operational visibility and enables quick troubleshooting.

**Status**: ✅ Complete  
**Implementation Date**: February 2026  
**Files Added**: 3 new SQL files, 2 updated procedures

---

## What Was Implemented

### 1. **Pipeline Run History Tables** 
**Location**: [sql/00_database_setup/06_create_orchestration_logging_tables.sql](sql/00_database_setup/06_create_orchestration_logging_tables.sql)

Three comprehensive tables for tracking pipeline execution:

#### PIPELINE_RUN_HISTORY Table
Tracks every procedure execution with full details:
- **run_id**: Auto-incremented primary key
- **procedure_name**: Name of executed procedure
- **status**: `RUNNING`, `SUCCESS`, or `FAILED`
- **start_time**: Procedure start timestamp
- **end_time**: Procedure completion timestamp
- **duration_seconds**: Execution time in seconds
- **rows_affected**: Total rows modified
- **rows_inserted, rows_updated, rows_deleted**: Granular row counts
- **error_code, error_message, error_detail**: Full error context
- **execution_context**: Additional metadata
- **created_by**: User who executed the procedure

#### PROCEDURE_EXECUTION_METRICS Table
SLA tracking and performance metrics:
- Daily aggregation of execution statistics
- Success rates and failure counts
- Average/min/max execution duration
- SLA compliance tracking
- Failed execution details for alerting

#### ALERT_TRIGGERS Table
Alert configuration for automated notifications:
- Threshold-based alerts (execution time, error rate, etc.)
- Severity levels (INFO, WARNING, CRITICAL)
- Notification channels (email, Slack, etc.)

---

### 2. **Logging Procedures**
**Location**: [sql/05_orchestration/00_create_logging_procedures.sql](sql/05_orchestration/00_create_logging_procedures.sql)

#### LOG_PIPELINE_RUN_START()
Initiates logging for a procedure execution
```sql
CALL SAAS_ANALYTICS.ORCHESTRATION.LOG_PIPELINE_RUN_START(
    p_procedure_name VARCHAR,
    p_execution_context VARCHAR DEFAULT NULL
)
```
**Returns**: `run_id` (used to track the execution)

#### LOG_PIPELINE_RUN_END()
Completes logging with status and metrics
```sql
CALL SAAS_ANALYTICS.ORCHESTRATION.LOG_PIPELINE_RUN_END(
    p_run_id INT,
    p_status VARCHAR,
    p_rows_affected NUMBER DEFAULT NULL,
    p_rows_inserted NUMBER DEFAULT NULL,
    p_rows_updated NUMBER DEFAULT NULL,
    p_rows_deleted NUMBER DEFAULT NULL,
    p_error_code VARCHAR DEFAULT NULL,
    p_error_message VARCHAR DEFAULT NULL,
    p_error_detail VARCHAR DEFAULT NULL
)
```

#### LOG_PIPELINE_ERROR()
Logs detailed error information
```sql
CALL SAAS_ANALYTICS.ORCHESTRATION.LOG_PIPELINE_ERROR(
    p_run_id INT,
    p_error_code VARCHAR,
    p_error_message VARCHAR,
    p_error_detail VARCHAR,
    p_recovery_status VARCHAR DEFAULT 'PENDING_RETRY'
)
```

#### Utility Functions
- **GET_LAST_RUN_STATUS()**: Check last execution status of a procedure
- **GET_PROCEDURE_HEALTH()**: Get 24-hour health metrics for a procedure

---

### 3. **Enhanced Procedures with Error Handling**

#### LOAD_BRONZE_DATA
**Location**: [sql/02_bronze/02_procedure_load_bronze_data.sql](sql/02_bronze/02_procedure_load_bronze_data.sql)

**Enhancements**:
✅ Comprehensive TRY-CATCH error handling  
✅ Validates stage accessibility before loading  
✅ Tracks row counts before/after COPY operation  
✅ Logs execution start and completion with timing  
✅ Returns detailed execution metrics (status, rows_loaded, duration, run_id, error details)  
✅ Data validation: Ensures data was actually loaded  

**Return Values**:
```sql
status              -- SUCCESS or FAILED
rows_loaded         -- Number of rows loaded
duration_seconds    -- Execution time
run_id             -- Tracking ID
error_code         -- Error code if failed
error_message      -- Human-readable error message
```

#### TRANSFORM_BRONZE_TO_SILVER
**Location**: [sql/03_silver/02_procedure_bronze_to_silver.sql](sql/03_silver/02_procedure_bronze_to_silver.sql)

**Enhancements**:
✅ Full error handling around MERGE operation  
✅ Pre-transformation validation (checks for source data)  
✅ Handles edge cases (empty stream gracefully)  
✅ Tracks inserted/updated row counts  
✅ Detailed error logging with recovery information  
✅ Returns comprehensive execution metrics  

**Return Values**:
```sql
ROWS_INSERTED       -- Number of new rows inserted
ROWS_UPDATED        -- Number of rows updated
STATUS              -- SUCCESS or FAILED
DURATION_SECONDS    -- Execution time
RUN_ID             -- Tracking ID
ERROR_CODE         -- Error code if failed
ERROR_MESSAGE      -- Error details
```

---

## How to Use

### 1. **Automatic Logging (Integrated in Procedures)**
The enhanced procedures automatically log execution:

```sql
-- Simply call the procedure - logging happens automatically
CALL SAAS_ANALYTICS.BRONZE.LOAD_BRONZE_DATA();

-- Returns execution details including run_id
SELECT * FROM TABLE(RESULT_SCAN(LAST_QUERY_ID()));
```

### 2. **Query Execution History**
Find all procedure executions:

```sql
-- Get all executions of LOAD_BRONZE_DATA
SELECT * 
FROM SAAS_ANALYTICS.ORCHESTRATION.PIPELINE_RUN_HISTORY
WHERE procedure_name = 'LOAD_BRONZE_DATA'
ORDER BY start_time DESC;

-- Get failed executions
SELECT * 
FROM SAAS_ANALYTICS.ORCHESTRATION.PIPELINE_RUN_HISTORY
WHERE status = 'FAILED'
ORDER BY start_time DESC;

-- Get execution duration statistics
SELECT 
    procedure_name,
    COUNT(*) as total_runs,
    AVG(duration_seconds) as avg_duration,
    MAX(duration_seconds) as max_duration,
    MIN(duration_seconds) as min_duration
FROM SAAS_ANALYTICS.ORCHESTRATION.PIPELINE_RUN_HISTORY
WHERE start_time >= DATEADD(DAY, -7, CURRENT_DATE())
GROUP BY procedure_name;
```

### 3. **Check Procedure Health**
Use the utility function to get health metrics:

```sql
-- Get health metrics for a procedure (last 24 hours)
SELECT * 
FROM TABLE(SAAS_ANALYTICS.ORCHESTRATION.GET_PROCEDURE_HEALTH('LOAD_BRONZE_DATA', 24));

-- Output shows:
-- procedure_name, total_runs, successful_runs, failed_runs
-- success_rate, avg_duration_seconds, last_run_time, last_run_status
```

### 4. **Get Last Execution Status**
Quickly check the last run:

```sql
SELECT * 
FROM TABLE(SAAS_ANALYTICS.ORCHESTRATION.GET_LAST_RUN_STATUS('LOAD_BRONZE_DATA'));
```

### 5. **Create Alerts**
Set up SLA alerts manually:

```sql
INSERT INTO SAAS_ANALYTICS.ORCHESTRATION.ALERT_TRIGGERS (
    procedure_name, alert_type, threshold_value, comparison_operator, severity
) VALUES (
    'LOAD_BRONZE_DATA', 'EXECUTION_TIME', 300, 'GT', 'WARNING'
);
-- Alerts if execution takes > 300 seconds
```

---

## Error Handling Flow

### Scenario 1: Successful Execution
```
1. LOG_PIPELINE_RUN_START() called -> run_id = 123
2. Data validation passes ✓
3. COPY/MERGE operation succeeds ✓
4. LOG_PIPELINE_RUN_END(123, 'SUCCESS', rows_affected=1000, ...)
5. Return status='SUCCESS' with run_id=123
```

### Scenario 2: Validation Failure
```
1. LOG_PIPELINE_RUN_START() called -> run_id = 124
2. Data validation fails ✗ (e.g., stage not accessible)
3. TRY-CATCH captures error
4. LOG_PIPELINE_RUN_END(124, 'FAILED', error_code='STAGE_ACCESS_ERROR', ...)
5. Return status='FAILED' with error details
```

### Scenario 3: Merge/Load Failure
```
1. LOG_PIPELINE_RUN_START() called -> run_id = 125
2. MERGE operation throws exception
3. EXCEPTION block captures: SQLCODE, SQLERRM, context
4. LOG_PIPELINE_RUN_END(125, 'FAILED', error_code=SQLCODE, error_message=SQLERRM, ...)
5. Return status='FAILED' with full error context
```

---

## Integration with Tasks

When using with Snowflake Tasks, the logging will track:
- Task execution start
- Procedure execution time
- Success/failure status
- Error details for failed tasks

Example task definition:
```sql
CREATE OR REPLACE TASK bronze_load_task
WAREHOUSE = COMPUTE_WH
SCHEDULE = 'USING CRON 0 */4 * * * UTC'
AS
CALL SAAS_ANALYTICS.BRONZE.LOAD_BRONZE_DATA();
-- This automatically logs in PIPELINE_RUN_HISTORY
```

---

## Monitoring Queries

### Daily Pipeline Status Report
```sql
SELECT 
    DATE(start_time) as execution_date,
    procedure_name,
    COUNT(*) as total_runs,
    COUNT(CASE WHEN status='SUCCESS' THEN 1 END) as successful,
    COUNT(CASE WHEN status='FAILED' THEN 1 END) as failed,
    ROUND(100.0 * COUNT(CASE WHEN status='SUCCESS' THEN 1 END) / COUNT(*), 2) as success_rate,
    ROUND(AVG(duration_seconds), 2) as avg_duration_sec
FROM SAAS_ANALYTICS.ORCHESTRATION.PIPELINE_RUN_HISTORY
WHERE start_time >= DATEADD(DAY, -1, CURRENT_DATE())
GROUP BY DATE(start_time), procedure_name
ORDER BY execution_date DESC, procedure_name;
```

### Recent Errors
```sql
SELECT 
    run_id,
    procedure_name,
    start_time,
    error_code,
    error_message,
    error_detail
FROM SAAS_ANALYTICS.ORCHESTRATION.PIPELINE_RUN_HISTORY
WHERE status = 'FAILED'
AND start_time >= DATEADD(HOUR, -24, CURRENT_TIMESTAMP())
ORDER BY start_time DESC;
```

### Slow Procedures (SLA Violations)
```sql
SELECT 
    procedure_name,
    MAX(duration_seconds) as max_duration,
    AVG(duration_seconds) as avg_duration,
    COUNT(CASE WHEN duration_seconds > 600 THEN 1 END) as slow_runs
FROM SAAS_ANALYTICS.ORCHESTRATION.PIPELINE_RUN_HISTORY
WHERE start_time >= DATEADD(DAY, -7, CURRENT_DATE())
GROUP BY procedure_name
HAVING MAX(duration_seconds) > 600
ORDER BY max_duration DESC;
```

---

## Deployment

The new components are automatically included in the deployment order:

```python
# In deployment/deploy.py, the sequence is:
1. Create database setup
2. Create orchestration logging tables       ← NEW
3. Create other schemas
4. Create procedures (with logging integrated)
5. Create logging procedures                 ← NEW
```

To deploy:
```bash
python deployment/deploy.py dev
# or
bash deployment/deploy.sh dev
```

---

## Future Enhancements

Recommended additions:
1. **External event notifications**: Send alerts to Slack/Email on failures
2. **Retry logic**: Automatic retry for timed-out procedures
3. **Metrics aggregation**: Hourly/daily summaries to separate analytics tables
4. **Cost tracking**: Log compute credits consumed per procedure
5. **Data lineage**: Column-level tracking of data transformations
6. **Dead letter queue**: Capture and store failed records for review

---

## FAQ

**Q: How much storage does logging consume?**  
A: Each procedure execution creates ~1KB record. With hourly executions, expect ~8.7MB/year per procedure.

**Q: Can I archive old logs?**  
A: Yes, you can create a retention policy:
```sql
DELETE FROM SAAS_ANALYTICS.ORCHESTRATION.PIPELINE_RUN_HISTORY
WHERE start_time < DATEADD(MONTH, -3, CURRENT_TIMESTAMP());
```

**Q: How do I create alerts?**  
A: Use the ALERT_TRIGGERS table and integrate with Snowflake Tasks/External Functions for notifications.

**Q: What if a procedure fails - can it auto-retry?**  
A: Not automatically, but you can enhance the procedures with retry logic using WHILE loops.

---

## Support
For issues or questions about the logging system, check the PIPELINE_RUN_HISTORY table for execution details and error messages.
