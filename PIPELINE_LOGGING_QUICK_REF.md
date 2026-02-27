# Pipeline Logging - Quick Reference

## 🎯 What's New - SIMPLIFIED!

### ✅ 3 Lines of Code Per Procedure
- **Line 1**: `CALL LOG_START('PROCEDURE_NAME')` → get `run_id`
- **Line 2**: `CALL LOG_SUCCESS(run_id)` → log success
- **Line 3**: `CALL LOG_FAILED(run_id, SQLCODE, SQLERRM)` → log error (in EXCEPTION block)

### ✅ One Simple Table
- **PIPELINE_RUN_HISTORY**: procedure name, status (RUNNING/SUCCESS/FAILED), rows, errors, timestamp

### ✅ Three Simple Procedures
- `LOG_START()` - Inserts RUNNING record, returns run_id
- `LOG_SUCCESS()` - Updates record to SUCCESS
- `LOG_FAILED()` - Updates record to FAILED with error details

---

## 📊 Essential Queries

### Check status
```sql
SELECT status, COUNT(*) 
FROM SAAS_ANALYTICS.ORCHESTRATION.PIPELINE_RUN_HISTORY 
GROUP BY status;
```

### See failures
```sql
SELECT procedure_name, created_at, error_message 
FROM SAAS_ANALYTICS.ORCHESTRATION.PIPELINE_RUN_HISTORY 
WHERE status = 'FAILED' 
ORDER BY created_at DESC LIMIT 10;
```

### Success rate
```sql
SELECT 
    procedure_name,
    COUNT(*) as total,
    COUNT(CASE WHEN status='SUCCESS' THEN 1 END) as successful,
    ROUND(100.0 * COUNT(CASE WHEN status='SUCCESS' THEN 1 END) / COUNT(*), 2) as success_pct
FROM SAAS_ANALYTICS.ORCHESTRATION.PIPELINE_RUN_HISTORY
WHERE created_at >= DATEADD(DAY, -1, CURRENT_TIMESTAMP())
GROUP BY procedure_name;
```

---

## 🔧 How to Use

```sql
CREATE OR REPLACE PROCEDURE MY_PROCEDURE()
RETURNS VARCHAR
LANGUAGE SQL
AS
$$
DECLARE
    v_run_id INT;
BEGIN
    -- Line 1: Log start
    CALL SAAS_ANALYTICS.ORCHESTRATION.LOG_START('MY_PROCEDURE') INTO v_run_id;
    
    -- Do work
    INSERT INTO MY_TABLE SELECT * FROM SOURCE;
    
    -- Line 2: Log success
    CALL SAAS_ANALYTICS.ORCHESTRATION.LOG_SUCCESS(v_run_id);
    
    RETURN 'SUCCESS';
    
EXCEPTION
    WHEN STATEMENT_ERROR THEN
        -- Line 3: Log error
        CALL SAAS_ANALYTICS.ORCHESTRATION.LOG_FAILED(v_run_id, SQLCODE, SQLERRM);
        RETURN 'FAILED: ' || SQLERRM;
END;
$$;
```

---

## 🚀 Deploy & Test

```bash
python deployment/deploy.py dev

CALL SAAS_ANALYTICS.BRONZE.LOAD_BRONZE_DATA();

SELECT procedure_name, status, created_at, error_message
FROM SAAS_ANALYTICS.ORCHESTRATION.PIPELINE_RUN_HISTORY 
ORDER BY created_at DESC;
```

---

## 📁 Files Modified

New:
- `sql/00_database_setup/06_create_orchestration_logging_tables.sql`
- `sql/05_orchestration/00_create_logging_procedures.sql`

Updated:
- `sql/02_bronze/02_procedure_load_bronze_data.sql` (added 3 logging calls)
- `sql/03_silver/02_procedure_bronze_to_silver.sql` (added 3 logging calls)

---

## 📊 Table Schema

```
PIPELINE_RUN_HISTORY
├── run_id (auto-increment)
├── procedure_name (VARCHAR)
├── status (RUNNING / SUCCESS / FAILED)
├── rows_affected, rows_inserted, rows_updated (optional)
├── error_code, error_message (optional)
└── created_at (TIMESTAMP - auto set)
```

---

## That's it!

Simple logging. No complexity.
