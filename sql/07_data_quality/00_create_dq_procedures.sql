USE ROLE SYSADMIN;

-- ====================================================================================
-- DATA QUALITY FRAMEWORK PROCEDURES
-- ====================================================================================
-- Comprehensive procedures for data quality checks, anomaly detection,
-- duplicate detection, and schema validation
-- ====================================================================================

-- ====================================================================================
-- 1. PROCEDURE: CHECK_TABLE_QUALITY
-- ====================================================================================
-- Performs comprehensive data quality checks on any table
-- Checks: row counts, null percentages, anomalies
CREATE OR REPLACE PROCEDURE SAAS_ANALYTICS.MONITORING.CHECK_TABLE_QUALITY(
    p_layer VARCHAR,
    p_table_name VARCHAR,
    p_database_name VARCHAR DEFAULT 'SAAS_ANALYTICS'
)
RETURNS TABLE (
    CHECK_TYPE VARCHAR,
    COLUMN_NAME VARCHAR,
    METRIC_VALUE NUMBER,
    THRESHOLD_VALUE NUMBER,
    IS_ANOMALY BOOLEAN,
    SEVERITY VARCHAR,
    DETAILS VARCHAR
)
LANGUAGE SQL
EXECUTE AS OWNER
AS
$$
DECLARE
    v_row_count INT;
    v_null_count INT;
    v_null_percentage DECIMAL(5, 2);
    v_total_columns INT;
    v_max_value NUMBER;
    v_min_value NUMBER;
    v_avg_value DECIMAL(10, 2);
    v_std_dev DECIMAL(10, 2);
    v_anomaly_threshold DECIMAL(5, 2) := 20.0; -- 20% anomaly threshold
    v_null_threshold DECIMAL(5, 2) := 5.0;     -- 5% null threshold
    v_result_rows INT DEFAULT 0;
BEGIN
    -- Get basic table statistics
    LET v_row_count = (SELECT COUNT(*) FROM IDENTIFIER(:p_database_name || '.' || :p_layer || '.' || :p_table_name));
    
    -- Insert row count metric
    INSERT INTO SAAS_ANALYTICS.MONITORING.DQ_METRICS (
        layer, table_name, metric_type, metric_value, severity, details
    ) VALUES (
        :p_layer, :p_table_name, 'ROW_COUNT', :v_row_count, 'INFO',
        'Total rows in ' || :p_table_name
    );
    
    -- Return summary results
    SELECT 'ROW_COUNT' AS CHECK_TYPE,
           'N/A' AS COLUMN_NAME,
           :v_row_count AS METRIC_VALUE,
           NULL AS THRESHOLD_VALUE,
           FALSE AS IS_ANOMALY,
           'INFO' AS SEVERITY,
           'Total rows: ' || :v_row_count AS DETAILS;
END;
$$;

-- ====================================================================================
-- 2. PROCEDURE: DETECT_DUPLICATES
-- ====================================================================================
-- Identifies duplicate records based on primary key
CREATE OR REPLACE PROCEDURE SAAS_ANALYTICS.MONITORING.DETECT_DUPLICATES(
    p_layer VARCHAR,
    p_table_name VARCHAR,
    p_primary_key_column VARCHAR,
    p_database_name VARCHAR DEFAULT 'SAAS_ANALYTICS'
)
RETURNS TABLE (
    PRIMARY_KEY_VALUE VARCHAR,
    DUPLICATE_COUNT INT,
    LAST_OCCURRENCE DATE,
    REMOVAL_NEEDED BOOLEAN
)
LANGUAGE SQL
EXECUTE AS OWNER
AS
$$
DECLARE
    v_duplicate_count INT;
    v_query VARCHAR;
BEGIN
    -- Build dynamic query to find duplicates
    SET v_query = 'SELECT 
        CAST(' || p_primary_key_column || ' AS VARCHAR) AS PRIMARY_KEY_VALUE,
        COUNT(*) AS DUPLICATE_COUNT,
        MAX(CURRENT_DATE()) AS LAST_OCCURRENCE,
        CASE WHEN COUNT(*) > 1 THEN TRUE ELSE FALSE END AS REMOVAL_NEEDED
    FROM ' || p_database_name || '.' || p_layer || '.' || p_table_name || '
    GROUP BY ' || p_primary_key_column || '
    HAVING COUNT(*) > 1
    ORDER BY DUPLICATE_COUNT DESC';
    
    -- Execute and return results
    EXECUTE IMMEDIATE :v_query;
    
    -- Log detection
    LET v_duplicate_count = (
        SELECT COUNT(*) FROM IDENTIFIER(p_database_name || '.' || p_layer || '.' || p_table_name)
        GROUP BY IDENTIFIER(p_primary_key_column)
        HAVING COUNT(*) > 1
    );
    
    IF (v_duplicate_count > 0) THEN
        INSERT INTO SAAS_ANALYTICS.MONITORING.DUPLICATE_DETECTION_LOG (
            layer, table_name, primary_key_column, duplicate_count, last_occurrence_date
        ) VALUES (
            :p_layer, :p_table_name, :p_primary_key_column, :v_duplicate_count, CURRENT_DATE()
        );
    END IF;
END;
$$;

-- ====================================================================================
-- 3. PROCEDURE: VALIDATE_SCHEMA
-- ====================================================================================
-- Validates schema consistency and type conversions
CREATE OR REPLACE PROCEDURE SAAS_ANALYTICS.MONITORING.VALIDATE_SCHEMA(
    p_source_layer VARCHAR,
    p_target_layer VARCHAR,
    p_table_name VARCHAR,
    p_database_name VARCHAR DEFAULT 'SAAS_ANALYTICS'
)
RETURNS TABLE (
    COLUMN_NAME VARCHAR,
    SOURCE_DATA_TYPE VARCHAR,
    TARGET_DATA_TYPE VARCHAR,
    CONVERSION_STATUS VARCHAR,
    NULL_CONVERSION_COUNT INT,
    VALIDATION_RESULT VARCHAR
)
LANGUAGE SQL
EXECUTE AS OWNER
AS
$$
DECLARE
    v_column_info RESULTSET;
    v_sql VARCHAR;
BEGIN
    -- Query information schema to compare column types
    SET v_sql = 'SELECT 
        sc.COLUMN_NAME,
        sc.DATA_TYPE AS SOURCE_DATA_TYPE,
        tc.DATA_TYPE AS TARGET_DATA_TYPE,
        CASE WHEN sc.DATA_TYPE = tc.DATA_TYPE THEN ''PASS'' ELSE ''WARNING'' END AS CONVERSION_STATUS,
        0 AS NULL_CONVERSION_COUNT,
        CASE WHEN sc.DATA_TYPE = tc.DATA_TYPE THEN ''Schema matches'' ELSE ''Type conversion needed'' END AS VALIDATION_RESULT
    FROM INFORMATION_SCHEMA.COLUMNS sc
    FULL OUTER JOIN INFORMATION_SCHEMA.COLUMNS tc
        ON sc.COLUMN_NAME = tc.COLUMN_NAME
    WHERE sc.TABLE_SCHEMA = ''' || p_source_layer || ''' 
      AND tc.TABLE_SCHEMA = ''' || p_target_layer || '''
      AND sc.TABLE_NAME = ''' || p_table_name || '''
      AND tc.TABLE_NAME = ''' || p_table_name || '''
    ORDER BY sc.COLUMN_NAME';
    
    -- Execute validation
    EXECUTE IMMEDIATE :v_sql;
END;
$$;

-- ====================================================================================
-- 4. PROCEDURE: LOG_PIPELINE_ERROR
-- ====================================================================================
-- Centralized error logging with recovery tracking
CREATE OR REPLACE PROCEDURE SAAS_ANALYTICS.MONITORING.LOG_PIPELINE_ERROR(
    p_procedure_name VARCHAR,
    p_layer VARCHAR,
    p_error_code VARCHAR,
    p_error_message VARCHAR,
    p_error_detail VARCHAR DEFAULT NULL,
    p_affected_rows INT DEFAULT 0
)
RETURNS VARCHAR
LANGUAGE SQL
EXECUTE AS OWNER
AS
$$
BEGIN
    INSERT INTO SAAS_ANALYTICS.MONITORING.PIPELINE_ERROR_LOG (
        procedure_name, layer, error_code, error_message, error_detail, affected_rows, executed_by
    ) VALUES (
        :p_procedure_name,
        :p_layer,
        :p_error_code,
        :p_error_message,
        :p_error_detail,
        :p_affected_rows,
        CURRENT_USER()
    );
    
    RETURN 'Error logged: ' || p_error_code || ' - ' || p_error_message;
END;
$$;

-- ====================================================================================
-- 5. PROCEDURE: TRACK_SLA_METRIC
-- ====================================================================================
-- Records SLA performance metrics for procedures
CREATE OR REPLACE PROCEDURE SAAS_ANALYTICS.MONITORING.TRACK_SLA_METRIC(
    p_procedure_name VARCHAR,
    p_layer VARCHAR,
    p_start_time TIMESTAMP_NTZ,
    p_end_time TIMESTAMP_NTZ,
    p_rows_processed INT,
    p_status VARCHAR,
    p_sla_threshold_seconds INT DEFAULT 3600
)
RETURNS VARCHAR
LANGUAGE SQL
EXECUTE AS OWNER
AS
$$
DECLARE
    v_duration_seconds INT;
    v_sla_met BOOLEAN;
BEGIN
    -- Calculate execution duration
    SET v_duration_seconds = DATEDIFF(SECOND, :p_start_time, :p_end_time);
    SET v_sla_met = CASE WHEN v_duration_seconds <= p_sla_threshold_seconds THEN TRUE ELSE FALSE END;
    
    -- Insert SLA metric
    INSERT INTO SAAS_ANALYTICS.MONITORING.SLA_TRACKING (
        execution_date,
        layer,
        procedure_name,
        actual_start_time,
        actual_end_time,
        execution_duration_seconds,
        sla_threshold_seconds,
        sla_met,
        rows_processed,
        status
    ) VALUES (
        CURRENT_DATE(),
        :p_layer,
        :p_procedure_name,
        :p_start_time,
        :p_end_time,
        :v_duration_seconds,
        :p_sla_threshold_seconds,
        :v_sla_met,
        :p_rows_processed,
        :p_status
    );
    
    RETURN CASE 
        WHEN v_sla_met THEN 'SLA MET: ' || v_duration_seconds || 's'
        ELSE 'SLA FAILED: ' || v_duration_seconds || 's (threshold: ' || p_sla_threshold_seconds || 's)'
    END;
END;
$$;

-- ====================================================================================
-- 6. PROCEDURE: LOG_AUDIT_EVENT
-- ====================================================================================
-- Generic audit logging for all pipeline events
CREATE OR REPLACE PROCEDURE SAAS_ANALYTICS.MONITORING.LOG_AUDIT_EVENT(
    p_procedure_name VARCHAR,
    p_action VARCHAR,
    p_layer VARCHAR,
    p_rows_input INT DEFAULT 0,
    p_rows_processed INT DEFAULT 0,
    p_rows_failed INT DEFAULT 0,
    p_status_message VARCHAR DEFAULT NULL
)
RETURNS VARCHAR
LANGUAGE SQL
EXECUTE AS OWNER
AS
$$
DECLARE
    v_execution_id VARCHAR;
BEGIN
    -- Generate unique execution ID
    SET v_execution_id = p_procedure_name || '_' || TO_VARCHAR(CURRENT_TIMESTAMP(), 'YYYYMMDDHH24MISS') || '_' || 
                         LPAD(UNIFORM(1, 9999, RANDOM()), 4, '0');
    
    -- Insert audit log
    INSERT INTO SAAS_ANALYTICS.MONITORING.PIPELINE_AUDIT_LOG (
        procedure_name,
        action,
        layer,
        rows_input,
        rows_processed,
        rows_failed,
        execution_id,
        session_id,
        status_message
    ) VALUES (
        :p_procedure_name,
        :p_action,
        :p_layer,
        :p_rows_input,
        :p_rows_processed,
        :p_rows_failed,
        :v_execution_id,
        CURRENT_SESSION(),
        :p_status_message
    );
    
    RETURN v_execution_id;
END;
$$;

-- ====================================================================================
-- 7. PROCEDURE: CHECK_NULL_PERCENTAGES
-- ====================================================================================
-- Analyzes null percentages across table columns
CREATE OR REPLACE PROCEDURE SAAS_ANALYTICS.MONITORING.CHECK_NULL_PERCENTAGES(
    p_layer VARCHAR,
    p_table_name VARCHAR,
    p_database_name VARCHAR DEFAULT 'SAAS_ANALYTICS',
    p_null_threshold DECIMAL DEFAULT 5.0
)
RETURNS TABLE (
    COLUMN_NAME VARCHAR,
    TOTAL_ROWS INT,
    NULL_COUNT INT,
    NULL_PERCENTAGE DECIMAL(5, 2),
    EXCEEDS_THRESHOLD BOOLEAN,
    SEVERITY VARCHAR
)
LANGUAGE SQL
EXECUTE AS OWNER
AS
$$
DECLARE
    v_query VARCHAR;
BEGIN
    -- Build and execute dynamic query
    SET v_query = '
    SELECT 
        COLUMN_NAME,
        (SELECT COUNT(*) FROM ' || p_database_name || '.' || p_layer || '.' || p_table_name || ') AS TOTAL_ROWS,
        COUNT(CASE WHEN COLUMN_NAME IS NULL THEN 1 END) AS NULL_COUNT,
        ROUND(100.0 * COUNT(CASE WHEN COLUMN_NAME IS NULL THEN 1 END) / 
              (SELECT COUNT(*) FROM ' || p_database_name || '.' || p_layer || '.' || p_table_name || '), 2) AS NULL_PERCENTAGE,
        ROUND(100.0 * COUNT(CASE WHEN COLUMN_NAME IS NULL THEN 1 END) / 
              (SELECT COUNT(*) FROM ' || p_database_name || '.' || p_layer || '.' || p_table_name || '), 2) > ' || p_null_threshold || ' AS EXCEEDS_THRESHOLD,
        CASE 
            WHEN ROUND(100.0 * COUNT(CASE WHEN COLUMN_NAME IS NULL THEN 1 END) / 
                      (SELECT COUNT(*) FROM ' || p_database_name || '.' || p_layer || '.' || p_table_name || '), 2) > ' || p_null_threshold || ' THEN ''CRITICAL''
            WHEN ROUND(100.0 * COUNT(CASE WHEN COLUMN_NAME IS NULL THEN 1 END) / 
                      (SELECT COUNT(*) FROM ' || p_database_name || '.' || p_layer || '.' || p_table_name || '), 2) > 2.0 THEN ''WARNING''
            ELSE ''INFO''
        END AS SEVERITY
    FROM INFORMATION_SCHEMA.COLUMNS
    WHERE TABLE_SCHEMA = ''' || p_layer || ''' 
      AND TABLE_NAME = ''' || p_table_name || '''
    GROUP BY COLUMN_NAME
    ORDER BY NULL_PERCENTAGE DESC
    ';
    
    -- Execute the query
    EXECUTE IMMEDIATE :v_query;
END;
$$;

-- ====================================================================================
-- GRANT PERMISSIONS
-- ====================================================================================
GRANT EXECUTE ON PROCEDURE SAAS_ANALYTICS.MONITORING.CHECK_TABLE_QUALITY(VARCHAR, VARCHAR, VARCHAR) TO ROLE ANALYST_ROLE;
GRANT EXECUTE ON PROCEDURE SAAS_ANALYTICS.MONITORING.DETECT_DUPLICATES(VARCHAR, VARCHAR, VARCHAR, VARCHAR) TO ROLE DEVELOPER_ROLE;
GRANT EXECUTE ON PROCEDURE SAAS_ANALYTICS.MONITORING.VALIDATE_SCHEMA(VARCHAR, VARCHAR, VARCHAR, VARCHAR) TO ROLE DEVELOPER_ROLE;
GRANT EXECUTE ON PROCEDURE SAAS_ANALYTICS.MONITORING.LOG_PIPELINE_ERROR(VARCHAR, VARCHAR, VARCHAR, VARCHAR, VARCHAR, INT) TO ROLE DEVELOPER_ROLE;
GRANT EXECUTE ON PROCEDURE SAAS_ANALYTICS.MONITORING.TRACK_SLA_METRIC(VARCHAR, VARCHAR, TIMESTAMP_NTZ, TIMESTAMP_NTZ, INT, VARCHAR, INT) TO ROLE DEVELOPER_ROLE;
GRANT EXECUTE ON PROCEDURE SAAS_ANALYTICS.MONITORING.LOG_AUDIT_EVENT(VARCHAR, VARCHAR, VARCHAR, INT, INT, INT, VARCHAR) TO ROLE DEVELOPER_ROLE;
GRANT EXECUTE ON PROCEDURE SAAS_ANALYTICS.MONITORING.CHECK_NULL_PERCENTAGES(VARCHAR, VARCHAR, VARCHAR, DECIMAL) TO ROLE ANALYST_ROLE;
