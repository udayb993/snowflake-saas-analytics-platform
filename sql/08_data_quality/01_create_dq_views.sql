USE ROLE SYSADMIN;

-- ====================================================================================
-- DATA QUALITY MONITORING VIEWS & DASHBOARDS
-- ====================================================================================
-- Comprehensive views for monitoring, alerting, and SLA tracking
-- ====================================================================================

-- ====================================================================================
-- 1. VIEW: DQ_SUMMARY_TODAY
-- ====================================================================================
-- Today's data quality metrics summary
CREATE OR REPLACE VIEW SAAS_ANALYTICS.MONITORING.DQ_SUMMARY_TODAY AS
SELECT 
    layer,
    table_name,
    metric_type,
    COUNT(*) AS total_checks,
    COUNTIF(is_anomaly) AS anomaly_count,
    COUNT_IF(severity = 'CRITICAL') AS critical_issues,
    MAX(check_timestamp) AS last_check_time
FROM SAAS_ANALYTICS.MONITORING.DQ_METRICS
WHERE DATE(check_timestamp) = CURRENT_DATE()
GROUP BY layer, table_name, metric_type
ORDER BY critical_issues DESC, last_check_time DESC;

-- ====================================================================================
-- 2. VIEW: ACTIVE_PIPELINE_ERRORS
-- ====================================================================================
-- Active/unresolved errors from the last 24 hours
CREATE OR REPLACE VIEW SAAS_ANALYTICS.MONITORING.ACTIVE_PIPELINE_ERRORS AS
SELECT 
    error_id,
    error_timestamp,
    procedure_name,
    layer,
    error_code,
    error_message,
    affected_rows,
    recovery_status,
    recovery_details,
    DATEDIFF(MINUTE, error_timestamp, CURRENT_TIMESTAMP()) AS minutes_since_error,
    CASE 
        WHEN recovery_status = 'SUCCESS' THEN 'RESOLVED'
        WHEN recovery_status = 'FAILED' THEN 'UNRESOLVED'
        ELSE 'PENDING'
    END AS resolution_status
FROM SAAS_ANALYTICS.MONITORING.PIPELINE_ERROR_LOG
WHERE DATE(error_timestamp) = CURRENT_DATE()
  AND recovery_status != 'SUCCESS'
ORDER BY error_timestamp DESC;

-- ====================================================================================
-- 3. VIEW: SLA_COMPLIANCE_REPORT
-- ====================================================================================
-- SLA compliance tracking and alerts
CREATE OR REPLACE VIEW SAAS_ANALYTICS.MONITORING.SLA_COMPLIANCE_REPORT AS
SELECT 
    execution_date,
    layer,
    procedure_name,
    COUNT(*) AS total_executions,
    COUNTIF(sla_met) AS sla_met_count,
    ROUND(100.0 * COUNTIF(sla_met) / COUNT(*), 2) AS sla_compliance_percentage,
    AVG(execution_duration_seconds) AS avg_duration_seconds,
    MAX(execution_duration_seconds) AS max_duration_seconds,
    MIN(execution_duration_seconds) AS min_duration_seconds,
    COUNTIF(status = 'FAILED') AS failed_executions,
    SUM(rows_processed) AS total_rows_processed,
    SUM(credits_used) AS total_credits_used,
    CASE 
        WHEN ROUND(100.0 * COUNTIF(sla_met) / COUNT(*), 2) >= 95.0 THEN 'PASS'
        WHEN ROUND(100.0 * COUNTIF(sla_met) / COUNT(*), 2) >= 90.0 THEN 'WARNING'
        ELSE 'CRITICAL'
    END AS sla_status
FROM SAAS_ANALYTICS.MONITORING.SLA_TRACKING
GROUP BY execution_date, layer, procedure_name
ORDER BY sla_compliance_percentage ASC, execution_date DESC;

-- ====================================================================================
-- 4. VIEW: DUPLICATE_SUMMARY
-- ====================================================================================
-- Summary of all detected duplicates
CREATE OR REPLACE VIEW SAAS_ANALYTICS.MONITORING.DUPLICATE_SUMMARY AS
SELECT 
    layer,
    table_name,
    primary_key_column,
    COUNT(*) AS detection_count,
    MAX(detection_timestamp) AS last_detection,
    SUM(duplicate_count) AS total_duplicates,
    COUNTIF(duplicate_removal_status = 'SUCCESS') AS resolved_count,
    COUNTIF(duplicate_removal_status IS NULL) AS pending_removal,
    CASE 
        WHEN COUNTIF(duplicate_removal_status IS NULL) > 0 THEN 'REQUIRES_ACTION'
        WHEN COUNTIF(duplicate_removal_status = 'FAILED') > 0 THEN 'FAILED_ACTION'
        ELSE 'RESOLVED'
    END AS status
FROM SAAS_ANALYTICS.MONITORING.DUPLICATE_DETECTION_LOG
GROUP BY layer, table_name, primary_key_column
ORDER BY pending_removal DESC, total_duplicates DESC;

-- ====================================================================================
-- 5. VIEW: SCHEMA_VALIDATION_SUMMARY
-- ====================================================================================
-- Schema consistency and type conversion overview
CREATE OR REPLACE VIEW SAAS_ANALYTICS.MONITORING.SCHEMA_VALIDATION_SUMMARY AS
SELECT 
    layer,
    table_name,
    COUNT(*) AS total_columns,
    COUNTIF(validation_status = 'PASS') AS passed_validations,
    COUNTIF(validation_status = 'FAIL') AS failed_validations,
    COUNTIF(validation_status = 'WARNING') AS warning_validations,
    SUM(conversion_failure_count) AS total_conversion_failures,
    SUM(null_conversion_count) AS total_null_conversions,
    CASE 
        WHEN COUNTIF(validation_status = 'FAIL') > 0 THEN 'CRITICAL'
        WHEN COUNTIF(validation_status = 'WARNING') > 0 THEN 'WARNING'
        ELSE 'PASS'
    END AS overall_status
FROM SAAS_ANALYTICS.MONITORING.SCHEMA_VALIDATION_LOG
GROUP BY layer, table_name
ORDER BY failed_validations DESC, warning_validations DESC;

-- ====================================================================================
-- 6. VIEW: PIPELINE_PERFORMANCE
-- ====================================================================================
-- Pipeline performance metrics and throughput analysis
CREATE OR REPLACE VIEW SAAS_ANALYTICS.MONITORING.PIPELINE_PERFORMANCE AS
SELECT 
    procedure_name,
    layer,
    DATE(audit_timestamp) AS execution_date,
    COUNT(*) AS execution_count,
    SUM(rows_input) AS total_rows_input,
    SUM(rows_processed) AS total_rows_processed,
    SUM(rows_failed) AS total_rows_failed,
    CASE 
        WHEN SUM(rows_input) > 0 
        THEN ROUND(100.0 * SUM(rows_processed) / SUM(rows_input), 2)
        ELSE 0
    END AS success_rate_percentage,
    COUNT_IF(action = 'SUCCESS') AS successful_runs,
    COUNT_IF(action = 'FAILURE') AS failed_runs,
    AVG(DATEDIFF(SECOND, audit_timestamp, CURRENT_TIMESTAMP())) AS avg_age_seconds
FROM SAAS_ANALYTICS.MONITORING.PIPELINE_AUDIT_LOG
WHERE DATE(audit_timestamp) >= DATEADD(DAY, -30, CURRENT_DATE())
GROUP BY procedure_name, layer, DATE(audit_timestamp)
ORDER BY execution_date DESC, procedure_name;

-- ====================================================================================
-- 7. VIEW: DATA_QUALITY_SCORECARD
-- ====================================================================================
-- Overall data quality health scorecard
CREATE OR REPLACE VIEW SAAS_ANALYTICS.MONITORING.DATA_QUALITY_SCORECARD AS
SELECT 
    CURRENT_DATE() AS scorecard_date,
    CURRENT_TIMESTAMP() AS generated_at,
    (SELECT COUNT(DISTINCT layer) FROM SAAS_ANALYTICS.MONITORING.DQ_METRICS WHERE DATE(check_timestamp) = CURRENT_DATE()) AS layers_monitored,
    (SELECT COUNT(DISTINCT table_name) FROM SAAS_ANALYTICS.MONITORING.DQ_METRICS WHERE DATE(check_timestamp) = CURRENT_DATE()) AS tables_checked,
    (SELECT COUNT(*) FROM SAAS_ANALYTICS.MONITORING.PIPELINE_ERROR_LOG WHERE DATE(error_timestamp) = CURRENT_DATE() AND recovery_status != 'SUCCESS') AS active_errors,
    (SELECT COUNT(*) FROM SAAS_ANALYTICS.MONITORING.DUPLICATE_DETECTION_LOG WHERE DATE(detection_timestamp) = CURRENT_DATE()) AS duplicates_detected,
    (SELECT ROUND(AVG(CASE WHEN sla_met THEN 1 ELSE 0 END) * 100, 2) FROM SAAS_ANALYTICS.MONITORING.SLA_TRACKING WHERE DATE(execution_date) = CURRENT_DATE()) AS daily_sla_compliance,
    (SELECT COUNT(*) FROM SAAS_ANALYTICS.MONITORING.SCHEMA_VALIDATION_LOG WHERE DATE(validation_timestamp) = CURRENT_DATE() AND validation_status = 'FAIL') AS schema_failures,
    CASE 
        WHEN (SELECT COUNT(*) FROM SAAS_ANALYTICS.MONITORING.PIPELINE_ERROR_LOG WHERE DATE(error_timestamp) = CURRENT_DATE() AND recovery_status != 'SUCCESS') = 0
         AND (SELECT ROUND(AVG(CASE WHEN sla_met THEN 1 ELSE 0 END) * 100, 2) FROM SAAS_ANALYTICS.MONITORING.SLA_TRACKING WHERE DATE(execution_date) = CURRENT_DATE()) >= 95
         AND (SELECT COUNT(*) FROM SAAS_ANALYTICS.MONITORING.SCHEMA_VALIDATION_LOG WHERE DATE(validation_timestamp) = CURRENT_DATE() AND validation_status = 'FAIL') = 0
        THEN 'HEALTHY'
        WHEN (SELECT COUNT(*) FROM SAAS_ANALYTICS.MONITORING.PIPELINE_ERROR_LOG WHERE DATE(error_timestamp) = CURRENT_DATE() AND recovery_status != 'SUCCESS') <= 2
         AND (SELECT ROUND(AVG(CASE WHEN sla_met THEN 1 ELSE 0 END) * 100, 2) FROM SAAS_ANALYTICS.MONITORING.SLA_TRACKING WHERE DATE(execution_date) = CURRENT_DATE()) >= 90
        THEN 'DEGRADED'
        ELSE 'CRITICAL'
    END AS overall_health_status;

-- ====================================================================================
-- 8. VIEW: ANOMALY_DETECTION_ALERTS
-- ====================================================================================
-- Alerts for detected anomalies
CREATE OR REPLACE VIEW SAAS_ANALYTICS.MONITORING.ANOMALY_DETECTION_ALERTS AS
SELECT 
    metric_id,
    check_timestamp,
    layer,
    table_name,
    metric_type,
    column_name,
    metric_value,
    threshold_expected,
    severity,
    details,
    DATEDIFF(MINUTE, check_timestamp, CURRENT_TIMESTAMP()) AS minutes_since_detection
FROM SAAS_ANALYTICS.MONITORING.DQ_METRICS
WHERE is_anomaly = TRUE
  AND severity IN ('CRITICAL', 'WARNING')
  AND DATE(check_timestamp) = CURRENT_DATE()
ORDER BY severity DESC, check_timestamp DESC;

-- ====================================================================================
-- GRANT PERMISSIONS ON VIEWS
-- ====================================================================================
GRANT SELECT ON VIEW SAAS_ANALYTICS.MONITORING.DQ_SUMMARY_TODAY TO ROLE ANALYST_ROLE;
GRANT SELECT ON VIEW SAAS_ANALYTICS.MONITORING.ACTIVE_PIPELINE_ERRORS TO ROLE ANALYST_ROLE;
GRANT SELECT ON VIEW SAAS_ANALYTICS.MONITORING.SLA_COMPLIANCE_REPORT TO ROLE ANALYST_ROLE;
GRANT SELECT ON VIEW SAAS_ANALYTICS.MONITORING.DUPLICATE_SUMMARY TO ROLE ANALYST_ROLE;
GRANT SELECT ON VIEW SAAS_ANALYTICS.MONITORING.SCHEMA_VALIDATION_SUMMARY TO ROLE ANALYST_ROLE;
GRANT SELECT ON VIEW SAAS_ANALYTICS.MONITORING.PIPELINE_PERFORMANCE TO ROLE ANALYST_ROLE;
GRANT SELECT ON VIEW SAAS_ANALYTICS.MONITORING.DATA_QUALITY_SCORECARD TO ROLE ANALYST_ROLE;
GRANT SELECT ON VIEW SAAS_ANALYTICS.MONITORING.ANOMALY_DETECTION_ALERTS TO ROLE ANALYST_ROLE;
