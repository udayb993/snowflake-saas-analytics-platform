USE ROLE SYSADMIN;

-- ====================================================================================
-- DATA QUALITY & MONITORING INFRASTRUCTURE
-- ====================================================================================
-- Creates tables, views, and utilities for tracking data quality metrics,
-- error logs, SLA tracking, and pipeline performance monitoring
-- ====================================================================================

-- Create MONITORING schema for all data quality and SLA tracking
CREATE SCHEMA IF NOT EXISTS SAAS_ANALYTICS.MONITORING
  COMMENT = 'Data quality metrics, error tracking, and SLA monitoring';

-- ====================================================================================
-- 1. DATA QUALITY METRICS TABLE
-- ====================================================================================
-- Tracks row counts, null percentages, and anomalies per layer/table
CREATE OR REPLACE TABLE SAAS_ANALYTICS.MONITORING.DQ_METRICS (
    metric_id INT AUTOINCREMENT PRIMARY KEY,
    check_timestamp TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP(),
    layer VARCHAR(20) COMMENT 'BRONZE, SILVER, GOLD',
    table_name VARCHAR(100) COMMENT 'e.g., SOCIAL_MEDIA_USERS_RAW',
    metric_type VARCHAR(50) COMMENT 'ROW_COUNT, NULL_CHECK, ANOMALY, DUPLICATE',
    column_name VARCHAR(100),
    metric_value NUMBER,
    threshold_expected NUMBER,
    is_anomaly BOOLEAN DEFAULT FALSE,
    severity VARCHAR(20) COMMENT 'INFO, WARNING, CRITICAL',
    details VARCHAR(500),
    created_at TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP()
)
COMMENT = 'Centralized data quality metrics tracking';

-- ====================================================================================
-- 2. PIPELINE ERROR LOG TABLE
-- ====================================================================================
-- Comprehensive error tracking with full error context and recovery info
CREATE OR REPLACE TABLE SAAS_ANALYTICS.MONITORING.PIPELINE_ERROR_LOG (
    error_id INT AUTOINCREMENT PRIMARY KEY,
    error_timestamp TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP(),
    procedure_name VARCHAR(100),
    layer VARCHAR(20),
    error_code VARCHAR(50),
    error_message VARCHAR(1000),
    error_detail VARCHAR(2000),
    affected_rows INT,
    recovery_attempted BOOLEAN DEFAULT FALSE,
    recovery_status VARCHAR(50) COMMENT 'SUCCESS, FAILED, PENDING',
    recovery_details VARCHAR(1000),
    executed_by VARCHAR(100),
    created_at TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP()
)
COMMENT = 'Detailed pipeline error tracking with recovery info';

-- ====================================================================================
-- 3. SLA TRACKING TABLE
-- ====================================================================================
-- Monitors pipeline execution time, SLA compliance, and performance metrics
CREATE OR REPLACE TABLE SAAS_ANALYTICS.MONITORING.SLA_TRACKING (
    sla_id INT AUTOINCREMENT PRIMARY KEY,
    execution_date DATE,
    layer VARCHAR(20),
    procedure_name VARCHAR(100),
    scheduled_time TIMESTAMP_NTZ,
    actual_start_time TIMESTAMP_NTZ,
    actual_end_time TIMESTAMP_NTZ,
    execution_duration_seconds INT,
    sla_threshold_seconds INT,
    sla_met BOOLEAN,
    rows_processed INT,
    status VARCHAR(50) COMMENT 'SUCCESS, FAILED, PARTIAL',
    credits_used DECIMAL(10, 2),
    created_at TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP()
)
COMMENT = 'SLA tracking for pipeline procedures';

-- ====================================================================================
-- 4. DUPLICATE DETECTION LOG TABLE
-- ====================================================================================
-- Logs detected duplicate records and related metadata
CREATE OR REPLACE TABLE SAAS_ANALYTICS.MONITORING.DUPLICATE_DETECTION_LOG (
    duplicate_id INT AUTOINCREMENT PRIMARY KEY,
    detection_timestamp TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP(),
    layer VARCHAR(20),
    table_name VARCHAR(100),
    primary_key_column VARCHAR(100),
    primary_key_value VARCHAR(500),
    duplicate_count INT,
    last_occurrence_date DATE,
    duplicate_removal_attempted BOOLEAN DEFAULT FALSE,
    duplicate_removal_status VARCHAR(50),
    created_at TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP()
)
COMMENT = 'Duplicate record detection and tracking';

-- ====================================================================================
-- 5. SCHEMA VALIDATION LOG TABLE
-- ====================================================================================
-- Tracks schema changes, type conversion failures, and validation issues
CREATE OR REPLACE TABLE SAAS_ANALYTICS.MONITORING.SCHEMA_VALIDATION_LOG (
    validation_id INT AUTOINCREMENT PRIMARY KEY,
    validation_timestamp TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP(),
    layer VARCHAR(20),
    table_name VARCHAR(100),
    column_name VARCHAR(100),
    source_data_type VARCHAR(100),
    target_data_type VARCHAR(100),
    conversion_failure_count INT,
    null_conversion_count INT,
    validation_status VARCHAR(50) COMMENT 'PASS, FAIL, WARNING',
    details VARCHAR(500),
    created_at TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP()
)
COMMENT = 'Schema validation and type conversion tracking';

-- ====================================================================================
-- 6. PIPELINE AUDIT LOG TABLE
-- ====================================================================================
-- General audit trail for all pipeline executions
CREATE OR REPLACE TABLE SAAS_ANALYTICS.MONITORING.PIPELINE_AUDIT_LOG (
    audit_id INT AUTOINCREMENT PRIMARY KEY,
    audit_timestamp TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP(),
    procedure_name VARCHAR(100),
    action VARCHAR(100) COMMENT 'START, SUCCESS, FAILURE, RETRY',
    layer VARCHAR(20),
    rows_input INT,
    rows_processed INT,
    rows_failed INT,
    execution_id VARCHAR(100),
    session_id VARCHAR(100),
    status_message VARCHAR(500),
    created_at TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP()
)
COMMENT = 'Comprehensive audit trail for pipeline operations';

-- ====================================================================================
-- GRANT PERMISSIONS
-- ====================================================================================
GRANT ALL PRIVILEGES ON SCHEMA SAAS_ANALYTICS.MONITORING TO ROLE ANALYST_ROLE;
GRANT ALL PRIVILEGES ON SCHEMA SAAS_ANALYTICS.MONITORING TO ROLE DEVELOPER_ROLE;
GRANT SELECT ON SCHEMA SAAS_ANALYTICS.MONITORING TO ROLE QA_ROLE;
GRANT ALL PRIVILEGES ON ALL TABLES IN SCHEMA SAAS_ANALYTICS.MONITORING TO ROLE DEVELOPER_ROLE;
