USE ROLE SYSADMIN;

-- ====================================================================================
-- DATA QUALITY MONITORING TASKS & ALERTS
-- ====================================================================================
-- Scheduled tasks for continuous data quality monitoring and alerting
-- ====================================================================================

-- ====================================================================================
-- 1. TASK: HOURLY_TABLE_QUALITY_CHECK
-- ====================================================================================
-- Runs hourly quality checks on all key tables
CREATE OR REPLACE TASK SAAS_ANALYTICS.MONITORING.HOURLY_TABLE_QUALITY_CHECK
WAREHOUSE = SAAS_WH
SCHEDULE = 'USING CRON 0 * * * * UTC'
COMMENT = 'Hourly data quality checks on bronze, silver, and gold layers'
AS
BEGIN
    -- Bronze layer quality checks
    CALL SAAS_ANALYTICS.MONITORING.CHECK_TABLE_QUALITY('BRONZE', 'SOCIAL_MEDIA_USERS_RAW', 'SAAS_ANALYTICS');
    
    -- Silver layer quality checks
    CALL SAAS_ANALYTICS.MONITORING.CHECK_TABLE_QUALITY('SILVER', 'SOCIAL_MEDIA_USERS_CLEAN', 'SAAS_ANALYTICS');
    
    -- Gold layer quality checks
    CALL SAAS_ANALYTICS.MONITORING.CHECK_TABLE_QUALITY('GOLD', 'TENANT_ENGAGEMENT_METRICS', 'SAAS_ANALYTICS');
    CALL SAAS_ANALYTICS.MONITORING.CHECK_TABLE_QUALITY('GOLD', 'USER_ENGAGEMENT_SNAPSHOT', 'SAAS_ANALYTICS');
    CALL SAAS_ANALYTICS.MONITORING.CHECK_TABLE_QUALITY('GOLD', 'CONTENT_PERFORMANCE_METRICS', 'SAAS_ANALYTICS');
END;

-- ====================================================================================
-- 2. TASK: DAILY_DUPLICATE_DETECTION
-- ====================================================================================
-- Daily duplicate detection across all layers
CREATE OR REPLACE TASK SAAS_ANALYTICS.MONITORING.DAILY_DUPLICATE_DETECTION
WAREHOUSE = SAAS_WH
SCHEDULE = 'USING CRON 0 2 * * * UTC'
COMMENT = 'Daily duplicate record detection'
AS
BEGIN
    -- Bronze duplicate detection
    CALL SAAS_ANALYTICS.MONITORING.DETECT_DUPLICATES('BRONZE', 'SOCIAL_MEDIA_USERS_RAW', 'USER_ID', 'SAAS_ANALYTICS');
    
    -- Silver duplicate detection
    CALL SAAS_ANALYTICS.MONITORING.DETECT_DUPLICATES('SILVER', 'SOCIAL_MEDIA_USERS_CLEAN', 'USER_ID', 'SAAS_ANALYTICS');
    
    -- Gold duplicate detection
    CALL SAAS_ANALYTICS.MONITORING.DETECT_DUPLICATES('GOLD', 'TENANT_ENGAGEMENT_METRICS', 'TENANT_ID', 'SAAS_ANALYTICS');
    CALL SAAS_ANALYTICS.MONITORING.DETECT_DUPLICATES('GOLD', 'USER_ENGAGEMENT_SNAPSHOT', 'USER_ID', 'SAAS_ANALYTICS');
END;

-- ====================================================================================
-- 3. TASK: DAILY_SCHEMA_VALIDATION
-- ====================================================================================
-- Daily schema validation and type conversion monitoring
CREATE OR REPLACE TASK SAAS_ANALYTICS.MONITORING.DAILY_SCHEMA_VALIDATION
WAREHOUSE = SAAS_WH
SCHEDULE = 'USING CRON 0 3 * * * UTC'
COMMENT = 'Daily schema validation across layers'
AS
BEGIN
    -- Validate Bronze to Silver schema
    CALL SAAS_ANALYTICS.MONITORING.VALIDATE_SCHEMA('BRONZE', 'SILVER', 'SOCIAL_MEDIA_USERS_RAW', 'SAAS_ANALYTICS');
    
    -- Validate Silver to Gold schema
    CALL SAAS_ANALYTICS.MONITORING.VALIDATE_SCHEMA('SILVER', 'GOLD', 'SOCIAL_MEDIA_USERS_CLEAN', 'SAAS_ANALYTICS');
END;

-- ====================================================================================
-- 4. TASK: NULL_PERCENTAGE_MONITORING
-- ====================================================================================
-- Monitor null percentages hourly for data quality
CREATE OR REPLACE TASK SAAS_ANALYTICS.MONITORING.NULL_PERCENTAGE_MONITORING
WAREHOUSE = SAAS_WH
SCHEDULE = 'USING CRON 15 * * * * UTC'
COMMENT = 'Hourly null percentage monitoring'
AS
BEGIN
    -- Check Bronze layer nulls
    CALL SAAS_ANALYTICS.MONITORING.CHECK_NULL_PERCENTAGES('BRONZE', 'SOCIAL_MEDIA_USERS_RAW', 'SAAS_ANALYTICS', 10.0);
    
    -- Check Silver layer nulls (lower threshold for cleaned data)
    CALL SAAS_ANALYTICS.MONITORING.CHECK_NULL_PERCENTAGES('SILVER', 'SOCIAL_MEDIA_USERS_CLEAN', 'SAAS_ANALYTICS', 5.0);
    
    -- Check Gold layer nulls (lowest threshold for metrics)
    CALL SAAS_ANALYTICS.MONITORING.CHECK_NULL_PERCENTAGES('GOLD', 'TENANT_ENGAGEMENT_METRICS', 'SAAS_ANALYTICS', 2.0);
END;

-- ====================================================================================
-- 5. TASK: CLEANUP_OLD_LOGS
-- ====================================================================================
-- Cleanup old log entries (older than 90 days) to manage table sizes
CREATE OR REPLACE TASK SAAS_ANALYTICS.MONITORING.CLEANUP_OLD_LOGS
WAREHOUSE = SAAS_WH
SCHEDULE = 'USING CRON 0 4 * * 0 UTC'
COMMENT = 'Weekly cleanup of old monitoring logs'
AS
BEGIN
    -- Cleanup DQ metrics older than 90 days
    DELETE FROM SAAS_ANALYTICS.MONITORING.DQ_METRICS 
    WHERE DATE(created_at) < DATEADD(DAY, -90, CURRENT_DATE());
    
    -- Cleanup error logs older than 90 days
    DELETE FROM SAAS_ANALYTICS.MONITORING.PIPELINE_ERROR_LOG 
    WHERE DATE(created_at) < DATEADD(DAY, -90, CURRENT_DATE());
    
    -- Cleanup SLA tracking older than 60 days
    DELETE FROM SAAS_ANALYTICS.MONITORING.SLA_TRACKING 
    WHERE DATE(created_at) < DATEADD(DAY, -60, CURRENT_DATE());
    
    -- Cleanup audit logs older than 120 days
    DELETE FROM SAAS_ANALYTICS.MONITORING.PIPELINE_AUDIT_LOG 
    WHERE DATE(created_at) < DATEADD(DAY, -120, CURRENT_DATE());
END;

-- ====================================================================================
-- Enable all monitoring tasks
-- ====================================================================================
ALTER TASK SAAS_ANALYTICS.MONITORING.HOURLY_TABLE_QUALITY_CHECK RESUME;
ALTER TASK SAAS_ANALYTICS.MONITORING.DAILY_DUPLICATE_DETECTION RESUME;
ALTER TASK SAAS_ANALYTICS.MONITORING.DAILY_SCHEMA_VALIDATION RESUME;
ALTER TASK SAAS_ANALYTICS.MONITORING.NULL_PERCENTAGE_MONITORING RESUME;
ALTER TASK SAAS_ANALYTICS.MONITORING.CLEANUP_OLD_LOGS RESUME;

-- ====================================================================================
-- Grant task execution permissions
-- ====================================================================================
GRANT EXECUTE ON TASK SAAS_ANALYTICS.MONITORING.HOURLY_TABLE_QUALITY_CHECK TO ROLE DEVELOPER_ROLE;
GRANT EXECUTE ON TASK SAAS_ANALYTICS.MONITORING.DAILY_DUPLICATE_DETECTION TO ROLE DEVELOPER_ROLE;
GRANT EXECUTE ON TASK SAAS_ANALYTICS.MONITORING.DAILY_SCHEMA_VALIDATION TO ROLE DEVELOPER_ROLE;
GRANT EXECUTE ON TASK SAAS_ANALYTICS.MONITORING.NULL_PERCENTAGE_MONITORING TO ROLE DEVELOPER_ROLE;
GRANT EXECUTE ON TASK SAAS_ANALYTICS.MONITORING.CLEANUP_OLD_LOGS TO ROLE DEVELOPER_ROLE;
