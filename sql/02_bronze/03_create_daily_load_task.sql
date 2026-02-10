USE ROLE SYSADMIN;

-- ====================================================================================
-- SNOWFLAKE TASK: DAILY BRONZE DATA LOAD
-- ====================================================================================
-- This task automatically runs the LOAD_BRONZE_DATA procedure every morning at 6 AM
-- Ingests new data from the S3 stage into the bronze layer daily
-- Schedule: 6:00 AM UTC every day
-- ====================================================================================

CREATE OR REPLACE TASK SAAS_ANALYTICS.BRONZE.DAILY_LOAD_BRONZE_DATA
WAREHOUSE = SAAS_WH
SCHEDULE = 'USING CRON 0 6 * * * UTC'
AS
CALL SAAS_ANALYTICS.BRONZE.LOAD_BRONZE_DATA();

-- Resume the task (tasks are created in suspended state by default)
ALTER TASK SAAS_ANALYTICS.BRONZE.DAILY_LOAD_BRONZE_DATA RESUME;

-- Display task details
DESCRIBE TASK SAAS_ANALYTICS.BRONZE.DAILY_LOAD_BRONZE_DATA;
