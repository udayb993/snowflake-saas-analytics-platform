USE ROLE SYSADMIN;

-- ====================================================================================
-- SNOWFLAKE TASK: DAILY BRONZE DATA LOAD
-- ====================================================================================
-- This task automatically runs the LOAD_BRONZE_DATA procedure every morning at 6 AM
-- Ingests new data from the S3 stage into the bronze layer daily
-- Schedule: 6:00 AM UTC every day
-- ====================================================================================

CREATE OR REPLACE TASK SAAS_ANALYTICS.ORCHESTRATION.TASK_DAILY_LOAD_BRONZE_DATA
WAREHOUSE = SAAS_WH
SCHEDULE = 'USING CRON 0 6 * * * UTC'
AS
CALL SAAS_ANALYTICS.BRONZE.LOAD_BRONZE_DATA();

CALL SYSTEM$WAIT(5);
