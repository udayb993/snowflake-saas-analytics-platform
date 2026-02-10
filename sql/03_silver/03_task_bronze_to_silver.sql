USE ROLE SYSADMIN;

-- ====================================================================================
-- SNOWFLAKE TASK: TRIGGER BRONZE TO SILVER TRANSFORMATION
-- ====================================================================================
-- This task automatically runs the TRANSFORM_BRONZE_TO_SILVER procedure
-- Triggers when the SOCIAL_MEDIA_USERS_STREAM has new data to process
-- Enables incremental processing of changes from Bronze layer
-- ====================================================================================

CREATE OR REPLACE TASK SAAS_ANALYTICS.SILVER.TRANSFORM_BRONZE_TO_SILVER_TASK
WAREHOUSE = SAAS_WH
WHEN SYSTEM$STREAM_HAS_DATA('SAAS_ANALYTICS.BRONZE.SOCIAL_MEDIA_USERS_STREAM')
AS
CALL SAAS_ANALYTICS.SILVER.TRANSFORM_BRONZE_TO_SILVER();

-- Resume the task (tasks are created in suspended state by default)
ALTER TASK SAAS_ANALYTICS.SILVER.TRANSFORM_BRONZE_TO_SILVER_TASK RESUME;

-- Display task details
DESCRIBE TASK SAAS_ANALYTICS.SILVER.TRANSFORM_BRONZE_TO_SILVER_TASK;
