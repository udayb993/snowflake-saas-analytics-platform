USE ROLE SYSADMIN;

-- ====================================================================================
-- ORCHESTRATION TASKS FOR GOLD LAYER
-- ====================================================================================
-- These tasks schedule the gold layer procedures to run on a regular basis
-- Runs after silver layer transformations to ensure dependent data is ready
-- ====================================================================================

-- Task depends on Silver layer task completing first
-- Runs 2 hours after the start of each day (allowing time for Silver processing)
CREATE OR REPLACE TASK SAAS_ANALYTICS.GOLD.TASK_LOAD_GOLD_METRICS
WAREHOUSE = SAAS_WH
SCHEDULE = 'USING CRON 0 2 * * * UTC'  -- 2:00 AM UTC daily
AS
CALL SAAS_ANALYTICS.GOLD.LOAD_ALL_GOLD_METRICS();

-- Resume the task (tasks are created in suspended state)
ALTER TASK SAAS_ANALYTICS.GOLD.TASK_LOAD_GOLD_METRICS RESUME;
