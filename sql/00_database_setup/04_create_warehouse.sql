USE ROLE SYSADMIN;

CREATE OR REPLACE WAREHOUSE SAAS_WH
WITH
    WAREHOUSE_SIZE = 'XSMALL'
    AUTO_SUSPEND = 60
    AUTO_RESUME = TRUE
    INITIALLY_SUSPENDED = TRUE;

-- ====================================================================================
-- GRANT USAGE ON WAREHOUSE TO ROLES
-- ====================================================================================
-- Grant warehouse usage to custom roles
GRANT USAGE ON WAREHOUSE SAAS_WH TO ROLE ANALYST_ROLE;
GRANT USAGE ON WAREHOUSE SAAS_WH TO ROLE QA_ROLE;
GRANT USAGE ON WAREHOUSE SAAS_WH TO ROLE DEVELOPER_ROLE;