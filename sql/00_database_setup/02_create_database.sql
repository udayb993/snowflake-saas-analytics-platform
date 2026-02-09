USE ROLE SYSADMIN;

CREATE DATABASE IF NOT EXISTS SAAS_ANALYTICS;

-- ====================================================================================
-- GRANT USAGE ON DATABASE TO ROLES
-- ====================================================================================
-- Grant database usage to custom roles
GRANT USAGE ON DATABASE SAAS_ANALYTICS TO ROLE ANALYST_ROLE;
GRANT USAGE ON DATABASE SAAS_ANALYTICS TO ROLE QA_ROLE;
GRANT USAGE ON DATABASE SAAS_ANALYTICS TO ROLE DEVELOPER_ROLE;