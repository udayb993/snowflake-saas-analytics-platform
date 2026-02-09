-- ====================================================================================
-- SNOWFLAKE ROLE CREATION
-- ====================================================================================
-- This file creates custom roles used in the Snowflake SaaS Analytics Platform
-- for data governance, access control, and multi-tenancy
--
-- Note: SYSADMIN role is built-in and handles administrative tasks
--
-- Custom Roles:
--   - ANALYST_ROLE: Limited analyst access with data masking applied
--   - DEVELOPER_ROLE: Technical development access for ETL/pipeline work
-- ====================================================================================

-- Create Analyst role with masked data access
CREATE ROLE IF NOT EXISTS ANALYST_ROLE;

-- Create Developer role for ETL and technical work
CREATE ROLE IF NOT EXISTS DEVELOPER_ROLE;

-- ====================================================================================
-- GRANT BASIC PRIVILEGES TO ROLES
-- ====================================================================================

-- Grant usage on database to custom roles
GRANT USAGE ON DATABASE SAAS_ANALYTICS TO ROLE ANALYST_ROLE;
GRANT USAGE ON DATABASE SAAS_ANALYTICS TO ROLE DEVELOPER_ROLE;

-- Grant usage on warehouse to custom roles
GRANT USAGE ON WAREHOUSE SAAS_WH TO ROLE ANALYST_ROLE;
GRANT USAGE ON WAREHOUSE SAAS_WH TO ROLE DEVELOPER_ROLE;

-- ====================================================================================
-- GRANT SCHEMA PRIVILEGES
-- ====================================================================================

-- DEVELOPER_ROLE - Full access to Bronze, Silver, Orchestration for ETL work
GRANT ALL PRIVILEGES ON SCHEMA SAAS_ANALYTICS.BRONZE TO ROLE DEVELOPER_ROLE;
GRANT ALL PRIVILEGES ON SCHEMA SAAS_ANALYTICS.SILVER TO ROLE DEVELOPER_ROLE;
GRANT ALL PRIVILEGES ON SCHEMA SAAS_ANALYTICS.ORCHESTRATION TO ROLE DEVELOPER_ROLE;
GRANT ALL PRIVILEGES ON SCHEMA SAAS_ANALYTICS.COMMON TO ROLE DEVELOPER_ROLE;

-- ANALYST_ROLE - Read-only access to Silver and Gold schemas
GRANT USAGE ON SCHEMA SAAS_ANALYTICS.SILVER TO ROLE ANALYST_ROLE;
GRANT USAGE ON SCHEMA SAAS_ANALYTICS.GOLD TO ROLE ANALYST_ROLE;
GRANT SELECT ON ALL TABLES IN SCHEMA SAAS_ANALYTICS.SILVER TO ROLE ANALYST_ROLE;
GRANT SELECT ON ALL TABLES IN SCHEMA SAAS_ANALYTICS.GOLD TO ROLE ANALYST_ROLE;
