USE ROLE SECURITYADMIN;

-- ====================================================================================
-- SNOWFLAKE ROLE CREATION
-- ====================================================================================
-- This file creates custom roles used in the Snowflake SaaS Analytics Platform
-- for data governance, access control, and multi-tenancy
--
-- Note: SYSADMIN role is built-in and handles administrative tasks
--
-- Custom Roles:
--   - ANALYST_ROLE: Full access to Silver and Gold data (no masking)
--   - QA_ROLE: Quality assurance role with masked sensitive data
--   - DEVELOPER_ROLE: Technical development access for ETL/pipeline work
-- ====================================================================================

-- Create Analyst role with full access (no masking)
CREATE ROLE IF NOT EXISTS ANALYST_ROLE;

-- Create QA role with masked sensitive data access
CREATE ROLE IF NOT EXISTS QA_ROLE;

-- Create Developer role for ETL and technical work
CREATE ROLE IF NOT EXISTS DEVELOPER_ROLE;

-- ====================================================================================
-- GRANT ROLES TO SYSADMIN
-- ====================================================================================
-- Grant custom roles to SYSADMIN so it inherits their privileges
GRANT ROLE ANALYST_ROLE TO ROLE SYSADMIN;
GRANT ROLE QA_ROLE TO ROLE SYSADMIN;
GRANT ROLE DEVELOPER_ROLE TO ROLE SYSADMIN;
