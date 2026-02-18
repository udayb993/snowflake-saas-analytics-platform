USE ROLE SYSADMIN;

-- ====================================================================================
-- ROW ACCESS POLICIES FOR MULTI-TENANCY
-- ====================================================================================
-- This file creates and applies row-level security (RLS) policies for multi-tenant isolation
-- Roles: SYSADMIN (built-in), DEVELOPER_ROLE (see 00_database_setup/03_create_roles.sql)
-- ====================================================================================

-- Create Row Access Policy for multi-tenancy
CREATE OR REPLACE ROW ACCESS POLICY SAAS_ANALYTICS.GOVERNANCE.TENANT_RLS
AS (TENANT_ID STRING) RETURNS BOOLEAN ->
CURRENT_ROLE() IN ('SYSADMIN') 
OR CURRENT_ROLE() = 'DEVELOPER_ROLE'
OR EXISTS (
    SELECT 1 FROM SAAS_ANALYTICS.GOVERNANCE.ROLE_TENANT_MAPPING
    WHERE role_name = CURRENT_ROLE()
    AND tenant_id = TENANT_ID
);

-- Apply to Silver table
ALTER TABLE SAAS_ANALYTICS.SILVER.SOCIAL_MEDIA_USERS_CLEAN
ADD ROW ACCESS POLICY SAAS_ANALYTICS.GOVERNANCE.TENANT_RLS ON (TENANT_ID);

-- Create role to tenant mapping table for flexible RLS
CREATE TABLE IF NOT EXISTS SAAS_ANALYTICS.GOVERNANCE.ROLE_TENANT_MAPPING (
    role_name STRING,
    tenant_id STRING,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP()
);