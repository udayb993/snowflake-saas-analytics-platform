USE ROLE SYSADMIN;
USE DATABASE SAAS_ANALYTICS;

-- ====================================================================================
-- ROLE_TENANT_MAPPING — SEED DATA
-- ====================================================================================
-- Populates the mapping that drives the TENANT_RLS row access policy.
--
-- Rules:
--   - tenant_id MUST exactly match UPPER(country) as stored in the Silver table
--   - role_name MUST exactly match the Snowflake role name (case-sensitive)
--
-- Region assignments:
--   AMERICAS     → Canada, United States, Brazil
--   EUROPE       → United Kingdom, Germany
--   ASIA_PACIFIC → India, Japan, South Korea, Other
--   AUSTRALIA    → Australia
--
-- Run AFTER:
--   00_create_governance_tables.sql
--   01_row_access_policies.sql
--   02_create_dashboard_views.sql
-- ====================================================================================

-- Safe to re-run — clears existing rows before reinserting
DELETE FROM SAAS_ANALYTICS.GOVERNANCE.ROLE_TENANT_MAPPING;

-- ====================================================================================
-- AMERICAS  (Canada, United States, Brazil)
-- ====================================================================================

INSERT INTO SAAS_ANALYTICS.GOVERNANCE.ROLE_TENANT_MAPPING (role_name, tenant_id, region)
VALUES
    ('SOCIAL_DASHBOARD_AMERICAS_VIEWER', 'CANADA',        'AMERICAS'),
    ('SOCIAL_DASHBOARD_AMERICAS_VIEWER', 'UNITED STATES', 'AMERICAS'),
    ('SOCIAL_DASHBOARD_AMERICAS_VIEWER', 'BRAZIL',        'AMERICAS');

-- ====================================================================================
-- EUROPE  (United Kingdom, Germany)
-- ====================================================================================

INSERT INTO SAAS_ANALYTICS.GOVERNANCE.ROLE_TENANT_MAPPING (role_name, tenant_id, region)
VALUES
    ('SOCIAL_DASHBOARD_EUROPE_VIEWER', 'UNITED KINGDOM', 'EUROPE'),
    ('SOCIAL_DASHBOARD_EUROPE_VIEWER', 'GERMANY',        'EUROPE');

-- ====================================================================================
-- ASIA PACIFIC  (India, Japan, South Korea, Other)
-- ====================================================================================

INSERT INTO SAAS_ANALYTICS.GOVERNANCE.ROLE_TENANT_MAPPING (role_name, tenant_id, region)
VALUES
    ('SOCIAL_DASHBOARD_ASIA_PACIFIC_VIEWER', 'INDIA',       'ASIA_PACIFIC'),
    ('SOCIAL_DASHBOARD_ASIA_PACIFIC_VIEWER', 'JAPAN',       'ASIA_PACIFIC'),
    ('SOCIAL_DASHBOARD_ASIA_PACIFIC_VIEWER', 'SOUTH KOREA', 'ASIA_PACIFIC'),
    ('SOCIAL_DASHBOARD_ASIA_PACIFIC_VIEWER', 'OTHER',       'ASIA_PACIFIC');

-- ====================================================================================
-- AUSTRALIA
-- ====================================================================================

INSERT INTO SAAS_ANALYTICS.GOVERNANCE.ROLE_TENANT_MAPPING (role_name, tenant_id, region)
VALUES
    ('SOCIAL_DASHBOARD_AUSTRALIA_VIEWER', 'AUSTRALIA', 'AUSTRALIA');


-- ====================================================================================
-- VERIFY
-- ====================================================================================

SELECT
    region,
    role_name,
    tenant_id,
    created_at
FROM SAAS_ANALYTICS.GOVERNANCE.ROLE_TENANT_MAPPING
ORDER BY region, tenant_id;