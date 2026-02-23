USE ROLE SECURITYADMIN;

-- ====================================================================================
-- SNOWFLAKE ROLE CREATION
-- ====================================================================================
-- Creates all custom roles for the Snowflake SaaS Analytics Platform.
--
-- Functional Roles:
--   - ANALYST_ROLE    : Full unmasked read access to Silver and Gold data
--   - QA_ROLE         : Read access with masked sensitive fields
--   - DEVELOPER_ROLE  : Full technical access for ETL/pipeline development
--
-- Regional Dashboard Viewer Roles (tenant-scoped, dashboard schema only):
--   - SOCIAL_DASHBOARD_AMERICAS_VIEWER      : Canada, United States, Brazil
--   - SOCIAL_DASHBOARD_EUROPE_VIEWER        : United Kingdom, Germany
--   - SOCIAL_DASHBOARD_ASIA_PACIFIC_VIEWER  : India, Japan, South Korea, Other
--   - SOCIAL_DASHBOARD_AUSTRALIA_VIEWER     : Australia
--
-- Regional roles can ONLY access the DASHBOARD schema (secure views).
-- They cannot query Silver or Gold tables directly.
-- Tenant isolation is enforced via TENANT_RLS row access policy on Silver,
-- which propagates through the secure views automatically.
--
-- See also:
--   sql/05_governance/00_create_governance_tables.sql
--   sql/05_governance/01_row_access_policies.sql
--   sql/05_governance/02_create_dashboard_views.sql
--   sql/05_governance/03_insert_role_tenant_mapping.sql
-- ====================================================================================


-- ====================================================================================
-- FUNCTIONAL ROLES
-- ====================================================================================

CREATE ROLE IF NOT EXISTS ANALYST_ROLE;
CREATE ROLE IF NOT EXISTS QA_ROLE;
CREATE ROLE IF NOT EXISTS DEVELOPER_ROLE;


-- ====================================================================================
-- REGIONAL DASHBOARD VIEWER ROLES
-- ====================================================================================

CREATE ROLE IF NOT EXISTS SOCIAL_DASHBOARD_AMERICAS_VIEWER;
CREATE ROLE IF NOT EXISTS SOCIAL_DASHBOARD_EUROPE_VIEWER;
CREATE ROLE IF NOT EXISTS SOCIAL_DASHBOARD_ASIA_PACIFIC_VIEWER;
CREATE ROLE IF NOT EXISTS SOCIAL_DASHBOARD_AUSTRALIA_VIEWER;


-- ====================================================================================
-- GRANT ALL ROLES TO SYSADMIN
-- ====================================================================================

GRANT ROLE ANALYST_ROLE                          TO ROLE SYSADMIN;
GRANT ROLE QA_ROLE                               TO ROLE SYSADMIN;
GRANT ROLE DEVELOPER_ROLE                        TO ROLE SYSADMIN;
GRANT ROLE SOCIAL_DASHBOARD_AMERICAS_VIEWER      TO ROLE SYSADMIN;
GRANT ROLE SOCIAL_DASHBOARD_EUROPE_VIEWER        TO ROLE SYSADMIN;
GRANT ROLE SOCIAL_DASHBOARD_ASIA_PACIFIC_VIEWER  TO ROLE SYSADMIN;
GRANT ROLE SOCIAL_DASHBOARD_AUSTRALIA_VIEWER     TO ROLE SYSADMIN;
