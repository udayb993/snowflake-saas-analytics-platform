USE ROLE SYSADMIN;

-- ====================================================================================
-- ROW ACCESS POLICIES FOR MULTI-TENANCY
-- ====================================================================================
-- Creates and applies TENANT_RLS on the Silver table.
-- This policy propagates automatically through any secure views built on Silver,
-- including all views in the DASHBOARD schema — no additional policy needed there.
--
-- Access logic (evaluated per row at query time):
--   SYSADMIN / ANALYST_ROLE / DEVELOPER_ROLE  → all rows (no tenant filter)
--   SOCIAL_DASHBOARD_*_VIEWER roles            → only rows matching their
--                                                ROLE_TENANT_MAPPING entries
--
-- Tenant mapping:
--   SOCIAL_DASHBOARD_AMERICAS_VIEWER      → CANADA, UNITED STATES, BRAZIL
--   SOCIAL_DASHBOARD_EUROPE_VIEWER        → UNITED KINGDOM, GERMANY
--   SOCIAL_DASHBOARD_ASIA_PACIFIC_VIEWER  → INDIA, JAPAN, SOUTH KOREA, OTHER
--   SOCIAL_DASHBOARD_AUSTRALIA_VIEWER     → AUSTRALIA
--
-- Depends on:
--   sql/00_database_setup/01_create_roles.sql           (all roles must exist)
--   sql/05_governance/00_create_governance_tables.sql   (ROLE_TENANT_MAPPING must exist)
-- ====================================================================================


-- ====================================================================================
-- DROP EXISTING POLICY BINDING BEFORE REPLACING
-- ====================================================================================
-- Required because Snowflake won't drop or replace a policy that is still applied.
-- Comment out on first-time setup (policy won't exist yet).

ALTER TABLE SAAS_ANALYTICS.SILVER.SOCIAL_MEDIA_USERS_CLEAN
    DROP ROW ACCESS POLICY SAAS_ANALYTICS.GOVERNANCE.TENANT_RLS;


-- ====================================================================================
-- CREATE ROW ACCESS POLICY
-- ====================================================================================

CREATE OR REPLACE ROW ACCESS POLICY SAAS_ANALYTICS.GOVERNANCE.TENANT_RLS
AS (P_TENANT_ID STRING) RETURNS BOOLEAN ->

    -- Full-access roles: bypass tenant filtering entirely
    CURRENT_ROLE() IN ('SYSADMIN', 'ANALYST_ROLE', 'DEVELOPER_ROLE')

    -- Regional dashboard viewer roles: scoped to their mapped tenant_ids
    OR EXISTS (
        SELECT 1
          FROM SAAS_ANALYTICS.GOVERNANCE.ROLE_TENANT_MAPPING
         WHERE role_name = CURRENT_ROLE()
           AND tenant_id = P_TENANT_ID
    );


-- ====================================================================================
-- APPLY POLICY TO SILVER TABLE
-- ====================================================================================
-- Applied once here on Silver. All DASHBOARD secure views built on Silver
-- inherit this filtering automatically — no need to apply the policy on views.

ALTER TABLE SAAS_ANALYTICS.SILVER.SOCIAL_MEDIA_USERS_CLEAN
    ADD ROW ACCESS POLICY SAAS_ANALYTICS.GOVERNANCE.TENANT_RLS ON (TENANT_ID);


-- ====================================================================================
-- VERIFY
-- ====================================================================================

SHOW ROW ACCESS POLICIES IN SCHEMA SAAS_ANALYTICS.GOVERNANCE;
