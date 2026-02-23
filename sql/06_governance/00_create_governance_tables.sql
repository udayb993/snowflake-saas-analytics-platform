USE ROLE SYSADMIN;

-- ====================================================================================
-- GOVERNANCE SCHEMA + TABLES
-- ====================================================================================
-- Creates the GOVERNANCE schema and ROLE_TENANT_MAPPING table used by the
-- TENANT_RLS row access policy to enforce regional data isolation.
--
-- How it works:
--   - tenant_id is derived from UPPER(country) in the Silver layer
--   - Each row grants a specific role visibility into one tenant (country)
--   - The TENANT_RLS policy subqueries this table at query time
--   - Secure views in the DASHBOARD schema inherit this filtering automatically
--
-- Execution order:
--   1. sql/00_database_setup/01_create_roles.sql            (roles must exist first)
--   2. sql/05_governance/00_create_governance_tables.sql    (this file)
--   3. sql/05_governance/01_row_access_policies.sql         (create + apply RAP)
--   4. sql/05_governance/02_create_dashboard_views.sql      (create dashboard schema + views)
--   5. sql/05_governance/03_insert_role_tenant_mapping.sql  (populate mapping data)
-- ====================================================================================


CREATE TABLE IF NOT EXISTS SAAS_ANALYTICS.GOVERNANCE.ROLE_TENANT_MAPPING (
    role_name   STRING    NOT NULL,  -- Must exactly match CURRENT_ROLE() (case-sensitive)
    tenant_id   STRING    NOT NULL,  -- Must exactly match UPPER(country) in Silver table
    region      STRING    NOT NULL,  -- AMERICAS | EUROPE | ASIA_PACIFIC | AUSTRALIA
    created_at  TIMESTAMP DEFAULT CURRENT_TIMESTAMP(),
    CONSTRAINT pk_role_tenant PRIMARY KEY (role_name, tenant_id)
);