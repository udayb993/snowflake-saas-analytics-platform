USE ROLE SYSADMIN;

-- ====================================================================================
-- DYNAMIC DATA MASKING POLICIES
-- ====================================================================================
-- This file creates and applies masking policies to sensitive columns
--
-- Data Visibility:
--   - SYSADMIN: Full unmasked access
--   - ANALYST_ROLE: Full unmasked access
--   - QA_ROLE: Masked view of sensitive data
--   - Others: NULL (no access)
--
-- Roles: SYSADMIN (built-in), ANALYST_ROLE, QA_ROLE (see 00_database_setup/03_create_roles.sql)
-- ====================================================================================

-- Create masking policy for sensitive health data
CREATE OR REPLACE MASKING POLICY SAAS_ANALYTICS.GOVERNANCE.SENSITIVE_DATA_MASKING_POLICY
AS (VAL STRING) 
RETURNS STRING ->
CASE
    WHEN CURRENT_ROLE() IN ('SYSADMIN', 'ANALYST_ROLE') THEN VAL
    ELSE '***MASKED***'
END;

-- Apply masking to sensitive columns
ALTER TABLE SAAS_ANALYTICS.SILVER.SOCIAL_MEDIA_USERS_CLEAN
MODIFY COLUMN age SET MASKING POLICY SAAS_ANALYTICS.GOVERNANCE.SENSITIVE_DATA_MASKING_POLICY;

ALTER TABLE SAAS_ANALYTICS.SILVER.SOCIAL_MEDIA_USERS_CLEAN
MODIFY COLUMN body_mass_index SET MASKING POLICY SAAS_ANALYTICS.GOVERNANCE.SENSITIVE_DATA_MASKING_POLICY;

ALTER TABLE SAAS_ANALYTICS.SILVER.SOCIAL_MEDIA_USERS_CLEAN
MODIFY COLUMN blood_pressure_systolic SET MASKING POLICY SAAS_ANALYTICS.GOVERNANCE.SENSITIVE_DATA_MASKING_POLICY;

ALTER TABLE SAAS_ANALYTICS.SILVER.SOCIAL_MEDIA_USERS_CLEAN
MODIFY COLUMN blood_pressure_diastolic SET MASKING POLICY SAAS_ANALYTICS.GOVERNANCE.SENSITIVE_DATA_MASKING_POLICY;