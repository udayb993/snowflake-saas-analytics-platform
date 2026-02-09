-- ====================================================================================
-- SNOWFLAKE STORAGE INTEGRATION SETUP - AWS S3
-- ====================================================================================
-- This file creates a storage integration that allows Snowflake to access data files
-- stored in an AWS S3 bucket using IAM role-based authentication
--
-- Prerequisites:
--   1. AWS Account with permissions to create IAM roles
--   2. S3 bucket created (e.g., saas-analytics-data)
--   3. AWS Account ID (available in AWS Console)
--   4. IAM role name (e.g., snowflake-s3-role)
--
-- Setup Steps:
--   1. Create an IAM role in AWS with S3 permissions
--   2. Update the STORAGE_AWS_ROLE_ARN below with your AWS Account ID and role name
--   3. Run this script to create the storage integration
--   4. Copy the STORAGE_AWS_EXTERNAL_ID from the DESC output
--   5. Add this External ID to your IAM role's trust policy in AWS
--   6. Test with: LIST @SAAS_ANALYTICS.BRONZE.RAW_STAGE;
-- ====================================================================================

CREATE OR REPLACE STORAGE INTEGRATION saas_s3_integration
  TYPE = S3
  STORAGE_PROVIDER = S3
  ENABLED = TRUE
  STORAGE_AWS_ROLE_ARN = 'arn:aws:iam::123456789012:role/snowflake-s3-role'
  STORAGE_ALLOWED_LOCATIONS = ('s3://saas-analytics-data/raw/');

-- ====================================================================================
-- VERIFY STORAGE INTEGRATION
-- ====================================================================================
-- Run this command to see the External ID that needs to be added to your IAM trust policy
DESC STORAGE INTEGRATION saas_s3_integration;

-- ====================================================================================
-- GRANT PRIVILEGES
-- ====================================================================================
-- Grant the integration to SYSADMIN role (system admin)
GRANT USAGE ON STORAGE INTEGRATION saas_s3_integration TO ROLE SYSADMIN;
