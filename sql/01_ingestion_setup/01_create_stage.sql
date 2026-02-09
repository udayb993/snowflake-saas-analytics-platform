USE ROLE SYSADMIN;

-- ====================================================================================
-- EXTERNAL STAGE CREATION - AWS S3
-- ====================================================================================
-- This creates an external stage that references an S3 bucket location
-- The stage uses the storage integration created in create_storage_integration.sql
--
-- Prerequisites:
--   1. Storage integration 'saas_s3_integration' must exist
--   2. CSV file must be uploaded to the S3 bucket
--   3. File format 'SAAS_ANALYTICS.COMMON.CSV_FORMAT' must exist
--
-- Usage:
--   LIST @SAAS_ANALYTICS.BRONZE.RAW_STAGE;  -- List files in stage
--   COPY INTO ... FROM @SAAS_ANALYTICS.BRONZE.RAW_STAGE;  -- Load data
-- ====================================================================================

CREATE OR REPLACE STAGE SAAS_ANALYTICS.BRONZE.RAW_STAGE
  URL = 's3://saas-analytics-data/raw/'
  STORAGE_INTEGRATION = saas_s3_integration
  FILE_FORMAT = SAAS_ANALYTICS.COMMON.CSV_FORMAT;

-- ====================================================================================
-- VERIFY STAGE CREATION
-- ====================================================================================
-- List files in the stage (verify CSV file is present)
-- LIST @SAAS_ANALYTICS.BRONZE.RAW_STAGE;

-- Check stage properties
DESCRIBE STAGE SAAS_ANALYTICS.BRONZE.RAW_STAGE;