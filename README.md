# Snowflake SaaS Analytics Platform

## Project Overview

This project demonstrates a **production-ready Snowflake-based SaaS analytics platform** with:

✅ **Medallion Architecture** (Bronze → Silver → Gold) - Complete data pipeline  
✅ **Multi-Tenant Data Modeling** - Tenant isolation with row-level security  
✅ **Incremental Data Processing** - Change Data Capture (CDC) with streams  
✅ **Data Governance** - Dynamic masking & row-level security  
✅ **Environment-Aware Deployment** - dev/qa/prod with automatic substitution  
✅ **Production-Ready Procedures** - MERGE-based idempotent transformations  

The platform processes Instagram user behavior data with 58+ attributes including engagement metrics, health metrics, and social activity patterns, simulating a real-world SaaS analytics backend.

## Repository Structure
```
snowflake-saas-analytics-platform/
├── deployment/                          🆕 Deployment scripts & configuration
│   ├── README.md                        Quick start guide
│   ├── DEPLOYMENT.md                    Comprehensive deployment documentation
│   ├── deploy.py                        Main deployment script (Python) ✅ RECOMMENDED
│   ├── deploy.sh                        Bash deployment alternative
│   └── config/
│       └── environment.yml              Environment variable configuration
│
├── sql/                                 SQL scripts (organized by layer)
│   ├── 00_database_setup/
│   │   ├── 01_create_roles.sql          Create ANALYST_ROLE, QA_ROLE, DEVELOPER_ROLE
│   │   ├── 02_create_database.sql       Create SAAS_ANALYTICS database (placeholder)
│   │   ├── 03_create_schemas.sql        Create schemas: BRONZE, SILVER, GOLD, COMMON, GOVERNANCE, ORCHESTRATION
│   │   └── 04_create_warehouse.sql      Create SAAS_WH warehouse
│   │
│   ├── 01_ingestion_setup/
│   │   ├── 00_create_storage_integration.sql    AWS S3 storage integration
│   │   ├── 01_create_file_format.sql            CSV file format configuration
│   │   └── 02_create_stage.sql                  External stage for S3 access
│   │
│   ├── 02_bronze/                       Raw data layer (58 columns, all STRING)
│   │   ├── 00_create_bronze_tables.sql          Create raw data table
│   │   ├── 01_create_bronze_streams.sql         Create CDC stream
│   │   ├── 02_procedure_load_bronze_data.sql    Procedure to load from S3
│   │   └── 03_create_daily_load_task.sql        Task to run daily load
│   │
│   ├── 03_silver/                       Cleaned & typed data (data quality layer)
│   │   ├── 00_create_silver_tables.sql          Create cleaned table with proper types
│   │   ├── 01_create_silver_streams.sql         Create CDC stream for changes
│   │   ├── 02_procedure_bronze_to_silver.sql    MERGE procedure (idempotent)
│   │   └── 03_task_bronze_to_silver.sql         Hourly transformation task
│   │
│   ├── 04_gold/                         Business metrics (analytics layer)
│   │   ├── 00_create_metrics_tables.sql         Create 3 gold tables
│   │   ├── 01_procedure_silver_to_gold.sql      MERGE procedures + master orchestrator
│   │   └── 02_task_gold_metrics.sql             Daily metrics task
│   │
│   └── 06_governance/
│       ├── masking_policies.sql         Dynamic data masking (age, BMI, blood pressure)
│       └── row_access_policies.sql      Multi-tenant row-level security (RLS)
│
├── data/
│   └── social_media_part_ad.csv         Sample dataset (58 columns, Instagram analytics)
│
└── README.md                            This file
```

## Data Source & Schema
The dataset contains Instagram user analytics with 58 columns:
- **User Demographics**: age, gender, country, urban_rural, income_level, employment_status, education_level, relationship_status
- **Health Metrics**: exercise_hours_per_week, sleep_hours_per_night, body_mass_index, blood_pressure_systolic/diastolic, smoking, alcohol_frequency, perceived_stress_score, self_reported_happiness
- **Engagement Metrics**: daily_active_minutes_instagram, sessions_per_day, posts_created_per_week, reels_watched_per_day, stories_viewed_per_day, likes_given_per_day, comments_written_per_day, followers_count, following_count, user_engagement_score
- **Account Data**: account_creation_year, last_login_date, subscription_status, two_factor_auth_enabled, biometric_login_used
- **Platform Activity**: app_name, uses_premium_features, notification_response_rate, linked_accounts_count, content_type_preference, preferred_content_theme, privacy_setting_level, ads_viewed_per_day, ads_clicked_per_day, time_on_feed_per_day, time_on_explore_per_day, time_on_messages_per_day, time_on_reels_per_day, dms_sent/received_per_week, and more

## Architecture

### Medallion Architecture
- **Bronze Layer**: Raw data ingestion - all 58 columns loaded as STRING data types with minimal transformation
- **Silver Layer**: Cleaned and typed data - proper data type conversions (NUMBER, DATE, BOOLEAN), calculated fields (tenant_id), data quality checks
- **Gold Layer**: Business-ready aggregated metrics - tenant engagement metrics including user counts, average usage, engagement scores, subscription status

### Multi-Tenancy
- Tenants are derived from the `country` field (converted to uppercase)
- `UPPER(country)` serves as the `tenant_id` for SaaS multi-tenancy simulation
- Row Access Policies enforce tenant isolation at the database level

### Data Governance
1. **Dynamic Data Masking (DDM)**
   - Sensitive health fields (age, BMI, blood pressure) are masked based on role
   - SYSADMIN and ANALYST_ROLE see unmasked values
   - QA_ROLE and all other roles see "***MASKED***"

2. **Row-Level Security (RLS)**
   - Multi-tenant isolation via TENANT_RLS policy
   - SYSADMIN role (built-in) can see all data
   - DEVELOPER_ROLE can access all data
   - Tenant-specific roles can be mapped via ROLE_TENANT_MAPPING table
   - Flexible role-to-tenant assignment for enterprise scenarios

3. **Custom Roles Created**
   - ANALYST_ROLE - Full unmasked read-only access to Silver and Gold data
   - QA_ROLE - Read-only access with masked sensitive data
   - DEVELOPER_ROLE - Full technical access for ETL/development work
   - SYSADMIN - Built-in role with administrative privileges

### Real-Time Data Pipeline
- **Streams**: `SOCIAL_MEDIA_USERS_STREAM` captures all changes (inserts, updates, deletes) to Bronze data
- **Tasks**: Hourly scheduled transformation tasks run automatically via Snowflake Task scheduler
- **Incremental Processing**: Only changed records are processed using streams, optimizing compute costs

## Execution Order

Run the SQL scripts in the following sequence on Snowflake:

```
1. sql/00_database_setup/create_database.sql
2. sql/00_database_setup/create_schemas.sql
3. sql/00_database_setup/create_warehouse.sql
4. sql/00_database_setup/03_create_roles.sql
5. sql/01_ingestion_setup/00_create_storage_integration.sql
6. sql/01_ingestion_setup/01_create_stage.sql
7. sql/01_ingestion_setup/02_create_file_format.sql
8. sql/02_bronze/create_bronze_tables.sql
9. sql/02_bronze/load_bronze_data.sql (requires CSV file uploaded to cloud storage)
10. sql/03_silver/create_silver_tables.sql
11. sql/03_silver/transform_bronze_to_silver.sql
12. sql/04_gold/create_metrics_tables.sql
13. sql/04_gold/load_business_metrics.sql
14. sql/05_orchestration/create_streams.sql
15. sql/05_orchestration/create_tasks.sql
16. sql/06_governance/masking_policies.sql
17. sql/06_governance/row_access_policies.sql
```

## Key Features

✅ **Complete Data Pipeline** - All 58 data columns properly mapped and transformed  
✅ **Type Safety** - Proper data types (STRING → NUMBER, DATE, BOOLEAN conversions)  
✅ **Multi-Tenancy** - Built-in tenant isolation with row access policies  
✅ **Data Governance** - Dynamic masking for sensitive PII and health data  
✅ **Real-Time Processing** - Streams and Tasks for incremental data pipelines  
✅ **Production Ready** - All schemas, error handling, and role management in place  

## Setup Instructions

### Option 1: External Cloud Storage (Recommended for Production)

**Using AWS S3**

1. **Configure Storage Integration**
   - Open `sql/01_ingestion_setup/00_create_storage_integration.sql`
   - Update the AWS Account ID and IAM role name with your details
   - Run the script to create the storage integration
   - Execute `DESC STORAGE INTEGRATION saas_s3_integration;` and copy the `STORAGE_AWS_EXTERNAL_ID`
   - Add this External ID to your IAM role's trust policy in AWS

2. **Create External Stage**
   - Open `sql/01_ingestion_setup/01_create_stage.sql`
   - Update the S3 bucket path if needed
   - Run the script to create the stage

3. **Create File Format**
   - Open `sql/01_ingestion_setup/02_create_file_format.sql`
   - Run the script

4. **Upload Data to S3**
   ```bash
   aws s3 cp data/social_media_part_ad.csv s3://your-bucket/raw/
   ```

5. **Verify Connection**
   ```sql
   LIST @SAAS_ANALYTICS.BRONZE.RAW_STAGE;
   ```

6. **Execute SQL Scripts**
   - Connect to your Snowflake account
   - Run scripts in the order specified below using the Snowflake Web UI or SnowSQL CLI

### Option 2: Internal Stage (Development Only)

**Quick local testing without cloud storage**

```sql
-- Create internal stage
CREATE OR REPLACE STAGE SAAS_ANALYTICS.BRONZE.RAW_STAGE_INTERNAL
FILE_FORMAT = SAAS_ANALYTICS.COMMON.CSV_FORMAT;

-- Upload from local machine
PUT file:///path/to/social_media_part_ad.csv @SAAS_ANALYTICS.BRONZE.RAW_STAGE_INTERNAL;

-- Load data
COPY INTO SAAS_ANALYTICS.BRONZE.SOCIAL_MEDIA_USERS_RAW
FROM @SAAS_ANALYTICS.BRONZE.RAW_STAGE_INTERNAL;
```

⚠️ **Note**: Internal stages are not recommended for production. Use external stages with storage integrations for enterprise deployments.

3. **Verify Installation**
   ```sql
   SELECT COUNT(*) FROM SAAS_ANALYTICS.BRONZE.SOCIAL_MEDIA_USERS_RAW;
   SELECT COUNT(*) FROM SAAS_ANALYTICS.SILVER.SOCIAL_MEDIA_USERS_CLEAN;
   SELECT * FROM SAAS_ANALYTICS.GOLD.TENANT_ENGAGEMENT_METRICS;
   ```

## Configuration

### Warehouse Settings
- **Name**: SAAS_WH
- **Size**: XSMALL (adjustable based on workload)
- **Auto-Suspend**: 60 minutes
- **Auto-Resume**: Enabled

### Task Schedule
- **Frequency**: Hourly (CRON: `0 * * * * UTC`)
- **Trigger**: Automatically runs Bronze-to-Silver transformations on schedule
- **Status**: Enabled (RESUME command included in script)

## Data Quality & Validation

- **TRY_TO_* Functions**: All type conversions use safe functions to prevent load failures
- **NULL Handling**: Conversion failures result in NULL values (no data loss)
- **Metadata Tracking**: All records include source_file_name and load_timestamp for audit trails
- **Incremental Loading**: Change Data Capture prevents duplicate processing

## Current Status
✅ All SQL scripts complete and validated  
✅ Complete schema matching with 58-column dataset  
✅ All dependencies resolved (schemas, roles, etc.)  
✅ Production-ready for Snowflake deployment  
✅ Data governance policies implemented  
✅ Real-time pipeline orchestration configured  

## Next Steps
- Deploy to your Snowflake environment using the execution order above
- Configure external stage with actual S3/Azure Blob/GCS credentials
- Customize masking and RLS policies based on your security requirements
- Monitor task execution and performance metrics
