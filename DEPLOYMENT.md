# Environment-Aware Deployment Guide

## Overview

The deployment system now supports **three environments** (dev, qa, prod) with automatic database name substitution:

- **dev** → `SAAS_ANALYTICS_DEV`
- **qa** → `SAAS_ANALYTICS_QA`
- **prod** → `SAAS_ANALYTICS_PROD`

All schemas, roles, tables, and procedures remain the same across environments. Only the database name changes.

---

## Quick Start

### Option 1: Python Script (Recommended) ✅

```bash
# Dry-run (preview what will be deployed)
python3 deploy.py dev --dry-run

# Deploy to dev environment
python3 deploy.py dev

# Deploy to qa environment
python3 deploy.py qa

# Deploy to prod environment (requires confirmation)
python3 deploy.py prod
```

### Option 2: Bash Script

```bash
# Deploy to dev
./deploy.sh dev

# Deploy to qa
./deploy.sh qa

# Deploy to prod (requires confirmation)
./deploy.sh prod
```

---

## Setup Requirements

### 1. Install Snowflake CLI

```bash
# Using pip
pip install snowflake-cli-labs

# Verify installation
snow --version
```

### 2. Configure Snowflake Connection

```bash
# Add connection configuration
snow connection add

# Follow prompts:
# - Connection name: snowflake_saas (or your choice)
# - Account ID: xy12345 (e.g., az12345)
# - User: your_username
# - Password: your_password (or leave blank for prompt)
# - Warehouse: SAAS_WH
# - Database: SAAS_ANALYTICS_DEV (temporary, will be created)
# - Schema: PUBLIC (temporary)
```

### 3. Verify Connection

```bash
snow connection list
snow sql -q "SELECT 1"
```

---

## How It Works

### Database Substitution

All SQL files use `SAAS_ANALYTICS` as the placeholder. The deployment script:

1. **Reads** the SQL file
2. **Replaces** `SAAS_ANALYTICS` with environment-specific name
3. **Executes** the modified SQL

**Example:**

**Input SQL (sql/00_database_setup/03_create_schemas.sql):**
```sql
CREATE SCHEMA IF NOT EXISTS SAAS_ANALYTICS.BRONZE;
```

**For dev environment, becomes:**
```sql
CREATE SCHEMA IF NOT EXISTS SAAS_ANALYTICS_DEV.BRONZE;
```

**For prod environment, becomes:**
```sql
CREATE SCHEMA IF NOT EXISTS SAAS_ANALYTICS_PROD.BRONZE;
```

### Deployment Order

The deployment follows this strict order to ensure dependencies:

```
1. Roles (must exist first)
2. Database (must exist before schemas)
3. Schemas
4. Warehouse
5. Storage Integration
6. File Format
7. Stage
8. Bronze Tables → Bronze Streams → Bronze Procedures → Bronze Tasks
9. Silver Tables → Silver Streams → Silver Procedures → Silver Tasks
10. Gold Tables → Gold Procedures → Gold Tasks
11. Governance (Masking Policies, RLS)
```

---

## Environment-Specific Workflows

### Development (dev)

```bash
# Initial setup
python3 deploy.py dev --dry-run

# Review the SQL that will be executed
# If looks good, deploy
python3 deploy.py dev

# Load sample data
python3 load_data.py dev --sample

# Run tests
python3 test.py dev
```

### QA/Staging (qa)

```bash
# Deploy after dev testing
python3 deploy.py qa

# Run full test suite
python3 test.py qa

# Monitor for issues
snow sql -q "SHOW TASKS IN DATABASE SAAS_ANALYTICS_QA"
```

### Production (prod)

```bash
# Dry-run ALWAYS before prod
python3 deploy.py prod --dry-run

# Review carefully
# Deploy with confirmation
python3 deploy.py prod

# Verify deployment
python3 verify.py prod
```

---

## Practical Examples

### Deploy to Multiple Environments

```bash
#!/bin/bash
# Deploy to dev, qa, and prod in sequence

set -e

echo "Deploying to DEV..."
python3 deploy.py dev

echo "Deploying to QA..."
python3 deploy.py qa

echo "Deploying to PROD..."
python3 deploy.py prod

echo "✅ All environments deployed!"
```

Save as `deploy_all.sh` and run:
```bash
chmod +x deploy_all.sh
./deploy_all.sh
```

### Check Current Environment Status

```bash
#!/bin/bash
# Check status of all environments

for env in dev qa prod; do
    echo "Checking $env..."
    python3 deploy.py $env --dry-run | head -5
done
```

### Migrate Data Between Environments

```bash
#!/bin/bash
# Copy dev schema to qa for testing

SOURCE_DB="SAAS_ANALYTICS_DEV"
TARGET_DB="SAAS_ANALYTICS_QA"

snow sql -q "
  CREATE OR REPLACE TABLE ${TARGET_DB}.SILVER.SOCIAL_MEDIA_USERS_CLEAN_BACKUP AS
  SELECT * FROM ${SOURCE_DB}.SILVER.SOCIAL_MEDIA_USERS_CLEAN;
"
```

---

## Troubleshooting

### Issue: "Invalid environment"
```bash
# ❌ Wrong
python3 deploy.py development

# ✅ Correct
python3 deploy.py dev
```

### Issue: "SQL file not found"
```bash
# Ensure you're running from project root
cd /Users/udaybalerao/Documents/Learning/projects/snowflake-saas-analytics-platform
python3 deploy.py dev
```

### Issue: "Snowflake CLI not found"
```bash
# Install snowflake-cli
pip install snowflake-cli-labs

# Verify
snow --version
```

### Issue: Connection Error
```bash
# Check connections
snow connection list

# Test connection
snow sql -q "SELECT CURRENT_ACCOUNT();"

# If fails, reconfigure
snow connection add --name snowflake_saas
```

### Issue: Permission Denied
```bash
# Make scripts executable
chmod +x deploy.sh
chmod +x deploy.py
```

---

## File Structure

```
snowflake-saas-analytics-platform/
├── config/
│   └── environment.yml          (environment config)
├── deploy.sh                    (bash deployment script)
├── deploy.py                    (Python deployment script - recommended)
├── sql/
│   ├── 00_database_setup/       (database, schemas, roles)
│   ├── 01_ingestion_setup/      (storage, stages, formats)
│   ├── 02_bronze/               (raw data layer)
│   ├── 03_silver/               (cleaned data layer)
│   ├── 04_gold/                 (metrics/analytics layer)
│   └── 06_governance/           (security policies)
├── data/
│   └── social_media_part_ad.csv (sample data)
└── README.md                    (original project docs)
```

---

## Best Practices

### ✅ DO

- ✅ Always run `--dry-run` before deploying
- ✅ Deploy in order: dev → qa → prod
- ✅ Use version control (git) for all SQL changes
- ✅ Test in dev/qa before touching prod
- ✅ Keep deployment logs for audit trail
- ✅ Document any manual changes made outside deployment

### ❌ DON'T

- ❌ Modify database names in SQL files directly
- ❌ Skip the dry-run step for prod
- ❌ Deploy directly to prod without qa testing
- ❌ Run multiple environments simultaneously (can cause conflicts)
- ❌ Edit environment.yml without updating scripts

---

## Advanced: Customize Database Names

If you want different naming conventions:

**1. Edit config/environment.yml:**
```yaml
database_names:
  dev: MY_DEV_DB
  qa: MY_QA_DB
  prod: MY_PROD_DB
```

**2. Update deploy.py:**
```python
DATABASE_MAP = {
    'dev': 'MY_DEV_DB',
    'qa': 'MY_QA_DB',
    'prod': 'MY_PROD_DB'
}
```

---

## Next Steps

1. **Test dry-run:** `python3 deploy.py dev --dry-run`
2. **Deploy dev:** `python3 deploy.py dev`
3. **Verify:** Check Snowflake UI for `SAAS_ANALYTICS_DEV`
4. **Deploy qa/prod:** `python3 deploy.py qa` then `python3 deploy.py prod`
5. **Monitor:** Check task execution and data freshness

---

## Support

For issues or questions:
1. Check deployment logs
2. Review SQL file for errors
3. Verify Snowflake CLI connection
4. Check environment configuration

