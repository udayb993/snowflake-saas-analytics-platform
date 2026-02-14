# Deployment Scripts

Environment-aware deployment for Snowflake (dev/qa/prod).

## Quick Start

```bash
# From project root:
cd deployment

# Dry-run
python3 deploy.py dev --dry-run

# Deploy to dev
python3 deploy.py dev

# Deploy to qa
python3 deploy.py qa

# Deploy to prod (requires confirmation)
python3 deploy.py prod
```

## Files

- **deploy.py** - Main deployment script (Python) ✅ Recommended
- **deploy.sh** - Bash alternative
- **config/environment.yml** - Environment configuration
- **DEPLOYMENT.md** - Detailed guide

## Usage

```bash
# From project root or deployment folder
python3 deployment/deploy.py [dev|qa|prod]
./deployment/deploy.sh [dev|qa|prod]
```

## What It Does

1. Reads environment parameter (dev, qa, prod)
2. Substitutes `SAAS_ANALYTICS` → `SAAS_ANALYTICS_DEV/QA/PROD`
3. Deploys SQL files in correct order
4. Logs all operations

## Database Naming

| Environment | Database Name |
|-------------|---------------|
| dev | SAAS_ANALYTICS_DEV |
| qa | SAAS_ANALYTICS_QA |
| prod | SAAS_ANALYTICS_PROD |

All schemas remain the same: BRONZE, SILVER, GOLD, COMMON, GOVERNANCE, ORCHESTRATION

## For More Details

Read [DEPLOYMENT.md](DEPLOYMENT.md) for comprehensive guide.
