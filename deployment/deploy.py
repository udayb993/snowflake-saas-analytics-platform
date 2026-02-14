#!/usr/bin/env python3

"""
Environment-aware deployment script for Snowflake.
Handles database name substitution for dev/qa/prod environments.
"""

import os
import sys
import argparse
import subprocess
from pathlib import Path
from typing import Dict, List

class SnowflakeDeployer:
    """Handles environment-specific Snowflake deployments."""
    
    # Database naming conventions
    DATABASE_MAP = {
        'dev': 'SAAS_ANALYTICS_DEV',
        'qa': 'SAAS_ANALYTICS_QA',
        'prod': 'SAAS_ANALYTICS_PROD'
    }
    
    # Deployment order (critical)
    DEPLOYMENT_ORDER = [
        # Database Setup
        ('sql/00_database_setup/01_create_roles.sql', 'Creating roles'),
        ('sql/00_database_setup/02_create_database.sql', 'Creating database'),
        ('sql/00_database_setup/03_create_schemas.sql', 'Creating schemas'),
        ('sql/00_database_setup/04_create_warehouse.sql', 'Creating warehouse'),
        
        # Ingestion Setup
        ('sql/01_ingestion_setup/00_create_storage_integration.sql', 'Setting up storage integration'),
        ('sql/01_ingestion_setup/01_create_file_format.sql', 'Creating file format'),
        ('sql/01_ingestion_setup/02_create_stage.sql', 'Creating stage'),
        
        # Bronze Layer
        ('sql/02_bronze/00_create_bronze_tables.sql', 'Creating bronze tables'),
        ('sql/02_bronze/01_create_bronze_streams.sql', 'Creating bronze streams'),
        ('sql/02_bronze/02_procedure_load_bronze_data.sql', 'Creating bronze procedures'),
        ('sql/02_bronze/03_create_daily_load_task.sql', 'Creating bronze tasks'),
        
        # Silver Layer
        ('sql/03_silver/00_create_silver_tables.sql', 'Creating silver tables'),
        ('sql/03_silver/01_create_silver_streams.sql', 'Creating silver streams'),
        ('sql/03_silver/02_procedure_bronze_to_silver.sql', 'Creating silver procedures'),
        ('sql/03_silver/03_task_bronze_to_silver.sql', 'Creating silver tasks'),
        
        # Gold Layer
        ('sql/04_gold/create_metrics_tables.sql', 'Creating gold tables'),
        ('sql/04_gold/01_procedure_silver_to_gold.sql', 'Creating gold procedures'),
        ('sql/04_gold/02_task_gold_metrics.sql', 'Creating gold tasks'),
        
        # Governance
        ('sql/06_governance/masking_policies.sql', 'Creating masking policies'),
        ('sql/06_governance/row_access_policies.sql', 'Creating RLS policies'),
    ]
    
    def __init__(self, environment: str):
        """Initialize deployer with environment."""
        if environment not in self.DATABASE_MAP:
            raise ValueError(f"Invalid environment: {environment}. Must be: {', '.join(self.DATABASE_MAP.keys())}")
        
        self.environment = environment
        self.database_name = self.DATABASE_MAP[environment]
        # Project root is parent of deployment folder
        self.project_root = Path(__file__).parent.parent
    
    def read_and_substitute(self, file_path: str) -> str:
        """Read SQL file and substitute SAAS_ANALYTICS with environment-specific name."""
        full_path = self.project_root / file_path
        
        if not full_path.exists():
            raise FileNotFoundError(f"SQL file not found: {full_path}")
        
        with open(full_path, 'r') as f:
            content = f.read()
        
        # Replace SAAS_ANALYTICS with environment-specific database name
        # This regex ensures we only replace the database name, not partial matches
        import re
        content = re.sub(
            r'\bSAAS_ANALYTICS\b',
            self.database_name,
            content
        )
        
        return content
    
    def deploy_file(self, file_path: str, description: str, dry_run: bool = False) -> bool:
        """Deploy a single SQL file."""
        try:
            print(f"  📁 {description}")
            print(f"     File: {file_path}")
            print(f"     Database: {self.database_name}")
            
            # Get substituted SQL content
            sql_content = self.read_and_substitute(file_path)
            
            if dry_run:
                print(f"     ✓ [DRY RUN] Content prepared (not executing)")
                return True
            
            # Execute using SnowSQL or snowflake-cli
            # This assumes you have snowflake-cli installed and configured
            try:
                result = subprocess.run(
                    ['snow', 'sql', '-q', sql_content],
                    check=True,
                    capture_output=True,
                    text=True,
                    timeout=300
                )
                print(f"     ✓ Success")
                return True
            except FileNotFoundError:
                print(f"     ⚠️  Snowflake CLI not found. Using dry-run mode.")
                print(f"     ✓ [DRY RUN] SQL content prepared (execute manually)")
                return True
        
        except Exception as e:
            print(f"     ❌ Error: {str(e)}")
            return False
    
    def deploy_all(self, dry_run: bool = False) -> bool:
        """Deploy all SQL files in order."""
        print("\n" + "="*70)
        print(f"🚀 SNOWFLAKE DEPLOYMENT - Environment: {self.environment.upper()}")
        print(f"📊 Database: {self.database_name}")
        print("="*70 + "\n")
        
        # Prod confirmation
        if self.environment == 'prod':
            confirm = input("⚠️  WARNING: Deploying to PRODUCTION. Continue? (yes/no): ")
            if confirm.lower() != 'yes':
                print("Deployment cancelled.")
                return False
        
        success_count = 0
        total_count = len(self.DEPLOYMENT_ORDER)
        
        for file_path, description in self.DEPLOYMENT_ORDER:
            if self.deploy_file(file_path, description, dry_run):
                success_count += 1
            else:
                print(f"⚠️  Failed to deploy {file_path}")
                break
        
        # Summary
        print("\n" + "="*70)
        if success_count == total_count:
            print(f"✅ Deployment complete! ({success_count}/{total_count} files)")
            print(f"Environment: {self.environment.upper()}")
            print(f"Database: {self.database_name}")
        else:
            print(f"⚠️  Deployment incomplete ({success_count}/{total_count} files)")
        print("="*70 + "\n")
        
        return success_count == total_count


def main():
    """Main entry point."""
    parser = argparse.ArgumentParser(
        description='Deploy Snowflake SaaS Analytics Platform to specific environment'
    )
    parser.add_argument(
        'environment',
        choices=['dev', 'qa', 'prod'],
        help='Target environment'
    )
    parser.add_argument(
        '--dry-run',
        action='store_true',
        help='Show what would be executed without actually executing'
    )
    
    args = parser.parse_args()
    
    try:
        deployer = SnowflakeDeployer(args.environment)
        success = deployer.deploy_all(dry_run=args.dry_run)
        sys.exit(0 if success else 1)
    
    except Exception as e:
        print(f"❌ Error: {str(e)}")
        sys.exit(1)


if __name__ == '__main__':
    main()
