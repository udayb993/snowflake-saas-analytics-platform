# Snowflake SaaS Analytics Platform

## Project Overview
This project demonstrates the design and implementation of a Snowflake-based SaaS analytics platform. It focuses on multi-tenant data modeling, incremental data ingestion, and analytics pipelines at scale. The project uses public datasets for reproducibility.

## Repository Structure
- `data/` - Raw datasets (e.g., Kaggle CSVs)
- `sql/00_setup/` - Database, schema, and warehouse creation scripts
- `sql/01_ingestion/` - Scripts to load raw data (Bronze layer)
- `sql/02_transformations/` - Scripts to transform data from Bronze → Silver → Gold
- `sql/03_governance/` - Row-level security, masking, and roles
- `sql/04_metrics/` - Analytics queries and dashboards
- `architecture/` - Diagrams of Snowflake setup and pipelines
- `tasks_and_streams/` - Stream and Task scripts for incremental pipelines

## Data Source
The data for this project is sourced from public datasets (e.g., Kaggle) to simulate real-world SaaS events, users, and transactions.

## Current Status
- Repository initialized
- Folder structure created
- Day 1 goal: raw data tables (Bronze layer) will be loaded into Snowflake and validated
