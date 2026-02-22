

-- Create role to tenant mapping table for flexible RLS
CREATE TABLE IF NOT EXISTS SAAS_ANALYTICS.GOVERNANCE.ROLE_TENANT_MAPPING (
    role_name STRING,
    tenant_id STRING,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP()
);
