USE ROLE SYSADMIN;

-- ====================================================================================
-- CREATE GOLD LAYER METRICS TABLES
-- ====================================================================================
-- These tables are populated by procedures with MERGE logic for idempotent,
-- incremental updates. This is production-ready and supports scheduled tasks.
-- ====================================================================================

-- Table 1: Tenant Engagement Metrics (aggregated by tenant)
CREATE TABLE IF NOT EXISTS SAAS_ANALYTICS.GOLD.TENANT_ENGAGEMENT_METRICS (
    tenant_id                    STRING,
    total_users                  NUMBER,
    avg_daily_usage_minutes      NUMBER,
    avg_posts_per_week           NUMBER,
    avg_likes_per_day            NUMBER,
    avg_followers                NUMBER,
    avg_engagement_score         NUMBER,
    active_subscriptions         NUMBER,
    last_updated                 TIMESTAMP DEFAULT CURRENT_TIMESTAMP(),
    PRIMARY KEY (tenant_id)
);

-- Table 2: User Engagement Snapshot (for cohort analysis)
CREATE TABLE IF NOT EXISTS SAAS_ANALYTICS.GOLD.USER_ENGAGEMENT_SNAPSHOT (
    user_id                      STRING,
    tenant_id                    STRING,
    engagement_level             STRING,  -- HIGH, MEDIUM, LOW, INACTIVE
    last_active_date             DATE,
    daily_avg_usage_minutes      NUMBER,
    weekly_post_count            NUMBER,
    subscriber                   BOOLEAN,
    snapshot_date                DATE DEFAULT CURRENT_DATE(),
    PRIMARY KEY (user_id, snapshot_date)
);

-- Table 3: Content Performance Metrics
CREATE TABLE IF NOT EXISTS SAAS_ANALYTICS.GOLD.CONTENT_PERFORMANCE_METRICS (
    tenant_id                    STRING,
    content_type_preference      STRING,
    avg_likes_per_interaction    NUMBER,
    avg_comments_per_interaction NUMBER,
    avg_reels_watched            NUMBER,
    avg_stories_viewed           NUMBER,
    snapshot_date                DATE DEFAULT CURRENT_DATE(),
    last_updated                 TIMESTAMP DEFAULT CURRENT_TIMESTAMP(),
    PRIMARY KEY (tenant_id, content_type_preference, snapshot_date)
);

-- ====================================================================================
-- GRANT PRIVILEGES ON GOLD TABLES
-- ====================================================================================
-- Grant read access to analysts and QA roles
GRANT SELECT ON TABLE SAAS_ANALYTICS.GOLD.TENANT_ENGAGEMENT_METRICS TO ROLE ANALYST_ROLE;
GRANT SELECT ON TABLE SAAS_ANALYTICS.GOLD.TENANT_ENGAGEMENT_METRICS TO ROLE QA_ROLE;

GRANT SELECT ON TABLE SAAS_ANALYTICS.GOLD.USER_ENGAGEMENT_SNAPSHOT TO ROLE ANALYST_ROLE;
GRANT SELECT ON TABLE SAAS_ANALYTICS.GOLD.USER_ENGAGEMENT_SNAPSHOT TO ROLE QA_ROLE;

GRANT SELECT ON TABLE SAAS_ANALYTICS.GOLD.CONTENT_PERFORMANCE_METRICS TO ROLE ANALYST_ROLE;
GRANT SELECT ON TABLE SAAS_ANALYTICS.GOLD.CONTENT_PERFORMANCE_METRICS TO ROLE QA_ROLE;

-- Grant all privileges to developers
GRANT ALL PRIVILEGES ON TABLE SAAS_ANALYTICS.GOLD.TENANT_ENGAGEMENT_METRICS TO ROLE DEVELOPER_ROLE;
GRANT ALL PRIVILEGES ON TABLE SAAS_ANALYTICS.GOLD.USER_ENGAGEMENT_SNAPSHOT TO ROLE DEVELOPER_ROLE;
GRANT ALL PRIVILEGES ON TABLE SAAS_ANALYTICS.GOLD.CONTENT_PERFORMANCE_METRICS TO ROLE DEVELOPER_ROLE;