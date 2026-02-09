USE ROLE SYSADMIN;

CREATE OR REPLACE TABLE SAAS_ANALYTICS.GOLD.TENANT_ENGAGEMENT_METRICS AS
SELECT
    tenant_id,
    COUNT(DISTINCT user_id) AS total_users,
    AVG(CAST(daily_active_minutes_instagram AS NUMBER)) AS avg_daily_usage_minutes,
    AVG(CAST(posts_created_per_week AS NUMBER)) AS avg_posts_per_week,
    AVG(CAST(likes_given_per_day AS NUMBER)) AS avg_likes_per_day,
    AVG(CAST(followers_count AS NUMBER)) AS avg_followers,
    AVG(CAST(user_engagement_score AS NUMBER)) AS avg_engagement_score,
    COUNT(CASE WHEN subscription_status = 'active' THEN 1 END) AS active_subscriptions,
    CURRENT_TIMESTA

-- ====================================================================================
-- GRANT PRIVILEGES ON GOLD TABLE
-- ====================================================================================
-- Grant read access to analysts and QA roles
GRANT SELECT ON TABLE SAAS_ANALYTICS.GOLD.TENANT_ENGAGEMENT_METRICS TO ROLE ANALYST_ROLE;
GRANT SELECT ON TABLE SAAS_ANALYTICS.GOLD.TENANT_ENGAGEMENT_METRICS TO ROLE QA_ROLE;

-- Grant all privileges to developers
GRANT ALL PRIVILEGES ON TABLE SAAS_ANALYTICS.GOLD.TENANT_ENGAGEMENT_METRICS TO ROLE DEVELOPER_ROLE;MP() AS last_updated
FROM SAAS_ANALYTICS.SILVER.SOCIAL_MEDIA_USERS_CLEAN
GROUP BY tenant_id;