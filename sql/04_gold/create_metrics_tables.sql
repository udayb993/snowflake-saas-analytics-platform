CREATE OR REPLACE TABLE SAAS_ANALYTICS.GOLD.TENANT_ENGAGEMENT_METRICS AS
SELECT
    tenant_id,
    COUNT(DISTINCT user_id) AS total_users,
    AVG(daily_usage_minutes) AS avg_daily_usage,
    AVG(posts_per_day) AS avg_posts_per_day,
    AVG(engagement_score) AS avg_engagement_score
FROM SAAS_ANALYTICS.SILVER.SOCIAL_MEDIA_USERS_CLEAN
GROUP BY tenant_id;