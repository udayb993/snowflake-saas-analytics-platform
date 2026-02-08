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
    CURRENT_TIMESTAMP() AS last_updated
FROM SAAS_ANALYTICS.SILVER.SOCIAL_MEDIA_USERS_CLEAN
GROUP BY tenant_id;