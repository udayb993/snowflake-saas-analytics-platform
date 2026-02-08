-- Query to view top tenant engagement metrics
SELECT 
    tenant_id,
    total_users,
    avg_daily_usage_minutes,
    avg_posts_per_week,
    avg_likes_per_day,
    avg_followers,
    avg_engagement_score,
    active_subscriptions,
    last_updated
FROM SAAS_ANALYTICS.GOLD.TENANT_ENGAGEMENT_METRICS
ORDER BY avg_engagement_score DESC;