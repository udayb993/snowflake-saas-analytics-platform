CREATE OR REPLACE TABLE SAAS_ANALYTICS.SILVER.SOCIAL_MEDIA_USERS_CLEAN (
    user_id                 STRING,
    age                     NUMBER,
    gender                  STRING,
    country                 STRING,
    tenant_id               STRING,
    platform                STRING,
    daily_usage_minutes     NUMBER,
    posts_per_day           NUMBER,
    likes_per_day           NUMBER,
    comments_per_day        NUMBER,
    messages_sent_per_day   NUMBER,
    account_created_date    DATE,
    last_active_date        DATE,
    engagement_score        FLOAT,
    device_type             STRING,
    load_timestamp          TIMESTAMP
);