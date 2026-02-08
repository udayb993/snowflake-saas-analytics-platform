CREATE TABLE IF NOT EXISTS SAAS_ANALYTICS.BRONZE.SOCIAL_MEDIA_USERS_RAW (
    user_id                 STRING,
    age                     STRING,
    gender                  STRING,
    country                 STRING,
    platform                STRING,
    daily_usage_minutes     STRING,
    posts_per_day           STRING,
    likes_per_day           STRING,
    comments_per_day        STRING,
    messages_sent_per_day   STRING,
    account_created_date    STRING,
    last_active_date        STRING,
    engagement_score        STRING,
    device_type             STRING,
    source_file_name        STRING,
    load_timestamp          TIMESTAMP DEFAULT CURRENT_TIMESTAMP()
);