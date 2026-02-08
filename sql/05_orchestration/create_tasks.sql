CREATE OR REPLACE TASK SAAS_ANALYTICS.ORCHESTRATION.TRANSFORM_BRONZE_TO_SILVER
WAREHOUSE = SAAS_WH
SCHEDULE = 'USING CRON 0 * * * * UTC'  -- runs every hour
AS
INSERT INTO SAAS_ANALYTICS.SILVER.SOCIAL_MEDIA_USERS_CLEAN
SELECT
    user_id,
    TRY_TO_NUMBER(age),
    gender,
    country,
    UPPER(country) AS tenant_id,
    platform,
    TRY_TO_NUMBER(daily_usage_minutes),
    TRY_TO_NUMBER(posts_per_day),
    TRY_TO_NUMBER(likes_per_day),
    TRY_TO_NUMBER(comments_per_day),
    TRY_TO_NUMBER(messages_sent_per_day),
    TRY_TO_DATE(account_created_date),
    TRY_TO_DATE(last_active_date),
    TRY_TO_DOUBLE(engagement_score),
    device_type,
    load_timestamp
FROM SAAS_ANALYTICS.BRONZE.SOCIAL_MEDIA_USERS_STREAM;


ALTER TASK SAAS_ANALYTICS.ORCHESTRATION.TRANSFORM_BRONZE_TO_SILVER RESUME;