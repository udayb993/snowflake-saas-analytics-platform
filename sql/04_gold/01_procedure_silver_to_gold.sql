USE ROLE SYSADMIN;

-- ====================================================================================
-- STORED PROCEDURES: SILVER TO GOLD TRANSFORMATIONS
-- ====================================================================================
-- These procedures populate gold layer tables using MERGE for idempotent, incremental
-- updates. They can be safely called by scheduled tasks and re-run without data loss.
-- ====================================================================================

-- ==================================================================================
-- PROCEDURE 1: LOAD_TENANT_ENGAGEMENT_METRICS
-- ==================================================================================
-- Loads aggregated tenant-level engagement metrics from Silver layer
-- Uses MERGE for upsert (insert new tenants, update existing ones)
-- ====================================================================================

CREATE OR REPLACE PROCEDURE SAAS_ANALYTICS.GOLD.LOAD_TENANT_ENGAGEMENT_METRICS()
RETURNS VARCHAR
LANGUAGE SQL
AS
$$
DECLARE
    v_run_id        INT DEFAULT NULL;
    v_rows_inserted INT DEFAULT NULL;
    v_rows_updated  INT DEFAULT NULL;
BEGIN
   -- Log start
    CALL SAAS_ANALYTICS.ORCHESTRATION.LOG_START('TRANSFORM_BRONZE_TO_SILVER') INTO :v_run_id;
    
     -- MERGE into tenant engagement metrics table
    MERGE INTO SAAS_ANALYTICS.GOLD.TENANT_ENGAGEMENT_METRICS tgt
    USING (
        SELECT
            tenant_id,
            COUNT(DISTINCT user_id) AS total_users,
            AVG(CAST(daily_active_minutes_instagram AS NUMBER)) AS avg_daily_usage_minutes,
            AVG(CAST(posts_created_per_week AS NUMBER)) AS avg_posts_per_week,
            AVG(CAST(likes_given_per_day AS NUMBER)) AS avg_likes_per_day,
            AVG(CAST(followers_count AS NUMBER)) AS avg_followers,
            AVG(CAST(user_engagement_score AS NUMBER)) AS avg_engagement_score,
            COUNT(CASE WHEN subscription_status = 'active' THEN 1 END) AS active_subscriptions
        FROM SAAS_ANALYTICS.SILVER.SOCIAL_MEDIA_USERS_CLEAN
        GROUP BY tenant_id
    ) src
    ON tgt.tenant_id = src.tenant_id
    WHEN MATCHED THEN
        UPDATE SET
            total_users = src.total_users,
            avg_daily_usage_minutes = src.avg_daily_usage_minutes,
            avg_posts_per_week = src.avg_posts_per_week,
            avg_likes_per_day = src.avg_likes_per_day,
            avg_followers = src.avg_followers,
            avg_engagement_score = src.avg_engagement_score,
            active_subscriptions = src.active_subscriptions,
            last_updated = CURRENT_TIMESTAMP()
    WHEN NOT MATCHED THEN
        INSERT (
            tenant_id,
            total_users,
            avg_daily_usage_minutes,
            avg_posts_per_week,
            avg_likes_per_day,
            avg_followers,
            avg_engagement_score,
            active_subscriptions,
            last_updated
        )
        VALUES (
            src.tenant_id,
            src.total_users,
            src.avg_daily_usage_minutes,
            src.avg_posts_per_week,
            src.avg_likes_per_day,
            src.avg_followers,
            src.avg_engagement_score,
            src.active_subscriptions,
            CURRENT_TIMESTAMP()
        );

    SELECT "number of rows inserted", "number of rows updated"
     INTO :v_rows_inserted, :v_rows_updated
     FROM TABLE(RESULT_SCAN(LAST_QUERY_ID()));
    
    -- Log success
    CALL SAAS_ANALYTICS.ORCHESTRATION.LOG_SUCCESS(
        :v_run_id,
        :v_rows_inserted + :v_rows_updated,
        :v_rows_inserted,
        :v_rows_updated
    );

    RETURN 'SUCCESS';
    
EXCEPTION
    WHEN STATEMENT_ERROR THEN
        CALL SAAS_ANALYTICS.ORCHESTRATION.LOG_FAILED(:v_run_id, :SQLCODE, :SQLERRM);
        RETURN 'FAILED: ' || SQLERRM;
END;
$$;

-- ==================================================================================
-- PROCEDURE 2: LOAD_USER_ENGAGEMENT_SNAPSHOT
-- ==================================================================================
-- Creates daily snapshots of user engagement levels for cohort analysis
-- Classifies users as HIGH, MEDIUM, LOW, INACTIVE based on engagement_score
-- ====================================================================================

CREATE OR REPLACE PROCEDURE SAAS_ANALYTICS.GOLD.LOAD_USER_ENGAGEMENT_SNAPSHOT()
RETURNS VARCHAR
LANGUAGE SQL
AS
$$
DECLARE
    v_run_id        INT DEFAULT NULL;
    v_rows_inserted INT DEFAULT NULL;
    v_rows_updated  INT DEFAULT NULL;
BEGIN
   -- Log start
    CALL SAAS_ANALYTICS.ORCHESTRATION.LOG_START('TRANSFORM_BRONZE_TO_SILVER') INTO :v_run_id;
    
    -- Insert today's user engagement snapshot
    INSERT INTO SAAS_ANALYTICS.GOLD.USER_ENGAGEMENT_SNAPSHOT (
        user_id,
        tenant_id,
        engagement_level,
        last_active_date,
        daily_avg_usage_minutes,
        weekly_post_count,
        subscriber,
        snapshot_date
    )
    SELECT
        user_id,
        tenant_id,
        CASE
            WHEN CAST(user_engagement_score AS NUMBER) >= 80 THEN 'HIGH'
            WHEN CAST(user_engagement_score AS NUMBER) >= 50 THEN 'MEDIUM'
            WHEN CAST(user_engagement_score AS NUMBER) >= 20 THEN 'LOW'
            ELSE 'INACTIVE'
        END AS engagement_level,
        last_login_date,
        CAST(daily_active_minutes_instagram AS NUMBER),
        CAST(posts_created_per_week AS NUMBER),
        CAST(subscription_status AS STRING) = 'active',
        CURRENT_DATE()
    FROM SAAS_ANALYTICS.SILVER.SOCIAL_MEDIA_USERS_CLEAN
    WHERE CURRENT_DATE() > (
        SELECT COALESCE(MAX(snapshot_date), CURRENT_DATE() - INTERVAL '1 day')
        FROM SAAS_ANALYTICS.GOLD.USER_ENGAGEMENT_SNAPSHOT
        WHERE user_id = SAAS_ANALYTICS.SILVER.SOCIAL_MEDIA_USERS_CLEAN.user_id
    );


    SELECT "number of rows inserted", "number of rows updated"
     INTO :v_rows_inserted, :v_rows_updated
     FROM TABLE(RESULT_SCAN(LAST_QUERY_ID()));
    
    -- Log success
    CALL SAAS_ANALYTICS.ORCHESTRATION.LOG_SUCCESS(
        :v_run_id,
        :v_rows_inserted + :v_rows_updated,
        :v_rows_inserted,
        :v_rows_updated
    );

    RETURN 'SUCCESS';
    
EXCEPTION
    WHEN STATEMENT_ERROR THEN
        CALL SAAS_ANALYTICS.ORCHESTRATION.LOG_FAILED(:v_run_id, :SQLCODE, :SQLERRM);
        RETURN 'FAILED: ' || SQLERRM;

END;
$$;

-- ==================================================================================
-- PROCEDURE 3: LOAD_CONTENT_PERFORMANCE_METRICS
-- ==================================================================================
-- Aggregates content performance metrics by tenant and content type preference
-- Tracks engagement with different content types (reels, stories, feed, etc.)
-- ====================================================================================

CREATE OR REPLACE PROCEDURE SAAS_ANALYTICS.GOLD.LOAD_CONTENT_PERFORMANCE_METRICS()
RETURNS VARCHAR
LANGUAGE SQL
AS
$$
DECLARE
    v_run_id        INT DEFAULT NULL;
    v_rows_inserted INT DEFAULT NULL;
    v_rows_updated  INT DEFAULT NULL;
BEGIN
   -- Log start
    CALL SAAS_ANALYTICS.ORCHESTRATION.LOG_START('TRANSFORM_BRONZE_TO_SILVER') INTO :v_run_id;
    
    -- MERGE into content performance metrics table
    MERGE INTO SAAS_ANALYTICS.GOLD.CONTENT_PERFORMANCE_METRICS tgt
    USING (
        SELECT
            tenant_id,
            COALESCE(content_type_preference, 'UNKNOWN') AS content_type_preference,
            AVG(CAST(likes_given_per_day AS NUMBER) / 
                NULLIF(CAST(comments_written_per_day AS NUMBER) + 1, 0)) AS avg_likes_per_interaction,
            AVG(CAST(comments_written_per_day AS NUMBER)) AS avg_comments_per_interaction,
            AVG(CAST(reels_watched_per_day AS NUMBER)) AS avg_reels_watched,
            AVG(CAST(stories_viewed_per_day AS NUMBER)) AS avg_stories_viewed,
            CURRENT_DATE() AS snapshot_date
        FROM SAAS_ANALYTICS.SILVER.SOCIAL_MEDIA_USERS_CLEAN
        GROUP BY tenant_id, content_type_preference
    ) src
    ON tgt.tenant_id = src.tenant_id 
        AND tgt.content_type_preference = src.content_type_preference
        AND tgt.snapshot_date = src.snapshot_date
    WHEN MATCHED THEN
        UPDATE SET
            avg_likes_per_interaction = src.avg_likes_per_interaction,
            avg_comments_per_interaction = src.avg_comments_per_interaction,
            avg_reels_watched = src.avg_reels_watched,
            avg_stories_viewed = src.avg_stories_viewed,
            last_updated = CURRENT_TIMESTAMP()
    WHEN NOT MATCHED THEN
        INSERT (
            tenant_id,
            content_type_preference,
            avg_likes_per_interaction,
            avg_comments_per_interaction,
            avg_reels_watched,
            avg_stories_viewed,
            snapshot_date,
            last_updated
        )
        VALUES (
            src.tenant_id,
            src.content_type_preference,
            src.avg_likes_per_interaction,
            src.avg_comments_per_interaction,
            src.avg_reels_watched,
            src.avg_stories_viewed,
            src.snapshot_date,
            CURRENT_TIMESTAMP()
        );


    SELECT "number of rows inserted", "number of rows updated"
     INTO :v_rows_inserted, :v_rows_updated
     FROM TABLE(RESULT_SCAN(LAST_QUERY_ID()));
    
    -- Log success
    CALL SAAS_ANALYTICS.ORCHESTRATION.LOG_SUCCESS(
        :v_run_id,
        :v_rows_inserted + :v_rows_updated,
        :v_rows_inserted,
        :v_rows_updated
    );

    RETURN 'SUCCESS';
    
EXCEPTION
    WHEN STATEMENT_ERROR THEN
        CALL SAAS_ANALYTICS.ORCHESTRATION.LOG_FAILED(:v_run_id, :SQLCODE, :SQLERRM);
        RETURN 'FAILED: ' || SQLERRM;

END;
$$;

