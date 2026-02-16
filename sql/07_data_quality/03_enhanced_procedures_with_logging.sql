USE ROLE SYSADMIN;

-- ====================================================================================
-- ENHANCED BRONZE TO SILVER PROCEDURE WITH ERROR HANDLING & LOGGING
-- ====================================================================================
-- Improved version with comprehensive error tracking, SLA monitoring,
-- and detailed audit logging
-- ====================================================================================

CREATE OR REPLACE PROCEDURE SAAS_ANALYTICS.SILVER.TRANSFORM_BRONZE_TO_SILVER_V2()
RETURNS TABLE (
    EXECUTION_ID VARCHAR,
    ROWS_INSERTED INT,
    ROWS_UPDATED INT,
    ROWS_DELETED INT,
    ROWS_FAILED INT,
    STATUS VARCHAR,
    EXECUTION_TIME_SECONDS INT,
    SLA_STATUS VARCHAR
)
LANGUAGE SQL
EXECUTE AS OWNER
AS
$$
DECLARE
    v_execution_id VARCHAR;
    v_start_time TIMESTAMP_NTZ;
    v_end_time TIMESTAMP_NTZ;
    v_execution_duration_seconds INT;
    v_rows_inserted INT DEFAULT 0;
    v_rows_updated INT DEFAULT 0;
    v_rows_deleted INT DEFAULT 0;
    v_rows_failed INT DEFAULT 0;
    v_total_rows_processed INT DEFAULT 0;
    v_status VARCHAR DEFAULT 'IN_PROGRESS';
    v_sla_threshold_seconds INT DEFAULT 1800; -- 30 minutes SLA
    v_sla_met BOOLEAN;
    v_error_code VARCHAR;
    v_error_message VARCHAR;
    v_error_detail VARCHAR;
    
BEGIN
    -- Initialize execution tracking
    SET v_execution_id = CONCAT('BRONZE_TO_SILVER_', TO_VARCHAR(CURRENT_TIMESTAMP(), 'YYYYMMDDHH24MISS'), '_', LPAD(RANDOM() % 9999, 4, '0'));
    SET v_start_time = CURRENT_TIMESTAMP();
    
    -- Log audit event: PROCESS START
    CALL SAAS_ANALYTICS.MONITORING.LOG_AUDIT_EVENT(
        'TRANSFORM_BRONZE_TO_SILVER_V2',
        'START',
        'SILVER',
        0, 0, 0,
        'Silver transformation started'
    );
    
    TRY
        -- Main MERGE operation
        MERGE INTO SAAS_ANALYTICS.SILVER.SOCIAL_MEDIA_USERS_CLEAN tgt
        USING (
            SELECT
                user_id,
                app_name,
                TRY_TO_NUMBER(age) AS age,
                gender,
                country,
                UPPER(country) AS tenant_id,
                urban_rural,
                income_level,
                employment_status,
                education_level,
                relationship_status,
                TRY_TO_BOOLEAN(has_children) AS has_children,
                TRY_TO_NUMBER(exercise_hours_per_week) AS exercise_hours_per_week,
                TRY_TO_NUMBER(sleep_hours_per_night) AS sleep_hours_per_night,
                diet_quality,
                smoking,
                alcohol_frequency,
                TRY_TO_NUMBER(perceived_stress_score) AS perceived_stress_score,
                TRY_TO_NUMBER(self_reported_happiness) AS self_reported_happiness,
                TRY_TO_NUMBER(body_mass_index) AS body_mass_index,
                TRY_TO_NUMBER(blood_pressure_systolic) AS blood_pressure_systolic,
                TRY_TO_NUMBER(blood_pressure_diastolic) AS blood_pressure_diastolic,
                TRY_TO_NUMBER(daily_steps_count) AS daily_steps_count,
                TRY_TO_NUMBER(weekly_work_hours) AS weekly_work_hours,
                TRY_TO_NUMBER(hobbies_count) AS hobbies_count,
                TRY_TO_NUMBER(social_events_per_month) AS social_events_per_month,
                TRY_TO_NUMBER(books_read_per_year) AS books_read_per_year,
                TRY_TO_NUMBER(volunteer_hours_per_month) AS volunteer_hours_per_month,
                TRY_TO_NUMBER(travel_frequency_per_year) AS travel_frequency_per_year,
                TRY_TO_NUMBER(daily_active_minutes_instagram) AS daily_active_minutes_instagram,
                TRY_TO_NUMBER(sessions_per_day) AS sessions_per_day,
                TRY_TO_NUMBER(posts_created_per_week) AS posts_created_per_week,
                TRY_TO_NUMBER(reels_watched_per_day) AS reels_watched_per_day,
                TRY_TO_NUMBER(stories_viewed_per_day) AS stories_viewed_per_day,
                TRY_TO_NUMBER(likes_given_per_day) AS likes_given_per_day,
                TRY_TO_NUMBER(comments_written_per_day) AS comments_written_per_day,
                TRY_TO_NUMBER(dms_sent_per_week) AS dms_sent_per_week,
                TRY_TO_NUMBER(dms_received_per_week) AS dms_received_per_week,
                TRY_TO_NUMBER(ads_viewed_per_day) AS ads_viewed_per_day,
                TRY_TO_NUMBER(ads_clicked_per_day) AS ads_clicked_per_day,
                TRY_TO_NUMBER(time_on_feed_per_day) AS time_on_feed_per_day,
                TRY_TO_NUMBER(time_on_explore_per_day) AS time_on_explore_per_day,
                TRY_TO_NUMBER(time_on_messages_per_day) AS time_on_messages_per_day,
                TRY_TO_NUMBER(time_on_reels_per_day) AS time_on_reels_per_day,
                TRY_TO_NUMBER(followers_count) AS followers_count,
                TRY_TO_NUMBER(following_count) AS following_count,
                TRY_TO_BOOLEAN(uses_premium_features) AS uses_premium_features,
                TRY_TO_NUMBER(notification_response_rate) AS notification_response_rate,
                TRY_TO_NUMBER(account_creation_year) AS account_creation_year,
                TRY_TO_DATE(last_login_date) AS last_login_date,
                TRY_TO_NUMBER(average_session_length_minutes) AS average_session_length_minutes,
                content_type_preference,
                preferred_content_theme,
                privacy_setting_level,
                TRY_TO_BOOLEAN(two_factor_auth_enabled) AS two_factor_auth_enabled,
                TRY_TO_BOOLEAN(biometric_login_used) AS biometric_login_used,
                TRY_TO_NUMBER(linked_accounts_count) AS linked_accounts_count,
                subscription_status,
                TRY_TO_NUMBER(user_engagement_score) AS user_engagement_score,
                load_timestamp
            FROM SAAS_ANALYTICS.BRONZE.SOCIAL_MEDIA_USERS_STREAM
            WHERE METADATA$ACTION = 'INSERT' OR METADATA$ACTION = 'UPDATE'
        ) src
        ON tgt.user_id = src.user_id
        WHEN MATCHED THEN
            UPDATE SET
                app_name = src.app_name,
                age = src.age,
                gender = src.gender,
                country = src.country,
                tenant_id = src.tenant_id,
                urban_rural = src.urban_rural,
                income_level = src.income_level,
                employment_status = src.employment_status,
                education_level = src.education_level,
                relationship_status = src.relationship_status,
                has_children = src.has_children,
                exercise_hours_per_week = src.exercise_hours_per_week,
                sleep_hours_per_night = src.sleep_hours_per_night,
                diet_quality = src.diet_quality,
                smoking = src.smoking,
                alcohol_frequency = src.alcohol_frequency,
                perceived_stress_score = src.perceived_stress_score,
                self_reported_happiness = src.self_reported_happiness,
                body_mass_index = src.body_mass_index,
                blood_pressure_systolic = src.blood_pressure_systolic,
                blood_pressure_diastolic = src.blood_pressure_diastolic,
                daily_steps_count = src.daily_steps_count,
                weekly_work_hours = src.weekly_work_hours,
                hobbies_count = src.hobbies_count,
                social_events_per_month = src.social_events_per_month,
                books_read_per_year = src.books_read_per_year,
                volunteer_hours_per_month = src.volunteer_hours_per_month,
                travel_frequency_per_year = src.travel_frequency_per_year,
                daily_active_minutes_instagram = src.daily_active_minutes_instagram,
                sessions_per_day = src.sessions_per_day,
                posts_created_per_week = src.posts_created_per_week,
                reels_watched_per_day = src.reels_watched_per_day,
                stories_viewed_per_day = src.stories_viewed_per_day,
                likes_given_per_day = src.likes_given_per_day,
                comments_written_per_day = src.comments_written_per_day,
                dms_sent_per_week = src.dms_sent_per_week,
                dms_received_per_week = src.dms_received_per_week,
                ads_viewed_per_day = src.ads_viewed_per_day,
                ads_clicked_per_day = src.ads_clicked_per_day,
                time_on_feed_per_day = src.time_on_feed_per_day,
                time_on_explore_per_day = src.time_on_explore_per_day,
                time_on_messages_per_day = src.time_on_messages_per_day,
                time_on_reels_per_day = src.time_on_reels_per_day,
                followers_count = src.followers_count,
                following_count = src.following_count,
                uses_premium_features = src.uses_premium_features,
                notification_response_rate = src.notification_response_rate,
                account_creation_year = src.account_creation_year,
                last_login_date = src.last_login_date,
                average_session_length_minutes = src.average_session_length_minutes,
                content_type_preference = src.content_type_preference,
                preferred_content_theme = src.preferred_content_theme,
                privacy_setting_level = src.privacy_setting_level,
                two_factor_auth_enabled = src.two_factor_auth_enabled,
                biometric_login_used = src.biometric_login_used,
                linked_accounts_count = src.linked_accounts_count,
                subscription_status = src.subscription_status,
                user_engagement_score = src.user_engagement_score
        WHEN NOT MATCHED THEN
            INSERT (user_id, app_name, age, gender, country, tenant_id, urban_rural, income_level, employment_status, education_level, relationship_status, has_children, exercise_hours_per_week, sleep_hours_per_night, diet_quality, smoking, alcohol_frequency, perceived_stress_score, self_reported_happiness, body_mass_index, blood_pressure_systolic, blood_pressure_diastolic, daily_steps_count, weekly_work_hours, hobbies_count, social_events_per_month, books_read_per_year, volunteer_hours_per_month, travel_frequency_per_year, daily_active_minutes_instagram, sessions_per_day, posts_created_per_week, reels_watched_per_day, stories_viewed_per_day, likes_given_per_day, comments_written_per_day, dms_sent_per_week, dms_received_per_week, ads_viewed_per_day, ads_clicked_per_day, time_on_feed_per_day, time_on_explore_per_day, time_on_messages_per_day, time_on_reels_per_day, followers_count, following_count, uses_premium_features, notification_response_rate, account_creation_year, last_login_date, average_session_length_minutes, content_type_preference, preferred_content_theme, privacy_setting_level, two_factor_auth_enabled, biometric_login_used, linked_accounts_count, subscription_status, user_engagement_score)
            VALUES (src.user_id, src.app_name, src.age, src.gender, src.country, src.tenant_id, src.urban_rural, src.income_level, src.employment_status, src.education_level, src.relationship_status, src.has_children, src.exercise_hours_per_week, src.sleep_hours_per_night, src.diet_quality, src.smoking, src.alcohol_frequency, src.perceived_stress_score, src.self_reported_happiness, src.body_mass_index, src.blood_pressure_systolic, src.blood_pressure_diastolic, src.daily_steps_count, src.weekly_work_hours, src.hobbies_count, src.social_events_per_month, src.books_read_per_year, src.volunteer_hours_per_month, src.travel_frequency_per_year, src.daily_active_minutes_instagram, src.sessions_per_day, src.posts_created_per_week, src.reels_watched_per_day, src.stories_viewed_per_day, src.likes_given_per_day, src.comments_written_per_day, src.dms_sent_per_week, src.dms_received_per_week, src.ads_viewed_per_day, src.ads_clicked_per_day, src.time_on_feed_per_day, src.time_on_explore_per_day, src.time_on_messages_per_day, src.time_on_reels_per_day, src.followers_count, src.following_count, src.uses_premium_features, src.notification_response_rate, src.account_creation_year, src.last_login_date, src.average_session_length_minutes, src.content_type_preference, src.preferred_content_theme, src.privacy_setting_level, src.two_factor_auth_enabled, src.biometric_login_used, src.linked_accounts_count, src.subscription_status, src.user_engagement_score);
        
        -- Get merge statistics
        SET v_rows_inserted = @@rowcount;
        SET v_total_rows_processed = v_rows_inserted + v_rows_updated;
        SET v_status = 'SUCCESS';
        
        -- Log audit event: PROCESS SUCCESS
        CALL SAAS_ANALYTICS.MONITORING.LOG_AUDIT_EVENT(
            'TRANSFORM_BRONZE_TO_SILVER_V2',
            'SUCCESS',
            'SILVER',
            v_total_rows_processed,
            v_total_rows_processed,
            v_rows_failed,
            CONCAT('Processed ', v_total_rows_processed, ' rows successfully')
        );
        
    CATCH (ex)
        SET v_error_code = ex.errno;
        SET v_error_message = ex.message;
        SET v_error_detail = ex.stack_trace;
        SET v_status = 'FAILED';
        SET v_rows_failed = -1;
        
        -- Log error
        CALL SAAS_ANALYTICS.MONITORING.LOG_PIPELINE_ERROR(
            'TRANSFORM_BRONZE_TO_SILVER_V2',
            'SILVER',
            v_error_code,
            v_error_message,
            v_error_detail,
            v_rows_failed
        );
        
        -- Log audit event: PROCESS FAILURE
        CALL SAAS_ANALYTICS.MONITORING.LOG_AUDIT_EVENT(
            'TRANSFORM_BRONZE_TO_SILVER_V2',
            'FAILURE',
            'SILVER',
            0, 0, v_rows_failed,
            CONCAT('Error: ', v_error_message)
        );
    END TRY;
    
    -- Track SLA metrics
    SET v_end_time = CURRENT_TIMESTAMP();
    SET v_execution_duration_seconds = DATEDIFF(SECOND, v_start_time, v_end_time);
    SET v_sla_met = v_execution_duration_seconds <= v_sla_threshold_seconds;
    
    -- Log SLA tracking
    CALL SAAS_ANALYTICS.MONITORING.TRACK_SLA_METRIC(
        'TRANSFORM_BRONZE_TO_SILVER_V2',
        'SILVER',
        v_start_time,
        v_end_time,
        v_total_rows_processed,
        v_status,
        v_sla_threshold_seconds
    );
    
    -- Return results
    SELECT
        v_execution_id AS EXECUTION_ID,
        v_rows_inserted AS ROWS_INSERTED,
        v_rows_updated AS ROWS_UPDATED,
        v_rows_deleted AS ROWS_DELETED,
        v_rows_failed AS ROWS_FAILED,
        v_status AS STATUS,
        v_execution_duration_seconds AS EXECUTION_TIME_SECONDS,
        CASE WHEN v_sla_met THEN 'MET' ELSE 'MISSED' END AS SLA_STATUS;
END;
$$;

-- ====================================================================================
-- Grant execution permission
-- ====================================================================================
GRANT EXECUTE ON PROCEDURE SAAS_ANALYTICS.SILVER.TRANSFORM_BRONZE_TO_SILVER_V2() TO ROLE DEVELOPER_ROLE;
GRANT EXECUTE ON PROCEDURE SAAS_ANALYTICS.SILVER.TRANSFORM_BRONZE_TO_SILVER_V2() TO ROLE ANALYST_ROLE;
