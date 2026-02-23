USE ROLE SYSADMIN;
USE DATABASE SAAS_ANALYTICS;

-- ====================================================================================
-- DASHBOARD SCHEMA + SECURE VIEWS
-- ====================================================================================
-- Creates the DASHBOARD schema and four secure views that regional viewer roles
-- use exclusively. Viewer roles have NO direct access to SILVER or GOLD schemas.
--
-- Security model:
--   - TENANT_RLS row access policy on Silver propagates through views automatically.
--     Each viewer role only sees rows matching their ROLE_TENANT_MAPPING entries.
--   - Views are SECURE so Snowflake hides the view definition from non-owners,
--     preventing users from inferring column logic or filter conditions.
--   - PII and health columns (age, BMI, blood pressure) are excluded from
--     USER_ACTIVITY_VIEW — dashboards receive behavioural/engagement data only.
--
-- Views:
--   1. USER_ACTIVITY_VIEW          → Silver: per-user engagement & platform activity
--   2. TENANT_ENGAGEMENT_VIEW      → Gold:   aggregated metrics per tenant
--   3. USER_ENGAGEMENT_SNAPSHOT_VIEW → Gold: user cohort/engagement level snapshot
--   4. CONTENT_PERFORMANCE_VIEW    → Gold:   content type performance per tenant
--
-- Execution order:
--   1. sql/00_database_setup/01_create_roles.sql            (roles must exist)
--   2. sql/05_governance/00_create_governance_tables.sql    (governance table)
--   3. sql/05_governance/01_row_access_policies.sql         (RAP on Silver)
--   4. sql/05_governance/02_create_dashboard_views.sql      (this file)
--   5. sql/05_governance/03_insert_role_tenant_mapping.sql  (seed mapping data)
-- ====================================================================================


CREATE SCHEMA IF NOT EXISTS SAAS_ANALYTICS.DASHBOARD;


-- ====================================================================================
-- VIEW 1: USER_ACTIVITY_VIEW  (Silver → per-user engagement data)
-- ====================================================================================
-- Excludes PII and sensitive health columns:
--   age, body_mass_index, blood_pressure_systolic, blood_pressure_diastolic
--   perceived_stress_score, self_reported_happiness, smoking, alcohol_frequency
--   diet_quality, exercise_hours_per_week, sleep_hours_per_night
-- Tenant isolation is enforced automatically via TENANT_RLS on the base table.

CREATE OR REPLACE SECURE VIEW SAAS_ANALYTICS.DASHBOARD.USER_ACTIVITY_VIEW AS
SELECT
    -- Identity & segmentation (no PII)
    user_id,
    app_name,
    gender,
    country,
    tenant_id,
    urban_rural,
    income_level,
    employment_status,
    education_level,
    relationship_status,
    has_children,

    -- Lifestyle (non-sensitive)
    daily_steps_count,
    weekly_work_hours,
    hobbies_count,
    social_events_per_month,
    books_read_per_year,
    volunteer_hours_per_month,
    travel_frequency_per_year,

    -- Instagram engagement
    daily_active_minutes_instagram,
    sessions_per_day,
    posts_created_per_week,
    reels_watched_per_day,
    stories_viewed_per_day,
    likes_given_per_day,
    comments_written_per_day,
    dms_sent_per_week,
    dms_received_per_week,

    -- Ad interaction
    ads_viewed_per_day,
    ads_clicked_per_day,

    -- Time spent by section
    time_on_feed_per_day,
    time_on_explore_per_day,
    time_on_messages_per_day,
    time_on_reels_per_day,

    -- Account metrics
    followers_count,
    following_count,
    uses_premium_features,
    notification_response_rate,
    account_creation_year,
    last_login_date,
    average_session_length_minutes,

    -- Preferences & settings
    content_type_preference,
    preferred_content_theme,
    privacy_setting_level,
    two_factor_auth_enabled,
    biometric_login_used,
    linked_accounts_count,
    subscription_status,
    user_engagement_score,

    load_timestamp

FROM SAAS_ANALYTICS.SILVER.SOCIAL_MEDIA_USERS_CLEAN;


-- ====================================================================================
-- VIEW 2: TENANT_ENGAGEMENT_VIEW  (Gold → aggregated metrics per tenant)
-- ====================================================================================

CREATE OR REPLACE SECURE VIEW SAAS_ANALYTICS.DASHBOARD.TENANT_ENGAGEMENT_VIEW AS
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
FROM SAAS_ANALYTICS.GOLD.TENANT_ENGAGEMENT_METRICS;


-- ====================================================================================
-- VIEW 3: USER_ENGAGEMENT_SNAPSHOT_VIEW  (Gold → cohort/engagement level snapshot)
-- ====================================================================================

CREATE OR REPLACE SECURE VIEW SAAS_ANALYTICS.DASHBOARD.USER_ENGAGEMENT_SNAPSHOT_VIEW AS
SELECT
    user_id,
    tenant_id,
    engagement_level,
    last_active_date,
    daily_avg_usage_minutes,
    weekly_post_count,
    subscriber,
    snapshot_date
FROM SAAS_ANALYTICS.GOLD.USER_ENGAGEMENT_SNAPSHOT;


-- ====================================================================================
-- VIEW 4: CONTENT_PERFORMANCE_VIEW  (Gold → content type performance per tenant)
-- ====================================================================================

CREATE OR REPLACE SECURE VIEW SAAS_ANALYTICS.DASHBOARD.CONTENT_PERFORMANCE_VIEW AS
SELECT
    tenant_id,
    content_type_preference,
    avg_likes_per_interaction,
    avg_comments_per_interaction,
    avg_reels_watched,
    avg_stories_viewed,
    snapshot_date,
    last_updated
FROM SAAS_ANALYTICS.GOLD.CONTENT_PERFORMANCE_METRICS;


-- ====================================================================================
-- VERIFY
-- ====================================================================================

SHOW VIEWS IN SCHEMA SAAS_ANALYTICS.DASHBOARD;