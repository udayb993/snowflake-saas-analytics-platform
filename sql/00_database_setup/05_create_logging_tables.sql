USE ROLE SYSADMIN;

-- ====================================================================================
-- SEQUENCE FOR PIPELINE RUN IDS
-- ====================================================================================
-- Better for concurrent access than AUTOINCREMENT
CREATE SEQUENCE IF NOT EXISTS SAAS_ANALYTICS.ORCHESTRATION.PIPELINE_RUN_ID_SEQ
    START = 1
    INCREMENT = 1;

-- ====================================================================================
-- PIPELINE RUN HISTORY TABLE - HYBRID
-- ====================================================================================
-- HYBRID table for concurrent writes from multiple jobs
-- Tracks procedure execution with start and end timestamps
-- ====================================================================================

CREATE OR REPLACE TABLE SAAS_ANALYTICS.ORCHESTRATION.PIPELINE_RUN_HISTORY (
    run_id              INT PRIMARY KEY DEFAULT SAAS_ANALYTICS.ORCHESTRATION.PIPELINE_RUN_ID_SEQ.NEXTVAL,
    procedure_name      VARCHAR(500) NOT NULL,
    status              VARCHAR(50) NOT NULL COMMENT 'RUNNING, SUCCESS, FAILED',
    start_timestamp     TIMESTAMP_NTZ NOT NULL,
    end_timestamp       TIMESTAMP_NTZ,
    rows_affected       NUMBER,
    rows_inserted       NUMBER,
    rows_updated        NUMBER,
    error_code          VARCHAR(100),
    error_message       VARCHAR(2000)
);

