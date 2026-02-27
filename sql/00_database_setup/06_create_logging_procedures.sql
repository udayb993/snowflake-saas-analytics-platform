USE ROLE SYSADMIN;

-- ====================================================================================
-- SIMPLE LOGGING PROCEDURES
-- ====================================================================================

-- Log procedure start (uses sequence for concurrent safety)
CREATE OR REPLACE PROCEDURE SAAS_ANALYTICS.ORCHESTRATION.LOG_START(p_procedure_name VARCHAR)
RETURNS INT
LANGUAGE SQL
AS
$$
DECLARE
    v_run_id INT;
BEGIN
    -- Use SEQUENCE for thread-safe ID generation
    SELECT SAAS_ANALYTICS.ORCHESTRATION.PIPELINE_RUN_ID_SEQ.NEXTVAL INTO :v_run_id;
    
    INSERT INTO SAAS_ANALYTICS.ORCHESTRATION.PIPELINE_RUN_HISTORY 
        (run_id, procedure_name, status, start_timestamp) 
    VALUES 
        (:v_run_id, :p_procedure_name, 'RUNNING', CURRENT_TIMESTAMP());
    
    RETURN v_run_id;
END;
$$;


-- Log procedure success (sets end_timestamp)
CREATE OR REPLACE PROCEDURE SAAS_ANALYTICS.ORCHESTRATION.LOG_SUCCESS(
    p_run_id INT,
    p_rows_affected NUMBER DEFAULT NULL,
    p_rows_inserted NUMBER DEFAULT NULL,
    p_rows_updated NUMBER DEFAULT NULL
)
RETURNS VARCHAR
LANGUAGE SQL
AS
$$
    UPDATE SAAS_ANALYTICS.ORCHESTRATION.PIPELINE_RUN_HISTORY
    SET status = 'SUCCESS',
        end_timestamp = CURRENT_TIMESTAMP(),
        rows_affected = :p_rows_affected,
        rows_inserted = :p_rows_inserted,
        rows_updated = :p_rows_updated
    WHERE run_id = :p_run_id;
$$;


-- Log procedure failure (sets end_timestamp)
CREATE OR REPLACE PROCEDURE SAAS_ANALYTICS.ORCHESTRATION.LOG_FAILED(
    p_run_id INT,
    p_error_code VARCHAR,
    p_error_message VARCHAR
)
RETURNS VARCHAR
LANGUAGE SQL
AS
$$
    UPDATE SAAS_ANALYTICS.ORCHESTRATION.PIPELINE_RUN_HISTORY
    SET status = 'FAILED',
        end_timestamp = CURRENT_TIMESTAMP(),
        error_code = :p_error_code,
        error_message = :p_error_message
    WHERE run_id = :p_run_id;
$$;
