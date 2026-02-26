USE ROLE SYSADMIN;

-- ====================================================================================
-- STORED PROCEDURE: LOAD_BRONZE_DATA
-- ====================================================================================
-- Loads data from S3 stage into bronze layer with simple error logging
-- ====================================================================================

CREATE OR REPLACE PROCEDURE SAAS_ANALYTICS.BRONZE.LOAD_BRONZE_DATA()
RETURNS VARCHAR
LANGUAGE SQL
AS
$$
BEGIN
    -- Log start
    LET v_run_id INT := (CALL SAAS_ANALYTICS.ORCHESTRATION.LOG_START('LOAD_BRONZE_DATA'));

    -- Load data
    COPY INTO SAAS_ANALYTICS.BRONZE.SOCIAL_MEDIA_USERS_RAW
    FROM (
        SELECT
            $1, $2, $3, $4, $5, $6, $7, $8, $9, $10,
            $11, $12, $13, $14, $15, $16, $17, $18, $19, $20,
            $21, $22, $23, $24, $25, $26, $27, $28, $29, $30,
            $31, $32, $33, $34, $35, $36, $37, $38, $39, $40,
            $41, $42, $43, $44, $45, $46, $47, $48, $49, $50,
            $51, $52, $53, $54, $55, $56, $57, $58,
            METADATA$FILENAME,
            CURRENT_TIMESTAMP()
        FROM @SAAS_ANALYTICS.BRONZE.RAW_STAGE/SOCIAL_MEDIA_USERS_RAW/
    )
    FILE_FORMAT = SAAS_ANALYTICS.COMMON.CSV_FORMAT;
    
    -- Log success
    CALL SAAS_ANALYTICS.ORCHESTRATION.LOG_SUCCESS(v_run_id);
    
    RETURN 'SUCCESS';
    
EXCEPTION
    WHEN STATEMENT_ERROR THEN
        CALL SAAS_ANALYTICS.ORCHESTRATION.LOG_FAILED(v_run_id, SQLCODE, SQLERRM);
        RETURN 'FAILED: ' || SQLERRM;
END;
$$;