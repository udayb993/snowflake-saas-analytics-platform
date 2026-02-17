USE ROLE SYSADMIN;

-- ====================================================================================
-- STORED PROCEDURE: LOAD_BRONZE_DATA
-- ====================================================================================
-- This procedure loads data from the external S3 stage into the bronze layer
-- Reads all 58 columns from CSV files and adds metadata tracking
-- ====================================================================================

CREATE OR REPLACE PROCEDURE SAAS_ANALYTICS.BRONZE.LOAD_BRONZE_DATA()
RETURNS TABLE (
    ROWS_LOADED NUMBER,
    STATUS VARCHAR
)
LANGUAGE SQL
AS
$$
DECLARE
    ROWS_LOADED NUMBER DEFAULT 0;
BEGIN
    -- Load data from S3 stage into bronze table
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
        FROM @SAAS_ANALYTICS.BRONZE.RAW_STAGE
    )
    FILE_FORMAT = SAAS_ANALYTICS.COMMON.CSV_FORMAT;
    
    -- Get count of rows loaded from COPY command result
    LET rows_loaded RESULTSET := (
        SELECT COALESCE(SUM(rows_inserted),0) AS ROWS_LOADED,
               'SUCCESSFUL'::VARCHAR AS STATUS  
          FROM TABLE(RESULT_SCAN(LAST_QUERY_ID()))
    );
    
    -- Return summary
    RETURN TABLE (rows_loaded);

END;
$$;