-- Bronze load: store_master for one day.
-- Run by Airflow once per day. Airflow fills in the day (for example 2026-09-16) before running.

-- 1. Remove this day's rows first, so running the same day twice never creates duplicates.
DELETE FROM SNOWFLAKE_LEARNING_DB.MATINA_ETL.STG_STORE_MASTER
WHERE BATCH_DT = '{{ ds }}'::DATE;

-- 2. Copy the file from the stage into the table. Every column stays text, nothing is cleaned here.
--    $1, $2 ... means "column 1, column 2 ..." of the CSV file.
COPY INTO SNOWFLAKE_LEARNING_DB.MATINA_ETL.STG_STORE_MASTER (
    STORE,
    STORE_NAME,
    STORE_NAME10,
    STORE_TYPE,
    DISTRICT,
    CHANNEL_ID,
    CURRENCY_CODE,
    STORE_OPEN_DATE,
    STORE_CLOSE_DATE,
    COUNTRY,
    STATE,
    CITY,
    POSTAL_CODE,
    TOTAL_SQUARE_FT,
    COMP_FLAG,
    DISTRICT_NAME,
    REGION,
    REGION_NAME,
    SNAPSHOT_DT,
    BATCH_DT,
    SRC_FILE_NM
)
FROM (
    SELECT $1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13, $14, $15, $16, $17, $18, $19,
           '{{ ds }}'::DATE,
           METADATA$FILENAME
    FROM @SNOWFLAKE_LEARNING_DB.MATINA_ETL.ETL_STAGE/{{ ds_nodash }}/
)
PATTERN = '.*store_master_{{ ds_nodash }}[.]csv.*'
FILE_FORMAT = (FORMAT_NAME = 'SNOWFLAKE_LEARNING_DB.MATINA_ETL.ETL_CSV')
FORCE = TRUE;

-- 3. Write down how many rows arrived, for the reconcile step later.
INSERT INTO SNOWFLAKE_LEARNING_DB.MATINA_ETL.C_LOAD_LOG
    (BATCH_DT, STEP, TARGET_TABLE, ROWS_IN, ROWS_OUT, ROWS_REJECTED)
SELECT '{{ ds }}'::DATE, 'bronze', 'STG_STORE_MASTER', COUNT(*), COUNT(*), 0
FROM SNOWFLAKE_LEARNING_DB.MATINA_ETL.STG_STORE_MASTER
WHERE BATCH_DT = '{{ ds }}'::DATE;
