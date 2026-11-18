-- Bronze load: pos_sales for one day.
-- Run by Airflow once per day. Airflow fills in the day (for example 2026-09-16) before running.

-- 1. Remove this day's rows first, so running the same day twice never creates duplicates.
DELETE FROM SNOWFLAKE_LEARNING_DB.MATINA_ETL.STG_POS_SALES
WHERE BATCH_DT = '{{ ds }}'::DATE;

-- 2. Copy the file from the stage into the table. Every column stays text, nothing is cleaned here.
--    $1, $2 ... means "column 1, column 2 ..." of the CSV file.
COPY INTO SNOWFLAKE_LEARNING_DB.MATINA_ETL.STG_POS_SALES (
    RTL_LOC_ID,
    WKSTN_ID,
    TRANS_SEQ,
    RTRANS_LINEITM_SEQ,
    BUSINESS_DATE,
    BEGIN_DATETIME,
    TRANS_TYPCODE,
    ITEM_ID,
    ITEM_SCAN_ID,
    QTY,
    UNIT_PRICE,
    EXTENDED_AMT,
    DISCOUNT_AMT,
    TAX_AMT,
    NET_AMT,
    CURRENCY,
    CASHIER_ID,
    VOID_FLAG,
    POST_VOID_FLAG,
    BATCH_DT,
    SRC_FILE_NM
)
FROM (
    SELECT $1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13, $14, $15, $16, $17, $18, $19,
           '{{ ds }}'::DATE,
           METADATA$FILENAME
    FROM @SNOWFLAKE_LEARNING_DB.MATINA_ETL.ETL_STAGE/{{ ds_nodash }}/
)
PATTERN = '.*pos_sales_{{ ds_nodash }}[.]csv.*'
FILE_FORMAT = (FORMAT_NAME = 'SNOWFLAKE_LEARNING_DB.MATINA_ETL.ETL_CSV')
FORCE = TRUE;

-- 3. Write down how many rows arrived, for the reconcile step later.
INSERT INTO SNOWFLAKE_LEARNING_DB.MATINA_ETL.C_LOAD_LOG
    (BATCH_DT, STEP, TARGET_TABLE, ROWS_IN, ROWS_OUT, ROWS_REJECTED)
SELECT '{{ ds }}'::DATE, 'bronze', 'STG_POS_SALES', COUNT(*), COUNT(*), 0
FROM SNOWFLAKE_LEARNING_DB.MATINA_ETL.STG_POS_SALES
WHERE BATCH_DT = '{{ ds }}'::DATE;
