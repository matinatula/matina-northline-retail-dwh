-- Silver: sales, with rejects for bad rows.
-- Run by Airflow once per day. Airflow fills in the day (for example 2026-09-16) before running.
-- Needs TMP_ITEM to be loaded first, because it checks that every sold item exists.
-- Keeps the unit price (UNIT_PRC) so that TMP_PRICE can be built from the clean sales lines.
-- A row gets the FIRST reason that applies. Nothing is dropped silently: Bronze rows = Silver rows + rejected rows.

-- 1. Clear this day's earlier output so a re-run starts clean.
DELETE FROM SNOWFLAKE_LEARNING_DB.MATINA_ETL.TMP_SALES WHERE BATCH_DT = '{{ ds }}'::DATE;
DELETE FROM SNOWFLAKE_LEARNING_DB.MATINA_ETL.C_REJ_TABLE WHERE BATCH_DT = '{{ ds }}'::DATE AND SRC_TABLE = 'STG_POS_SALES';

-- 2. Quarantine. Reasons, in the order they are checked:
--    NULL_ITEM_ID, BAD_DATE_OR_NUMBER, VOID (type VOID or a void flag = Y), BAD_CURRENCY (anything except USD / US$;
--    US$ is just another spelling of USD, CAD has no exchange rate here), SENTINEL_QTY (99999 placeholder),
--    NEGATIVE_QTY_ON_SALE, UNKNOWN_ITEM, DUPLICATE_LINE (same transaction line and day, extra copies only).
--    Returns (negative qty on a RETURN) are valid and are NOT rejected.
INSERT INTO SNOWFLAKE_LEARNING_DB.MATINA_ETL.C_REJ_TABLE (BATCH_DT, SRC_TABLE, GATE, REASON_CODE, SRC_FILE_NM, ROW_PAYLOAD)
WITH parsed AS (
    SELECT NULLIF(NULLIF(TRIM(ITEM_ID), ''), 'NULL') AS ITEM_ID,
           TRY_TO_DATE(BUSINESS_DATE) AS BUSINESS_DT,
           CONCAT_WS('-', TRIM(RTL_LOC_ID), TRIM(WKSTN_ID), TRIM(TRANS_SEQ)) AS TRANSACTION_ID,
           TRY_TO_NUMBER(RTRANS_LINEITM_SEQ) AS LINE_NUMBER,
           TRIM(RTL_LOC_ID) AS LOCN_ID,
           TRY_TO_NUMBER(QTY, 18, 3) AS SLS_QTY,
           TRY_TO_NUMBER(EXTENDED_AMT, 18, 2) AS SLS_AMT,
           TRY_TO_NUMBER(DISCOUNT_AMT, 18, 2) AS DSC_AMT,
           TRY_TO_NUMBER(NET_AMT, 18, 2) AS NET_AMT,
           UPPER(TRIM(TRANS_TYPCODE)) AS TRANS_TYPE,
           UPPER(TRIM(CURRENCY)) AS CURRENCY,
           UPPER(TRIM(VOID_FLAG)) AS VOID_FLAG,
           UPPER(TRIM(POST_VOID_FLAG)) AS POST_VOID_FLAG,
           SRC_FILE_NM,
           TRY_TO_NUMBER(UNIT_PRICE, 18, 2) AS UNIT_PRC
    FROM SNOWFLAKE_LEARNING_DB.MATINA_ETL.STG_POS_SALES
    WHERE BATCH_DT = '{{ ds }}'::DATE
),
checked AS (
    SELECT p.*,
           CASE WHEN p.ITEM_ID IS NULL THEN 'NULL_ITEM_ID'
                WHEN p.BUSINESS_DT IS NULL OR p.SLS_QTY IS NULL OR p.NET_AMT IS NULL THEN 'BAD_DATE_OR_NUMBER'
                WHEN p.TRANS_TYPE = 'VOID' OR p.VOID_FLAG = 'Y' OR p.POST_VOID_FLAG = 'Y' THEN 'VOID'
                WHEN p.CURRENCY IS NULL OR p.CURRENCY NOT IN ('USD', 'US$') THEN 'BAD_CURRENCY'
                WHEN ABS(p.SLS_QTY) >= 99999 THEN 'SENTINEL_QTY'
                WHEN p.SLS_QTY < 0 AND p.TRANS_TYPE = 'SALE' THEN 'NEGATIVE_QTY_ON_SALE'
                WHEN i.ITEM_ID IS NULL THEN 'UNKNOWN_ITEM'
           END AS FIRST_REASON
    FROM parsed p
    LEFT JOIN (SELECT DISTINCT ITEM_ID FROM SNOWFLAKE_LEARNING_DB.MATINA_ETL.TMP_ITEM
               WHERE BATCH_DT = '{{ ds }}'::DATE) i
           ON i.ITEM_ID = p.ITEM_ID
),
flagged AS (
    -- Duplicates are only counted among rows that passed every other check, so a bad copy can never
    -- push the good copy out. Valid rows sit in their own partition (FIRST_REASON IS NULL).
    SELECT c.*,
           COALESCE(c.FIRST_REASON,
                    CASE WHEN ROW_NUMBER() OVER (PARTITION BY c.TRANSACTION_ID, c.LINE_NUMBER, c.BUSINESS_DT, (c.FIRST_REASON IS NULL)
                                                 ORDER BY c.SRC_FILE_NM, c.NET_AMT, c.SLS_QTY) > 1
                         THEN 'DUPLICATE_LINE' END) AS REASON
    FROM checked c
)
SELECT '{{ ds }}'::DATE, 'STG_POS_SALES',
       CASE REASON WHEN 'BAD_DATE_OR_NUMBER' THEN 'G3'
                   WHEN 'NULL_ITEM_ID' THEN 'G4'
                   WHEN 'DUPLICATE_LINE' THEN 'G5'
                   WHEN 'UNKNOWN_ITEM' THEN 'G6'
                   ELSE 'G7' END,
       REASON, SRC_FILE_NM, OBJECT_CONSTRUCT(*)
FROM flagged
WHERE REASON IS NOT NULL;

-- 3. Good rows go to Silver (exactly the rows with no reason, same rules as above).
INSERT INTO SNOWFLAKE_LEARNING_DB.MATINA_ETL.TMP_SALES
    (BATCH_DT, BUSINESS_DT, TRANSACTION_ID, LINE_NUMBER, ITEM_ID, LOCN_ID,
     SLS_QTY, SLS_AMT, DSC_AMT, NET_AMT, TRANS_TYPE, SRC_FILE_NM, UNIT_PRC)
WITH parsed AS (
    SELECT NULLIF(NULLIF(TRIM(ITEM_ID), ''), 'NULL') AS ITEM_ID,
           TRY_TO_DATE(BUSINESS_DATE) AS BUSINESS_DT,
           CONCAT_WS('-', TRIM(RTL_LOC_ID), TRIM(WKSTN_ID), TRIM(TRANS_SEQ)) AS TRANSACTION_ID,
           TRY_TO_NUMBER(RTRANS_LINEITM_SEQ) AS LINE_NUMBER,
           TRIM(RTL_LOC_ID) AS LOCN_ID,
           TRY_TO_NUMBER(QTY, 18, 3) AS SLS_QTY,
           TRY_TO_NUMBER(EXTENDED_AMT, 18, 2) AS SLS_AMT,
           TRY_TO_NUMBER(DISCOUNT_AMT, 18, 2) AS DSC_AMT,
           TRY_TO_NUMBER(NET_AMT, 18, 2) AS NET_AMT,
           UPPER(TRIM(TRANS_TYPCODE)) AS TRANS_TYPE,
           UPPER(TRIM(CURRENCY)) AS CURRENCY,
           UPPER(TRIM(VOID_FLAG)) AS VOID_FLAG,
           UPPER(TRIM(POST_VOID_FLAG)) AS POST_VOID_FLAG,
           SRC_FILE_NM,
           TRY_TO_NUMBER(UNIT_PRICE, 18, 2) AS UNIT_PRC
    FROM SNOWFLAKE_LEARNING_DB.MATINA_ETL.STG_POS_SALES
    WHERE BATCH_DT = '{{ ds }}'::DATE
),
checked AS (
    SELECT p.*,
           CASE WHEN p.ITEM_ID IS NULL THEN 'NULL_ITEM_ID'
                WHEN p.BUSINESS_DT IS NULL OR p.SLS_QTY IS NULL OR p.NET_AMT IS NULL THEN 'BAD_DATE_OR_NUMBER'
                WHEN p.TRANS_TYPE = 'VOID' OR p.VOID_FLAG = 'Y' OR p.POST_VOID_FLAG = 'Y' THEN 'VOID'
                WHEN p.CURRENCY IS NULL OR p.CURRENCY NOT IN ('USD', 'US$') THEN 'BAD_CURRENCY'
                WHEN ABS(p.SLS_QTY) >= 99999 THEN 'SENTINEL_QTY'
                WHEN p.SLS_QTY < 0 AND p.TRANS_TYPE = 'SALE' THEN 'NEGATIVE_QTY_ON_SALE'
                WHEN i.ITEM_ID IS NULL THEN 'UNKNOWN_ITEM'
           END AS FIRST_REASON
    FROM parsed p
    LEFT JOIN (SELECT DISTINCT ITEM_ID FROM SNOWFLAKE_LEARNING_DB.MATINA_ETL.TMP_ITEM
               WHERE BATCH_DT = '{{ ds }}'::DATE) i
           ON i.ITEM_ID = p.ITEM_ID
),
flagged AS (
    SELECT c.*,
           COALESCE(c.FIRST_REASON,
                    CASE WHEN ROW_NUMBER() OVER (PARTITION BY c.TRANSACTION_ID, c.LINE_NUMBER, c.BUSINESS_DT, (c.FIRST_REASON IS NULL)
                                                 ORDER BY c.SRC_FILE_NM, c.NET_AMT, c.SLS_QTY) > 1
                         THEN 'DUPLICATE_LINE' END) AS REASON
    FROM checked c
)
SELECT '{{ ds }}'::DATE, BUSINESS_DT, TRANSACTION_ID, LINE_NUMBER, ITEM_ID, LOCN_ID,
       SLS_QTY, SLS_AMT, DSC_AMT, NET_AMT, TRANS_TYPE, SRC_FILE_NM, UNIT_PRC
FROM flagged
WHERE REASON IS NULL;