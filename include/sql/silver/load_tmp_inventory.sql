-- Silver: inventory snapshot, with rejects for bad rows.
-- Run by Airflow once per day. Airflow fills in the day (for example 2026-09-16) before running.
-- A row gets the FIRST reason that applies. Nothing is dropped silently: Bronze rows = Silver rows + rejected rows.

-- 1. Clear this day's earlier output so a re-run starts clean.
DELETE FROM SNOWFLAKE_LEARNING_DB.MATINA_ETL.TMP_INVENTORY WHERE BATCH_DT = '{{ ds }}'::DATE;
DELETE FROM SNOWFLAKE_LEARNING_DB.MATINA_ETL.C_REJ_TABLE WHERE BATCH_DT = '{{ ds }}'::DATE AND SRC_TABLE = 'STG_INVENTORY_SNAPSHOT';

-- 2. Quarantine. Reasons, in the order they are checked:
--    NULL_ITEM_ID, BAD_SNAPSHOT_DT (missing, after the load day, or more than 14 days before it: this catches the
--    2099 and 1999 placeholder dates, which are valid dates), BAD_NUMBER, NEGATIVE_STOCK, DUPLICATE (item + location + date).
--    Stock at a location that is not a store is caught in Gold (G11, no location key).
INSERT INTO SNOWFLAKE_LEARNING_DB.MATINA_ETL.C_REJ_TABLE (BATCH_DT, SRC_TABLE, GATE, REASON_CODE, SRC_FILE_NM, ROW_PAYLOAD)
WITH parsed AS (
    SELECT NULLIF(NULLIF(TRIM(ITEM), ''), 'NULL') AS ITEM_ID,
           TRIM(LOC) AS LOCN_ID,
           TRY_TO_DATE(SNAPSHOT_DT, 'YYYYMMDD') AS SNAPSHOT_DT,
           TRY_TO_NUMBER(STOCK_ON_HAND, 18, 3) AS STK_QTY,
           ROUND(TRY_TO_NUMBER(STOCK_ON_HAND, 18, 3) * TRY_TO_NUMBER(AV_COST, 18, 4), 2) AS STK_VAL,
           SRC_FILE_NM
    FROM SNOWFLAKE_LEARNING_DB.MATINA_ETL.STG_INVENTORY_SNAPSHOT
    WHERE BATCH_DT = '{{ ds }}'::DATE
),
flagged AS (
    SELECT p.*,
           CASE WHEN ITEM_ID IS NULL THEN 'NULL_ITEM_ID'
                WHEN SNAPSHOT_DT IS NULL OR SNAPSHOT_DT > '{{ ds }}'::DATE
                     OR SNAPSHOT_DT < DATEADD(day, -14, '{{ ds }}'::DATE) THEN 'BAD_SNAPSHOT_DT'
                WHEN STK_QTY IS NULL THEN 'BAD_NUMBER'
                WHEN STK_QTY < 0 THEN 'NEGATIVE_STOCK'
                WHEN ROW_NUMBER() OVER (PARTITION BY ITEM_ID, LOCN_ID, SNAPSHOT_DT ORDER BY SRC_FILE_NM) > 1 THEN 'DUPLICATE'
           END AS REASON
    FROM parsed p
)
SELECT '{{ ds }}'::DATE, 'STG_INVENTORY_SNAPSHOT',
       CASE REASON WHEN 'NULL_ITEM_ID' THEN 'G4'
                   WHEN 'DUPLICATE' THEN 'G5'
                   WHEN 'NEGATIVE_STOCK' THEN 'G7'
                   ELSE 'G3' END,
       REASON, SRC_FILE_NM, OBJECT_CONSTRUCT(*)
FROM flagged
WHERE REASON IS NOT NULL;

-- 3. Good rows go to Silver (the same rules).
INSERT INTO SNOWFLAKE_LEARNING_DB.MATINA_ETL.TMP_INVENTORY
    (BATCH_DT, SNAPSHOT_DT, ITEM_ID, LOCN_ID, STK_QTY, STK_VAL, SRC_FILE_NM)
WITH parsed AS (
    SELECT NULLIF(NULLIF(TRIM(ITEM), ''), 'NULL') AS ITEM_ID,
           TRIM(LOC) AS LOCN_ID,
           TRY_TO_DATE(SNAPSHOT_DT, 'YYYYMMDD') AS SNAPSHOT_DT,
           TRY_TO_NUMBER(STOCK_ON_HAND, 18, 3) AS STK_QTY,
           ROUND(TRY_TO_NUMBER(STOCK_ON_HAND, 18, 3) * TRY_TO_NUMBER(AV_COST, 18, 4), 2) AS STK_VAL,
           SRC_FILE_NM
    FROM SNOWFLAKE_LEARNING_DB.MATINA_ETL.STG_INVENTORY_SNAPSHOT
    WHERE BATCH_DT = '{{ ds }}'::DATE
),
flagged AS (
    SELECT p.*,
           CASE WHEN ITEM_ID IS NULL THEN 'NULL_ITEM_ID'
                WHEN SNAPSHOT_DT IS NULL OR SNAPSHOT_DT > '{{ ds }}'::DATE
                     OR SNAPSHOT_DT < DATEADD(day, -14, '{{ ds }}'::DATE) THEN 'BAD_SNAPSHOT_DT'
                WHEN STK_QTY IS NULL THEN 'BAD_NUMBER'
                WHEN STK_QTY < 0 THEN 'NEGATIVE_STOCK'
                WHEN ROW_NUMBER() OVER (PARTITION BY ITEM_ID, LOCN_ID, SNAPSHOT_DT ORDER BY SRC_FILE_NM) > 1 THEN 'DUPLICATE'
           END AS REASON
    FROM parsed p
)
SELECT '{{ ds }}'::DATE, SNAPSHOT_DT, ITEM_ID, LOCN_ID, STK_QTY, STK_VAL, SRC_FILE_NM
FROM flagged
WHERE REASON IS NULL;