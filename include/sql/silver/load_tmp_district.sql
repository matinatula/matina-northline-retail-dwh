-- Silver: districts (child of region).
-- Run by Airflow once per day. Airflow fills in the day (for example 2026-09-16) before running.

-- 1. Clear this day's rows first, so a re-run never duplicates.
DELETE FROM SNOWFLAKE_LEARNING_DB.MATINA_ETL.TMP_DISTRICT WHERE BATCH_DT = '{{ ds }}'::DATE;

-- 2. Clean the text (trim spaces, turn blank and the word NULL into a real NULL),
--    keep only rows whose keys are present, and write one row per key.
INSERT INTO SNOWFLAKE_LEARNING_DB.MATINA_ETL.TMP_DISTRICT (BATCH_DT, DISTRICT_ID, DISTRICT_NAME, REGION_ID)
WITH clean AS (
    SELECT NULLIF(NULLIF(TRIM(STORE), ''), 'NULL') AS LOCN_ID,
           NULLIF(NULLIF(TRIM(STORE_NAME), ''), 'NULL') AS LOCN_NAME,
           NULLIF(NULLIF(TRIM(STORE_TYPE), ''), 'NULL') AS STORE_TYPE,
           NULLIF(NULLIF(TRIM(DISTRICT), ''), 'NULL') AS DISTRICT_ID,
           NULLIF(NULLIF(TRIM(DISTRICT_NAME), ''), 'NULL') AS DISTRICT_NAME,
           NULLIF(NULLIF(TRIM(REGION), ''), 'NULL') AS REGION_ID,
           NULLIF(NULLIF(TRIM(REGION_NAME), ''), 'NULL') AS REGION_NAME,
           TRY_TO_DATE(NULLIF(NULLIF(TRIM(STORE_OPEN_DATE), ''), 'NULL'))  AS OPEN_DT,
           TRY_TO_DATE(NULLIF(NULLIF(TRIM(STORE_CLOSE_DATE), ''), 'NULL')) AS CLOSE_DT,
           SRC_FILE_NM
    FROM SNOWFLAKE_LEARNING_DB.MATINA_ETL.STG_STORE_MASTER
    WHERE BATCH_DT = '{{ ds }}'::DATE
)
SELECT '{{ ds }}'::DATE, DISTRICT_ID, MAX(DISTRICT_NAME), MAX(REGION_ID)
FROM clean
WHERE DISTRICT_ID IS NOT NULL AND REGION_ID IS NOT NULL
GROUP BY DISTRICT_ID;
