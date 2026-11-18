-- Silver: brands (the source has only a name, so the id is the name in capitals).
-- Run by Airflow once per day. Airflow fills in the day (for example 2026-09-16) before running.

-- 1. Clear this day's rows first, so a re-run never duplicates.
DELETE FROM SNOWFLAKE_LEARNING_DB.MATINA_ETL.TMP_BRAND WHERE BATCH_DT = '{{ ds }}'::DATE;

-- 2. Clean the text (trim spaces, turn blank and the word NULL into a real NULL),
--    keep only rows whose keys are present, and write one row per key.
INSERT INTO SNOWFLAKE_LEARNING_DB.MATINA_ETL.TMP_BRAND (BATCH_DT, BRAND_ID, BRAND_NAME)
WITH clean AS (
    SELECT NULLIF(NULLIF(TRIM(ITEM), ''), 'NULL') AS ITEM_ID,
           NULLIF(NULLIF(TRIM(ITEM_DESC), ''), 'NULL') AS ITEM_NAME,
           NULLIF(NULLIF(TRIM(DEPT), ''), 'NULL') AS DEPT,
           NULLIF(NULLIF(TRIM(CLASS), ''), 'NULL') AS CLASS,
           NULLIF(NULLIF(TRIM(SUBCLASS), ''), 'NULL') AS SUBCLASS,
           NULLIF(NULLIF(TRIM(DEPT_NAME), ''), 'NULL') AS DEPT_NAME,
           NULLIF(NULLIF(TRIM(SUB_NAME), ''), 'NULL') AS SUB_NAME,
           NULLIF(NULLIF(TRIM(BRAND_NAME), ''), 'NULL') AS BRAND_NAME,
           SRC_FILE_NM
    FROM SNOWFLAKE_LEARNING_DB.MATINA_ETL.STG_ITEM_MASTER
    WHERE BATCH_DT = '{{ ds }}'::DATE
)
SELECT '{{ ds }}'::DATE, UPPER(BRAND_NAME), MAX(BRAND_NAME)
FROM clean
WHERE BRAND_NAME IS NOT NULL
GROUP BY UPPER(BRAND_NAME);
