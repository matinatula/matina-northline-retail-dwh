-- Silver: items, with rejects for missing keys.
-- Run by Airflow once per day. Airflow fills in the day (for example 2026-09-16) before running.

-- 1. Clear this day's earlier output so a re-run starts clean.
DELETE FROM SNOWFLAKE_LEARNING_DB.MATINA_ETL.TMP_ITEM WHERE BATCH_DT = '{{ ds }}'::DATE;
DELETE FROM SNOWFLAKE_LEARNING_DB.MATINA_ETL.C_REJ_TABLE WHERE BATCH_DT = '{{ ds }}'::DATE AND SRC_TABLE = 'STG_ITEM_MASTER';

-- 2. Quarantine: an item with no id, or with no department/class/subclass, cannot be placed
--    in the model. It goes to the reject table with a reason, and is NOT filled with a default.
INSERT INTO SNOWFLAKE_LEARNING_DB.MATINA_ETL.C_REJ_TABLE (BATCH_DT, SRC_TABLE, GATE, REASON_CODE, SRC_FILE_NM, ROW_PAYLOAD)
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
SELECT '{{ ds }}'::DATE, 'STG_ITEM_MASTER', 'G4',
       CASE WHEN ITEM_ID IS NULL THEN 'NULL_ITEM_ID' ELSE 'NULL_HIERARCHY' END,
       SRC_FILE_NM, OBJECT_CONSTRUCT(*)
FROM clean
WHERE NOT (ITEM_ID IS NOT NULL AND DEPT IS NOT NULL AND CLASS IS NOT NULL AND SUBCLASS IS NOT NULL);

-- 3. Good rows go to Silver. The subclass id joins department, class and subclass,
--    because a subclass number alone can repeat in different classes.
--    QUALIFY keeps one row per item, in case the file lists an item twice.
INSERT INTO SNOWFLAKE_LEARNING_DB.MATINA_ETL.TMP_ITEM (BATCH_DT, ITEM_ID, ITEM_NAME, SUBCLASS_ID, BRAND_ID)
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
SELECT '{{ ds }}'::DATE, ITEM_ID, ITEM_NAME, DEPT || '-' || CLASS || '-' || SUBCLASS, UPPER(BRAND_NAME)
FROM clean
WHERE ITEM_ID IS NOT NULL AND DEPT IS NOT NULL AND CLASS IS NOT NULL AND SUBCLASS IS NOT NULL
QUALIFY ROW_NUMBER() OVER (PARTITION BY ITEM_ID ORDER BY ITEM_NAME) = 1;
