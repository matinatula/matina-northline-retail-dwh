-- Gold: DIM_LOCN (store, SCD2 = history kept). Built from TMP_LOCN.
-- A version is valid from EFF_FROM_DT to EFF_TO_DT (9999-12-31 = still open) and CURR_FLG marks the open one.
-- Run by Airflow once per day. Reload-safe: steps 1 and 2 undo anything this day did before, then 3 and 4 redo it.

-- 1. Undo: delete the versions that this business day opened earlier.
DELETE FROM SNOWFLAKE_LEARNING_DB.MATINA_ETL.DIM_LOCN WHERE EFF_FROM_DT >= '{{ ds }}'::DATE;

-- 2. Undo: reopen the versions that this business day closed (they were closed at day - 1).
UPDATE SNOWFLAKE_LEARNING_DB.MATINA_ETL.DIM_LOCN
SET EFF_TO_DT = '9999-12-31'::DATE, CURR_FLG = TRUE, REC_UPDT_TS = SYSDATE()
WHERE EFF_TO_DT = DATEADD(day, -1, '{{ ds }}'::DATE);

-- 3. Close: the open version of every locn whose tracked attributes changed (name, district).
UPDATE SNOWFLAKE_LEARNING_DB.MATINA_ETL.DIM_LOCN d
SET EFF_TO_DT = DATEADD(day, -1, '{{ ds }}'::DATE), CURR_FLG = FALSE, REC_UPDT_TS = SYSDATE(), BATCH_ID = '{{ ds }}'
FROM (
  SELECT s.LOCN_ID AS ID, s.LOCN_NAME AS NAME, c.DISTRICT_KY AS CKY
  FROM SNOWFLAKE_LEARNING_DB.MATINA_ETL.TMP_LOCN s
  JOIN SNOWFLAKE_LEARNING_DB.MATINA_ETL.DIM_DISTRICT c ON c.DISTRICT_ID = s.DISTRICT_ID AND c.CURR_FLG
  WHERE s.BATCH_DT = '{{ ds }}'::DATE
) s
WHERE d.LOCN_ID = s.ID AND d.CURR_FLG
  AND (d.LOCN_NAME IS DISTINCT FROM s.NAME OR d.DISTRICT_KY IS DISTINCT FROM s.CKY);

-- 4. Open: a new version for every id that has no open version (brand new ids, and the ones closed in step 3).
INSERT INTO SNOWFLAKE_LEARNING_DB.MATINA_ETL.DIM_LOCN
  (LOCN_KY, LOCN_ID, LOCN_NAME, DISTRICT_KY, CURR_FLG, EFF_FROM_DT, EFF_TO_DT, REC_INS_TS, REC_UPDT_TS, SRC_SYS_CD, BATCH_ID)
SELECT (SELECT COALESCE(MAX(LOCN_KY), 0) FROM SNOWFLAKE_LEARNING_DB.MATINA_ETL.DIM_LOCN) + ROW_NUMBER() OVER (ORDER BY s.ID),
       s.ID, s.NAME, s.CKY, TRUE, '{{ ds }}'::DATE, '9999-12-31'::DATE, SYSDATE(), SYSDATE(), 'RMS', '{{ ds }}'
FROM (
  SELECT s.LOCN_ID AS ID, s.LOCN_NAME AS NAME, c.DISTRICT_KY AS CKY
  FROM SNOWFLAKE_LEARNING_DB.MATINA_ETL.TMP_LOCN s
  JOIN SNOWFLAKE_LEARNING_DB.MATINA_ETL.DIM_DISTRICT c ON c.DISTRICT_ID = s.DISTRICT_ID AND c.CURR_FLG
  WHERE s.BATCH_DT = '{{ ds }}'::DATE
) s
WHERE NOT EXISTS (SELECT 1 FROM SNOWFLAKE_LEARNING_DB.MATINA_ETL.DIM_LOCN d WHERE d.LOCN_ID = s.ID AND d.CURR_FLG);
