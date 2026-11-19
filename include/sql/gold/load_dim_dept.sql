-- Gold: DIM_DEPT (department, SCD1 = overwrite, no history). Built from TMP_DEPT.
-- Run by Airflow once per day. Safe to re-run: unchanged rows are untouched, new rows are only added once.

-- 1. Existing rows whose name changed are overwritten in place.
UPDATE SNOWFLAKE_LEARNING_DB.MATINA_ETL.DIM_DEPT d
SET DEPT_NAME = s.NAME,
    REC_UPDT_TS = SYSDATE(),
    BATCH_ID = '{{ ds }}'
FROM (
  SELECT s.DEPT_ID AS ID, s.DEPT_NAME AS NAME
  FROM SNOWFLAKE_LEARNING_DB.MATINA_ETL.TMP_DEPT s
  WHERE s.BATCH_DT = '{{ ds }}'::DATE
) s
WHERE d.DEPT_ID = s.ID
  AND (d.DEPT_NAME IS DISTINCT FROM s.NAME);

-- 2. Ids that are not in the dimension yet get a new surrogate key (current max + 1, 2, 3 ...).
INSERT INTO SNOWFLAKE_LEARNING_DB.MATINA_ETL.DIM_DEPT
  (DEPT_KY, DEPT_ID, DEPT_NAME, CURR_FLG, EFF_FROM_DT, EFF_TO_DT, REC_INS_TS, REC_UPDT_TS, SRC_SYS_CD, BATCH_ID)
SELECT (SELECT COALESCE(MAX(DEPT_KY), 0) FROM SNOWFLAKE_LEARNING_DB.MATINA_ETL.DIM_DEPT) + ROW_NUMBER() OVER (ORDER BY s.ID),
       s.ID, s.NAME, TRUE, '{{ ds }}'::DATE, '9999-12-31'::DATE, SYSDATE(), SYSDATE(), 'RMS', '{{ ds }}'
FROM (
  SELECT s.DEPT_ID AS ID, s.DEPT_NAME AS NAME
  FROM SNOWFLAKE_LEARNING_DB.MATINA_ETL.TMP_DEPT s
  WHERE s.BATCH_DT = '{{ ds }}'::DATE
) s
WHERE NOT EXISTS (SELECT 1 FROM SNOWFLAKE_LEARNING_DB.MATINA_ETL.DIM_DEPT d WHERE d.DEPT_ID = s.ID);
