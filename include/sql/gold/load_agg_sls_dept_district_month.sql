-- Gold: AGG_SLS_DEPT_DISTRICT_MONTH (aggregate). Grain: department x district x month.
-- Why this grain: management reports ask "how did each department sell in each district this month", and
-- this answers it from a few hundred rows instead of scanning every sales line.
-- Rebuilt from FCT_SALES for the month of the run day: delete the month, then insert it again (never incremental),
-- so a re-run, a backfill and a late correction all give the same numbers.
-- The district comes from the store version valid on the sale date, so a store move splits correctly.

-- 1. Clear the month.
DELETE FROM SNOWFLAKE_LEARNING_DB.MATINA_ETL.AGG_SLS_DEPT_DISTRICT_MONTH
WHERE MONTH_KY = (SELECT MONTH_KY FROM SNOWFLAKE_LEARNING_DB.MATINA_ETL.DIM_DAY WHERE FULL_DT = '{{ ds }}'::DATE);

-- 2. Rebuild it from the fact, walking item -> subclass -> department, store -> district, day -> month.
INSERT INTO SNOWFLAKE_LEARNING_DB.MATINA_ETL.AGG_SLS_DEPT_DISTRICT_MONTH
  (AGG_SLS_KY, DEPT_KY, DISTRICT_KY, MONTH_KY, SLS_QTY, SLS_AMT, DSC_AMT,
   REC_INS_TS, REC_UPDT_TS, SRC_SYS_CD, BATCH_ID)
SELECT (SELECT COALESCE(MAX(AGG_SLS_KY), 0) FROM SNOWFLAKE_LEARNING_DB.MATINA_ETL.AGG_SLS_DEPT_DISTRICT_MONTH)
         + ROW_NUMBER() OVER (ORDER BY sc.DEPT_KY, l.DISTRICT_KY),
       sc.DEPT_KY, l.DISTRICT_KY, d.MONTH_KY,
       SUM(f.SLS_QTY), SUM(f.SLS_AMT), SUM(f.DSC_AMT),
       SYSDATE(), SYSDATE(), 'GOLD', '{{ ds }}'
FROM SNOWFLAKE_LEARNING_DB.MATINA_ETL.FCT_SALES f
JOIN SNOWFLAKE_LEARNING_DB.MATINA_ETL.DIM_ITEM     i  ON i.ITEM_KY = f.ITEM_KY
JOIN SNOWFLAKE_LEARNING_DB.MATINA_ETL.DIM_SUBCLASS sc ON sc.SUBCLASS_KY = i.SUBCLASS_KY
JOIN SNOWFLAKE_LEARNING_DB.MATINA_ETL.DIM_LOCN     l  ON l.LOCN_KY = f.LOCN_KY
JOIN SNOWFLAKE_LEARNING_DB.MATINA_ETL.DIM_DAY      d  ON d.DAY_KY = f.DAY_KY
WHERE d.MONTH_KY = (SELECT MONTH_KY FROM SNOWFLAKE_LEARNING_DB.MATINA_ETL.DIM_DAY WHERE FULL_DT = '{{ ds }}'::DATE)
GROUP BY sc.DEPT_KY, l.DISTRICT_KY, d.MONTH_KY;
