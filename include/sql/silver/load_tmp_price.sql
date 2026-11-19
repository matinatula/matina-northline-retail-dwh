-- Silver: price. There is no price file in the daily drops, so the price of an item at a store on a day
-- is taken from the unit price on that day's clean SALE lines (TMP_SALES). Returns are ignored.
-- If a pair was sold at more than one price that day, the most common price wins (the higher one on a tie).
-- Lines with no price, or a price of 0, do not count.

-- 1. Clear this day's earlier output so a re-run starts clean.
DELETE FROM SNOWFLAKE_LEARNING_DB.MATINA_ETL.TMP_PRICE WHERE BATCH_DT = '{{ ds }}'::DATE;

-- 2. One price per item, store and business date.
INSERT INTO SNOWFLAKE_LEARNING_DB.MATINA_ETL.TMP_PRICE (BATCH_DT, BUSINESS_DT, ITEM_ID, LOCN_ID, PRICE_AMT, SRC_FILE_NM)
SELECT '{{ ds }}'::DATE, BUSINESS_DT, ITEM_ID, LOCN_ID, UNIT_PRC, MAX(SRC_FILE_NM)
FROM SNOWFLAKE_LEARNING_DB.MATINA_ETL.TMP_SALES
WHERE BATCH_DT = '{{ ds }}'::DATE
  AND TRANS_TYPE = 'SALE' AND SLS_QTY > 0 AND UNIT_PRC IS NOT NULL AND UNIT_PRC > 0
GROUP BY BUSINESS_DT, ITEM_ID, LOCN_ID, UNIT_PRC
QUALIFY ROW_NUMBER() OVER (PARTITION BY BUSINESS_DT, ITEM_ID, LOCN_ID ORDER BY COUNT(*) DESC, UNIT_PRC DESC) = 1;
