-- One-time migration (run as SNOWFLAKE_LEARNING_ROLE, not ACCOUNTADMIN, so the role owns the new table):
-- FCT_PRICE has no price file to load from, so it is derived from the unit price on the POS sales lines.
-- Silver therefore needs to keep the unit price, and needs a small price table.
USE ROLE SNOWFLAKE_LEARNING_ROLE;
USE SCHEMA SNOWFLAKE_LEARNING_DB.MATINA_ETL;

ALTER TABLE TMP_SALES ADD COLUMN IF NOT EXISTS UNIT_PRC NUMBER(18,2);

CREATE TABLE IF NOT EXISTS TMP_PRICE (
  BATCH_DT     DATE,
  BUSINESS_DT  DATE,
  ITEM_ID      VARCHAR,
  LOCN_ID      VARCHAR,
  PRICE_AMT    NUMBER(18,2),
  SRC_FILE_NM  VARCHAR
);
