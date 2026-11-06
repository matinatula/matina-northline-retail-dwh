# Model Notes: Assignment 2.1 (Retail Sales Snowflake Schema)

## 1. Grain statements

| Fact table | Type | Grain (one row = ...) |
|---|---|---|
| FCT_SALES | Transaction | one transaction line, for one item, at one location, on one day |
| FCT_PRICE | Positional | the price of one item at one location on one day; EFF_FROM_DT / EFF_TO_DT give the period the price holds (open-ended = 9999-12-31) |
| FCT_INVENTORY | Positional | the stock of one item at one location on one day; EFF_FROM_DT / EFF_TO_DT give the period the stock level holds |
| AGG_SLS_CATEGORY_CITY_MONTH | Aggregate | the total sales of one category, in one city, in one month |

Why the unique constraints match the grain: each fact has a unique constraint on exactly the columns that define its grain, so a duplicate row cannot be loaded.

## 2. SCD type per dimension attribute

Business keys (`*_ID`, `FULL_DT`, `YEAR_NUMBER`) and the maintenance columns (surrogate keys, `CURR_FLG`, `EFF_FROM_DT`, `EFF_TO_DT`, `REC_INS_TS`, `REC_UPDT_TS`, `SRC_SYS_CD`, `BATCH_ID`) are **NA** (not tracked).

| Dimension | Attribute | SCD |
|---|---|---|
| DIM_CATEGORY | CATEGORY_NAME | SCD1 |
| DIM_SUBCATEGORY | SUBCATEGORY_NAME | SCD1 |
| DIM_SUBCATEGORY | CATEGORY_KY | SCD1 |
| DIM_BRAND | BRAND_NAME | SCD1 |
| DIM_ITEM | ITEM_NAME | **SCD2** |
| DIM_ITEM | SUBCATEGORY_KY | **SCD2** |
| DIM_ITEM | BRAND_KY | **SCD2** |
| DIM_REGION | REGION_NAME | SCD1 |
| DIM_CITY | CITY_NAME | SCD1 |
| DIM_CITY | REGION_KY | SCD1 |
| DIM_LOCN | LOCN_NAME | **SCD2** |
| DIM_LOCN | CITY_KY | **SCD2** |
| DIM_CUSTOMER | CUSTOMER_NAME, EMAIL, PHONE | SCD1 |
| DIM_YEAR | YEAR_NUMBER | NA |
| DIM_MONTH | MONTH_NUMBER, MONTH_NAME, YEAR_KY | SCD1 |
| DIM_DAY | DAY_NUMBER, MONTH_KY | SCD1 |

For SCD1 and seeded dimensions the table keeps one row per entity: `CURR_FLG = TRUE`, `EFF_FROM_DT` = load date, `EFF_TO_DT` = 9999-12-31. The columns are present because the maintenance-column standard requires them on every dimension; they do not mean history is kept.

## 3. Why each SCD2 choice 

| Attribute | Reason |
|---|---|
| DIM_ITEM.ITEM_NAME | A renamed item must keep its old sales under the name it was sold as. |
| DIM_ITEM.SUBCATEGORY_KY | If an item moves subcategory, past sales must stay in the old subcategory and category so past category totals do not change. |
| DIM_ITEM.BRAND_KY | If an item changes brand, past sales must stay with the brand it had when sold. |
| DIM_LOCN.LOCN_NAME | A renamed location must keep its old sales under the old name. |
| DIM_LOCN.CITY_KY | If a location is reassigned to another city, past sales must stay in the old city so past city totals do not change. |

All other attributes are SCD1: the change is a correction or a relabel, and no report needs the old value. Customer is SCD1 as specified in the brief; no report analyses sales by an old email or phone.

## 4. Aggregate table: AGG_SLS_CATEGORY_CITY_MONTH

- **Grain:** category x city x month, built from FCT_SALES only.
- **Measures:** sum of SLS_QTY, SLS_AMT, DSC_AMT (all additive).
- **Why this grain:** a typical management report asks for sales by product group, area and month. Rolling up to this level cuts millions of transaction lines to a few rows per month, so those reports run quickly.
- **Business need:** the business only needs insight at category level, so a coarse grain is enough. It keeps the table small, so queries run faster and cost less to run.
- **Cost:** it cannot answer questions about a specific item, a specific location, a day, or a subcategory. Those go to FCT_SALES.
- **Why only FCT_SALES:** sales are events, so adding them up is meaningful. Price is not additive. Stock is a point-in-time level and cannot be summed across time.
- **Why not item x location:** it would have far more rows and give a much smaller speed-up. The base fact already holds the finest detail, so the aggregate deliberately gives it up in exchange for speed.
- **As-was:** category and city are derived through the item and location version recorded on each sale (ITEM_KY, LOCN_KY), so history is reported as it was at the time of sale.

## 5. Assumptions about the source data

1. `TRANSACTION_ID` and `LINE_NUMBER` identify a sales line within the combination of item, location and day, so the grain constraint is unique. If `TRANSACTION_ID` repeats across locations, the constraint still holds because `LOCN_KY` is part of it.
2. `CUSTOMER_KY` in FCT_SALES is nullable because some sales are anonymous.
3. `BRAND_KY` in DIM_ITEM is nullable because some items have no brand.
4. `EFF_TO_DT` = 9999-12-31 means the row or period is still open-ended. A far-future date is used instead of NULL so date-range queries (`BETWEEN EFF_FROM_DT AND EFF_TO_DT`) need no NULL handling.
5. For FCT_PRICE and FCT_INVENTORY, `DAY_KY` is the day the record is for, and `EFF_FROM_DT` / `EFF_TO_DT` is the period it stays valid.
6. `SLS_AMT` is the amount before discount and `DSC_AMT` the discount, held separately.
7. All amounts are in one currency.
8. The calendar (year, month, day) is seeded in advance and never changes.
9. Customer contact fields (EMAIL, PHONE) are available in the source and approved for use.
10. Surrogate keys are supplied by the load process unless the training database auto-numbers them.
11. The aggregate is rebuilt or refreshed from FCT_SALES after each load.
12. The target database is Snowflake, as instructed. Its PRIMARY KEY, UNIQUE and FOREIGN KEY constraints are not enforced, so they document the model and the load process is responsible for key integrity.
13. The business is satisfied with sales insight at category, city and month level; finer questions are answered from FCT_SALES.
