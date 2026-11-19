# matina-northline-retail-dwh

## Assignment 2.2: Airflow DAG that loads CSV files into Snowflake

**Goal:** wait for a `.COMPLETE` signal file in a local folder, then load every CSV in that folder into its own Snowflake table, with no transformation.

**DAG:** `dags/load_files_to_snowflake.py` (helpers in `dags/loader_utils.py`)
**Target:** `SNOWFLAKE_LEARNING_DB.MATINA_ORCHESTRATION` (separate from the 2.1 schema `MATINA_ASSIGNMENT`)

| File | Table | Rows |
|---|---|---|
| `sales.csv` | `STG_SALES` | 500 |
| `inventory.csv` | `STG_INVENTORY` | 500 |
| `item.csv` | `STG_ITEM` | 738 |
| `location.csv` | `STG_LOCATION` | 40 |

All columns are loaded as text (`VARCHAR`) because the assignment asks for no transformation.

### Flow

```
wait_for_complete_signal -> list_files -> load_file (one task per file) -> report -> archive_files
                                                       \-> alert_on_failure (only if a load fails)
```

1. `wait_for_complete_signal` waits until `.COMPLETE` exists in the source folder.
2. `list_files` finds the CSV files and passes the list to the next task.
3. `load_file` runs once per file. It creates the stage and the table, uploads the file (`PUT`) and loads it (`COPY INTO`).
4. `report` collects the result of every load.
5. `archive_files` moves the loaded files and the signal into `archive/<batch>/`.
6. `alert_on_failure` runs only if something failed.

### Airflow concepts used

| Concept | Where |
|---|---|
| File Sensor | `wait_for_complete_signal`: `FileSensor`, `reschedule` mode, 30 s poke interval, 6 h timeout |
| XCom | `list_files` returns the file list, `load_file` returns a result dict, `report` and `archive_files` read them |
| Dynamic Task Mapping | `load_file.expand(path=files)` creates one task per file (map index shows the file name) |
| Connections | `snowflake_default` (Snowflake login) and `northline_fs` (File path, used by the sensor) |
| Hooks | `SnowflakeHook` runs the SQL inside each `load_file` task |
| Variables | `northline_source_folder`, `northline_target_schema`, `northline_stage_name`, `northline_file_table_map`, `northline_expected_files` |
| Params | `source_folder` and `archive_after_load` can be set per run when triggering |
| Trigger rules | `report` uses `all_done`, `alert_on_failure` uses `one_failed` |
| Retries and scheduling | 2 retries per task, daily schedule at 06:00 UTC, `max_active_runs=1` |

### How to run

1. Start Airflow: `docker compose up -d`. The Snowflake provider is installed through `_PIP_ADDITIONAL_REQUIREMENTS` in `.env`.
2. In the Airflow UI, create the two Connections (`snowflake_default`, `northline_fs`) and import `include/airflow_variables.json` under Admin, Variables. Credentials are stored only in the Airflow Connection, never in the repository.
3. Create the schema once in Snowflake: `CREATE SCHEMA IF NOT EXISTS SNOWFLAKE_LEARNING_DB.MATINA_ORCHESTRATION;`
4. The four sample CSV files are already in `data/incoming/`. If you re-run, turn off `archive_after_load` when triggering, or copy the files back.
5. Trigger the DAG. `wait_for_complete_signal` shows *Up For Reschedule* while it waits.
6. Create the signal: `New-Item data\incoming\.COMPLETE -ItemType File`. The rest of the DAG then runs.

### Evidence (`output_screenshots/`)

- `2.2_02_waiting_for_complete_signal.png`: sensor waiting for the signal
- `2.2_03_after_complete_file.png` and `2.2_04_dag_run_success.png`: all tasks green, four mapped `load_file` tasks
- `2.2_05_snowflake_row_counts.png`: 500 / 500 / 738 / 40 rows in Snowflake

### Notes and limitations

- Each load uses `CREATE OR REPLACE TABLE` and `COPY ... FORCE = TRUE`, so running the DAG again with the same files gives the same row counts.
- The DAG also runs on its daily schedule. With `max_active_runs=1`, a manual run queues behind a scheduled run that is still waiting for the signal.
- Files are archived after a successful run, so a rerun needs the files and a new `.COMPLETE` placed in `data/incoming/` again.
- The helper tests are in `tests/`.

Your answer is right on both counts, and it's a strong demo answer. I'd add one more: a made-up region would also put store 503 into the region roll-ups, so a distribution center would silently distort regional totals.

Here is the documentation for this step, in plain language you can edit into your own voice. Check the screenshot filenames match what's in your folder.

---

### 2.3 Step 5: Silver layer for items and stores

**What was done**

The ETL DAG (`retail_etl`) was extended with seven Silver tasks. They run for the business date 2026-09-16 after the Bronze loads finish. The item file feeds four tasks (department, subclass, brand, item). The store file feeds three tasks (region, district, store). Each SQL file first deletes that day's rows, so re-running the same day does not create duplicates.

**Why it was done**

Bronze stores the files exactly as they arrived, all as text. Silver is where the data is cleaned and typed (for example, store open dates become real dates). Rows that cannot be placed in the hierarchy are not fixed with made-up values. They go to `C_REJ_TABLE` with a reason code, so they can be investigated.

**Evidence**

- `2.3_07_silver_dag_green.png`: all Silver tasks succeeded.
- `2.3_18_silver_counts.png`: row counts in the Silver tables and in `C_REJ_TABLE`.

**Results for 2026-09-16**

| Table | Rows |
|---|---|
| TMP_DEPT | 13 |
| TMP_SUBCLASS | 83 |
| TMP_BRAND | 11 |
| TMP_ITEM | 728 |
| TMP_REGION | 7 |
| TMP_DISTRICT | 13 |
| TMP_LOCN | 39 |
| C_REJ_TABLE | 10 |

**Reconciliation (Bronze = Silver + rejects + duplicates removed)**

- Items: 738 in Bronze = 728 in Silver + 9 rejected + 1 duplicate removed.
  - 8 items (10000011 to 10000018) have a class and subclass but no department, so they were rejected as `NULL_HIERARCHY`.
  - 1 row has no item id, so it was rejected as `NULL_ITEM_ID`.
  - Item 10000004 appears twice with identical values. One copy is kept by `QUALIFY ROW_NUMBER() = 1`.
- Stores: 40 in Bronze = 39 in Silver + 1 rejected.
  - Store 503 ("DC West - Reno") is a distribution center (type C) with no region, so it was rejected as `NULL_HIERARCHY`.

The duplicate can be verified with: `SELECT ITEM, COUNT(*) FROM STG_ITEM_MASTER GROUP BY ITEM HAVING COUNT(*) > 1;`

**Design decisions**

1. **Reject instead of fill with a default.** A made-up region or department could be wrong, and it would hide the defect. It would also distort the roll-ups (for example, a distribution center counted in a region's sales). A rejected row can be investigated and fixed at the source.
2. **Silent removal of exact duplicates.** The duplicate copy is identical to the kept row, so no information is lost. This is a known limitation: the removed copy is not recorded in `C_REJ_TABLE`.
3. **The "clean" file was not fully clean.** The baseline file still contained 10 defective rows. Silver caught all of them, which shows the quarantine logic working before the faulty-data test.

---

**The duplicate explained.** It isn't an exact copy this time. The two rows for item 10000002 at store 9901 are:
- A normal row: snapshot `20260913`, stock 28.
- A broken row: snapshot `20990104`, stock 12, with blank and NULL fields (`IN_TRANSIT_QTY` blank, `TSF_RESERVED_QTY` NULL).

This is the same `20990104` row we found earlier, which is why that date had only one row. It also means the duplicate pair and the sentinel date are one problem. Rejecting the `20990104` row as `BAD_SNAPSHOT_DT` leaves exactly one row for that item and store, so no silent dedup is needed. The copy to keep is the `20260913` one.

---

### 2.3 Step 6: Silver layer for sales

**What was done**

A new task, `silver_sales`, was added to the `retail_etl` DAG. It runs `load_tmp_sales.sql` for the business date 2026-09-16. It waits for two things: the Bronze sales load (`load_bronze_pos_sales`) and the Silver item load (`silver_item`). It needs `TMP_ITEM` because every sold item is checked against it. The SQL first deletes that day's rows from `TMP_SALES` and from the sales rejects, so re-running the same day does not create duplicates.

**Why it was done**

Bronze keeps the sales file as text. Silver converts it to proper types (dates and numbers with `TRY_TO_DATE` and `TRY_TO_NUMBER`, which return NULL for a bad value instead of failing the load) and builds a transaction id from store, register and sequence number. Rows that cannot be trusted go to `C_REJ_TABLE` with a reason code. They are not filled with default values.

**Reject rules, checked in this order**

| Reason code | Rule |
|---|---|
| `NULL_ITEM_ID` | Item id is blank or NULL |
| `BAD_DATE_OR_NUMBER` | Date, quantity or net amount cannot be converted |
| `NEGATIVE_QTY_ON_SALE` | Negative quantity on a transaction of type SALE |
| `UNKNOWN_ITEM` | Item is not in `TMP_ITEM` |

A row gets the first reason that applies, so it is rejected only once. Negative quantities on `RETURN` rows are valid and are kept.

**Evidence**

- `2.3_09_silver_sales_dag_green.png`: `silver_sales` succeeded after `silver_item`.
- `2.3_10_silver_sales_counts.png`: `TMP_SALES` row count and rejects by reason.

**Results for 2026-09-16**

| Table or reason | Rows |
|---|---|
| Bronze `STG_POS_SALES` | 1,471 |
| `TMP_SALES` | 1,376 |
| Rejected: `NULL_ITEM_ID` | 82 |
| Rejected: `UNKNOWN_ITEM` | 13 |

Reconciliation: Bronze 1,471 = Silver 1,376 + 82 + 13.

Breakdown of the 1,376 Silver rows:

| Transaction type | Rows | Net amount |
|---|---|---|
| SALE | 1,264 | 166,050.78 |
| RETURN | 112 | -8,629.42 |

**Findings**

1. **Cascading rejects.** All 13 `UNKNOWN_ITEM` sales belong to items 10000014 to 10000018. Those items were already rejected in the item step because they have no department. A rejected item therefore also removes its sales from Silver, which is intended: a sale for an item that is not in the model cannot be placed in any roll-up.
2. **Returns are kept.** Returns have negative quantities and negative net amounts. They are valid business events and reduce net sales. Rejecting them would overstate sales.
3. **Typing precision.** Amounts are converted with explicit precision and scale (`TRY_TO_NUMBER(x, 18, 2)`), because the default scale of 0 would round 97.54 to 98.

**Design decisions**

1. Null items are rejected, not filled with a default, so defects stay visible and can be fixed at the source.
2. The item check uses the Silver item table of the same day, so the item step must finish first. This order is enforced in the DAG.

---

### 2.3 Step 7: Silver layer for inventory

**What was done**

A new task, `silver_inventory`, was added to the `retail_etl` DAG. It runs `load_tmp_inventory.sql` for the business date 2026-09-16, after the Bronze inventory load finishes. It does not depend on the Silver item load, because it does not join to items. The SQL first deletes that day's rows from `TMP_INVENTORY` and from its rejects, so re-running the same day does not create duplicates.

**Why it was done**

Bronze keeps the snapshot as text. Silver converts the snapshot date (format `YYYYMMDD`) and the quantity, and calculates the stock value as `STOCK_ON_HAND × AV_COST`, rounded to 2 decimals. Rows that cannot be trusted go to `C_REJ_TABLE` with a reason code.

**Reject rules, checked in this order**

| Reason code | Rule |
|---|---|
| `NULL_ITEM_ID` | Item id is blank or NULL |
| `BAD_SNAPSHOT_DT` | Snapshot date is missing, or later than the load day |
| `BAD_NUMBER` | Stock quantity cannot be converted |

**Evidence**

- `2.3_13_silver_inventory_dag_green.png`: `silver_inventory` succeeded.
- `2.3_14_silver_inventory_counts.png`: `TMP_INVENTORY` row count and rejects by reason.

**Results for 2026-09-16**

| Table or reason | Rows |
|---|---|
| Bronze `STG_INVENTORY_SNAPSHOT` | 12,485 |
| `TMP_INVENTORY` | 12,484 |
| Rejected: `BAD_SNAPSHOT_DT` | 1 |

Reconciliation: Bronze 12,485 = Silver 12,484 + 1 rejected.

**Findings**

1. **Placeholder snapshot date.** One row (item 10000002, store 9901) has the snapshot date 2099-01-04 with blank and NULL fields. 2099-01-04 is a valid date, so a plain date conversion does not catch it. The rule "snapshot date later than the load day" does. The same item and store also has a normal row dated 2026-09-13, so rejecting the placeholder row also removes the only duplicate in the file.
2. **Warehouse stock is kept.** The file has store rows (`S`) and warehouse rows (`W`, 653 rows). Warehouse stock is real stock and is not rejected.
3. **Stock value.** `STK_VAL` is calculated in Silver from quantity and average cost, because Bronze has no value column.

**Design decisions**

1. A snapshot cannot be dated after the day it was loaded, so a future date is treated as a placeholder and rejected instead of corrected.
2. The weekly snapshot is repeated in each daily file. The day's file is stored under its own `BATCH_DT`, which is what the later positional (close-and-reopen) load will compare.

---

## 2.3 Step 8: Gold dimensions

### What was done
Seven Gold tasks were added to the `retail_etl` DAG, one per dimension. Each runs its own SQL file from `include/sql/gold/` for the business date 2026-09-16. Parents load before children, and independent dimensions run in parallel:
- Roots, in parallel: `DIM_DEPT`, `DIM_BRAND`, `DIM_REGION`.
- `DIM_SUBCLASS` waits for `DIM_DEPT`, and `DIM_DISTRICT` waits for `DIM_REGION`.
- `DIM_ITEM` waits for `DIM_SUBCLASS` and `DIM_BRAND`, and `DIM_LOCN` waits for `DIM_DISTRICT`.

Each task also waits for the Silver task that feeds it. The time dimensions (`DIM_YEAR`, `DIM_MONTH`, `DIM_DAY`) and `DIM_CUSTOMER` are seeds and were already loaded, so no task was needed.

### Why it was done
Gold serves the dimensional model. Every row needs a surrogate key (`_KY`), and every child row must point to a real parent key. Silver holds the cleaned business ids, so the Gold SQL translates each id to its parent's surrogate key.

### Dimension types
| Dimension | Type | Why |
|---|---|---|
| `DIM_DEPT`, `DIM_SUBCLASS`, `DIM_BRAND`, `DIM_REGION`, `DIM_DISTRICT` | SCD1 (overwrite) | A changed name is a correction. No history is needed. |
| `DIM_ITEM` | SCD2 on name, subclass, brand | A change of subclass or brand changes which group old sales belong to, so the old version is kept. |
| `DIM_LOCN` | SCD2 on name and district | A store that moves district (for example store 1112) must keep its old sales in the old district. |

SCD2 works in four steps: delete the versions this day opened, reopen the versions this day closed, close the open version of every changed key (`EFF_TO_DT` = day − 1, `CURR_FLG` = FALSE), then insert a new open version. The first two steps make a reload of the same day give the same result.

### Naming
Gold uses the 2.2 names, to match Silver and the architecture documents. The 2.1 tables `DIM_CATEGORY`, `DIM_SUBCATEGORY` and `DIM_CITY` were renamed, with their columns, to `DIM_DEPT`, `DIM_SUBCLASS` and `DIM_DISTRICT` (script: `include/sql/ddl/rename_gold_to_2_2.sql`).

### Evidence
- `2.3_15_full_dag_green.png`: the full DAG, all green.
- `2.3_16_gold_dim_counts.png`: Gold row counts and the orphan check, after a re-run.

### Results for 2026-09-16
| Dimension | Silver rows | Gold rows | Current rows |
|---|---|---|---|
| DIM_DEPT | 13 | 13 | 13 |
| DIM_SUBCLASS | 83 | 83 | 83 |
| DIM_BRAND | 11 | 11 | 11 |
| DIM_ITEM | 728 | 728 | 728 |
| DIM_REGION | 7 | 7 | 7 |
| DIM_DISTRICT | 13 | 13 | 13 |
| DIM_LOCN | 39 | 39 | 39 |

Orphan items (items with no matching subclass): 0. A re-run of the same day gave identical counts.

### Findings
1. **Ownership.** The Gold tables had been created under `ACCOUNTADMIN`, so the Airflow role could not write to them. Ownership was transferred to `SNOWFLAKE_LEARNING_ROLE`.
2. **Surrogate keys.** The tables have no identity column, so the SQL generates each key as the current maximum plus the row number, for new rows only.
3. **Rejected rows do not reach Gold.** The 9 rejected items and store 503 were stopped in Silver, which is why Gold has 728 items and 39 stores.

### Known limits
- An item or store that disappears from a later file is not closed.
- Reloading a dimension replaces that day's keys, so facts must be reloaded after it (the order in the reload policy).

---

