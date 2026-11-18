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

