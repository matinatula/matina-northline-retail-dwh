# matina-northline-retail-dwh

## Assignment 3: Airflow DAG that loads CSV files into Snowflake

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
4. Put the four CSV files in `data/incoming/` (this folder is git-ignored).
5. Trigger the DAG. `wait_for_complete_signal` shows *Up For Reschedule* while it waits.
6. Create the signal: `New-Item data\incoming\.COMPLETE -ItemType File`. The rest of the DAG then runs.

### Evidence (`output_ss/`)

- `03_waiting_for_complete_signal.png`: sensor waiting for the signal
- `04_after_complete_file.png` and `05_dag_run_success.png`: all tasks green, four mapped `load_file` tasks
- `06_snowflake_row_counts.png`: 500 / 500 / 738 / 40 rows in Snowflake

### Notes and limitations

- Each load uses `CREATE OR REPLACE TABLE` and `COPY ... FORCE = TRUE`, so running the DAG again with the same files gives the same row counts.
- The DAG also runs on its daily schedule. With `max_active_runs=1`, a manual run queues behind a scheduled run that is still waiting for the signal.
- Files are archived after a successful run, so a rerun needs the files and a new `.COMPLETE` placed in `data/incoming/` again.
- The helper tests are in `tests/`.