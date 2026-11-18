"""2.3 ETL, built one step at a time.

Step 4: wait for the day's files on the stage, then load them into Bronze.
Step 5 (this file): build Silver for items and stores (typed, cleaned, with rejects).
Later steps add the Silver sales and stock, the dimensions, the facts and the reconcile check.

The "day" is the run's logical date: set it when you trigger or backfill (2026-09-16 and so on).
"""
from datetime import datetime, timedelta

from airflow.providers.common.sql.operators.sql import SQLExecuteQueryOperator
from airflow.providers.snowflake.hooks.snowflake import SnowflakeHook
from airflow.sdk import dag, task

SNOWFLAKE_CONN = "snowflake_default"
STAGE = "@SNOWFLAKE_LEARNING_DB.MATINA_ETL.ETL_STAGE"
FILE_TYPES = ["item_master", "store_master", "pos_sales", "inventory_snapshot"]

# Silver tables that are built from each Bronze file (one SQL file and one task per table)
SILVER_FROM_ITEMS = ["dept", "subclass", "brand", "item"]
SILVER_FROM_STORES = ["region", "district", "locn"]


def run_sql(task_id, sql_file):
    """One task = one SQL file inside include/sql."""
    return SQLExecuteQueryOperator(
        task_id=task_id,
        conn_id=SNOWFLAKE_CONN,
        sql=sql_file,
        hook_params={"schema": "MATINA_ETL"},
        split_statements=True,
    )


@dag(
    dag_id="retail_etl",
    start_date=datetime(2026, 9, 16),
    schedule="@daily",
    catchup=False,            # old days are loaded on purpose with a backfill, not automatically
    max_active_runs=1,        # one day at a time, in order
    template_searchpath=["/opt/airflow/include/sql"],   # where the .sql files live inside Docker
    default_args={"retries": 1, "retry_delay": timedelta(minutes=1)},
)
def retail_etl():

    # Checks the stage every 30 seconds until all 4 files for the day are there.
    # "reschedule" means it frees its worker between checks instead of sitting on it.
    @task.sensor(poke_interval=30, timeout=60 * 60, mode="reschedule")
    def wait_for_files(day: str) -> bool:
        hook = SnowflakeHook(snowflake_conn_id=SNOWFLAKE_CONN)
        on_stage = [row[0] for row in hook.get_records(f"LIST {STAGE}/{day}/")]
        missing = [n for n in FILE_TYPES if not any(f"{n}_{day}.csv" in f for f in on_stage)]
        print(f"day {day}: missing files = {missing}")
        return not missing

    # BRONZE: one task per file, each runs one SQL file in include/sql/bronze.
    bronze = {name: run_sql(f"load_bronze_{name}", f"bronze/load_stg_{name}.sql") for name in FILE_TYPES}

    # SILVER: one task per table, each runs one SQL file in include/sql/silver.
    # Each one starts as soon as the Bronze file it reads has loaded, and they run side by side.
    silver_items = [run_sql(f"silver_{n}", f"silver/load_tmp_{n}.sql") for n in SILVER_FROM_ITEMS]
    silver_stores = [run_sql(f"silver_{n}", f"silver/load_tmp_{n}.sql") for n in SILVER_FROM_STORES]
        # Silver sales needs TWO things first: the Bronze sales file, and TMP_ITEM
    # (it checks every sold item exists there).
    silver_sales = run_sql("silver_sales", "silver/load_tmp_sales.sql")

    wait_for_files("{{ ds_nodash }}") >> list(bronze.values())
    bronze["item_master"] >> silver_items
    bronze["store_master"] >> silver_stores
    bronze["pos_sales"] >> silver_sales
    silver_items[-1] >> silver_sales   # silver_items[-1] is the "item" task, the last in the list


retail_etl()
