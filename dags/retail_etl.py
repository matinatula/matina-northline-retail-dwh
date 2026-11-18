"""2.3 ETL, built one step at a time.

Step 4 (this file): wait for the day's files on the stage, then load them into Bronze.
Later steps add Silver, the dimensions, the facts and the reconcile check.

The "day" is the run's logical date: set it when you trigger or backfill (2026-09-16 and so on).
"""
from datetime import datetime, timedelta

from airflow.providers.common.sql.operators.sql import SQLExecuteQueryOperator
from airflow.providers.snowflake.hooks.snowflake import SnowflakeHook
from airflow.sdk import dag, task

SNOWFLAKE_CONN = "snowflake_default"
STAGE = "@SNOWFLAKE_LEARNING_DB.MATINA_ETL.ETL_STAGE"
FILE_TYPES = ["item_master", "store_master", "pos_sales", "inventory_snapshot"]


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

    # One task per file; each just runs one SQL file in the include/sql/bronze folder.
    load_bronze = [
        SQLExecuteQueryOperator(
            task_id=f"load_bronze_{name}",
            conn_id=SNOWFLAKE_CONN,
            sql=f"bronze/load_stg_{name}.sql",
            hook_params={"schema": "MATINA_ETL"},
            split_statements=True,
        )
        for name in FILE_TYPES
    ]

    wait_for_files("{{ ds_nodash }}") >> load_bronze


retail_etl()
