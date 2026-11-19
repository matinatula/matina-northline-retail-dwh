"""2.3 ETL, built one step at a time.

Step 4: wait for the day's files on the stage, then load them into Bronze.
Step 5: build Silver (typed, cleaned, with rejects) for items, stores, sales and inventory.
Step 6: build the Gold dimensions. Parents load before children.
Step 7 (this file): build the Gold facts and the aggregate, then reconcile. The reconcile task FAILS the run if Gold does not tie back.

The "day" is the run's logical date: set it when you trigger or backfill (2026-09-16 and so on).
Backfill runs the days one after another (max_active_runs=1), which the SCD2 and positional loads rely on.
"""
from datetime import datetime, timedelta
from pathlib import Path

from airflow.providers.common.sql.operators.sql import SQLExecuteQueryOperator
from airflow.providers.snowflake.hooks.snowflake import SnowflakeHook
from airflow.sdk import dag, task

try:  # fails the task at once, without a retry (a failed reconciliation is not a flaky error)
    from airflow.sdk.exceptions import AirflowFailException
except ImportError:
    from airflow.exceptions import AirflowFailException

SNOWFLAKE_CONN = "snowflake_default"
STAGE = "@SNOWFLAKE_LEARNING_DB.MATINA_ETL.ETL_STAGE"
SQL_DIR = Path("/opt/airflow/include/sql")
FILE_TYPES = ["item_master", "store_master", "pos_sales", "inventory_snapshot"]

# Silver tables that are built from each Bronze file (one SQL file and one task per table)
SILVER_FROM_ITEMS = ["dept", "subclass", "brand", "item"]
SILVER_FROM_STORES = ["region", "district", "locn"]

# Gold dimensions and facts (one SQL file and one task per table)
GOLD_DIMS = ["dept", "subclass", "brand", "item", "region", "district", "locn"]
GOLD_FACTS = ["sales", "inventory"]


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
    catchup=False,           # old days are loaded on purpose with a backfill, not automatically
    max_active_runs=1,       # one day at a time, in order
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

    # BRONZE
    bronze = {name: run_sql(f"load_bronze_{name}", f"bronze/load_stg_{name}.sql") for name in FILE_TYPES}

    # SILVER
    silver_items = [run_sql(f"silver_{n}", f"silver/load_tmp_{n}.sql") for n in SILVER_FROM_ITEMS]
    silver_stores = [run_sql(f"silver_{n}", f"silver/load_tmp_{n}.sql") for n in SILVER_FROM_STORES]
    silver_sales = run_sql("silver_sales", "silver/load_tmp_sales.sql")          # needs TMP_ITEM
    silver_inventory = run_sql("silver_inventory", "silver/load_tmp_inventory.sql")
    silver = dict(zip(SILVER_FROM_ITEMS + SILVER_FROM_STORES, silver_items + silver_stores))

    # GOLD
    gold = {n: run_sql(f"gold_{n}", f"gold/load_dim_{n}.sql") for n in GOLD_DIMS}
    fct = {n: run_sql(f"gold_fct_{n}", f"gold/load_fct_{n}.sql") for n in GOLD_FACTS}
    agg = run_sql("gold_agg_sls_dept_district_month", "gold/load_agg_sls_dept_district_month.sql")

    # RECONCILE: runs reconcile.sql for the day, prints every check, and fails the run if any check differs.
    @task
    def reconcile(day: str):
        hook = SnowflakeHook(snowflake_conn_id=SNOWFLAKE_CONN)
        sql = (SQL_DIR / "reconcile" / "reconcile.sql").read_text().replace("@DAY@", day)
        rows = hook.get_records(sql)
        failures = []
        print(f"RECONCILIATION for {day}")
        for name, expected, actual in rows:
            ok = round(float(expected), 2) == round(float(actual), 2)
            print(f"  {'OK  ' if ok else 'FAIL'} {name}: expected {expected}, actual {actual}")
            if not ok:
                failures.append(name)
        if failures:
            raise AirflowFailException(f"Gold does not tie back for {day}: {failures}")

    wait_for_files("{{ ds_nodash }}") >> list(bronze.values())
    bronze["item_master"] >> silver_items
    bronze["store_master"] >> silver_stores
    bronze["pos_sales"] >> silver_sales
    silver["item"] >> silver_sales
    bronze["inventory_snapshot"] >> silver_inventory

    # Gold dimensions: parents before children. The roots (dept, brand, region) run in parallel.
    silver["dept"] >> gold["dept"]
    silver["brand"] >> gold["brand"]
    silver["region"] >> gold["region"]
    [gold["dept"], silver["subclass"]] >> gold["subclass"]
    [gold["region"], silver["district"]] >> gold["district"]
    [gold["subclass"], gold["brand"], silver["item"]] >> gold["item"]
    [gold["district"], silver["locn"]] >> gold["locn"]

    # Gold facts: need the item and store versions, and their own Silver table.
    [gold["item"], gold["locn"], silver_sales] >> fct["sales"]
    [gold["item"], gold["locn"], silver_inventory] >> fct["inventory"]

    # The aggregate is rebuilt from the sales fact (the whole month, never incremental).
    fct["sales"] >> agg

    # Reconcile last: fails the DAG if Gold does not tie back.
    [fct["inventory"], agg] >> reconcile("{{ ds }}")


retail_etl()