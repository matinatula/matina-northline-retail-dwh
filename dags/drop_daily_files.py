"""Step 3 of 2.3: act as the upstream system that drops one day's files onto the Snowflake stage.

Run it by hand for a day (for example 20260916). It uploads that day's four files
to the internal stage, so the ETL DAG has something to wait for.
"""
import os
from datetime import datetime

from airflow.providers.snowflake.hooks.snowflake import SnowflakeHook
from airflow.sdk import Param, dag, get_current_context, task

SNOWFLAKE_CONN = "snowflake_default"                 # the Connection made in the Airflow UI
LANDING_DIR = "/opt/airflow/data/landing"            # where the daily files are, inside Docker
STAGE = "@SNOWFLAKE_LEARNING_DB.MATINA_ETL.ETL_STAGE"
FILE_TYPES = ["item_master", "store_master", "pos_sales", "inventory_snapshot"]


@dag(
    dag_id="drop_daily_files",
    start_date=datetime(2026, 9, 1),
    schedule=None,          # only runs when you trigger it
    catchup=False,
    params={
        "day": Param("20260916", type="string", description="Business date, YYYYMMDD"),
        "dataset": Param("clean", enum=["clean", "raw_defects"], description="Which set of files"),
    },
)
def drop_daily_files():

    @task
    def put_files():
        params = get_current_context()["params"]
        day, dataset = params["day"], params["dataset"]
        hook = SnowflakeHook(snowflake_conn_id=SNOWFLAKE_CONN)

        for name in FILE_TYPES:
            local_file = f"{LANDING_DIR}/{dataset}/{day}/{name}_{day}.csv"
            if not os.path.exists(local_file):
                raise FileNotFoundError(local_file)
            # PUT = upload a local file to the stage. OVERWRITE makes a re-run safe.
            hook.run(f"PUT 'file://{local_file}' {STAGE}/{day}/ AUTO_COMPRESS=FALSE OVERWRITE=TRUE")

        # show what is on the stage now, in the task log
        for row in hook.get_records(f"LIST {STAGE}/{day}/"):
            print(row[0], row[1], "bytes")

    put_files()


drop_daily_files()
