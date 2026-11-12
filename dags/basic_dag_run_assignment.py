from datetime import datetime

from airflow import DAG
from airflow.operators.empty import EmptyOperator

with DAG(
    dag_id="retail_load",
    dag_display_name="Matina Tuladhar - Retail Load Pipeline",
    description="Retail data loading pipeline",
    start_date=datetime(2026,11,5),
    schedule=None,
    catchup=False,
) as dag:

    wait_for_files = EmptyOperator(
        task_id="wait_for_files"
    )

    ingest_item_master = EmptyOperator(
        task_id="ingest_item_master"
    )

    ingest_pos_sales=EmptyOperator(
        task_id="ingest_pos_sales"
    )

    ingest_inventory = EmptyOperator(
        task_id="ingest_inventory"
    )

    silver=EmptyOperator(
        task_id="silver"
    )

    gold = EmptyOperator(
        task_id="gold"
    )

    reconcile = EmptyOperator(
        task_id="reconcile"
    )

    # Dependencies
    
    wait_for_files >> [ingest_item_master, ingest_pos_sales, ingest_inventory] >> silver >> gold >> reconcile