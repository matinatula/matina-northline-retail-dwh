"""Tests for the 2.3 DAGs: drop_daily_files and retail_etl.

They check that the DAGs load without errors (a cycle shows up as an import error),
that retries are set, that the order of the layers is right, and that every SQL file
a task points at really exists in include/sql.
"""
from pathlib import Path

import pytest
from airflow.models import DagBag

ROOT = Path(__file__).resolve().parents[1]
DAGS_DIR = ROOT / "dags"
SQL_DIR = ROOT / "include" / "sql"

FILE_TYPES = ["item_master", "store_master", "pos_sales", "inventory_snapshot"]
SILVER_TASKS = ["silver_dept", "silver_subclass", "silver_brand", "silver_item",
                "silver_region", "silver_district", "silver_locn",
                "silver_sales", "silver_inventory"]


@pytest.fixture(scope="module")
def dagbag():
    return DagBag(dag_folder=str(DAGS_DIR))


@pytest.fixture(scope="module")
def etl(dagbag):
    return dagbag.get_dag("retail_etl")


def ancestors(dag, task_id):
    """All tasks that must run before task_id."""
    seen, stack = set(), list(dag.get_task(task_id).upstream_task_ids)
    while stack:
        t = stack.pop()
        if t not in seen:
            seen.add(t)
            stack.extend(dag.get_task(t).upstream_task_ids)
    return seen


# ---- the DAGs load ---------------------------------------------------------

def test_no_import_errors(dagbag):
    # a cycle between tasks also appears here
    assert dagbag.import_errors == {}, dagbag.import_errors


def test_both_dags_exist(dagbag):
    assert dagbag.get_dag("retail_etl") is not None
    assert dagbag.get_dag("drop_daily_files") is not None


# ---- error handling --------------------------------------------------------

def test_retries_are_set(etl):
    assert etl.default_args["retries"] >= 1
    for task in etl.tasks:
        assert task.retries >= 1, f"{task.task_id} has no retries"


def test_one_run_at_a_time(etl):
    assert etl.max_active_runs == 1


# ---- order of the layers ---------------------------------------------------

def test_sensor_comes_first(etl):
    for name in FILE_TYPES:
        assert "wait_for_files" in ancestors(etl, f"load_bronze_{name}")


def test_silver_waits_for_its_bronze_file(etl):
    for t in ["silver_dept", "silver_subclass", "silver_brand", "silver_item"]:
        assert "load_bronze_item_master" in ancestors(etl, t)
    for t in ["silver_region", "silver_district", "silver_locn"]:
        assert "load_bronze_store_master" in ancestors(etl, t)
    assert "load_bronze_pos_sales" in ancestors(etl, "silver_sales")
    assert "load_bronze_inventory_snapshot" in ancestors(etl, "silver_inventory")


def test_sales_waits_for_items(etl):
    # silver_sales checks every sold item against TMP_ITEM
    assert "silver_item" in ancestors(etl, "silver_sales")


def test_reconcile_is_last(etl):
    leaves = [t.task_id for t in etl.tasks if not t.downstream_task_ids]
    assert leaves == ["reconcile"]
    before = ancestors(etl, "reconcile")
    for t in SILVER_TASKS:
        assert t in before, f"reconcile does not wait for {t}"


# ---- the SQL files exist ---------------------------------------------------

def test_every_sql_file_exists(etl):
    for task in etl.tasks:
        sql = getattr(task, "sql", None)
        if isinstance(sql, str) and sql.endswith(".sql"):
            assert (SQL_DIR / sql).exists(), f"{task.task_id}: missing {sql}"


# ---- the drop DAG ----------------------------------------------------------

def test_drop_dag_params(dagbag):
    dag = dagbag.get_dag("drop_daily_files")
    assert "day" in dag.params and "dataset" in dag.params
    assert dag.schedule is None
