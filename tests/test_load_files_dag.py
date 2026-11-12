"""Tests for load_files_to_snowflake. Run from the repo root:  pytest tests -v

Three kinds of test, matching the session's "test before you schedule":
  1. Integrity: the DAG parses, has an owner, has retries, sensor has a timeout.
  2. Unit: helper functions, outside Airflow.
  3. Load logic against a fake Snowflake hook (no account needed).
"""
import ast
import re
import sys
from pathlib import Path

import pytest

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / "dags"))

import loader_utils as lu  # noqa: E402


# --------------------------------------------------------------------------
# 1. DAG integrity
# --------------------------------------------------------------------------
@pytest.fixture(scope="module")
def dag():
    from airflow.models import DagBag

    bag = DagBag(dag_folder=str(ROOT / "dags"), include_examples=False)
    assert bag.import_errors == {}, bag.import_errors
    return bag.get_dag("load_files_to_snowflake")


def test_dag_parses_and_has_expected_tasks(dag):
    assert set(dag.task_ids) == {
        "wait_for_complete_signal", "list_files", "load_file",
        "report", "alert_on_failure", "archive_files",
    }


def test_dag_has_owner_and_retries(dag):
    for t in dag.tasks:
        assert t.owner and t.owner != "airflow"
        assert t.retries >= 1


def test_sensor_waits_safely(dag):
    sensor = dag.get_task("wait_for_complete_signal")
    assert sensor.timeout and sensor.timeout > 0, "a sensor without a timeout is a hang"
    assert sensor.mode == "reschedule"


def test_load_file_is_dynamically_mapped(dag):
    assert dag.get_task("load_file").get_needs_expansion()


def test_archive_only_after_every_load_succeeds(dag):
    upstream = dag.get_task("archive_files").upstream_task_ids
    assert "load_file" in upstream, "archive must wait on the loads, not just on report"


def test_trigger_rules(dag):
    assert str(dag.get_task("alert_on_failure").trigger_rule) .endswith("one_failed")
    assert str(dag.get_task("report").trigger_rule).endswith("all_done")


def test_no_credentials_and_no_clock_calls_in_dag_files():
    for f in (ROOT / "dags").glob("*.py"):
        text = f.read_text().lower()
        assert not re.search(r"password\s*[=:]", text), "credentials must live in Connections"
        # No now()/today()/utcnow() CALLS in code (mentions in comments are fine).
        for node in ast.walk(ast.parse(f.read_text())):
            if isinstance(node, ast.Call):
                name = getattr(node.func, "attr", getattr(node.func, "id", ""))
                assert name not in {"now", "utcnow", "today"}, f"{f.name}: use the logical date, not the clock"


# --------------------------------------------------------------------------
# 2. Helpers
# --------------------------------------------------------------------------
def test_sanitize_identifier():
    assert lu.sanitize_identifier("Net Amount ($)") == "NET_AMOUNT"
    assert lu.sanitize_identifier("2026 sales") == "C_2026_SALES"
    assert lu.sanitize_identifier("   ", fallback="COL_3") == "COL_3"


def test_unique_columns_dedupes_and_protects_audit_columns():
    assert lu.unique_columns(["id", "ID", "batch_id", "id"]) == ["ID", "ID_2", "BATCH_ID_2", "ID_3"]


def test_table_name_mapping_wins_over_default():
    assert lu.table_name_for("/x/sales.csv", {"sales.csv": "stg_sales_raw"}) == "STG_SALES_RAW"
    assert lu.table_name_for("/x/inventory.csv", {}) == "STG_INVENTORY"


def test_header_and_row_count(tmp_path):
    f = tmp_path / "a.csv"
    f.write_text('id,name\n1,"Smith, Jo"\n2,"Line\nbreak"\n\n3,x\n', encoding="utf-8")
    assert lu.read_header(str(f)) == ["id", "name"]
    assert lu.count_data_rows(str(f)) == 3  # quoted comma, quoted newline, blank line all handled


def test_empty_file_is_rejected(tmp_path):
    f = tmp_path / "empty.csv"
    f.write_text("")
    with pytest.raises(ValueError):
        lu.read_header(str(f))


def test_copy_sql_forces_reload():
    sql = lu.sql_copy("T", ["A", "B"], "STG", "f.csv")
    assert "FORCE = TRUE" in sql and "SKIP_HEADER = 1" in sql


# --------------------------------------------------------------------------
# 3. Load logic with a fake hook
# --------------------------------------------------------------------------
class FakeHook:
    def __init__(self, rows_to_report=None):
        self.statements = []
        self.rows_to_report = rows_to_report

    def run(self, sql, **kwargs):
        self.statements.append(sql)

    def get_first(self, sql):
        return (self.rows_to_report if self.rows_to_report is not None else self.expected,)


@pytest.fixture
def csv_file(tmp_path):
    f = tmp_path / "sales.csv"
    f.write_text("ticket,amount\n1,9.99\n2,5.00\n3,1.25\n")
    return str(f)


def test_load_one_file_runs_steps_in_order_and_reconciles(csv_file):
    hook = FakeHook()
    hook.expected = 3
    result = lu.load_one_file(hook, csv_file, table="STG_SALES", stage="S", batch_id="20261106")
    kinds = [s.split()[0] for s in hook.statements]
    assert kinds == ["CREATE", "CREATE", "REMOVE", "PUT", "COPY", "UPDATE"]
    assert result["rows_loaded"] == result["rows_in_file"] == 3


def test_row_count_mismatch_fails_the_task(csv_file):
    hook = FakeHook(rows_to_report=2)
    with pytest.raises(ValueError, match="mismatch"):
        lu.load_one_file(hook, csv_file, table="STG_SALES", stage="S", batch_id="b")


def test_load_is_idempotent_same_statements_each_run(csv_file):
    a, b = FakeHook(), FakeHook()
    a.expected = b.expected = 3
    lu.load_one_file(a, csv_file, "STG_SALES", "S", "20261106")
    lu.load_one_file(b, csv_file, "STG_SALES", "S", "20261106")
    assert a.statements == b.statements
    assert "CREATE OR REPLACE TABLE" in a.statements[1]
