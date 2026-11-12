"""Load every CSV from a local folder into Snowflake, once a .COMPLETE signal appears.

Flow
----
    wait_for_complete_signal          (FileSensor, reschedule mode, with timeout)
              |
          list_files                  (returns file PATHS through XCom)
              |
    load_file  x N                    (dynamic task mapping: one task per file,
              |                        each uses SnowflakeHook, each retries alone)
        +-----+------------------+
        |                        |
      report                alert_on_failure     (trigger rule: one_failed)
        |
    archive_files

No transformation happens: every column lands as text, as it is in the file.
Airflow triggers the work; Snowflake performs it (PUT + COPY INTO).

Where each session concept is used is listed in docs/orchestration_assignment.md.
"""
from __future__ import annotations

import json
import logging
import re
import shutil
from datetime import timedelta
from pathlib import Path

import pendulum

# Imports work on Airflow 3 (airflow.sdk) and fall back to Airflow 2.
try:
    from airflow.sdk import Param, Variable, dag, get_current_context, task
except ImportError:  # pragma: no cover - Airflow 2.x
    from airflow.decorators import dag, task
    from airflow.models import Param, Variable
    from airflow.operators.python import get_current_context

try:
    from airflow.providers.standard.sensors.filesystem import FileSensor
except ImportError:  # pragma: no cover - Airflow 2.x
    from airflow.sensors.filesystem import FileSensor

from airflow.providers.snowflake.hooks.snowflake import SnowflakeHook

from loader_utils import load_one_file, table_name_for

log = logging.getLogger(__name__)

# Connection ids are names only. Hosts, accounts and passwords live in the
# Airflow Connections store, never in this file.
SNOWFLAKE_CONN_ID = "snowflake_default"
FS_CONN_ID = "northline_fs"

DEFAULT_FOLDER = "/opt/airflow/data/incoming"
SIGNAL_FILE = ".COMPLETE"


def _var(key: str, default: str) -> str:
    """Read an Airflow Variable with a default (works on Airflow 2 and 3).

    Always called INSIDE a task: reading Variables at the top of a DAG file
    hits the database on every parse, which is a known anti-pattern.
    """
    try:
        return Variable.get(key, default=default)
    except TypeError:  # pragma: no cover - Airflow 2.x signature
        return Variable.get(key, default_var=default)


def _source_folder(context) -> str:
    """A run-level Param overrides the Variable; the Variable overrides the default."""
    return context["params"].get("source_folder") or _var("northline_source_folder", DEFAULT_FOLDER)


def _batch_id(context) -> str:
    """Batch id comes from the run's LOGICAL date, never from the clock (now()).

    That is what makes a backfill or a re-run for the same date reproducible.
    """
    return context.get("ds_nodash") or re.sub(r"\W", "", context["run_id"])


@dag(
    dag_id="load_files_to_snowflake",
    description="Wait for .COMPLETE, then load local CSV files into Snowflake tables",
    schedule="0 6 * * *",
    start_date=pendulum.datetime(2026, 11, 1, tz="UTC"),
    catchup=False,
    max_active_runs=1,
    default_args={
        "owner": "matina",
        "retries": 2,
        "retry_delay": timedelta(minutes=5),
    },
    params={
        "source_folder": Param(
            "", type="string",
            description="Optional. Overrides the northline_source_folder Variable for this run.",
        ),
        "archive_after_load": Param(
            True, type="boolean",
            description="Move loaded files and the signal into archive/<batch>/ after success.",
        ),
    },
    tags=["northline", "snowflake", "assignment"],
    doc_md=__doc__,
)
def load_files_to_snowflake():

    # 1) File sensor -------------------------------------------------------
    # Waits for the signal file. `reschedule` frees the worker slot between
    # checks, and the timeout stops it hanging forever.
    wait_for_complete_signal = FileSensor(
        task_id="wait_for_complete_signal",
        fs_conn_id=FS_CONN_ID,
        filepath=(
            "{{ (params.source_folder or var.value.get('northline_source_folder', '"
            + DEFAULT_FOLDER
            + "')) }}/"
            + SIGNAL_FILE
        ),
        poke_interval=30,
        timeout=6 * 60 * 60,
        mode="reschedule",
    )

    # 2) XCom: the return value is a small list of PATHS, not file contents.
    @task
    def list_files() -> list[str]:
        context = get_current_context()
        folder = Path(_source_folder(context))
        files = sorted(
            str(p) for p in folder.glob("*.csv") if p.is_file() and not p.name.startswith(".")
        )
        if not files:
            raise FileNotFoundError(f"{SIGNAL_FILE} is present but no .csv files were found in {folder}")

        expected = [n.strip() for n in _var("northline_expected_files", "").split(",") if n.strip()]
        missing = sorted(set(expected) - {Path(f).name for f in files})
        if missing:
            raise FileNotFoundError(f"Expected files are missing from {folder}: {missing}")

        log.info("Found %d files: %s", len(files), [Path(f).name for f in files])
        return files

    # 3) Dynamic task mapping: one task instance per file, decided at runtime.
    #    4) Connection + 5) Hook + 6) Variables are all used in here.
    @task(map_index_template="{{ my_custom_map_index }}")
    def load_file(path: str) -> dict:
        context = get_current_context()
        context["my_custom_map_index"] = Path(path).name  # shows the file name in the UI

        mapping = json.loads(_var("northline_file_table_map", "{}"))
        table = table_name_for(path, mapping)
        stage = _var("northline_stage_name", "NORTHLINE_STAGE")
        schema = _var("northline_target_schema", "")

        hook = SnowflakeHook(snowflake_conn_id=SNOWFLAKE_CONN_ID, schema=schema or None)
        return load_one_file(hook, path, table=table, stage=stage, batch_id=_batch_id(context))

    # Runs even if some files failed (trigger rule all_done), so the report
    # always says what happened.
    @task(trigger_rule="all_done")
    def report(results) -> dict:
        loaded = [r for r in results if r]
        summary = {
            "files_loaded": len(loaded),
            "total_rows": sum(r["rows_loaded"] for r in loaded),
            "tables": {r["table"]: r["rows_loaded"] for r in loaded},
        }
        log.info("Load summary: %s", json.dumps(summary))
        return summary

    # Only runs when something upstream failed: the alert hook point.
    @task(trigger_rule="one_failed")
    def alert_on_failure() -> None:
        context = get_current_context()
        log.error(
            "ALERT: a file load failed in run %s. Check the mapped load_file tasks. "
            "(Swap this log line for email or Slack in production.)",
            context["run_id"],
        )

    @task
    def archive_files(summary: dict, paths: list[str]) -> str:
        """Move loaded files and the signal away so the next run waits for a fresh .COMPLETE."""
        context = get_current_context()
        if not context["params"].get("archive_after_load", True):
            log.info("archive_after_load is False: leaving files in place")
            return "skipped"
        folder = Path(_source_folder(context))
        target = folder / "archive" / _batch_id(context)
        target.mkdir(parents=True, exist_ok=True)
        for item in [*paths, str(folder / SIGNAL_FILE)]:
            source = Path(item)
            if source.exists():
                shutil.move(str(source), str(target / source.name))
        log.info("Archived %d files to %s", len(paths), target)
        return str(target)

    # Wiring ---------------------------------------------------------------
    files = list_files()
    wait_for_complete_signal >> files

    results = load_file.expand(path=files)  # dynamic mapping over the list
    summary = report(results)
    archived = archive_files(summary, files)
    # report() runs even after failures (all_done), so archiving must ALSO wait on
    # the loads themselves (default all_success). Otherwise one failed file would
    # still get archived and the run would look green.
    results >> archived
    results >> alert_on_failure()


load_files_to_snowflake()
