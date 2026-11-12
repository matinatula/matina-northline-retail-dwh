"""Helpers for the load_files_to_snowflake DAG.

Why this file exists
--------------------
The session rule is "keep DAG files thin": the DAG file declares the *shape*
(tasks, order, schedule) and everything else lives here. This module imports
nothing from Airflow, so it can be unit-tested without a scheduler.

What it does (and does not do)
------------------------------
* Builds the SQL that Snowflake runs. Airflow only *triggers* the work; the
  database does it (PUT to a stage, then COPY INTO).
* NO data transformation: every source column lands as VARCHAR, exactly as
  it appears in the file.
* Three audit columns are added to each table (SRC_FILE_NM, BATCH_ID,
  INGEST_TS). They describe the load, they do not change the data.
"""
from __future__ import annotations

import csv
import re
from pathlib import Path

AUDIT_COLUMNS = ("SRC_FILE_NM", "BATCH_ID", "INGEST_TS")


# --------------------------------------------------------------------------
# Names
# --------------------------------------------------------------------------
def sanitize_identifier(raw: str, fallback: str = "COL") -> str:
    """Turn any text into a safe, upper-case Snowflake identifier."""
    name = re.sub(r"[^0-9A-Za-z_]+", "_", str(raw).strip()).strip("_").upper()
    if not name:
        name = fallback
    if name[0].isdigit():
        name = "C_" + name
    return name


def unique_columns(header: list[str]) -> list[str]:
    """Sanitised column names, de-duplicated, never clashing with audit columns."""
    seen = set(AUDIT_COLUMNS)
    result = []
    for position, raw in enumerate(header, start=1):
        base = sanitize_identifier(raw, fallback=f"COL_{position}")
        name, n = base, 2
        while name in seen:
            name = f"{base}_{n}"
            n += 1
        seen.add(name)
        result.append(name)
    return result


def table_name_for(path: str, mapping: dict[str, str] | None = None, prefix: str = "STG_") -> str:
    """Target table for a file: the Variable mapping wins, else STG_<FILE STEM>."""
    file_name = Path(path).name
    if mapping and file_name in mapping:
        return sanitize_identifier(mapping[file_name])
    return sanitize_identifier(prefix + Path(path).stem)


# --------------------------------------------------------------------------
# Reading the file (header and row count only; never the whole file in memory)
# --------------------------------------------------------------------------
def read_header(path: str) -> list[str]:
    with open(path, newline="", encoding="utf-8-sig") as handle:
        try:
            return next(csv.reader(handle))
        except StopIteration:
            raise ValueError(f"{path} is empty: no header row") from None


def count_data_rows(path: str) -> int:
    """Rows after the header, ignoring blank lines. Streams the file."""
    with open(path, newline="", encoding="utf-8-sig") as handle:
        rows = sum(1 for row in csv.reader(handle) if row)
    return max(rows - 1, 0)


# --------------------------------------------------------------------------
# SQL builders
# --------------------------------------------------------------------------
def _lit(value: str) -> str:
    """Escape a value for use inside a single-quoted SQL literal."""
    return str(value).replace("'", "''")


def sql_create_stage(stage: str) -> str:
    return f"CREATE STAGE IF NOT EXISTS {stage}"


def sql_create_table(table: str, columns: list[str]) -> str:
    """All-text table. CREATE OR REPLACE makes the step safe to re-run."""
    body = ",\n    ".join(f"{c} VARCHAR" for c in columns)
    return (
        f"CREATE OR REPLACE TABLE {table} (\n    {body},\n"
        f"    SRC_FILE_NM VARCHAR,\n"
        f"    BATCH_ID VARCHAR,\n"
        f"    INGEST_TS TIMESTAMP_NTZ DEFAULT SYSDATE()\n)"
    )


def sql_clear_stage_path(stage: str, table: str) -> str:
    return f"REMOVE @{stage}/{table}/"


def sql_put(path: str, stage: str, table: str) -> str:
    absolute = Path(path).resolve().as_posix()
    return (
        f"PUT 'file://{_lit(absolute)}' @{stage}/{table}/ "
        f"AUTO_COMPRESS = FALSE OVERWRITE = TRUE"
    )


def sql_copy(table: str, columns: list[str], stage: str, file_name: str) -> str:
    """FORCE = TRUE matters: without it Snowflake remembers the file was
    loaded and silently skips it on a re-run, which would break idempotency."""
    return (
        f"COPY INTO {table} ({', '.join(columns)})\n"
        f"FROM @{stage}/{table}/\n"
        f"FILES = ('{_lit(file_name)}')\n"
        f"FILE_FORMAT = (TYPE = CSV SKIP_HEADER = 1 "
        f"FIELD_OPTIONALLY_ENCLOSED_BY = '\"' ESCAPE_UNENCLOSED_FIELD = NONE)\n"
        f"FORCE = TRUE\n"
        f"ON_ERROR = ABORT_STATEMENT"
    )


def sql_stamp(table: str, file_name: str, batch_id: str) -> str:
    return (
        f"UPDATE {table} SET SRC_FILE_NM = '{_lit(file_name)}', "
        f"BATCH_ID = '{_lit(batch_id)}'"
    )


# --------------------------------------------------------------------------
# One file, end to end
# --------------------------------------------------------------------------
def load_one_file(hook, path: str, table: str, stage: str, batch_id: str) -> dict:
    """Load one CSV into one table and prove the row counts match.

    Idempotent: the table is rebuilt and the stage path is cleared first, so a
    retry or a re-run for the same date gives the same result.

    `hook` is a SnowflakeHook (or anything with .run and .get_first), which is
    what lets the tests run this function without a real Snowflake account.
    """
    file_name = Path(path).name
    columns = unique_columns(read_header(path))
    rows_in_file = count_data_rows(path)

    statements = [
        sql_create_stage(stage),
        sql_create_table(table, columns),
        sql_clear_stage_path(stage, table),
        sql_put(path, stage, table),
        sql_copy(table, columns, stage, file_name),
        sql_stamp(table, file_name, batch_id),
    ]
    for statement in statements:
        hook.run(statement, autocommit=True, split_statements=False)

    rows_loaded = int(hook.get_first(f"SELECT COUNT(*) FROM {table}")[0])
    if rows_loaded != rows_in_file:
        raise ValueError(
            f"Row count mismatch for {file_name} -> {table}: "
            f"file has {rows_in_file} data rows, table has {rows_loaded}"
        )

    return {
        "file": file_name,
        "table": table,
        "rows_in_file": rows_in_file,
        "rows_loaded": rows_loaded,
        "batch_id": batch_id,
    }
