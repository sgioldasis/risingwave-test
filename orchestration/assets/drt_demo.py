"""Run drt (https://github.com/drt-hub/drt) against the demo project in orchestration/drt_demo/.

drt is a third-party reverse-ETL tool, installed with the other Python dependencies
(pyproject.toml). It reads a Databricks table and writes it to Postgres. This module only
starts it, as a subprocess:

- the Databricks access token is minted here with the same service-principal flow the other
  assets use, and handed to drt in an environment variable, never written to a file;
- drt is started with a minimal environment (PATH, HOME and that token), so it never sees the
  service principal's client secret or the Kafka credentials in this container's environment;
- the profile file holds only the workspace host and SQL warehouse path.

For a comparison with the CDF pipeline; see docs/poc/REVERSE_ETL_DEBEZIUM_JDBC_SINK.md.
"""

import os
import shutil
import subprocess
import sys
from pathlib import Path

from dagster import OpExecutionContext, in_process_executor, job, op

from .databricks_optimize import DATABRICKS_HOST, WAREHOUSE_ID, _get_token
from .postgres_sink_setup import get_postgres_connection
from .reverse_etl_cdf_setup import _get_row_fields, _require_databricks_env
from .reverse_etl_notebook_job import _load_sync_config

DRT_BIN = Path(sys.executable).parent / "drt"  # the drt script of the environment running Dagster
PROJECT_SOURCE = Path(__file__).resolve().parent.parent / "drt_demo"
# drt writes state next to the project, so run it from a writable copy that persists.
WORKDIR = Path(os.environ.get("DRT_WORKDIR", "/home/dagster/drt-demo"))
PROFILE_NAME = "reverse_etl_drt"
TOKEN_ENV = "DRT_DATABRICKS_TOKEN"


def _write_profile(home: Path) -> None:
    profile_dir = home / ".drt"
    profile_dir.mkdir(parents=True, exist_ok=True)
    hostname = DATABRICKS_HOST.removeprefix("https://").removeprefix("http://").rstrip("/")
    (profile_dir / "profiles.yml").write_text(
        f"{PROFILE_NAME}:\n"
        "  type: databricks\n"
        f"  server_hostname: {hostname}\n"
        f"  http_path: /sql/1.0/warehouses/{WAREHOUSE_ID}\n"
        f"  access_token_env: {TOKEN_ENV}\n"
        "  catalog: de_dev\n"
        "  schema: sr_poc_external\n"
    )


def run_drt(args: list[str], timeout: int = 900, extra_env: dict[str, str] | None = None) -> subprocess.CompletedProcess:
    """Run `drt <args>` in the demo project and return the finished process (output captured)."""
    _require_databricks_env()
    if not DRT_BIN.exists():
        raise RuntimeError(f"drt is not installed at {DRT_BIN}; it is in pyproject.toml, so rebuild the Dagster image or run uv sync")

    WORKDIR.mkdir(parents=True, exist_ok=True)
    shutil.copytree(PROJECT_SOURCE, WORKDIR, dirs_exist_ok=True, ignore=shutil.ignore_patterns("__pycache__"))
    home = Path(os.environ.get("HOME", "/home/dagster"))
    _write_profile(home)

    env = {"PATH": f"{DRT_BIN.parent}:/usr/bin:/bin", "HOME": str(home), TOKEN_ENV: _get_token(), **(extra_env or {})}
    return subprocess.run(
        [str(DRT_BIN), *args],
        cwd=WORKDIR,
        env=env,
        capture_output=True,
        text=True,
        timeout=timeout,
    )


# Names follow reverse_etl_<label>_<role>; the label is cdf_drt (target table reverse_etl_cdf_drt_target).
DRT_SYNC_NAME = "cdf_to_postgres"


@op(name="reverse_etl_cdf_drt_sync")
def run_drt_sync(context: OpExecutionContext) -> None:
    # PGTZ=UTC: drt writes timestamps without a timezone, and Postgres would read them in its own zone.
    result = run_drt(["run", "--select", DRT_SYNC_NAME, "--verbose"], extra_env={"PGTZ": "UTC"})
    output = "\n".join(
        line for line in (result.stdout + result.stderr).splitlines() if "pyarrow" not in line and line.strip()
    )
    context.log.info(output)
    if result.returncode != 0:
        raise RuntimeError(f"drt sync {DRT_SYNC_NAME} failed (exit {result.returncode}):\n{output[-1500:]}")


@job(
    name="reverse_etl_cdf_drt_sync_job",
    description=(
        "Comparison demo: sync the Databricks table reverse_etl_cdf_source straight into the Postgres table "
        "reverse_etl_cdf_drt_target with drt (mirror, no Kafka). Does not alter the target table, so a new "
        "source column makes it fail until the table is altered by hand."
    ),
    executor_def=in_process_executor,
)
def reverse_etl_cdf_drt_sync_job():
    run_drt_sync()


# drt never creates or alters its destination, so this job creates it. Postgres types for the
# Connect types _get_row_fields() returns (the same mapping the Debezium sink applies).
DRT_TARGET_TABLE = "reverse_etl_cdf_drt_target"  # must match destination.table in syncs/cdf_to_postgres.yml
_POSTGRES_TYPE_BY_CONNECT_TYPE = {
    "int8": "smallint",
    "int16": "smallint",
    "int32": "integer",
    "int64": "bigint",
    "float": "real",
    "double": "double precision",
    "boolean": "boolean",
    "zoned_timestamp": "timestamptz",
    "string": "text",
}


@op(name="reverse_etl_cdf_drt_target_setup")
def setup_drt_target(context: OpExecutionContext) -> None:
    _require_databricks_env()
    cfg = _load_sync_config("cdf")
    columns = []
    for name, connect_type, _ in _get_row_fields(_get_token(), cfg):
        column_type = _POSTGRES_TYPE_BY_CONNECT_TYPE.get(connect_type, "text")
        suffix = " PRIMARY KEY" if name == cfg.key_column else ""
        columns.append(f'"{name}" {column_type}{suffix}')
    conn = get_postgres_connection()
    try:
        with conn.cursor() as cur:
            cur.execute(f'CREATE TABLE IF NOT EXISTS "{DRT_TARGET_TABLE}" ({", ".join(columns)})')
        conn.commit()
    finally:
        conn.close()
    context.log.info(f"Ensured Postgres table {DRT_TARGET_TABLE} with columns: {', '.join(columns)}")


@job(
    name="reverse_etl_cdf_drt_setup_job",
    description=(
        "Create the Postgres table reverse_etl_cdf_drt_target for the drt demo, with the source table's "
        "current columns. Does nothing if the table exists (it does not add columns)."
    ),
    executor_def=in_process_executor,
)
def reverse_etl_cdf_drt_setup_job():
    setup_drt_target()


# drt's own bookkeeping table for the tracked mirror (fixed name, created in the destination database).
DRT_KEYS_TABLE = "_drt_synced_keys"


@op(name="reverse_etl_cdf_drt_target_reset")
def reset_drt_target(context: OpExecutionContext) -> None:
    # Same idea as the allowlist in reverse_etl_reset.py: only drop a table that looks like ours.
    if not DRT_TARGET_TABLE.startswith("reverse_etl_"):
        raise RuntimeError(f"Refusing to reset: {DRT_TARGET_TABLE} does not start with reverse_etl_")
    conn = get_postgres_connection()
    try:
        with conn.cursor() as cur:
            cur.execute(f'DROP TABLE IF EXISTS "{DRT_TARGET_TABLE}", "{DRT_KEYS_TABLE}"')
        conn.commit()
    finally:
        conn.close()
    # drt's local run state and history live in the work directory copy; the project files are
    # copied again on the next run.
    for name in (".drt", "target"):
        shutil.rmtree(WORKDIR / name, ignore_errors=True)
    context.log.info(f"Dropped Postgres tables {DRT_TARGET_TABLE} and {DRT_KEYS_TABLE}, and drt's state in {WORKDIR}")


@job(
    name="reverse_etl_cdf_drt_reset_job",
    description=(
        "Reset the drt demo: drop the Postgres table reverse_etl_cdf_drt_target and drt's key-tracking table "
        "_drt_synced_keys, and clear drt's local run state. The Databricks source table is not touched. "
        "Run reverse_etl_cdf_drt_setup_job afterwards."
    ),
    executor_def=in_process_executor,
)
def reverse_etl_cdf_drt_reset_job():
    reset_drt_target()
