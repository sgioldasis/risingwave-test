"""Run drt (https://github.com/drt-hub/drt) against the demo project in orchestration/drt_demo/.

drt is a third-party reverse-ETL tool; with the community package dagster-drt (both in
pyproject.toml) its sync shows up as the Dagster asset reverse_etl_cdf_drt_to_postgres. It reads
the Databricks table and writes it to Postgres. dagster-drt runs drt inside the Dagster run
process, so unlike a subprocess it sees that process's whole environment. What this module adds:

- the Databricks access token is minted with the same service-principal flow the other assets
  use and put in an environment variable for the run, never written to a file;
- PGTZ=UTC, because drt writes timestamps without a timezone and Postgres would otherwise read
  them in its own zone;
- the profile file holds only the workspace host and SQL warehouse path;
- dagster-drt reports a sync with failed rows as a successful materialization, so the asset
  raises when rows_failed is above zero.

For a comparison with the CDF pipeline; see docs/poc/REVERSE_ETL_DRT_COMPARISON.md.
"""

import os
import shutil
from pathlib import Path

from dagster import AssetExecutionContext, AssetKey, MaterializeResult, in_process_executor, job, op, OpExecutionContext
from dagster_drt import DagsterDrtResource, DagsterDrtTranslator, drt_assets

from .databricks_optimize import DATABRICKS_HOST, WAREHOUSE_ID, _get_token
from .postgres_sink_setup import get_postgres_connection
from .reverse_etl_cdf_setup import _get_row_fields, _require_databricks_env
from .reverse_etl_notebook_job import _load_sync_config

PROJECT_SOURCE = Path(__file__).resolve().parent.parent / "drt_demo"
# drt writes state next to the project and the source mount is read-only, so it runs from a copy.
WORKDIR = Path(os.environ.get("DRT_WORKDIR", "/home/dagster/drt-demo"))
PROFILE_NAME = "reverse_etl_drt"
TOKEN_ENV = "DRT_DATABRICKS_TOKEN"

# Names follow reverse_etl_<label>_<role>; the label is cdf_drt (target table reverse_etl_cdf_drt_target).
DRT_SYNC_NAME = "cdf_to_postgres"
DRT_ASSET_KEY = AssetKey("reverse_etl_cdf_drt_to_postgres")

drt_resource = DagsterDrtResource(project_dir=str(WORKDIR))


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


class _DrtTranslator(DagsterDrtTranslator):
    def get_asset_spec(self, data):
        spec = super().get_asset_spec(data)
        return spec.replace_attributes(
            key=DRT_ASSET_KEY,
            group_name="reverse_etl_cdf_drt",
            deps=[AssetKey(_load_sync_config("cdf").table_setup_asset)],  # the Databricks source table
        )


# The sync definitions are read from the (read-only) project at load time; the run itself uses
# the resource's project_dir, a writable copy.
@drt_assets(
    project_dir=PROJECT_SOURCE,
    sync_names=[DRT_SYNC_NAME],
    dagster_drt_translator=_DrtTranslator(),
    name="reverse_etl_cdf_drt_to_postgres",
)
def reverse_etl_cdf_drt_to_postgres(context: AssetExecutionContext, drt: DagsterDrtResource):
    _require_databricks_env()
    WORKDIR.mkdir(parents=True, exist_ok=True)
    shutil.copytree(PROJECT_SOURCE, WORKDIR, dirs_exist_ok=True, ignore=shutil.ignore_patterns("__pycache__"))
    _write_profile(Path.home())
    os.environ[TOKEN_ENV] = _get_token()
    os.environ["PGTZ"] = "UTC"
    for event in drt.run(context=context):
        failed = event.metadata["rows_failed"].value if isinstance(event, MaterializeResult) else 0
        if failed:
            raise RuntimeError(
                f"drt sync {DRT_SYNC_NAME} failed {failed} row(s); the row errors are in the log above "
                "(for example a source column the target table lacks)"
            )
        yield event


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
