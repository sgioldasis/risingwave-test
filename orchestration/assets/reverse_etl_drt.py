"""Run drt (https://github.com/drt-hub/drt) syncs for the reverse-ETL tables, for comparison with the Kafka/Debezium pipeline.

drt is a third-party reverse-ETL tool; with the community package dagster-drt (both in
pyproject.toml) a sync shows up as a Dagster asset. `build_drt_defs(cfg)` builds, for one
ReverseEtlSyncConfig (the same config the pipeline uses), the asset `<sync_name>_drt_to_postgres`
(Databricks source table to a Postgres table), a setup job that creates the target table and a
reset job that drops it. Names are derived from `cfg.sync_name`, for example
reverse_etl_orders_drt_to_postgres and reverse_etl_orders_drt_target. The sync definition is a
file in orchestration/drt_demo/syncs/<sync_name>_drt.yml.

dagster-drt runs drt inside the Dagster run process, so unlike a subprocess it sees that
process's whole environment. What this module adds:

- the Databricks access token is minted with the same service-principal flow the other assets
  use and put in an environment variable for the run, never written to a file;
- PGTZ=UTC, because drt writes timestamps without a timezone and Postgres would otherwise read
  them in its own zone;
- the profile file holds only the workspace host and SQL warehouse path;
- dagster-drt reports a sync with failed rows as a successful materialization, so the asset
  raises when rows_failed is above zero;
- a check that the sync file agrees with the config (name, target table, key, source table).

See docs/poc/REVERSE_ETL_DRT_COMPARISON.md.
"""

import os
import shutil
from dataclasses import dataclass
from pathlib import Path

import yaml
from dagster import (
    AssetExecutionContext,
    AssetKey,
    Definitions,
    MaterializeResult,
    OpExecutionContext,
    in_process_executor,
    job,
    op,
)
from dagster_drt import DagsterDrtResource, DagsterDrtTranslator, drt_assets

from .databricks_optimize import DATABRICKS_HOST, WAREHOUSE_ID, _get_token
from .postgres_sink_setup import get_postgres_connection
from .reverse_etl_cdf_setup import _get_row_fields, _require_databricks_env
from .reverse_etl_config import ReverseEtlSyncConfig

PROJECT_SOURCE = Path(__file__).resolve().parent.parent / "drt_demo"
# drt writes state next to the project and the source mount is read-only, so it runs from a copy.
WORKDIR = Path(os.environ.get("DRT_WORKDIR", "/home/dagster/drt-demo"))
PROFILE_NAME = "reverse_etl_drt"
TOKEN_ENV = "DRT_DATABRICKS_TOKEN"
# drt's own bookkeeping table for the tracked mirror: a fixed name, one per database, with a
# sync_name column, shared by every drt sync that writes there.
DRT_KEYS_TABLE = "_drt_synced_keys"

# One resource for all drt syncs; register it once in definitions.py.
drt_resource = DagsterDrtResource(project_dir=str(WORKDIR))

# Postgres types for the Connect types _get_row_fields() returns (the mapping the Debezium sink uses).
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


@dataclass(frozen=True)
class DrtNames:
    """Every name of one drt sync, derived from the pipeline sync's name (reverse_etl_<label>)."""

    sync_name: str  # drt sync name and sync file stem
    asset: str
    target_table: str
    setup_job: str
    reset_job: str
    group_name: str

    @classmethod
    def for_config(cls, cfg: ReverseEtlSyncConfig) -> "DrtNames":
        base = f"{cfg.sync_name}_drt"
        return cls(
            sync_name=base,
            asset=f"{base}_to_postgres",
            target_table=f"{base}_target",
            setup_job=f"{base}_setup_job",
            reset_job=f"{base}_reset_job",
            group_name=base,
        )

    @property
    def sync_file(self) -> Path:
        return PROJECT_SOURCE / "syncs" / f"{self.sync_name}.yml"


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


def _check_sync_file(cfg: ReverseEtlSyncConfig, names: DrtNames) -> None:
    """The sync file repeats names that the config derives; refuse to run if they disagree."""
    if not names.sync_file.exists():
        raise RuntimeError(f"drt sync file {names.sync_file} not found (expected one per sync, named <sync_name>_drt.yml)")
    doc = yaml.safe_load(names.sync_file.read_text())
    destination = doc.get("destination", {})
    problems = []
    if doc.get("name") != names.sync_name:
        problems.append(f"name is {doc.get('name')!r}, expected {names.sync_name!r}")
    if destination.get("table") != names.target_table:
        problems.append(f"destination.table is {destination.get('table')!r}, expected {names.target_table!r}")
    if destination.get("upsert_key") != [cfg.key_column]:
        problems.append(f"destination.upsert_key is {destination.get('upsert_key')!r}, expected {[cfg.key_column]!r}")
    if cfg.source_fqn not in str(doc.get("model", "")):
        problems.append(f"model does not read {cfg.source_fqn}")
    if problems:
        raise RuntimeError(f"{names.sync_file.name} disagrees with the sync config: " + "; ".join(problems))


def _build_asset(cfg: ReverseEtlSyncConfig, names: DrtNames):
    class _Translator(DagsterDrtTranslator):
        def get_asset_spec(self, data):
            return super().get_asset_spec(data).replace_attributes(
                key=AssetKey(names.asset),
                group_name=names.group_name,
                deps=[AssetKey(cfg.table_setup_asset)],  # the Databricks source table
            )

    # The sync definitions are read from the (read-only) project at load time; the run uses the
    # resource's project_dir, a writable copy.
    @drt_assets(
        project_dir=PROJECT_SOURCE,
        sync_names=[names.sync_name],
        dagster_drt_translator=_Translator(),
        name=names.asset,
    )
    def drt_to_postgres(context: AssetExecutionContext, drt: DagsterDrtResource):
        _require_databricks_env()
        _check_sync_file(cfg, names)
        WORKDIR.mkdir(parents=True, exist_ok=True)
        shutil.copytree(PROJECT_SOURCE, WORKDIR, dirs_exist_ok=True, ignore=shutil.ignore_patterns("__pycache__"))
        _write_profile(Path.home())
        os.environ[TOKEN_ENV] = _get_token()
        os.environ["PGTZ"] = "UTC"
        for event in drt.run(context=context):
            failed = event.metadata["rows_failed"].value if isinstance(event, MaterializeResult) else 0
            if failed:
                raise RuntimeError(
                    f"drt sync {names.sync_name} failed {failed} row(s); the row errors are in the log above "
                    "(for example a source column the target table lacks)"
                )
            yield event

    return drt_to_postgres


def _build_setup_job(cfg: ReverseEtlSyncConfig, names: DrtNames):
    # drt never creates or alters its destination, so this job creates it.
    @op(name=f"{names.sync_name}_target_setup")
    def setup_target(context: OpExecutionContext) -> None:
        _require_databricks_env()
        columns = []
        for name, connect_type, _ in _get_row_fields(_get_token(), cfg):
            column_type = _POSTGRES_TYPE_BY_CONNECT_TYPE.get(connect_type, "text")
            suffix = " PRIMARY KEY" if name == cfg.key_column else ""
            columns.append(f'"{name}" {column_type}{suffix}')
        conn = get_postgres_connection()
        try:
            with conn.cursor() as cur:
                cur.execute(f'CREATE TABLE IF NOT EXISTS "{names.target_table}" ({", ".join(columns)})')
            conn.commit()
        finally:
            conn.close()
        context.log.info(f"Ensured Postgres table {names.target_table} with columns: {', '.join(columns)}")

    @job(
        name=names.setup_job,
        description=(
            f"Create the Postgres table {names.target_table} for the drt sync of {cfg.source_table}, with the "
            "source table's current columns. Does nothing if the table exists (it does not add columns)."
        ),
        executor_def=in_process_executor,
    )
    def setup_job():
        setup_target()

    return setup_job


def _build_reset_job(cfg: ReverseEtlSyncConfig, names: DrtNames):
    @op(name=f"{names.sync_name}_target_reset")
    def reset_target(context: OpExecutionContext) -> None:
        # Same idea as the allowlist in reverse_etl_reset.py: only drop a table that looks like ours.
        if not names.target_table.startswith(cfg.reset_name_prefixes):
            raise RuntimeError(f"Refusing to reset: {names.target_table} does not start with {cfg.reset_name_prefixes}")
        conn = get_postgres_connection()
        try:
            with conn.cursor() as cur:
                cur.execute(f'DROP TABLE IF EXISTS "{names.target_table}"')
                cur.execute("SELECT to_regclass(%s)", (DRT_KEYS_TABLE,))
                if cur.fetchone()[0] is not None:
                    # The key table is shared, so remove only this sync's rows.
                    cur.execute(f'DELETE FROM "{DRT_KEYS_TABLE}" WHERE sync_name = %s', (names.sync_name,))
            conn.commit()
        finally:
            conn.close()
        context.log.info(f"Dropped Postgres table {names.target_table} and the {names.sync_name} rows in {DRT_KEYS_TABLE}")

    @job(
        name=names.reset_job,
        description=(
            f"Reset the drt sync of {cfg.source_table}: drop the Postgres table {names.target_table} and this sync's "
            f"rows in drt's key table {DRT_KEYS_TABLE}. The Databricks source table and other drt syncs are not "
            f"touched. Run {names.setup_job} afterwards."
        ),
        executor_def=in_process_executor,
    )
    def reset_job():
        reset_target()

    return reset_job


def build_drt_defs(cfg: ReverseEtlSyncConfig) -> Definitions:
    names = DrtNames.for_config(cfg)
    return Definitions(
        assets=[_build_asset(cfg, names)],
        jobs=[_build_setup_job(cfg, names), _build_reset_job(cfg, names)],
    )
