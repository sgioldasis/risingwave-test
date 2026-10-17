"""One demo change (insert, update, delete, add or drop a column) against a reverse-ETL sync's source table.

The live-demo animation (docs/poc/reverse_etl_data_flow.html) launches this from step 1. The change is built
from the sync's source_columns, so it works for any sync: the key is `id` (max + 1 for an insert, the row with
the highest id for an update or delete), text columns get a timestamped value, numbers a random value, and
timestamp columns now. Nothing here is synced until the sync asset runs.
"""

import random
import re
import time

from dagster import AssetExecutionContext, AssetKey, AssetSelection, Config, asset, define_asset_job, in_process_executor, MetadataValue

from .databricks_cost_meter import reset as meter_reset, snapshot as meter_snapshot
from .databricks_optimize import _get_token
from .reverse_etl_cdf_setup import _require_databricks_env, _rows_as_dicts, _run_sql
from .reverse_etl_config import ReverseEtlSyncConfig

KEY = "id"


class ChangeConfig(Config):
    change: str = "update"  # insert, update, delete, add_column or drop_column


def _columns(cfg: ReverseEtlSyncConfig) -> list[tuple[str, str]]:
    """(name, type) per column of cfg.source_columns, without the key."""
    parsed = [tuple(column.split()[:2]) for column in cfg.source_columns]
    return [(name, sql_type.upper()) for name, sql_type in parsed if name != KEY]


def _value(sql_type: str) -> str:
    if sql_type == "STRING":
        return f"'demo {time.strftime('%H:%M:%S')}'"
    if sql_type == "TIMESTAMP":
        return "current_timestamp()"
    return str(round(random.uniform(1, 100), 2))


def _new_column(existing: set[str]) -> str:
    n = 1
    while f"new_col_{n}" in existing:
        n += 1
    return f"new_col_{n}"


def change_statements(cfg: ReverseEtlSyncConfig, change: str, existing_columns: set[str] = frozenset()) -> list[str]:
    """add_column adds the next free new_col_<n> STRING column, then updates the latest row so the
    change carries it (a new column alone makes no change event). drop_column is its inverse: it
    drops the highest-numbered new_col_<n>. A drop alone also makes no change event -- the sync
    detects it by diffing schemas (state table's last_schema vs the current read)."""
    table = cfg.source_fqn
    columns = _columns(cfg)
    if change == "add_column":
        column = _new_column(existing_columns)
        return [
            f"ALTER TABLE {table} ADD COLUMN {column} STRING",
            f"UPDATE {table} SET {column} = 'added {time.strftime('%H:%M:%S')}' WHERE {KEY} = (SELECT max({KEY}) FROM {table})",
        ]
    if change == "drop_column":
        numbers = [
            int(match.group(1))
            for name in existing_columns
            if (match := re.fullmatch(r"new_col_(\d+)", name))
        ]
        if not numbers:
            raise ValueError(
                f"no new_col_<n> column to drop on {table} -- run add_column first "
                "(the sync's configured columns are not dropped by this demo op)"
            )
        return [f"ALTER TABLE {table} DROP COLUMN new_col_{max(numbers)}"]
    if change == "insert":
        names = ", ".join([KEY] + [name for name, _ in columns])
        values = ", ".join([f"coalesce(max({KEY}), 0) + 1"] + [_value(t) for _, t in columns])
        return [f"INSERT INTO {table} ({names}) SELECT {values} FROM {table}"]
    latest = f"{KEY} = (SELECT max({KEY}) FROM {table})"
    if change == "update":
        assignments = ", ".join(f"{name} = {_value(t)}" for name, t in columns)
        return [f"UPDATE {table} SET {assignments} WHERE {latest}"]
    if change == "delete":
        return [f"DELETE FROM {table} WHERE {latest}"]
    raise ValueError(f"change must be insert, update, delete, add_column or drop_column, got {change!r}")


def build_change_asset(cfg: ReverseEtlSyncConfig):
    @asset(
        name=f"{cfg.sync_name}_make_change",
        description=f"Insert, update or delete one row of {cfg.source_fqn}. Not synced until {cfg.sync_asset} runs.",
    )
    def make_change(context: AssetExecutionContext, config: ChangeConfig):
        _require_databricks_env()
        token = _get_token()
        meter_reset()
        existing = set()
        if config.change in ("add_column", "drop_column"):
            existing = {r["col_name"] for r in _rows_as_dicts(_run_sql(token, f"DESCRIBE TABLE {cfg.source_fqn}"))}
        statements = list(change_statements(cfg, config.change, existing))
        for statement in statements:
            _run_sql(token, statement)
            context.log.info(f"{config.change} on {cfg.source_fqn}: {statement}")
        usage = meter_snapshot()
        context.add_output_metadata({
            "databricks_warehouse_seconds": MetadataValue.float(usage["warehouse_seconds"]),
            "databricks_statements": MetadataValue.int(usage["statements"]),
            # The exact SQL the demo page shows under the run result.
            "databricks_change_sql": MetadataValue.json({
                "change": config.change, "table": cfg.source_fqn, "sql": statements,
            }),
        })

    return make_change


def build_change_job(cfg: ReverseEtlSyncConfig, make_change_asset):
    return define_asset_job(
        name=f"{cfg.sync_name}_make_change_job",
        selection=AssetSelection.assets(AssetKey(f"{cfg.sync_name}_make_change")),
        executor_def=in_process_executor,
    )
