"""One demo change (insert, update, delete, add, rename or drop a column) against a reverse-ETL sync's source table.

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


class DemoChangeSkip(ValueError):
    """A demo change that has nothing to do (rename or drop with no demo column on the table). The
    make_change asset turns this into a green no-op run that tells the page why, instead of a red
    failure -- the demo columns come and go with the add/rename/drop cycle."""


class ChangeConfig(Config):
    change: str = "update"  # insert, update, delete, add_column, rename_column or drop_column


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


def _demo_column(existing: set[str], prefixes: tuple[str, ...] = ("new_col_", "renamed_col_")) -> tuple[str, int] | None:
    """The highest-numbered demo column matching any prefix, as (name, n)."""
    best: tuple[str, int] | None = None
    for name in existing:
        for prefix in prefixes:
            if (match := re.fullmatch(rf"{re.escape(prefix)}(\d+)", name)) and (best is None or int(match.group(1)) > best[1]):
                best = (f"{prefix}{match.group(1)}", int(match.group(1)))
    return best


def _rename_pair(existing: set[str]) -> tuple[str, str]:
    """The demo rename's (old, new): the highest new_col_<n> flips to renamed_col_<n>; when only
    renamed columns remain, the highest renames back -- one click each way, data surviving both."""
    new_numbers = [
        int(match.group(1))
        for name in existing
        if (match := re.fullmatch(r"new_col_(\d+)", name))
    ]
    if new_numbers:
        n = max(new_numbers)
        if f"renamed_col_{n}" in existing:
            raise DemoChangeSkip(
                f"both new_col_{n} and renamed_col_{n} exist on the table -- drop one before renaming again"
            )
        return f"new_col_{n}", f"renamed_col_{n}"
    renamed_numbers = [
        int(match.group(1))
        for name in existing
        if (match := re.fullmatch(r"renamed_col_(\d+)", name))
    ]
    if renamed_numbers:
        n = max(renamed_numbers)
        return f"renamed_col_{n}", f"new_col_{n}"
    raise DemoChangeSkip(
        "no new_col_<n> or renamed_col_<n> column to rename on the table -- run add_column first "
        "(the sync's configured columns are not renamed by this demo op)"
    )


def change_statements(cfg: ReverseEtlSyncConfig, change: str, existing_columns: set[str] = frozenset()) -> list[str]:
    """add_column adds the next free new_col_<n> STRING column, then updates the latest row so the
    change carries it (a new column alone makes no change event). rename_column flips the highest
    new_col_<n> to renamed_col_<n> (or back) -- a rename makes no change event either; the sync's
    rename propagation carries it with the data intact. drop_column is the inverse of both: it
    drops the highest-numbered new_col_<n> or renamed_col_<n>. A drop alone also makes no change
    event -- the sync detects it by diffing schemas (state table's last_schema vs the current read)."""
    table = cfg.source_fqn
    columns = _columns(cfg)
    if change == "add_column":
        column = _new_column(existing_columns)
        return [
            f"ALTER TABLE {table} ADD COLUMN {column} STRING",
            f"UPDATE {table} SET {column} = 'added {time.strftime('%H:%M:%S')}' WHERE {KEY} = (SELECT max({KEY}) FROM {table})",
        ]
    if change == "rename_column":
        old, new = _rename_pair(existing_columns)
        return [f"ALTER TABLE {table} RENAME COLUMN {old} TO {new}"]
    if change == "drop_column":
        demo_column = _demo_column(existing_columns)
        if demo_column is None:
            raise DemoChangeSkip(
                "no new_col_<n> or renamed_col_<n> column to drop on the table -- run add_column first "
                "(the sync's configured columns are not dropped by this demo op)"
            )
        return [f"ALTER TABLE {table} DROP COLUMN {demo_column[0]}"]
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
    raise ValueError(f"change must be insert, update, delete, add_column, rename_column or drop_column, got {change!r}")


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
        if config.change in ("add_column", "rename_column", "drop_column"):
            existing = {r["col_name"] for r in _rows_as_dicts(_run_sql(token, f"DESCRIBE TABLE {cfg.source_fqn}"))}
        try:
            statements = list(change_statements(cfg, config.change, existing))
        except DemoChangeSkip as skip:
            # "Nothing to rename/drop" is a normal demo condition (the demo columns come and go with
            # the add/rename/drop cycle), not a failure: end the run green and tell the page why.
            context.log.warning(f"{config.change} on {cfg.source_fqn} skipped: {skip}")
            context.add_output_metadata({
                "databricks_warehouse_seconds": MetadataValue.float(meter_snapshot()["warehouse_seconds"]),
                "databricks_statements": MetadataValue.int(0),
                "databricks_change_sql": MetadataValue.json({
                    "change": config.change, "table": cfg.source_fqn, "sql": [], "skipped": str(skip),
                }),
            })
            return
        for statement in statements:
            try:
                _run_sql(token, statement)
            except RuntimeError as error:
                if "UNRESOLVED_COLUMN" in str(error):
                    # The change ops build their SQL from the sync's source_columns: a configured
                    # column that no longer exists on the table (usually a RENAME COLUMN done by
                    # hand) means the defs.yaml has not followed the rename.
                    raise RuntimeError(
                        f"{error}\nThe change ops build their SQL from the sync's source_columns, and "
                        "one of those columns no longer exists on the table — the source schema was "
                        "changed (a rename?) without updating source_columns/seed_statements in the "
                        "sync's defs.yaml."
                    ) from error
                raise
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
