"""One demo change (insert, update, delete, add, rename or drop a column) against a reverse-ETL sync's source table.

The live-demo animation (docs/poc/reverse_etl_data_flow.html) launches this from step 1. The change is built
from the sync's source_columns, so it works for any sync: the key is `id` (max + 1 for an insert, the row with
the highest id for an update or delete), text columns get a timestamped value, numbers a random value, and
timestamp columns now. Nothing here is synced until the sync asset runs.
"""

import os
import random
import re
import time

from dagster import (
    AssetExecutionContext,
    AssetKey,
    AssetSelection,
    Config,
    MetadataValue,
    asset,
    define_asset_job,
    in_process_executor,
)

from .databricks_cost_meter import snapshot as meter_snapshot
from .reverse_etl_cdf_setup import _rows_as_dicts
from .reverse_etl_config import ReverseEtlSyncConfig
from .reverse_etl_resources import DatabricksResource

KEY = "id"
# Per-commit row ceiling for the bulk generator: chunk_versions=1 makes the sync
# read one commit per chunk, and ~100k rows is the empirically proven driver
# ceiling (a 1M-row single commit OOMs the run worker, SIGKILLed by the VM).
BULK_COMMIT_ROWS = 100_000
# Sanity cap for one demo bulk load.
BULK_MAX_ROWS = 10_000_000


class DemoChangeSkip(ValueError):
    """A demo change that has nothing to do (rename or drop with no demo column on the table). The
    make_change asset turns this into a green no-op run that tells the page why, instead of a red
    failure -- the demo columns come and go with the add/rename/drop cycle."""


class ChangeConfig(Config):
    change: str = "update"  # insert, update, delete, add_column, rename_column or drop_column
    # Total row count for the bulk_1m generator (its name is historical). Split into
    # commits of at most BULK_COMMIT_ROWS so every commit stays at the proven-safe
    # size for the chunked CDF read (chunk_versions x rows_per_commit <= ~100k).
    rows: int = 1_000_000


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


def change_statements(
    cfg: ReverseEtlSyncConfig, change: str, existing_columns: set[str] = frozenset(), rows: int = 1_000_000
) -> list[str]:
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
    if change == "bulk_1m":
        # The delta sync's bulk load generator, parameterized by total rows
        # (config.rows, default 1,000,000 -- the name bulk_1m is historical).
        # Rows are split into commits of at most BULK_COMMIT_ROWS each: with
        # REVERSE_ETL_CDF_CHUNK_VERSIONS=1 the sync reads one commit per chunk,
        # so per-commit size (not the total) is what must stay ~<=100k. Each
        # commit chains off the previous one's max(id) via a scalar subquery, so
        # no pre-read is needed and repeated loads append after the last bulk
        # row (the reset flow clears the table when the demo is done).
        if cfg.sync_name != "reverse_etl_delta":
            raise ValueError(
                "bulk_1m is the delta sync's bulk load generator (id/description/updated_at); "
                f"{cfg.sync_name} has a different schema — use the row-level ops"
            )
        if rows < 1 or rows > BULK_MAX_ROWS:
            raise ValueError(f"rows must be between 1 and {BULK_MAX_ROWS:,}, got {rows:,}")
        names = ", ".join([KEY] + [name for name, _ in columns])
        statements = []
        emitted = 0
        while emitted < rows:
            chunk = min(BULK_COMMIT_ROWS, rows - emitted)
            values = ", ".join(
                ["base + 1 + j"]
                + [
                    "'bulk-' || CAST(base + 1 + j AS STRING)" if sql_type == "STRING" else _value(sql_type)
                    for _, sql_type in columns
                ]
            )
            statements.append(
                f"INSERT INTO {table} ({names}) SELECT {values} "
                f"FROM (SELECT coalesce(max({KEY}), 0) AS base FROM {table}) m "
                f"CROSS JOIN (SELECT explode(sequence(0, {chunk - 1})) AS j) s"
            )
            emitted += chunk
        return statements
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
    raise ValueError(f"change must be insert, update, delete, add_column, rename_column, drop_column or bulk_1m, got {change!r}")


def build_change_asset(cfg: ReverseEtlSyncConfig):
    @asset(
        name=f"{cfg.sync_name}_make_change",
        description=f"Change {cfg.source_fqn}: insert/update/delete a row, add/rename/drop a column, or bulk-load 1,000,000 rows (delta sync). Not synced until {cfg.sync_asset} runs.",
    )
    def make_change(context: AssetExecutionContext, config: ChangeConfig, databricks: DatabricksResource):
        databricks.require_env()
        token = databricks.token()
        databricks.reset_meter()
        existing = set()
        if config.change in ("add_column", "rename_column", "drop_column"):
            existing = {r["col_name"] for r in _rows_as_dicts(databricks.run_sql(token, f"DESCRIBE TABLE {cfg.source_fqn}"))}
        try:
            statements = list(change_statements(cfg, config.change, existing, rows=config.rows))
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
                databricks.run_sql(token, statement)
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
        if config.change == "bulk_1m":
            chunk = os.environ.get("REVERSE_ETL_CDF_CHUNK_VERSIONS", "10")
            if chunk != "1":
                context.log.warning(
                    f"This load needs REVERSE_ETL_CDF_CHUNK_VERSIONS=1 for the next sync (currently "
                    f"{chunk!r}, set in .env + docker-compose.yml): 100k-row commits with the default "
                    "10 would put 1M rows in one read chunk and the sync would be OOM-killed."
                )
        usage = meter_snapshot()
        context.add_output_metadata({
            "databricks_warehouse_seconds": MetadataValue.float(usage["warehouse_seconds"]),
            "databricks_statements": MetadataValue.int(usage["statements"]),
            "rows_inserted": MetadataValue.int(config.rows if config.change == "bulk_1m" else len(statements)),
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
