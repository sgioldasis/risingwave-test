"""The demo's sync hooks: the RisingWave and Postgres target care the demo adds around the sync.

The sync asset (reverse_etl_cdf_setup.build_sync_asset) takes optional hooks and calls them at two
fixed points — before each event-bearing chunk is produced, and after the final Kafka flush but before
the watermark is saved. This module is what the demo passes: it keeps the RisingWave table ahead of the
messages (a column must exist before the first message that uses it) and propagates column drops and
renames to both targets, then restarts the JDBC sink task. A sync run without hooks touches neither
RisingWave nor Kafka Connect (that is the reusable component's behaviour).

The ordering these depend on is documented on SyncHooks (reverse_etl_cdf_setup) and pinned by
tests/sync_asset/test_sync_hooks.py.
"""

import json

from dagster import AssetExecutionContext

from .reverse_etl_cdf_setup import RowField, SyncHooks, _propagate_column_drops, get_column_ids
from .reverse_etl_config import ReverseEtlSyncConfig
from .reverse_etl_risingwave_setup import add_missing_columns


class DemoSyncHooks(SyncHooks):
    """Target care for the demo syncs: RisingWave columns ahead of the messages, drop/rename
    propagation to both targets, and the sink restart that goes with it."""

    def before_produce(self, context: AssetExecutionContext, cfg: ReverseEtlSyncConfig, row_fields: list[RowField]) -> dict:
        if "risingwave" not in cfg.targets:
            # The RisingWave table is a target (plan section 12, option A): unselected, there is
            # no table to keep ahead of the messages.
            return {"risingwave_columns_added": []}
        added = add_missing_columns(context.resources.risingwave, context.resources.registry, cfg, row_fields)
        if added:
            context.log.info(f"Added column(s) to the RisingWave table: {added}")
        return {"risingwave_columns_added": added}

    def after_delivery(self, context: AssetExecutionContext, cfg: ReverseEtlSyncConfig, schema: dict) -> dict:
        if "risingwave" not in cfg.targets and "postgres" not in cfg.targets:
            return {"columns_dropped": {}, "columns_renamed": []}
        if cfg.propagate_drops and not {"risingwave", "postgres"} <= set(cfg.targets):
            # _propagate_column_drops cares for BOTH targets and restarts the sink: with one of
            # them unselected there is no table or connector to change, so the combination fails
            # loudly instead of half-propagating a schema change.
            raise RuntimeError(
                f"propagate_drops needs both the risingwave and postgres targets selected, got {list(cfg.targets)}"
            )
        columns_dropped: dict[str, list[str]] = {}
        columns_renamed: list[str] = []
        schema_changes = _propagate_column_drops(
            context.resources.postgres,
            context.resources.risingwave,
            context.resources.http,
            context.resources.registry,
            cfg,
            schema["row_fields"],
            schema["last_schema"],
            context.log,
            schema["column_ids"],
        )
        columns_dropped = schema_changes["dropped"]
        columns_renamed = schema_changes["renamed"]
        if schema["event_row_fields"] is not None:
            # The targets were fed events under the EVENT-era schema. Anything changing between that
            # schema and the final one -- a RENAME or DROP landing inside the unsynced window, after
            # the last data event -- is invisible to the last_schema diff (the state predates those
            # columns) and must be applied here too, or the targets keep the old name while the source
            # moved on (verified live 2026-10-19: add + populate + rename in one unsynced window left
            # the value under new_col_1 in Postgres). The state diff above already EXECUTED its own
            # changes, so the event-era schema is adjusted by them first -- otherwise the same rename
            # would be detected twice and the second ALTER would fail on the already-renamed column.
            # The event-era ids come from the event-era version: the final ids no longer contain the
            # old name, and an id-less recorded column would read as a drop instead of the rename it was.
            event_ids = get_column_ids(schema["source_location"], version=schema["event_fields_version"])
            event_recorded = [{"name": field[0], "id": event_ids.get(field[0])} for field in schema["event_row_fields"]]
            for old_to_new in schema_changes["renamed"]:
                old, new = [part.strip() for part in old_to_new.split("->")]
                for entry in event_recorded:
                    if entry["name"] == old:
                        entry["name"] = new
            for target, names in schema_changes["dropped"].items():
                for name in names:
                    event_recorded = [entry for entry in event_recorded if entry["name"] != name]
            event_changes = _propagate_column_drops(
                context.resources.postgres,
                context.resources.risingwave,
                context.resources.http,
                context.resources.registry,
                cfg,
                schema["row_fields"],
                json.dumps(event_recorded),
                context.log,
                schema["column_ids"],
            )
            for target, names in event_changes["dropped"].items():
                for name in names:
                    if name not in columns_dropped[target]:
                        columns_dropped[target].append(name)
            for pair in event_changes["renamed"]:
                if pair not in columns_renamed:
                    columns_renamed.append(pair)
        if columns_renamed:
            context.log.info(f"Renamed column(s) on the targets: {columns_renamed}")
        if columns_dropped:
            context.log.info(f"Dropped column(s) from the targets: {columns_dropped}")
        return {"columns_dropped": columns_dropped, "columns_renamed": columns_renamed}

    def idle_metadata(self) -> dict:
        return {"risingwave_columns_added": [], "columns_dropped": {}, "columns_renamed": []}
