"""The demo's sync hooks: the RisingWave and Postgres target care the demo adds around the sync.

The sync asset (reverse_etl_cdf_setup.build_sync_asset) takes optional hooks and calls them at two
fixed points — before each event-bearing chunk is produced, and after the final Kafka flush but before
the watermark is saved. This module is what the demo passes: it keeps the RisingWave table ahead of the
messages (a column must exist before the first message that uses it) and propagates column drops and
renames to both targets, then restarts the JDBC sink task. A sync run without hooks touches neither
RisingWave nor Kafka Connect (that is the reusable component's behaviour).

The propagation lives here rather than in the sync asset module because it reads the demo-only
`cfg.propagate_drops` (release design step 2: the asset is base-clean) — a released sync runs
without these hooks and never touches the targets.

The ordering these depend on is documented on SyncHooks (reverse_etl_cdf_setup) and pinned by
tests/sync_asset/test_sync_hooks.py.
"""

import json
from typing import Any

from dagster import AssetExecutionContext

from .reverse_etl_cdf_setup import RowField, SyncHooks, get_column_ids
from .reverse_etl_config import ReverseEtlSyncConfig, risingwave_target_config
from .reverse_etl_resources import (
    HttpResource,
    PostgresResource,
    RisingWaveResource,
    SchemaRegistryResource,
)
from .reverse_etl_risingwave_setup import add_missing_columns


def _parse_last_schema(last_schema: str | None) -> list[tuple[str, int | None]]:
    """(column name, delta.columnMapping id | None) per recorded column, from the
    state row's last_schema JSON. Tolerates the original format — a plain list of
    names, written before rename propagation — as (name, None): those entries
    cannot be told apart from drops, so they follow the drop path."""
    if not last_schema:
        return []
    try:
        parsed = json.loads(last_schema)
    except ValueError:
        return []
    entries: list[tuple[str, int | None]] = []
    for item in parsed if isinstance(parsed, list) else []:
        if isinstance(item, dict) and item.get("name"):
            entries.append((str(item["name"]), item.get("id")))
        elif isinstance(item, str):
            entries.append((item, None))
    return entries


def _propagate_column_drops(
    postgres: PostgresResource,
    risingwave: RisingWaveResource,
    http: HttpResource,
    registry: SchemaRegistryResource,
    cfg: ReverseEtlSyncConfig,
    row_fields: list[RowField],
    last_schema: str | None,
    log: Any,
    column_ids: dict[str, int] | None = None,
) -> dict[str, Any]:
    """Follow the source's schema changes onto the targets, when
    cfg.propagate_drops is on. Returns {"renamed": ["old -> new", ...],
    "dropped": {"risingwave": [...], "postgres": [...]}} (all empty when nothing
    qualified).

    Runs only when the source version advanced since the last sync, and only on
    positive evidence from the state table's last_schema (recorded at the
    previous sync): a partial or failed schema read can never cascade into
    changes — and the evidence gate also protects target columns the source
    never had (e.g. RisingWave's generated <col>_ts companions are only touched
    as companions of a genuinely renamed or dropped field).

    A recorded column absent from the current read is a RENAME when its
    delta.columnMapping id reappears under a new name (ids survive renames;
    `column_ids` comes from the source's newest metaData action) — the targets
    rename too, preserving data. Without an id match it is a drop: RisingWave
    drops it (`drop_columns_from_risingwave`, taking the generated <col>_ts
    companions down first) and Postgres follows the broader rule (any column not
    mirrored by the current source schema — the sink creates that table purely
    from message fields, so there are no designed extras, and the broader rule
    additionally self-heals a sink replay re-adding a dropped column from an old
    message)."""
    if not (cfg.propagate_drops and last_schema and row_fields):
        return {"renamed": [], "dropped": {"risingwave": [], "postgres": []}}
    previous = _parse_last_schema(last_schema)
    current = {field[0] for field in row_fields}
    if not current:
        log.info("Current source schema read came back empty — skipping drop propagation")
        return {"renamed": [], "dropped": {"risingwave": [], "postgres": []}}
    ids = column_ids or {}
    renames: list[tuple[str, str]] = []
    dropped_fields: list[str] = []
    for name, column_id in previous:
        if name in current:
            continue
        new_name = None
        if column_id is not None:
            new_name = next(
                (candidate for candidate, candidate_id in ids.items() if candidate_id == column_id and candidate in current),
                None,
            )
        if new_name:
            renames.append((name, new_name))
        else:
            dropped_fields.append(name)
    if not renames and not dropped_fields:
        return {"renamed": [], "dropped": {"risingwave": [], "postgres": []}}
    result: dict[str, Any] = {"renamed": [], "dropped": {"risingwave": [], "postgres": []}}
    # The RisingWave helpers take the slim target config (release design step 4): the demo
    # bridge composes it from the merged config, source_fqn included (the Avro registration
    # re-derives the envelope from it).
    risingwave_cfg = risingwave_target_config(cfg.targets_config(), source_fqn=cfg.source_fqn)
    if renames:
        log.info(f"Source renamed column(s) {[f'{old} -> {new}' for old, new in renames]} — propagating to the targets")
        from .reverse_etl_postgres_setup import rename_columns_in_postgres
        from .reverse_etl_risingwave_setup import rename_columns_in_risingwave

        result["renamed"] = [f"{old} -> {new}" for old, new in renames]
        result["dropped"]["risingwave"] += rename_columns_in_risingwave(risingwave, registry, risingwave_cfg, renames, row_fields)
        result["dropped"]["postgres"] += rename_columns_in_postgres(postgres, http, cfg, renames)
    if dropped_fields:
        log.info(f"Source dropped column(s) {dropped_fields} — propagating to the targets")
        from .reverse_etl_postgres_setup import (
            drop_absent_columns as drop_from_postgres,
        )
        from .reverse_etl_risingwave_setup import drop_columns_from_risingwave

        result["dropped"]["risingwave"] += drop_columns_from_risingwave(risingwave, risingwave_cfg, dropped_fields)
        result["dropped"]["postgres"] += drop_from_postgres(postgres, cfg, current)
    return result


class DemoSyncHooks(SyncHooks):
    """Target care for the demo syncs: RisingWave columns ahead of the messages, drop/rename
    propagation to both targets, and the sink restart that goes with it."""

    def before_produce(self, context: AssetExecutionContext, cfg: ReverseEtlSyncConfig, row_fields: list[RowField]) -> dict:
        if "risingwave" not in cfg.targets:
            # The RisingWave table is a target (plan section 12, option A): unselected, there is
            # no table to keep ahead of the messages.
            return {"risingwave_columns_added": []}
        added = add_missing_columns(
            context.resources.risingwave,
            context.resources.registry,
            risingwave_target_config(cfg.targets_config(), source_fqn=cfg.source_fqn),
            row_fields,
        )
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
