"""Batch Databricks Change Data Feed (CDF) -> Kafka reverse-ETL POC (APR-233).

See docs/poc/REVERSE_ETL_CDF_POC_PLAN.md for the full design rationale: this
evaluates whether Databricks CDF, read in batch (table_changes(), not
Structured Streaming), can cheaply sync new/changed/deleted rows to Kafka --
a capability drt (the leading OSS reverse-ETL candidate) deliberately does
not provide (ADR 0004 in drt-hub/drt: "drt activates warehouse data; it does
not replicate into the warehouse").

Uses a dedicated POC table + topic in the author's own sandbox schema
(de_dev.sr_poc_external), not any real E&A table.

Payload: Debezium-style envelopes (`before`/`after`/`op`/`source`), not raw
CDF rows -- see `_to_debezium_events()`. CDF's own `update_preimage`/
`update_postimage` pair collapses into one event's `before`/`after` rather
than two separate messages, and `op` uses Debezium's `c`/`u`/`d` vocabulary
instead of CDF's four-value `_change_type`. This is what lets RisingWave's
`FORMAT DEBEZIUM` and StarRocks Routine Load do native upsert/delete
ingestion against this topic, rather than needing a derived "latest row per
key" view to reconstruct current state.

Auth: reuses `_get_token`/`_submit`/`_poll` from databricks_optimize.py --
the same Azure AD service-principal client-credentials + Statement Execution
API flow `casino_prd_setup.py` already uses. An earlier version of this
module used databricks-sdk's WorkspaceClient with the `personal` CLI
profile, which works from a laptop shell but not inside the Dagster
container (no ~/.databrickscfg there, and the container's own
DATABRICKS_AUTH_TYPE=azure-client-secret conflicts with profile-based
resolution) -- switched to match what's actually deployable. See "Auth for
the live run" in the design doc.
"""

import json
import os
import threading
import time
from itertools import zip_longest
from typing import Any

import pyarrow.compute as pc
import requests
from confluent_kafka import Producer
from confluent_kafka.schema_registry import (
    Schema,
    SchemaRegistryClient,
    SchemaRegistryError,
)
from confluent_kafka.schema_registry.avro import AvroSerializer
from confluent_kafka.serialization import MessageField, SerializationContext
from dagster import AssetExecutionContext, AssetKey, MetadataValue, asset

# delta-rs reads the tables' ABFS log/state without a JVM for the sync's
# no-op pre-check. The imports sit at module level because they cost
# ~1.5s and the pre-check runs inside the run's measured window.
from deltalake import DeltaTable

from .databricks_cost_meter import record as meter_record
from .databricks_cost_meter import reset as meter_reset
from .databricks_cost_meter import snapshot as meter_snapshot
from .databricks_optimize import (
    CLIENT_ID,
    CLIENT_SECRET,
    DATABRICKS_HOST,
    TENANT_ID,
    _get_token,
    _poll,
    _submit,
)
from .reverse_etl_config import ReverseEtlSyncConfig

# Every function below takes the sync's ReverseEtlSyncConfig; the names live in reverse_etl_config.

# CDF's own metadata columns -- see Databricks' Change Data Feed docs.
CHANGE_TYPE_COLUMN = "_change_type"


def get_table_location(token: str, cfg: ReverseEtlSyncConfig, fqn: str) -> str:
    """The table's abfss storage location, from the Unity Catalog REST API.

    Control-plane call — no compute, no DBUs. Replaces the DESCRIBE DETAIL the
    prototypes ran once on the warehouse."""
    resp = requests.get(
        f"{DATABRICKS_HOST}/api/2.1/unity-catalog/tables/{fqn}",
        headers={"Authorization": f"Bearer {token}"},
        timeout=30,
    )
    if not resp.ok:
        raise RuntimeError(f"UC REST lookup for {fqn} failed ({resp.status_code}): {resp.text[:300]}")
    location = resp.json().get("storage_location", "")
    if not location:
        raise RuntimeError(f"UC REST returned no storage_location for {fqn}")
    return location


def _azure_storage_options(location: str) -> dict[str, str]:
    """delta-rs storage options for the tables' ABFS location: the same service
    principal the sync already authenticates with, under the *bare* key names
    object_store's Azure builder accepts. The azure_-prefixed variants silently
    miss the tenant and secret, fall back to the IMDS managed-identity probe
    (169.254.169.254 — unreachable outside Azure) and hang until the retries
    exhaust (verified live)."""
    return {
        "account_name": location.split("@")[1].split(".")[0],
        "client_id": os.environ["DATABRICKS_AZURE_CLIENT_ID"],
        "tenant_id": os.environ["DATABRICKS_AZURE_TENANT_ID"],
        "client_secret": os.environ["DATABRICKS_AZURE_CLIENT_SECRET"],
    }


def precheck_versions(cfg: ReverseEtlSyncConfig, source_location: str, state_location: str) -> tuple[int | None, int, str | None]:
    """(watermark, source's current version, the state row's last_schema) without
    starting Spark, via delta-rs.

    Powers the sync's no-op pre-check: when the source's version equals
    the watermark there is nothing to sync — a schema change is always a commit,
    so an unchanged version cannot hide one. Only the log is read for the source;
    the state read is a full-snapshot scan of a one-row table, which is why the
    state tables must be created *plain* — delta-rs rejects tables with the
    deletionVectors or columnMapping reader features (verified live). The
    last_schema rides along so the Spark path can reuse all three values instead
    of re-reading them (read_state + get_current_version cost a planning cycle
    each). Raises on any problem; the caller fails open to the full sync. The
    deltalake and pyarrow imports are module-level (see the imports above):
    importing them here would add ~1.5s to every run's measured window."""
    options = _azure_storage_options(source_location)
    current_version = DeltaTable(source_location, storage_options=options).version()
    state = DeltaTable(state_location, storage_options=_azure_storage_options(state_location)).to_pyarrow_table()
    rows = state.filter(pc.equal(state.column("sync_name"), cfg.sync_name)).to_pylist()
    if not rows:
        return None, current_version, None
    freshest = max(rows, key=lambda r: int(r["last_commit_version"] or 0))
    return int(freshest["last_commit_version"]), current_version, freshest.get("last_schema")


# The control-plane lookups (the AAD token, the UC storage locations) are stable
# for hours and cost ~1.3s per sync when fetched fresh. They are cached on disk
# with TTLs: the token retires well before its ~1h validity, the locations
# because a drop+recreate (the reset job) can move a managed table. The location
# TTL is short on purpose — a stale location after a drop+recreate points at the
# OLD table's storage, which is worse than a fresh fetch. A missing or
# corrupted cache just means a fresh fetch; the write is best-effort.
_CONTROL_PLANE_CACHE_PATH = os.path.join(
    os.environ.get("DAGSTER_HOME", "/workspace"), "dagster_storage", "reverse_etl_control_plane_cache.json"
)
_TOKEN_CACHE_TTL = 45 * 60
_LOCATION_CACHE_TTL = 10 * 60


def _control_plane_cache_read() -> dict[str, Any]:
    try:
        with open(_CONTROL_PLANE_CACHE_PATH) as handle:
            parsed = json.load(handle)
        return parsed if isinstance(parsed, dict) else {}
    except (OSError, ValueError):
        return {}


def _control_plane_cache_write(cache: dict[str, Any]) -> None:
    try:
        os.makedirs(os.path.dirname(_CONTROL_PLANE_CACHE_PATH), exist_ok=True)
        temporary = _CONTROL_PLANE_CACHE_PATH + ".tmp"
        with open(temporary, "w") as handle:
            json.dump(cache, handle)
        os.replace(temporary, _CONTROL_PLANE_CACHE_PATH)
    except OSError:
        pass  # best-effort: a fresh fetch is always the fallback


def _get_token_cached() -> str:
    """_get_token() with a 45-minute disk cache (the AAD token lives ~1h). The
    sync's pre-check needs the token for the UC location lookups on
    every run; a fresh client-credentials request costs ~0.4s of each one."""
    cache = _control_plane_cache_read()
    entry = cache.get("token") or {}
    if entry.get("expires", 0) > time.time() and entry.get("value"):
        return str(entry["value"])
    token = _get_token()
    cache["token"] = {"value": token, "expires": time.time() + _TOKEN_CACHE_TTL}
    _control_plane_cache_write(cache)
    return token


def get_column_ids(location: str, version: int | None = None) -> dict[str, int]:
    """{column name: delta.columnMapping.id}, read via delta-rs.

    The mapping id is stable across RENAME COLUMN while the name changes, which is
    how the sync tells a rename from a drop. The ids live in the schema's per-field
    metadata: Spark's DataFrame schema strips it (verified live back when the sync
    ran on OSS Delta Spark — the plain load and the change-feed read both returned
    empty field metadata), but delta-rs
    preserves it, so this is a ~0.2s local read replacing the multi-second Spark
    JVM log-scan it replaced. Returns {} when the table has no column mapping;
    callers then treat renames as drop + add. The optional version pins the read
    to that commit, for the same reason get_row_fields pins."""
    if version is None:
        fields = DeltaTable(location, storage_options=_azure_storage_options(location)).schema().fields
    else:
        # Lazy import: reverse_etl_delta_reader imports this module at its top.
        from .reverse_etl_delta_reader import _version_handle

        fields = _version_handle(location, version).schema().fields
    return {
        field.name: int(field.metadata["delta.columnMapping.id"])
        for field in fields
        if field.metadata and "delta.columnMapping.id" in field.metadata
    }


def clear_control_plane_cache() -> None:
    """Delete the on-disk token/location cache (best-effort). Called by the reset
    job after it drops the tables: a drop+recreate moves the managed tables to
    new storage locations, and a cached location would point at the dead tables —
    the sync would pre-check 'no new commits' against them and silently no-op
    until the TTL expired (bit us live: reset → insert → sync produced nothing
    for ten minutes)."""
    try:
        os.remove(_CONTROL_PLANE_CACHE_PATH)
    except OSError:
        pass  # nothing cached (or already gone) — a fresh fetch is the fallback


def get_table_location_cached(token: str, cfg: ReverseEtlSyncConfig, fqn: str) -> str:
    """get_table_location() with a 10-minute disk cache. Managed tables' storage
    locations only move when the table is dropped and recreated — the reset job
    does exactly that and clears this cache when it does (see
    clear_control_plane_cache), so a stale entry can only come from a manual
    drop+recreate; the TTL bounds that and deleting the cache file clears it
    immediately. If a fetch fails (e.g. a rotated secret invalidated a cached
    token), the whole cache is discarded so the next run starts fresh."""
    cache = _control_plane_cache_read()
    entry = (cache.get("locations") or {}).get(fqn) or {}
    if entry.get("expires", 0) > time.time() and entry.get("value"):
        return str(entry["value"])
    try:
        location = get_table_location(token, cfg, fqn)
    except RuntimeError:
        _control_plane_cache_write({})
        raise
    cache.setdefault("locations", {})[fqn] = {"value": location, "expires": time.time() + _LOCATION_CACHE_TTL}
    _control_plane_cache_write(cache)
    return location
COMMIT_VERSION_COLUMN = "_commit_version"
COMMIT_TIMESTAMP_COLUMN = "_commit_timestamp"
CDF_METADATA_COLUMNS = (CHANGE_TYPE_COLUMN, COMMIT_VERSION_COLUMN, COMMIT_TIMESTAMP_COLUMN)


def _resolve_backfill_version() -> int | None:
    """Explicit first-run override, e.g. to force starting from the exact
    version CDF was enabled at even though earlier history has already aged
    out of retention. Takes precedence over the automatic
    _get_earliest_available_version()-based decision in
    reverse_etl_cdf_to_kafka.
    """
    value = os.environ.get("REVERSE_ETL_BACKFILL_FROM_VERSION")
    return int(value) if value else None


# --- Databricks Statement Execution API (REST, same pattern as
# databricks_optimize.py / casino_prd_setup.py's _run_sql) -----------------


def _run_sql(token: str, statement: str) -> dict:
    """Run one warehouse statement and meter its wall time (databricks_cost_meter)."""
    started = time.monotonic()
    try:
        return _run_sql_unmetered(token, statement)
    finally:
        meter_record(time.monotonic() - started)


def _run_sql_unmetered(token: str, statement: str) -> dict:
    try:
        data = _submit(token, statement)
    except requests.exceptions.HTTPError as e:
        # _submit()/_poll() call resp.raise_for_status(), which discards the
        # response body -- surface it here since Databricks' actual error
        # detail (error_code / message) lives there, not in the HTTPError str.
        body = e.response.text if e.response is not None else "<no response body>"
        raise RuntimeError(f"Databricks Statement Execution API request failed: {e}\nResponse body: {body}") from e
    state = data.get("status", {}).get("state", "")
    if state not in ("SUCCEEDED", "FAILED", "CANCELED", "CLOSED"):
        try:
            data = _poll(token, data["statement_id"])
        except requests.exceptions.HTTPError as e:
            body = e.response.text if e.response is not None else "<no response body>"
            raise RuntimeError(f"Databricks Statement Execution API poll failed: {e}\nResponse body: {body}") from e
    if data.get("status", {}).get("state") != "SUCCEEDED":
        raise RuntimeError(f"Statement failed: {data.get('status', {})}")
    return _with_all_chunks(token, data)


def _cost_metadata() -> dict:
    """Databricks compute usage since the last meter reset, as run metadata.

    The demo helper (scripts/reverse_etl_demo_server.py) reads these entries
    through the GraphQL API and renders cost chips on the demo page."""
    usage = meter_snapshot()
    return {
        "databricks_warehouse_seconds": MetadataValue.float(usage["warehouse_seconds"]),
        "databricks_statements": MetadataValue.int(usage["statements"]),
    }


def _with_all_chunks(token: str, data: dict) -> dict:
    """The API returns a large result in chunks and the first response holds only the first one; without
    the others a change read is silently partial and the watermark moves past the rows that were never read
    (found 2026-10-07: 49,152 of 80,300 delete events). Fetch every chunk into result.data_array and fail if
    the rows collected differ from the manifest's total_row_count or the result is flagged truncated."""
    result = data.get("result") or {}
    manifest = data.get("manifest") or {}
    if "data_array" not in result:
        return data
    rows = list(result["data_array"])
    next_index = result.get("next_chunk_index")
    while next_index is not None:
        try:
            resp = requests.get(
                f"{DATABRICKS_HOST}/api/2.0/sql/statements/{data['statement_id']}/result/chunks/{next_index}",
                headers={"Authorization": f"Bearer {token}"},
                timeout=60,
            )
            resp.raise_for_status()
        except requests.exceptions.HTTPError as e:
            body = e.response.text if e.response is not None else "<no response body>"
            raise RuntimeError(f"Fetching result chunk {next_index} failed: {e}\nResponse body: {body}") from e
        chunk = resp.json()
        rows.extend(chunk.get("data_array") or [])
        next_index = chunk.get("next_chunk_index")
    total = manifest.get("total_row_count")
    if manifest.get("truncated") or (total is not None and len(rows) != total):
        raise RuntimeError(
            f"Incomplete result: read {len(rows)} row(s), manifest total_row_count={total}, "
            f"truncated={manifest.get('truncated')}"
        )
    return {**data, "result": {**result, "data_array": rows, "next_chunk_index": None}}


def _rows_as_dicts(response: dict) -> list[dict[str, Any]]:
    columns = [col["name"] for col in response.get("manifest", {}).get("schema", {}).get("columns", [])]
    data_array = response.get("result", {}).get("data_array") or []
    return [dict(zip(columns, row, strict=True)) for row in data_array]


def _read_state(token: str, cfg: ReverseEtlSyncConfig) -> tuple[int | None, str | None]:
    """(watermark, last_schema) for this sync's state row. last_schema is the JSON
    list of source column names recorded at the previous sync — the evidence the
    drop-propagation pass needs (a column in last_schema but absent from the
    current read is a drop, not a partial read)."""
    response = _run_sql(
        token,
        f"SELECT last_commit_version, last_schema FROM {cfg.state_fqn} WHERE sync_name = '{cfg.sync_name}'",
    )
    rows = _rows_as_dicts(response)
    if not rows:
        return None, None
    # The state table can carry duplicate rows for one sync_name, and they can
    # diverge — the freshest row wins, not an arbitrary result order.
    freshest = max(rows, key=lambda r: int(r["last_commit_version"] or 0))
    version = int(freshest["last_commit_version"]) if freshest["last_commit_version"] is not None else None
    return version, freshest.get("last_schema")


def _write_state(token: str, cfg: ReverseEtlSyncConfig, version: int, last_schema: str) -> None:
    # Append-only, matching the deleted Spark backend's write_state: read_state takes the freshest
    # row for the sync_name, so history accumulates harmlessly and the write is a
    # cheap single-statement insert.
    escaped_schema = last_schema.replace("'", "''")
    _run_sql(
        token,
        f"INSERT INTO {cfg.state_fqn} VALUES ('{cfg.sync_name}', {version}, '{escaped_schema}')",
    )


_MICROSECOND_TIMESTAMP_FORMAT = "yyyy-MM-dd'T'HH:mm:ss.SSSSSS'Z'"


def _version_chunks(start: int, end: int, size: int) -> list[tuple[int, int]]:
    """Inclusive version sub-ranges covering [start, end], `size` versions each.

    The sync reads and produces one chunk at a time so a backlog of many commits
    streams through the driver instead of collecting every row up front. A single
    huge commit still arrives as one chunk — that is the memory ceiling, and it
    is comfortable to ~100k rows on the 512m driver."""
    if end < start:
        return []
    return [(version, min(version + size - 1, end)) for version in range(start, end + 1, size)]


def _next_watermark(rows: list[dict[str, Any]], current_version: int | None) -> int | None:
    """Highest `_commit_version` observed this run, or current_version if no rows.

    The Statement Execution API's JSON response returns every column value as
    a string regardless of its SQL type (confirmed directly against this
    workspace's responses) -- cast explicitly rather than relying on `max()`
    over strings, which would sort lexicographically instead of numerically.
    """
    versions = [
        int(row[COMMIT_VERSION_COLUMN]) for row in rows if row.get(COMMIT_VERSION_COLUMN) is not None
    ]
    if not versions:
        return current_version
    return max(versions)


def _row_without_cdf_columns(row: dict[str, Any]) -> dict[str, Any]:
    return {k: v for k, v in row.items() if k not in CDF_METADATA_COLUMNS}


def _to_debezium_events(rows: list[dict[str, Any]], key_columns: list[str]) -> list[dict[str, Any]]:
    """Group raw CDF rows into one Debezium-style envelope per logical change.

    CDF emits an update as *two* rows -- update_preimage and
    update_postimage, sharing the same key and `_commit_version` -- which is
    audit-useful but redundant for any consumer that just wants current
    state, and isn't a shape most downstream systems (StarRocks Routine
    Load, RisingWave's FORMAT DEBEZIUM, generic CDC consumers) understand
    natively. Grouping by (row key, commit version) rather than assuming
    preimage/postimage are adjacent in the input list -- table_changes()
    documents no ordering guarantee, and `_read_changes()` issues no
    ORDER BY.

    Groups accumulate *lists* of rows per change type, not a single row --
    the source table has no enforced uniqueness on key_columns (confirmed
    live: a duplicate-keyed row is a real, reachable state here, not a
    theoretical one), so a single commit can touch more than one physical
    row sharing the same key (e.g. `UPDATE ... WHERE id = 1` matching two
    duplicate rows produces two preimage/postimage pairs, both keyed `id=1`,
    same `_commit_version`). Preimages and postimages are paired
    positionally (`zip_longest`, in table_changes()'s own per-commit
    ordering) rather than assuming exactly one of each -- CDF emits each
    affected physical row's own preimage next to its own postimage, so this
    correctly reconstructs N independent update events for N duplicate rows
    instead of silently collapsing them into one (or dropping the pairing
    with an early implementation that used a dict, keyed by change_type,
    which could only ever hold the *last* row of each type per commit).

    Output shape per event: ``{"before": {...} | None, "after": {...} | None,
    "op": "c" | "u" | "d", "source": {"commit_version": int,
    "commit_timestamp": str}}`` -- before/after carry the row's own columns
    only (CDF's `_change_type`/`_commit_version`/`_commit_timestamp` moved
    into `source`, which is where Debezium's own convention puts
    change-event metadata as opposed to row content).
    """
    groups: dict[tuple[Any, ...], dict[str, list[dict[str, Any]]]] = {}
    order: list[tuple[Any, ...]] = []

    for row in rows:
        change_type = row.get(CHANGE_TYPE_COLUMN)
        row_key = tuple(row.get(col) for col in key_columns)
        group_key = (row_key, row.get(COMMIT_VERSION_COLUMN))
        if group_key not in groups:
            groups[group_key] = {}
            order.append(group_key)
        groups[group_key].setdefault(change_type, []).append(row)

    events: list[dict[str, Any]] = []
    for group_key in order:
        group = groups[group_key]
        _, commit_version = group_key
        any_row = next(iter(next(iter(group.values()))))
        source = {"commit_version": commit_version, "commit_timestamp": any_row.get(COMMIT_TIMESTAMP_COLUMN)}

        for insert_row in group.get("insert", []):
            events.append({
                "before": None,
                "after": _row_without_cdf_columns(insert_row),
                "op": "c",
                "source": source,
            })
        for delete_row in group.get("delete", []):
            events.append({
                "before": _row_without_cdf_columns(delete_row),
                "after": None,
                "op": "d",
                "source": source,
            })
        preimages = group.get("update_preimage", [])
        postimages = group.get("update_postimage", [])
        for pre, post in zip_longest(preimages, postimages):
            events.append({
                "before": _row_without_cdf_columns(pre) if pre is not None else None,
                "after": _row_without_cdf_columns(post) if post is not None else None,
                "op": "u",
                "source": source,
            })
    return events


def _summarize_ops(events: list[dict[str, Any]]) -> dict[str, int]:
    """Count events per Debezium `op` value, for run metadata."""
    counts: dict[str, int] = {}
    for event in events:
        op = event.get("op", "unknown")
        counts[op] = counts.get(op, 0) + 1
    return counts


# Not a Connect type: marks a column carried as an ISO-8601 string with a
# timezone (e.g. 2026-10-03T03:41:08.345Z) whose schema field is named
# io.debezium.time.ZonedTimestamp, so the Debezium sink creates a timestamptz
# column. See _connect_field().
ZONED_TIMESTAMP = "zoned_timestamp"
_ZONED_TIMESTAMP_SCHEMA_NAME = "io.debezium.time.ZonedTimestamp"

# Databricks SQL type -> Kafka Connect schema type. Anything not listed
# (STRING, DATE, DECIMAL, ...) is carried as a plain string, exactly as the
# Statement Execution API returned it.
_CONNECT_TYPE_BY_DATABRICKS_TYPE = {
    "TINYINT": "int8",
    "SMALLINT": "int16",
    "INT": "int32",
    "INTEGER": "int32",
    "BIGINT": "int64",
    "LONG": "int64",
    "FLOAT": "float",
    "DOUBLE": "double",
    "BOOLEAN": "boolean",
    "TIMESTAMP": ZONED_TIMESTAMP,
}

RowField = tuple[str, str, bool]  # (column name, connect type, optional)


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


def _last_schema_json(row_fields: list[RowField], column_ids: dict[str, int]) -> str:
    """The state row's last_schema: the source's current columns with their
    delta.columnMapping ids (None when unknown — the warehouse path cannot read
    them), so the next sync can tell a rename from a drop."""
    return json.dumps([{"name": field[0], "id": column_ids.get(field[0])} for field in row_fields])


def _propagate_column_drops(
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
    if renames:
        log.info(f"Source renamed column(s) {[f'{old} -> {new}' for old, new in renames]} — propagating to the targets")
        from .reverse_etl_postgres_setup import rename_columns_in_postgres
        from .reverse_etl_risingwave_setup import rename_columns_in_risingwave

        result["renamed"] = [f"{old} -> {new}" for old, new in renames]
        result["dropped"]["risingwave"] += rename_columns_in_risingwave(cfg, renames, row_fields)
        result["dropped"]["postgres"] += rename_columns_in_postgres(cfg, renames)
    if dropped_fields:
        log.info(f"Source dropped column(s) {dropped_fields} — propagating to the targets")
        from .reverse_etl_postgres_setup import (
            drop_absent_columns as drop_from_postgres,
        )
        from .reverse_etl_risingwave_setup import drop_columns_from_risingwave

        result["dropped"]["risingwave"] += drop_columns_from_risingwave(cfg, dropped_fields)
        result["dropped"]["postgres"] += drop_from_postgres(cfg, current)
    return result


def _coerce(value: Any, connect_type: str) -> Any:
    """The Statement Execution API returns every value as a string; cast to
    the declared Connect type, since Connect's JSON converter does not."""
    if value is None:
        return None
    if connect_type.startswith("int"):
        return int(value)
    if connect_type in ("float", "double"):
        return float(value)
    if connect_type == "boolean":
        return value if isinstance(value, bool) else str(value).lower() == "true"
    return value


def _coerce_row(row: dict[str, Any] | None, row_fields: list[RowField]) -> dict[str, Any] | None:
    if row is None:
        return None
    return {name: _coerce(row.get(name), connect_type) for name, connect_type, _ in row_fields}


def _connect_field(name: str, connect_type: str, optional: bool) -> dict[str, Any]:
    if connect_type == ZONED_TIMESTAMP:
        return {"field": name, "type": "string", "name": _ZONED_TIMESTAMP_SCHEMA_NAME, "optional": optional}
    return {"field": name, "type": connect_type, "optional": optional}


def _connect_row_schema(cfg: ReverseEtlSyncConfig, optional: bool, row_fields: list[RowField]) -> dict[str, Any]:
    return {
        "type": "struct",
        "name": f"{cfg.envelope_schema_name}.Value",
        "optional": optional,
        "fields": [_connect_field(n, t, o) for n, t, o in row_fields],
    }


def _build_connect_json_message(
    cfg: ReverseEtlSyncConfig, event: dict[str, Any], key_columns: list[str], row_fields: list[RowField]
) -> tuple[bytes, bytes]:
    """Build the (key, value) pair for one event as Kafka Connect JSON with
    embedded schemas (`{"schema": ..., "payload": ...}`), for the Debezium
    JDBC sink. Key must be non-null so the sink can resolve the primary key
    from `record_key`; deletes carry the row in `before` and `op` = "d"."""
    row_for_key = event.get("after") or event.get("before") or {}
    field_types = {n: t for n, t, _ in row_fields}
    key_schema = {
        "type": "struct",
        "name": f"{cfg.envelope_schema_name}.Key",
        "optional": False,
        "fields": [{"field": c, "type": field_types[c], "optional": False} for c in key_columns],
    }
    key = {"schema": key_schema, "payload": {c: _coerce(row_for_key.get(c), field_types[c]) for c in key_columns}}

    value_schema = {
        "type": "struct",
        "name": f"{cfg.envelope_schema_name}.Envelope",
        "optional": False,
        "fields": [
            {"field": "before", **_connect_row_schema(cfg, optional=True, row_fields=row_fields)},
            {"field": "after", **_connect_row_schema(cfg, optional=True, row_fields=row_fields)},
            {"field": "op", "type": "string", "optional": False},
            {
                "field": "source",
                "type": "struct",
                "optional": True,
                "fields": [
                    {"field": "commit_version", "type": "int64", "optional": True},
                    {"field": "commit_timestamp", "type": "string", "optional": True},
                ],
            },
        ],
    }
    source = event.get("source") or {}
    payload = {
        "before": _coerce_row(event.get("before"), row_fields),
        "after": _coerce_row(event.get("after"), row_fields),
        "op": event["op"],
        "source": {
            "commit_version": _coerce(source.get("commit_version"), "int64"),
            "commit_timestamp": source.get("commit_timestamp"),
        },
    }
    value = {"schema": value_schema, "payload": payload}
    return (
        json.dumps(key, default=str, sort_keys=True).encode("utf-8"),
        json.dumps(value, default=str).encode("utf-8"),
    )


# --- Avro (Confluent wire format, schemas in the registry) ----------------------
# Same envelope as the JSON message. Every row column is a nullable union defaulting to null, so a column
# added later is a backward-compatible schema change; timestamps stay ZonedTimestamp strings, the one form
# that gives timestamptz with microseconds in both the sink and RisingWave (section 14.1 of the sink doc).
# The notebook builds the same schema (notebooks/reverse_etl_cdf_to_kafka.py); change both together.
# The registry (Apicurio) matches a schema by its exact string, and the Confluent converter looks the schema up
# in the Avro library's compact canonical form, so schemas are registered in that form: compact JSON, with the
# namespace only on the top-level record (nested records inherit it and are referenced by their short name).

_AVRO_TYPE_BY_CONNECT_TYPE = {
    "int8": "int",
    "int16": "int",
    "int32": "int",
    "int64": "long",
    "float": "float",
    "double": "double",
    "boolean": "boolean",
    "string": "string",
}


def _avro_value_type(connect_type: str) -> Any:
    if connect_type == ZONED_TIMESTAMP:
        return {"type": "string", "connect.name": _ZONED_TIMESTAMP_SCHEMA_NAME}
    return _AVRO_TYPE_BY_CONNECT_TYPE[connect_type]


def _avro_schemas(
    cfg: ReverseEtlSyncConfig, key_columns: list[str], row_fields: list[RowField]
) -> tuple[dict[str, Any], dict[str, Any]]:
    """(key schema, value schema); the value schema is the Debezium envelope."""
    namespace = cfg.envelope_schema_name
    field_types = {n: t for n, t, _ in row_fields}
    key_schema = {
        "type": "record",
        "name": "Key",
        "namespace": namespace,
        "fields": [{"name": c, "type": _avro_value_type(field_types[c])} for c in key_columns],
    }
    row_schema = {
        "type": "record",
        "name": "Value",
        "fields": [{"name": n, "type": ["null", _avro_value_type(t)], "default": None} for n, t, _ in row_fields],
    }
    source_schema = {
        "type": "record",
        "name": "Source",
        "fields": [
            {"name": "commit_version", "type": ["null", "long"], "default": None},
            {"name": "commit_timestamp", "type": ["null", "string"], "default": None},
        ],
    }
    value_schema = {
        "type": "record",
        "name": "Envelope",
        "namespace": namespace,
        "fields": [
            {"name": "before", "type": ["null", row_schema], "default": None},
            {"name": "after", "type": ["null", "Value"], "default": None},
            {"name": "op", "type": "string"},
            {"name": "source", "type": ["null", source_schema], "default": None},
        ],
    }
    return key_schema, value_schema


def _avro_schema_string(schema: dict[str, Any]) -> str:
    return json.dumps(schema, separators=(",", ":"))


def _avro_payloads(
    event: dict[str, Any], key_columns: list[str], row_fields: list[RowField]
) -> tuple[dict[str, Any], dict[str, Any]]:
    row_for_key = event.get("after") or event.get("before") or {}
    field_types = {n: t for n, t, _ in row_fields}
    source = event.get("source") or {}
    key = {c: _coerce(row_for_key.get(c), field_types[c]) for c in key_columns}
    value = {
        "before": _coerce_row(event.get("before"), row_fields),
        "after": _coerce_row(event.get("after"), row_fields),
        "op": event["op"],
        "source": {
            "commit_version": _coerce(source.get("commit_version"), "int64"),
            "commit_timestamp": source.get("commit_timestamp"),
        },
    }
    return key, value


def _avro_serializers(
    cfg: ReverseEtlSyncConfig,
    key_columns: list[str],
    row_fields: list[RowField],
    client: SchemaRegistryClient | None = None,
) -> tuple[AvroSerializer, AvroSerializer]:
    """Serializers that write the Confluent wire format. When both schemas are already in the registry (checked
    here, one GET per subject) they point at the latest registered version instead of re-registering on the first
    message -- two fewer round-trips per unchanged sync. When they are not (first run), the serializers register
    them as before."""
    client = client or SchemaRegistryClient({"url": cfg.schema_registry_url})
    key_schema, value_schema = _avro_schemas(cfg, key_columns, row_fields)
    known = all(
        _schema_already_registered(client, subject, schema)
        for subject, schema in ((cfg.key_subject, key_schema), (cfg.value_subject, value_schema))
    )
    conf = {"auto.register.schemas": False, "use.latest.version": True} if known else None
    return (
        AvroSerializer(client, _avro_schema_string(key_schema), conf=conf),
        AvroSerializer(client, _avro_schema_string(value_schema), conf=conf),
    )


def _schema_already_registered(client: SchemaRegistryClient, subject: str, schema: dict[str, Any]) -> bool:
    """True when the registry's latest version for subject is identical to schema.

    Both sides are parsed before comparing, so key-order/whitespace differences
    between what we build and what the registry stores cannot false-negative.
    Any registry error -- a missing subject above all -- counts as "not
    registered": the caller then registers anyway, exactly as before this check
    existed, so a registry blip can never skip a needed registration."""
    try:
        latest = client.get_latest_version(subject)
        registered = latest.schema.schema_str if hasattr(latest.schema, "schema_str") else latest.schema
        return json.loads(registered) == json.loads(_avro_schema_string(schema))
    except SchemaRegistryError:
        return False


def _register_avro_schemas(cfg: ReverseEtlSyncConfig, key_columns: list[str], row_fields: list[RowField]) -> None:
    """Register both schemas and make sure the registry's LATEST version is the one
    the sync just built.

    Re-registering content that already exists as an OLDER version is a no-op on
    Confluent-compatible registries: the register returns that version's schema id
    and does not create a new version, so the latest stays wherever it was.
    RisingWave validates every ADD COLUMN against the registry's latest, so
    "registered" is not enough -- the built schema must BE the latest. The demo
    walks into this on every second cycle: add → rename → drop → add again builds
    a schema whose content equals the first add's version, the register dedupes to
    it, and RisingWave rejects the column with 'Column "new_col_1" is defined in
    SQL but not found in the source' (verified live 2026-10-19).

    The latest-version READ also lags the writes for seconds (Apicurio, seen live
    the same day: a register answered with a NEW version's schema id while
    get_latest_version still reported the old one past a 4s retry window), so the
    decision here never reads after writing: it takes a FRESH snapshot of the
    subject (versions, ids, contents) before the register, then reads the
    register's returned schema id -- authoritative for what the register did.
    A returned id unknown to the snapshot means a new version was created (the
    latest is ours); an id matching the version that WAS the latest means the
    register deduped onto it (the latest is ours); an id matching an OLDER
    version means the register deduped onto it, and the versions after it are
    soft-deleted before registering again -- the dedup then lands on that
    version, which is now the latest. Soft delete keeps the content resolvable
    by id, so messages already on the topic still deserialize (verified live:
    ids of the deleted versions still answer GET /schemas/ids/<id>)."""
    client = SchemaRegistryClient({"url": cfg.schema_registry_url})
    key_schema, value_schema = _avro_schemas(cfg, key_columns, row_fields)
    for subject, schema in ((cfg.key_subject, key_schema), (cfg.value_subject, value_schema)):
        built = _avro_schema_string(schema)
        # FRESH reads, before any write: version -> (schema id, content == built).
        snapshot: dict[int, tuple[int, bool]] = {}
        try:
            for version in client.get_versions(subject):
                entry = client.get_version(subject, version)
                stored = entry.schema.schema_str if hasattr(entry.schema, "schema_str") else entry.schema
                snapshot[version] = (entry.schema_id, json.loads(stored) == json.loads(built))
        except SchemaRegistryError:
            pass  # a missing subject registers below
        if snapshot and snapshot[max(snapshot)][1]:
            continue  # the latest already IS the built schema
        schema_id = client.register_schema(subject, Schema(built, "AVRO"))
        holder = next((v for v, (i, _) in snapshot.items() if i == schema_id), None)
        if holder is None or holder == max(snapshot):
            continue  # a new version was created, or the dedup landed on the version that was already latest
        # The register deduped onto an OLDER version: retire everything after it
        # (soft delete) and register again -- the dedup then lands on that
        # version, now the latest. No reads after the writes: the register's id
        # already told us everything this check would ask.
        for version in sorted((v for v in snapshot if v > holder), reverse=True):
            try:
                client.delete_version(subject, version)
            except SchemaRegistryError:
                pass  # already gone
        client.register_schema(subject, Schema(built, "AVRO"))


def _build_avro_message(
    cfg: ReverseEtlSyncConfig,
    event: dict[str, Any],
    key_columns: list[str],
    row_fields: list[RowField],
    serializers: tuple[AvroSerializer, AvroSerializer],
) -> tuple[bytes, bytes]:
    key, value = _avro_payloads(event, key_columns, row_fields)
    key_serializer, value_serializer = serializers
    return (
        key_serializer(key, SerializationContext(cfg.kafka_topic, MessageField.KEY)),
        value_serializer(value, SerializationContext(cfg.kafka_topic, MessageField.VALUE)),
    )


# --- Kafka (confluent-kafka) --------------------------------------------------


def _kafka_producer_config() -> dict[str, str]:
    """Same KAFKA_OUTPUT_* credential convention as kafka_topics_setup.py's
    _admin_client() -- this topic lives in that module's OUTPUT_TOPICS list,
    so it shares the same credential scope rather than introducing a third
    env-naming scheme (see scripts/produce_protobuf_casino_rounds.py's
    KAFKA_SASL_*/KAFKA_SECURITY_PROTOCOL convention, which is deliberately
    NOT reused here)."""
    bootstrap = os.environ.get("KAFKA_OUTPUT_BOOTSTRAP", "")
    if not bootstrap:
        raise ValueError("KAFKA_OUTPUT_BOOTSTRAP must be set in .env")

    conf: dict[str, str] = {"bootstrap.servers": bootstrap}
    username = os.environ.get("KAFKA_OUTPUT_SASL_USERNAME", "")
    if username:
        conf.update(
            {
                "security.protocol": "SASL_SSL",
                "sasl.mechanism": os.environ.get("KAFKA_OUTPUT_SASL_MECHANISM", "SCRAM-SHA-512"),
                "sasl.username": username,
                "sasl.password": os.environ.get("KAFKA_OUTPUT_SASL_PASSWORD", ""),
            }
        )
    # Volume headroom (100k+ event syncs): compress on the wire, let librdkafka
    # batch briefly (the produce loop's final flush absorbs the linger), and
    # raise the queue ceilings so a large backlog never blocks the enqueuer.
    conf.update(
        {
            "compression.type": "lz4",
            "linger.ms": "50",
            "batch.size": str(1024 * 1024),
            "queue.buffering.max.messages": str(500000),
            "batch.num.messages": str(20000),
        }
    )
    return conf


def _kafka_producer() -> Producer:
    """A producer whose TLS/SASL handshake starts now, in librdkafka's background
    threads. The brokers are reached over VPN and connecting takes ~9s; creating
    the producer before the Spark reads lets that happen off the critical path
    (a fresh process per run means there is nothing to reuse it from)."""
    producer = Producer(_kafka_producer_config())
    producer.poll(0)
    return producer


def _produce_to_kafka(
    cfg: ReverseEtlSyncConfig,
    events: list[dict[str, Any]],
    key_columns: list[str],
    row_fields: list[RowField],
    producer: Producer | None = None,
    flush: bool = True,
) -> list[str]:
    """Serialize + enqueue the events. Returns the delivery-error strings collected
    while enqueueing (the callbacks fire during flush()). With flush=False the
    caller owns the final wait — see _flush_producer."""
    producer = producer or _kafka_producer()
    # produce() is async (it only enqueues); delivery failures surface later in
    # librdkafka's background threads. Collect them per message via on_delivery
    # and fail the run on the flush -- BEFORE the watermark append that follows
    # -- so the next sync replays from the same version instead of silently
    # skipping events that never reached the topic (broker blip, rotated SASL
    # credentials, recreated topic). Duplicate replays are harmless: the targets
    # upsert by key.
    serializers = _avro_serializers(cfg, key_columns, row_fields) if cfg.encoding == "avro" else None
    delivery_errors: list[str] = []
    for event in events:
        if serializers:
            key, value = _build_avro_message(cfg, event, key_columns, row_fields, serializers)
        else:
            key, value = _build_connect_json_message(cfg, event, key_columns, row_fields)
        # Enqueuing outruns the wire: once librdkafka's queue ceiling is reached
        # (queue.buffering.max.messages = 500k -- above any 100k-row sync but
        # below a 1M-row one) produce() raises BufferError. Serve the delivery
        # callbacks with poll() to free queue space and retry, so a volume sync
        # backpressures on the broker instead of dying at the ceiling. Bounded
        # like the flush below: a wedged broker must fail the run, not hang it.
        queue_full_deadline = time.monotonic() + 120
        while True:
            try:
                producer.produce(
                    topic=cfg.kafka_topic,
                    key=key,
                    value=value,
                    on_delivery=lambda err, msg: delivery_errors.append(str(err)) if err is not None else None,
                )
                break
            except BufferError:
                if time.monotonic() > queue_full_deadline:
                    raise RuntimeError(
                        f"Kafka producer queue stayed full for 120s (topic {cfg.kafka_topic})"
                    )
                producer.poll(0.1)
    # Bounded wait, mirroring _get_risingwave_connection's connect_timeout: a
    # wedged broker must fail the run, not hang it. flush() returns the number
    # of messages still undelivered (their delivery callbacks have not fired).
    if flush:
        _flush_producer(cfg, producer, delivery_errors)
    return delivery_errors


def _flush_producer(cfg: ReverseEtlSyncConfig, producer: Producer, delivery_errors: list[str]) -> None:
    """Wait for everything enqueued so far to reach the broker, then fail the run
    if any delivery was rejected. Called once per produce site — the chunked read
    loop flushes only after the LAST chunk, so the broker transfers while the next
    chunk's Spark read runs instead of every chunk paying its own round-trip."""
    undelivered = producer.flush(timeout=120)
    if undelivered or delivery_errors:
        raise RuntimeError(
            f"Kafka delivery to {cfg.kafka_topic} failed: "
            f"{len(delivery_errors)} rejected ({delivery_errors[:3]}), {undelivered} undelivered after 120s"
        )


# Cap on how many produced messages are echoed back into the asset's run
# metadata -- a full backfill can emit far more events than are useful to
# render in the Dagster UI.
MESSAGE_PREVIEW_LIMIT = 50
# The CDF read runs one version chunk at a time (see _version_chunks): a backlog
# of many commits streams through the driver instead of collecting every row.
# The size is env-tunable because the bound that matters is ROWS per chunk
# (chunk_versions x rows_per_commit ≈ ≤100k — the empirically proven driver
# ceiling: a 1M-row collect OOMs the run worker, SIGKILLed by the VM). The
# default 10 fits the demo's small commits; bulk loads of big commits (10 x
# 100k rows) set REVERSE_ETL_CDF_CHUNK_VERSIONS=1.
_CDF_CHUNK_VERSIONS = int(os.environ.get("REVERSE_ETL_CDF_CHUNK_VERSIONS", "10"))


def _message_previews(
    cfg: ReverseEtlSyncConfig,
    events: list[dict[str, Any]],
    key_columns: list[str],
    row_fields: list[RowField],
    limit: int = MESSAGE_PREVIEW_LIMIT,
) -> list[dict[str, Any]]:
    """The (key, value) pairs actually sent to Kafka for the first `limit`
    events, so the run's materialization metadata can show real produced
    messages rather than just op counts. Only each message's `payload` is
    shown: the embedded schema repeats on every message and would drown it."""
    previews = []
    for event in events[:limit]:
        if cfg.encoding == "avro":
            key_payload, value_payload = _avro_payloads(event, key_columns, row_fields)
            previews.append({"key": key_payload, "value": value_payload})
            continue
        key, value = _build_connect_json_message(cfg, event, key_columns, row_fields)
        previews.append({
            "key": json.loads(key)["payload"],
            "value": json.loads(value)["payload"],
        })
    return previews


RAW_MESSAGE_LIMIT = 3


def _raw_messages(
    cfg: ReverseEtlSyncConfig,
    events: list[dict[str, Any]],
    key_columns: list[str],
    row_fields: list[RowField],
    limit: int = RAW_MESSAGE_LIMIT,
) -> list[dict[str, Any]]:
    """The first `limit` messages exactly as sent to Kafka, embedded
    `schema` included (_message_previews shows payloads only). Avro messages are shown as hex: the
    Confluent wire format, a zero byte and the 4-byte schema id before the Avro payload."""
    raw = []
    serializers = _avro_serializers(cfg, key_columns, row_fields) if cfg.encoding == "avro" and events else None
    for event in events[:limit]:
        if serializers:
            key, value = _build_avro_message(cfg, event, key_columns, row_fields, serializers)
            raw.append({"key_hex": key.hex(), "value_hex": value.hex()})
            continue
        key, value = _build_connect_json_message(cfg, event, key_columns, row_fields)
        raw.append({"key": json.loads(key), "value": json.loads(value)})
    return raw


# --- Dagster assets ------------------------------------------------------------


def _require_databricks_env() -> None:
    missing = [k for k, v in {
        "DBT_DATABRICKS_HOST":            DATABRICKS_HOST,
        "DATABRICKS_AZURE_TENANT_ID":     TENANT_ID,
        "DATABRICKS_AZURE_CLIENT_ID":     CLIENT_ID,
        "DATABRICKS_AZURE_CLIENT_SECRET": CLIENT_SECRET,
    }.items() if not v]
    if missing:
        raise ValueError(f"Missing required env vars: {missing}")


def build_table_setup_asset(cfg: ReverseEtlSyncConfig):
    @asset(
        name=cfg.table_setup_asset,
        group_name=cfg.group_name,
        description=(
            f"Create the reverse-ETL source table {cfg.source_fqn} (Change Data Feed enabled "
            "at creation) and its watermark bookkeeping table, if absent. "
            "See docs/poc/REVERSE_ETL_CDF_POC_PLAN.md."
        ),
    )
    def table_setup(context: AssetExecutionContext):
        _require_databricks_env()
        token = _get_token()
        meter_reset()

        columns = ",\n            ".join(
            [f"{cfg.key_column} BIGINT GENERATED ALWAYS AS IDENTITY", *cfg.source_columns]
        )
        if cfg.uniform:
            # UniForm (Iceberg compat) so other engines — StarRocks via UC's Iceberg
            # REST catalog — can read this table. Column mapping is an Iceberg-compat
            # requirement; deletion vectors must stay off for the same reason.
            properties = (
                "'delta.enableChangeDataFeed' = 'true',\n"
                "            'delta.columnMapping.mode' = 'name',\n"
                "            'delta.enableDeletionVectors' = 'false',\n"
                "            'delta.enableIcebergCompatV2' = 'true',\n"
                "            'delta.universalFormat.enabledFormats' = 'iceberg'"
            )
            ready = f"{cfg.source_fqn} ready (CDF + UniForm Iceberg enabled)"
        else:
            # Plain Delta on purpose: this sync shows the pipeline is
            # UniForm-agnostic. Column mapping is on anyway — it is NOT a
            # UniForm feature, and without it Delta refuses DROP COLUMN (and
            # renames) outright, which the drop-propagation demo needs. Note:
            # such a table still will NOT appear in StarRocks' databricks_uc
            # catalog (that needs UniForm's Iceberg metadata). Deletion vectors
            # are OFF on purpose too: with DVs enabled, a 1-row DELETE makes the
            # Change Data Feed emit the whole touched file (verified live: two
            # single-row deletes on 100k-row files produced 200000 delete +
            # 100000 insert CDF rows, and the targets lost 100000 legitimate
            # rows) — the sync would relay that verbatim.
            properties = (
                "'delta.enableChangeDataFeed' = 'true',\n"
                "            'delta.columnMapping.mode' = 'name',\n"
                "            'delta.enableDeletionVectors' = 'false'"
            )
            ready = f"{cfg.source_fqn} ready (plain Delta: CDF + column mapping, no UniForm)"
        _run_sql(
            token,
            f"""
        CREATE TABLE IF NOT EXISTS {cfg.source_fqn} (
            {columns}
        ) USING DELTA
        TBLPROPERTIES (
            {properties}
        )
        """,
        )
        context.log.info(ready)

        # Plain Delta on purpose: the watermark is internal bookkeeping that nothing
        # reads through Iceberg (unlike the source table, which keeps UniForm for
        # StarRocks). OSS Delta cannot commit to UniForm tables — the commit hook
        # runs the Iceberg conversion with a shaded Databricks-only class
        # ([DELTA_MISSING_ICEBERG_CLASS]) — and the zero-DBU sync writes this
        # table's watermark from Spark. See docs/poc/REVERSE_ETL_DBU_COST_NOTES.md
        # section 5. Column mapping is on for consistency with the source tables
        # (every demo table takes DROP COLUMN); the state table has no Change Data
        # Feed — nothing reads its change feed, reads are full snapshots + MERGE.
        # The state table is deliberately PLAIN Delta: no column mapping (it never
        # takes DROP COLUMN) and deletion vectors off — delta-rs rejects tables
        # with those reader features, and the sync's delta-rs no-op pre-check
        # reads the watermark from here (precheck_versions). Keep it Spark-
        # written: this runtime rejects 'disabled' feature statuses outright
        # ([DELTA_UNSUPPORTED_FEATURE_STATUS]), and any Spark-written table
        # carries a v7 protocol whose writer-feature list delta-rs cannot commit
        # to — so the state table has a single writer (the one small SQL INSERT
        # that advances the watermark). Never re-create it via CREATE OR REPLACE:
        # that inherits the source's bumped protocol.
        _run_sql(
            token,
            f"""
        CREATE TABLE IF NOT EXISTS {cfg.state_fqn} (
            sync_name STRING, last_commit_version BIGINT, last_schema STRING
        ) USING DELTA
        TBLPROPERTIES ('delta.enableDeletionVectors' = 'false')
        """,
        )
        context.log.info(
            f"{cfg.state_fqn} ready (plain Delta, no UniForm; last_schema records the source's "
            "columns for drop propagation)"
        )

        context.add_output_metadata({
            "source_table": MetadataValue.text(cfg.source_fqn),
            "state_table": MetadataValue.text(cfg.state_fqn),
            **_cost_metadata(),
        })

    return table_setup


def build_sync_asset(cfg: ReverseEtlSyncConfig):
    """The sync asset: delta-rs version-range reconstruction (see
    reverse_etl_delta_reader.py) -- no CDF read, no Spark, no warehouse scans;
    the only warehouse touch is the watermark write."""
    return _build_sync_asset_delta_rs(cfg)


def _build_sync_asset_delta_rs(cfg: ReverseEtlSyncConfig):
    @asset(
        name=cfg.sync_asset,
        group_name=cfg.group_name,
        deps=[AssetKey(cfg.table_setup_asset), AssetKey(cfg.topic_asset)],
        description=(
            f"Batch-read new/changed/deleted rows from {cfg.source_fqn}'s Change "
            "Data Feed since the last watermark, and produce them as Debezium-style "
            f"since the last watermark (delta-rs version-range reconstruction -- no CDF "
            f"read, no Spark, no warehouse scans), and produce them as Debezium-style "
            "— no Databricks compute, no DBUs. See "
            "docs/poc/REVERSE_ETL_DBU_COST_NOTES.md sections 4-5."
        ),
    )
    def cdf_to_kafka(context: AssetExecutionContext):
        meter_reset()
        # The token is control-plane only here: it resolves the tables' storage
        # locations through the UC REST API. No SQL runs against a warehouse.
        # Both come from the disk cache when warm (see _get_token_cached).
        _require_databricks_env()
        token = _get_token_cached()
        producer: Producer | None = None

        source_location = get_table_location_cached(token, cfg, cfg.source_fqn)
        state_location = get_table_location_cached(token, cfg, cfg.state_fqn)

        # delta-rs no-op pre-check: read the source's current version and the
        # state table's watermark (no Spark, no warehouse) and end the run when
        # nothing moved — a schema change is always a commit, so an unchanged
        # version cannot hide one. This is the whole run for a no-op. Bounded by a 5s
        # watchdog, because a hung pre-check must not make a no-op slower than the
        # sync it would have saved, and fail-open on any error: the pre-check is
        # an optimization, never a gate.
        precheck: dict[str, Any] = {}

        def _precheck() -> None:
            try:
                precheck["result"] = precheck_versions(cfg, source_location, state_location)
            except Exception as error:  # noqa: BLE001 — fail-open is the design
                precheck["error"] = error

        thread = threading.Thread(target=_precheck, daemon=True)
        thread.start()
        thread.join(5)
        if "error" in precheck:
            context.log.info(
                f"delta-rs pre-check unavailable ({type(precheck['error']).__name__}: {str(precheck['error'])[:120]}) — starting the full sync"
            )
        elif "result" not in precheck:
            context.log.info("delta-rs pre-check did not return in 5s — starting the full sync")
        else:
            last_version_pre, current_version_pre, _ = precheck["result"]
            if last_version_pre is not None and current_version_pre == last_version_pre:
                context.log.info(
                    f"No new commits since version {last_version_pre} for {cfg.sync_name} (delta-rs pre-check)"
                )
                context.add_output_metadata({
                    "events_synced": MetadataValue.int(0),
                    "op_counts": MetadataValue.json({}),
                    "last_commit_version": MetadataValue.int(last_version_pre),
                    "kafka_topic": MetadataValue.text(cfg.kafka_topic),
                    "risingwave_columns_added": MetadataValue.json([]),
                    "columns_dropped": MetadataValue.json({"risingwave": [], "postgres": []}),
                    "columns_renamed": MetadataValue.json([]),
                    "kafka_messages": MetadataValue.json([]),
                    "kafka_messages_truncated": MetadataValue.bool(False),
                    "kafka_messages_raw": MetadataValue.json([]),
                    "extraction": MetadataValue.text("delta_rs"),
                    "databricks_compute": MetadataValue.text("none — delta-rs reads storage directly; one small SQL write advances the watermark"),
                    **_cost_metadata(),
                })
                return

        from .reverse_etl_delta_reader import (
            get_current_version,
            get_earliest_available_version,
            get_row_fields,
            read_changes,
        )
        precheck_result = precheck.get("result")
        if precheck_result is not None:
            # The pre-check read the state row and the source version seconds
            # ago — re-reading both costs two snapshot loads for the same answer.
            last_version, current_version, last_schema = precheck_result
        else:
            # The pre-check failed (or hung): the SQL state read is the only
            # fallback that does not repeat whatever broke the delta-rs read.
            last_version, last_schema = _read_state(token, cfg)
            current_version = None
        chunk_start: int | None = None

        if last_version is None:
            backfill_from = _resolve_backfill_version()
            if backfill_from is not None:
                context.log.info(
                    f"First run for {cfg.sync_name} -- backfilling {cfg.source_table} "
                    f"from explicit override version {backfill_from}"
                )
            else:
                earliest_available = get_earliest_available_version(source_location)
                if earliest_available == 0:
                    backfill_from = 0
                    context.log.info(
                        f"First run for {cfg.sync_name} -- {cfg.source_table}'s full history "
                        "is intact (earliest available version is 0), backfilling "
                        "from the beginning"
                    )
                else:
                    context.log.info(
                        f"First run for {cfg.sync_name} -- earliest available version "
                        f"is {earliest_available} (history already partially aged "
                        f"out of retention), baselining at {cfg.source_table}'s current "
                        "version with no historical rows synced"
                    )

            if backfill_from is not None:
                # The Kafka TLS/SASL handshake runs in librdkafka's background
                # threads while the version-diff reads and event building run.
                producer = _kafka_producer()
                chunk_start = backfill_from
                if current_version is None:
                    current_version = get_current_version(source_location)
        else:
            # The reconstruction reads every commit since the watermark, and
            # last_version was already fully processed -- +1 to resume from the
            # first un-synced version.
            next_version = last_version + 1
            if current_version is None:
                current_version = get_current_version(source_location)
            if next_version > current_version:
                # No commits since the last run -- the common case for a daily
                # batch once caught up. Checked before reading so a no-op stays
                # a no-op.
                context.log.info(f"No new commits since version {last_version} for {cfg.sync_name}")
            else:
                context.log.info(f"Reading versions {next_version}-{current_version} of {cfg.source_table}")
                # The Kafka TLS/SASL handshake runs in librdkafka's background
                # threads while the version-diff read runs. The read is pinned to
                # current_version so its range matches the watermark exactly.
                producer = _kafka_producer()
                chunk_start = next_version

        key_columns = [cfg.key_column]
        events_synced = 0
        op_counts: dict[str, int] = {}
        preview_events: list[dict[str, Any]] = []
        raw_preview_events: list[dict[str, Any]] = []
        delivery_errors: list[str] = []
        risingwave_columns_added: list[str] = []
        # The schema the last produced events were enveloped with, and its
        # version -- the propagation needs it to catch schema changes landing
        # inside the unsynced window AFTER the last data event (see below).
        event_row_fields: list[RowField] | None = None
        event_fields_version: int | None = None
        row_fields: list[RowField] = []
        new_version: int | None = None
        rows_seen = False

        if chunk_start is not None:
            from .reverse_etl_risingwave_setup import add_missing_columns

            # Read and produce one version chunk at a time so a backlog of many
            # commits streams through the driver instead of collecting every row
            # up front (_version_chunks). A single huge commit still arrives as
            # one chunk -- that is the memory ceiling. No prefetch thread: it
            # measured 187s vs 126s on the 1M sync -- the reader thread's
            # collect->dict conversion and the main thread's events/serialization
            # are both GIL-bound, so they contend instead of overlapping. The
            # overlap that DOES pay is flush=False's: the broker transfers while
            # the next chunk's Spark read runs (GIL-free there).
            chunks = _version_chunks(chunk_start, current_version, _CDF_CHUNK_VERSIONS)
            for index, (chunk_from, chunk_to) in enumerate(chunks):
                rows = read_changes(cfg, source_location, chunk_from, chunk_to)
                # The watermark advances for EVERY chunk -- including 0-row
                # schema-only ones -- because it must reach the pinned top.
                # Updating it only for row-bearing chunks stranded the
                # watermark on the last EVENT-bearing chunk: a RENAME
                # committing after the last data event in the same sync was
                # read (0 rows) but never watermarked, deferring its
                # propagation to the next sync (verified live 2026-10-19 --
                # Postgres kept the old column name after the rename demo).
                chunk_watermark = _next_watermark(rows, current_version)
                new_version = chunk_watermark if new_version is None else max(new_version, chunk_watermark)
                if not rows:
                    continue
                rows_seen = True
                events = _to_debezium_events(rows, key_columns)
                if events:
                    # Resolve the schema PER EVENT-BEARING CHUNK, pinned to the
                    # chunk's top version -- the same commit the data was read
                    # at -- and make RisingWave ready before any message is
                    # produced (it only fills a column from messages read after
                    # the column exists). Set-once-and-reuse is wrong the
                    # moment the schema changes between chunks: later chunks'
                    # events would be enveloped with the FIRST chunk's column
                    # list, and _coerce_row's projection would silently drop
                    # every value under a column the first chunk predated
                    # (verified live 2026-10-19 -- with the seed backfill in
                    # the same sync, the populate's new-column value was lost
                    # exactly this way). Imported here because that module
                    # imports this one. A chunk spanning multiple versions only
                    # pins to its top; this deployment sets single-version
                    # chunks (REVERSE_ETL_CDF_CHUNK_VERSIONS=1) -- the code
                    # default is 10.
                    row_fields = get_row_fields(source_location, version=chunk_to)
                    event_row_fields = row_fields
                    event_fields_version = chunk_to
                    risingwave_added = add_missing_columns(cfg, row_fields)
                    for added in risingwave_added:
                        if added not in risingwave_columns_added:
                            risingwave_columns_added.append(added)
                    if risingwave_added:
                        context.log.info(f"Added column(s) to the RisingWave table: {risingwave_added}")
                    # flush=False: the broker transfers while the NEXT chunk's
                    # version-diff read runs; one bounded flush after the last
                    # chunk (below) is the single delivery checkpoint.
                    delivery_errors.extend(
                        _produce_to_kafka(cfg, events, key_columns=key_columns, row_fields=row_fields, producer=producer, flush=False)
                    )
                    events_synced += len(events)
                    for op, count in _summarize_ops(events).items():
                        op_counts[op] = op_counts.get(op, 0) + count
                    # The previews snapshot the events AS PRODUCED, with THIS
                    # chunk's schema -- re-projecting earlier chunks' events at
                    # metadata-build time with a later chunk's schema would
                    # show nulls for columns those events never carried.
                    if len(preview_events) < MESSAGE_PREVIEW_LIMIT:
                        preview_events += _message_previews(cfg, events, key_columns, row_fields)[: MESSAGE_PREVIEW_LIMIT - len(preview_events)]
                    if len(raw_preview_events) < RAW_MESSAGE_LIMIT:
                        raw_preview_events += _raw_messages(cfg, events, key_columns, row_fields)[: RAW_MESSAGE_LIMIT - len(raw_preview_events)]
                # Per-chunk heartbeat: the pipeline is bursty by design (read
                # 100k -> build -> enqueue -> repeat), and without this line a
                # demo audience just sees the targets plateau every chunk.
                context.log.info(
                    f"Chunk {index + 1}/{len(chunks)}: versions {chunk_from}-{chunk_to}, "
                    f"{len(rows)} rows, {len(events)} events (total {events_synced})"
                )
            if events_synced:
                context.log.info(f"Produced {events_synced} event(s) to {cfg.kafka_topic}: {op_counts}")
            if events_synced:
                # The single delivery checkpoint for the whole backlog: fail
                # here -- BEFORE the watermark append below -- and the next
                # sync replays the same versions (targets upsert by key).
                _flush_producer(cfg, producer, delivery_errors)

        if last_version is None and not rows_seen:
            if current_version is None:
                current_version = get_current_version(source_location)
            new_version = current_version
        elif not rows_seen:
            # No rows but the version advanced: schema-only commits (a DROP/ADD
            # COLUMN emits no row events) -- the watermark must still move,
            # otherwise the schema commit is re-read by every sync forever,
            # and drop propagation never fires.
            new_version = current_version

        # Skip the state write when the version is unchanged (no-op syncs):
        # writing the same version costs a full snapshot + commit for nothing.
        # A schema change is always a commit, so an unchanged version cannot
        # hide one.
        columns_dropped: dict[str, list[str]] = {}
        columns_renamed: list[str] = []
        if new_version is not None and new_version != last_version:
            # The propagation diffs the table's FINAL schema -- the version the
            # watermark advances to -- against last_schema. The chunk loop's
            # row_fields are pinned to the last EVENT-bearing chunk and must
            # not leak here: a RENAME committing after the last data event in
            # the same sync would otherwise never leave that chunk's schema,
            # the diff would see no change, and the rename would never reach
            # the targets (verified live 2026-10-19: add + populate + rename in
            # one unsynced window left the value under the old column name in
            # Postgres). Reading the ids here unconditionally too keeps the
            # pair from the same commit -- the leak wrote state rows pairing
            # one commit's names with another's ids (id=None), which would
            # turn the next sync's diff into a bogus drop.
            row_fields = get_row_fields(source_location, version=current_version)
            column_ids = get_column_ids(source_location, version=current_version)
            schema_changes = _propagate_column_drops(cfg, row_fields, last_schema, context.log, column_ids)
            columns_dropped = schema_changes["dropped"]
            columns_renamed = schema_changes["renamed"]
            if event_row_fields is not None and event_fields_version is not None:
                # The targets were fed events under the EVENT-era schema.
                # Anything changing between that schema and the final one -- a
                # RENAME or DROP landing inside the unsynced window, after the
                # last data event -- is invisible to the last_schema diff (the
                # state predates those columns) and must be applied here too,
                # or the targets keep the old name while the source moved on
                # (verified live 2026-10-19: add + populate + rename in one
                # unsynced window left the value under new_col_1 in Postgres).
                # The state diff above already EXECUTED its own changes, so the
                # event-era schema is adjusted by them first -- otherwise the
                # same rename would be detected twice and the second ALTER
                # would fail on the already-renamed column.
                # The event-era ids come from the event-era version: the final
                # ids no longer contain the old name, and an id-less recorded
                # column would read as a drop instead of the rename it was.
                event_ids = get_column_ids(source_location, version=event_fields_version)
                event_recorded = [
                    {"name": field[0], "id": event_ids.get(field[0])}
                    for field in event_row_fields
                ]
                for old_to_new in schema_changes["renamed"]:
                    old, new = [part.strip() for part in old_to_new.split("->")]
                    for entry in event_recorded:
                        if entry["name"] == old:
                            entry["name"] = new
                for target, names in schema_changes["dropped"].items():
                    for name in names:
                        event_recorded = [entry for entry in event_recorded if entry["name"] != name]
                event_changes = _propagate_column_drops(
                    cfg, row_fields, json.dumps(event_recorded), context.log, column_ids
                )
                for target, names in event_changes["dropped"].items():
                    for name in names:
                        if name not in columns_dropped[target]:
                            columns_dropped[target].append(name)
                for pair in event_changes["renamed"]:
                    if pair not in columns_renamed:
                        columns_renamed.append(pair)
            _write_state(token, cfg, new_version, _last_schema_json(row_fields, column_ids))
            if columns_renamed:
                context.log.info(f"Renamed column(s) on the targets: {columns_renamed}")
            if columns_dropped:
                context.log.info(f"Dropped column(s) from the targets: {columns_dropped}")

        context.add_output_metadata({
            "events_synced": MetadataValue.int(events_synced),
            "op_counts": MetadataValue.json(op_counts),
            "last_commit_version": (
                MetadataValue.int(new_version) if new_version is not None else MetadataValue.text("none")
            ),
            "kafka_topic": MetadataValue.text(cfg.kafka_topic),
            "risingwave_columns_added": MetadataValue.json(risingwave_columns_added),
            "columns_dropped": MetadataValue.json(columns_dropped),
            "columns_renamed": MetadataValue.json(columns_renamed),
            "kafka_messages": MetadataValue.json(preview_events),
            "kafka_messages_truncated": MetadataValue.bool(events_synced > MESSAGE_PREVIEW_LIMIT),
            "kafka_messages_raw": MetadataValue.json(raw_preview_events),
            "extraction": MetadataValue.text("delta_rs"),
            "databricks_compute": MetadataValue.text("none — delta-rs reads storage directly; one small SQL write advances the watermark"),
            **_cost_metadata(),
        })

    return cdf_to_kafka


def build_seed_asset(cfg: ReverseEtlSyncConfig):
    @asset(
        name=cfg.seed_asset,
        group_name=cfg.group_name,
        # After everything else, so the rows are not picked up by the setup job's
        # first sync and the demo can show them arriving.
        deps=[
            AssetKey(cfg.table_setup_asset),
            AssetKey(cfg.sync_asset),
            AssetKey(cfg.risingwave_asset),
            AssetKey(cfg.sink_asset),
        ],
        description=(
            f"Run the demo seed statements against {cfg.source_fqn} (insert, update, delete). "
            f"Not synced until {cfg.sync_asset} runs."
        ),
    )
    def seed(context: AssetExecutionContext):
        _require_databricks_env()
        token = _get_token()
        meter_reset()
        for statement in cfg.seed_statements:
            _run_sql(token, statement.replace("{source_table}", cfg.source_fqn))
        context.log.info(f"Ran {len(cfg.seed_statements)} seed statement(s) against {cfg.source_fqn}")
        context.add_output_metadata({
            "statements": MetadataValue.int(len(cfg.seed_statements)),
            **_cost_metadata(),
        })

    return seed
