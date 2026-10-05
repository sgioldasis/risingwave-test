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
from itertools import zip_longest
from typing import Any

import requests
from confluent_kafka import Producer
from dagster import AssetExecutionContext, AssetKey, MetadataValue, asset

from .databricks_optimize import CLIENT_ID, CLIENT_SECRET, DATABRICKS_HOST, TENANT_ID, _get_token, _poll, _submit
from .reverse_etl_config import ReverseEtlSyncConfig

# Every function below takes the sync's ReverseEtlSyncConfig; the names live in reverse_etl_config.

# CDF's own metadata columns -- see Databricks' Change Data Feed docs.
CHANGE_TYPE_COLUMN = "_change_type"
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
    return data


def _rows_as_dicts(response: dict) -> list[dict[str, Any]]:
    columns = [col["name"] for col in response.get("manifest", {}).get("schema", {}).get("columns", [])]
    data_array = response.get("result", {}).get("data_array") or []
    return [dict(zip(columns, row, strict=True)) for row in data_array]


def _read_last_version(token: str, cfg: ReverseEtlSyncConfig) -> int | None:
    response = _run_sql(
        token,
        f"SELECT last_commit_version FROM {cfg.state_fqn} WHERE sync_name = '{cfg.sync_name}'",
    )
    rows = _rows_as_dicts(response)
    return int(rows[0]["last_commit_version"]) if rows else None


def _write_last_version(token: str, cfg: ReverseEtlSyncConfig, version: int) -> None:
    # MERGE keeps this idempotent across retries of the same run.
    _run_sql(
        token,
        f"""
        MERGE INTO {cfg.state_fqn} AS target
        USING (SELECT '{cfg.sync_name}' AS sync_name, {version} AS last_commit_version) AS source
        ON target.sync_name = source.sync_name
        WHEN MATCHED THEN UPDATE SET target.last_commit_version = source.last_commit_version
        WHEN NOT MATCHED THEN INSERT (sync_name, last_commit_version)
            VALUES (source.sync_name, source.last_commit_version)
        """,
    )


_MICROSECOND_TIMESTAMP_FORMAT = "yyyy-MM-dd'T'HH:mm:ss.SSSSSS'Z'"


def _read_changes(token: str, cfg: ReverseEtlSyncConfig, since_version: int) -> list[dict[str, Any]]:
    """Batch-read CDF rows via table_changes(), from since_version (inclusive)
    through the latest available version.

    ORDER BY _commit_version is required, not cosmetic: table_changes()
    documents no ordering guarantee, but downstream consumers (RisingWave's
    FORMAT DEBEZIUM table, or any consumer applying before/after/op events in
    delivery order) need commits for the same key applied oldest-to-newest --
    confirmed live: without this, a later update could be produced to Kafka
    ahead of an earlier one for the same id, leaving the applied state one
    version behind. _to_debezium_events()'s (key, commit_version) grouping
    handles preimage/postimage pairing regardless of order, but does not by
    itself fix the order *between* different commit versions.
    """
    # The Statement Execution API returns a TIMESTAMP with three fractional digits, although the
    # column keeps six, so timestamps are formatted here (the API session is UTC, hence the 'Z').
    select_list = [
        f"date_format(`{name}`, \"{_MICROSECOND_TIMESTAMP_FORMAT}\") AS `{name}`" if connect_type == ZONED_TIMESTAMP else f"`{name}`"
        for name, connect_type, _ in _get_row_fields(token, cfg)
    ]
    select_list += [
        CHANGE_TYPE_COLUMN,
        COMMIT_VERSION_COLUMN,
        f"date_format({COMMIT_TIMESTAMP_COLUMN}, \"{_MICROSECOND_TIMESTAMP_FORMAT}\") AS {COMMIT_TIMESTAMP_COLUMN}",
    ]
    response = _run_sql(
        token,
        f"SELECT {', '.join(select_list)} FROM table_changes('{cfg.source_fqn}', {since_version}) "
        "ORDER BY _commit_version",
    )
    return _rows_as_dicts(response)


def _get_current_version(token: str, cfg: ReverseEtlSyncConfig) -> int:
    """The table's latest committed version, per DESCRIBE HISTORY.

    Used to baseline a sync's first run when a full backfill isn't safe (see
    _get_earliest_available_version): checkpointing here with no rows
    produced means the *next* run's table_changes() starts from a version
    that actually has CDF data, instead of guessing version 0.
    """
    response = _run_sql(token, f"DESCRIBE HISTORY {cfg.source_fqn} LIMIT 1")
    rows = _rows_as_dicts(response)
    return int(rows[0]["version"])


def _get_earliest_available_version(token: str, cfg: ReverseEtlSyncConfig) -> int:
    """Earliest version still present in the table's transaction log, per a
    full (unlimited) DESCRIBE HISTORY.

    This is what makes the first-run decision work for both a brand-new
    table and a real pre-existing one, without the caller having to know or
    declare which case applies: 0 here means the log's full history back to
    table creation is intact (bounded only by delta.logRetentionDuration,
    default 30 days), so a full backfill can safely reconstruct the whole
    CDF picture. Anything greater than 0 means some history has already
    aged out -- a full backfill from 0 would produce a silently incomplete
    "partial history as synthetic inserts" result rather than error, which
    is worse than not attempting it.
    """
    response = _run_sql(token, f"DESCRIBE HISTORY {cfg.source_fqn}")
    rows = _rows_as_dicts(response)
    return min(int(row["version"]) for row in rows)


# --- Pure logic (no I/O) -----------------------------------------------------
#
# Ported unchanged from dagster-poc/reverse-etl/src/reverse_etl/defs/cdf_to_kafka.py
# -- no Databricks/Kafka client dependency.


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


def _get_row_fields(token: str, cfg: ReverseEtlSyncConfig) -> list[RowField]:
    """The source table's *current* columns, read at sync time, so a column
    added in Databricks flows into the message schema -- and from there into
    Postgres via the sink's schema.evolution -- with no code change. CDF
    reads use the latest table schema, so rows and schema stay in step."""
    response = _run_sql(
        token,
        f"SELECT column_name, data_type, is_nullable FROM {cfg.catalog}.information_schema.columns "
        f"WHERE table_schema = '{cfg.schema}' AND table_name = '{cfg.source_table}' "
        "ORDER BY ordinal_position",
    )
    fields = [
        (
            row["column_name"],
            _CONNECT_TYPE_BY_DATABRICKS_TYPE.get(str(row["data_type"]).upper(), "string"),
            str(row["is_nullable"]).upper() != "NO",
        )
        for row in _rows_as_dicts(response)
    ]
    if not fields:
        raise RuntimeError(f"No columns found for {cfg.source_fqn} in information_schema")
    return fields


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
    return conf


def _produce_to_kafka(
    cfg: ReverseEtlSyncConfig, events: list[dict[str, Any]], key_columns: list[str], row_fields: list[RowField]
) -> None:
    producer = Producer(_kafka_producer_config())
    for event in events:
        key, value = _build_connect_json_message(cfg, event, key_columns, row_fields)
        producer.produce(topic=cfg.kafka_topic, key=key, value=value)
    producer.flush()


# Cap on how many produced messages are echoed back into the asset's run
# metadata -- a full backfill can emit far more events than are useful to
# render in the Dagster UI.
MESSAGE_PREVIEW_LIMIT = 50


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
    `schema` included (_message_previews shows payloads only)."""
    raw = []
    for event in events[:limit]:
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

        columns = ",\n            ".join(
            [f"{cfg.key_column} BIGINT GENERATED ALWAYS AS IDENTITY", *cfg.source_columns]
        )
        _run_sql(
            token,
            f"""
        CREATE TABLE IF NOT EXISTS {cfg.source_fqn} (
            {columns}
        ) USING DELTA
        TBLPROPERTIES ('delta.enableChangeDataFeed' = 'true')
        """,
        )
        context.log.info(f"{cfg.source_fqn} ready (CDF enabled)")

        _run_sql(
            token,
            f"""
        CREATE TABLE IF NOT EXISTS {cfg.state_fqn} (
            sync_name STRING, last_commit_version BIGINT
        )
        """,
        )
        context.log.info(f"{cfg.state_fqn} ready")

        context.add_output_metadata({
            "source_table": MetadataValue.text(cfg.source_fqn),
            "state_table": MetadataValue.text(cfg.state_fqn),
        })

    return table_setup


def build_sync_asset(cfg: ReverseEtlSyncConfig):
    @asset(
        name=cfg.sync_asset,
        group_name=cfg.group_name,
        deps=[AssetKey(cfg.table_setup_asset), AssetKey(cfg.topic_asset)],
        description=(
            f"Batch-read new/changed/deleted rows from {cfg.source_fqn}'s Change "
            "Data Feed since the last watermark, and produce them as Debezium-style "
            f"before/after/op events to the {cfg.kafka_topic} Kafka topic. "
            "See docs/poc/REVERSE_ETL_CDF_POC_PLAN.md."
        ),
    )
    def cdf_to_kafka(context: AssetExecutionContext):
        _require_databricks_env()
        token = _get_token()

        last_version = _read_last_version(token, cfg)

        if last_version is None:
            backfill_from = _resolve_backfill_version()
            if backfill_from is not None:
                context.log.info(
                    f"First run for {cfg.sync_name} -- backfilling {cfg.source_table} "
                    f"from explicit override version {backfill_from}"
                )
            else:
                earliest_available = _get_earliest_available_version(token, cfg)
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

            rows = _read_changes(token, cfg, backfill_from) if backfill_from is not None else []
        else:
            # table_changes()'s startingVersion is inclusive, and last_version was
            # already fully processed (it's what the previous run watermarked) --
            # resuming from last_version itself would re-deliver that version's
            # rows a second time. +1 to actually resume from the first
            # un-synced version.
            next_version = last_version + 1
            current_version = _get_current_version(token, cfg)
            if next_version > current_version:
                # No commits since the last run -- the common case for a daily
                # batch once caught up. table_changes() errors
                # (DELTA_CDC_START_VERSION_AFTER_LATEST) rather than returning
                # empty if asked for a version beyond the table's latest, so this
                # must be checked before querying rather than relying on an empty
                # result.
                context.log.info(f"No new commits since version {last_version} for {cfg.sync_name}")
                rows = []
            else:
                context.log.info(f"Reading CDF for {cfg.source_table} since version {next_version}")
                rows = _read_changes(token, cfg, next_version)

        key_columns = [cfg.key_column]
        events = _to_debezium_events(rows, key_columns) if rows else []

        risingwave_columns_added: list[str] = []
        row_fields: list[RowField] = []
        if events:
            row_fields = _get_row_fields(token, cfg)
            # Before producing: RisingWave only fills a column from messages it
            # reads after the column exists. Imported here because that module
            # imports this one.
            from .reverse_etl_risingwave_setup import add_missing_columns

            risingwave_columns_added = add_missing_columns(cfg, row_fields)
            if risingwave_columns_added:
                context.log.info(f"Added column(s) to the RisingWave table: {risingwave_columns_added}")
            _produce_to_kafka(cfg, events, key_columns=key_columns, row_fields=row_fields)

        if last_version is None and not rows:
            new_version = _get_current_version(token, cfg)
        else:
            new_version = _next_watermark(rows, last_version)

        if new_version is not None:
            _write_last_version(token, cfg, new_version)

        op_counts = _summarize_ops(events)
        context.log.info(f"Produced {len(events)} event(s) to {cfg.kafka_topic}: {op_counts}")

        context.add_output_metadata({
            "events_synced": MetadataValue.int(len(events)),
            "op_counts": MetadataValue.json(op_counts),
            "last_commit_version": (
                MetadataValue.int(new_version) if new_version is not None else MetadataValue.text("none")
            ),
            "kafka_topic": MetadataValue.text(cfg.kafka_topic),
            "risingwave_columns_added": MetadataValue.json(risingwave_columns_added),
            "kafka_messages": MetadataValue.json(_message_previews(cfg, events, key_columns, row_fields)),
            "kafka_messages_truncated": MetadataValue.bool(len(events) > MESSAGE_PREVIEW_LIMIT),
            "kafka_messages_raw": MetadataValue.json(_raw_messages(cfg, events, key_columns, row_fields)),
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
        for statement in cfg.seed_statements:
            _run_sql(token, statement.replace("{source_table}", cfg.source_fqn))
        context.log.info(f"Ran {len(cfg.seed_statements)} seed statement(s) against {cfg.source_fqn}")
        context.add_output_metadata({"statements": MetadataValue.int(len(cfg.seed_statements))})

    return seed
