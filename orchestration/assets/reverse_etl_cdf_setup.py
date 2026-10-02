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
from dagster import AssetExecutionContext, MetadataValue, asset

from .databricks_optimize import CLIENT_ID, CLIENT_SECRET, DATABRICKS_HOST, TENANT_ID, _get_token, _poll, _submit
from .kafka_topics_setup import kafka_output_topics_setup

CATALOG = "de_dev"
SCHEMA = "sr_poc_external"
SOURCE_TABLE = "reverse_etl_cdf_poc_source"
STATE_TABLE = "reverse_etl_cdf_poc_state"
SYNC_NAME = "reverse_etl_cdf_poc"

KAFKA_TOPIC = "rw_poc_reverse_etl_cdf_out"

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


def _read_last_version(token: str, sync_name: str) -> int | None:
    response = _run_sql(
        token,
        f"SELECT last_commit_version FROM {CATALOG}.{SCHEMA}.{STATE_TABLE} "
        f"WHERE sync_name = '{sync_name}'",
    )
    rows = _rows_as_dicts(response)
    return int(rows[0]["last_commit_version"]) if rows else None


def _write_last_version(token: str, sync_name: str, version: int) -> None:
    # MERGE keeps this idempotent across retries of the same run.
    _run_sql(
        token,
        f"""
        MERGE INTO {CATALOG}.{SCHEMA}.{STATE_TABLE} AS target
        USING (SELECT '{sync_name}' AS sync_name, {version} AS last_commit_version) AS source
        ON target.sync_name = source.sync_name
        WHEN MATCHED THEN UPDATE SET target.last_commit_version = source.last_commit_version
        WHEN NOT MATCHED THEN INSERT (sync_name, last_commit_version)
            VALUES (source.sync_name, source.last_commit_version)
        """,
    )


def _read_changes(token: str, since_version: int) -> list[dict[str, Any]]:
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
    response = _run_sql(
        token,
        f"SELECT * FROM table_changes('{CATALOG}.{SCHEMA}.{SOURCE_TABLE}', {since_version}) "
        "ORDER BY _commit_version",
    )
    return _rows_as_dicts(response)


def _get_current_version(token: str) -> int:
    """The table's latest committed version, per DESCRIBE HISTORY.

    Used to baseline a sync's first run when a full backfill isn't safe (see
    _get_earliest_available_version): checkpointing here with no rows
    produced means the *next* run's table_changes() starts from a version
    that actually has CDF data, instead of guessing version 0.
    """
    response = _run_sql(token, f"DESCRIBE HISTORY {CATALOG}.{SCHEMA}.{SOURCE_TABLE} LIMIT 1")
    rows = _rows_as_dicts(response)
    return int(rows[0]["version"])


def _get_earliest_available_version(token: str) -> int:
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
    response = _run_sql(token, f"DESCRIBE HISTORY {CATALOG}.{SCHEMA}.{SOURCE_TABLE}")
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


# Columns whose declared type downstream (RisingWave's reverse_etl_cdf_poc_current.id
# BIGINT) is numeric, but which arrive as JSON strings from the Statement
# Execution API's response (same issue _next_watermark already works around
# for _commit_version) -- RisingWave's Debezium JSON parser does not coerce
# a JSON string into an Int64 column; it drops the message instead (confirmed
# live: "Cannot parse value `1` with type `string` into expected type `Int64`").
NUMERIC_ROW_COLUMNS = ("id",)


def _row_without_cdf_columns(row: dict[str, Any]) -> dict[str, Any]:
    result = {k: v for k, v in row.items() if k not in CDF_METADATA_COLUMNS}
    for col in NUMERIC_ROW_COLUMNS:
        if result.get(col) is not None:
            result[col] = int(result[col])
    return result


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


def _build_kafka_message(event: dict[str, Any], key_columns: list[str]) -> tuple[bytes | None, bytes]:
    """Build the (key, value) pair for one Debezium-style event.

    Keyed from `after` (insert/update) or `before` (delete) -- whichever
    side of the envelope actually has the row.
    """
    key: bytes | None = None
    if key_columns:
        row_for_key = event.get("after") or event.get("before") or {}
        key_value = {col: row_for_key.get(col) for col in key_columns}
        key = json.dumps(key_value, default=str, sort_keys=True).encode("utf-8")
    value = json.dumps(event, default=str).encode("utf-8")
    return key, value


# Same events as KAFKA_TOPIC, re-encoded for the Debezium JDBC sink, which
# requires a Kafka Connect schema in every message (RisingWave's
# FORMAT DEBEZIUM does not, so KAFKA_TOPIC is left schemaless).
KAFKA_JDBC_TOPIC = "rw_poc_reverse_etl_cdf_out_jdbc"

# (name, connect type, optional) for the source table's row columns. The
# envelope name follows Debezium's `<server>.<schema>.<table>.Envelope`
# convention, which is what the sink uses to recognise a Debezium event.
# updated_at stays a string, matching the RisingWave table's VARCHAR.
ROW_SCHEMA_FIELDS = (("id", "int64", False), ("value", "string", True), ("updated_at", "string", True))
ENVELOPE_SCHEMA_NAME = f"{SYNC_NAME}.{SCHEMA}.{SOURCE_TABLE}"


def _connect_row_schema(optional: bool) -> dict[str, Any]:
    return {
        "type": "struct",
        "name": f"{ENVELOPE_SCHEMA_NAME}.Value",
        "optional": optional,
        "fields": [{"field": n, "type": t, "optional": o} for n, t, o in ROW_SCHEMA_FIELDS],
    }


def _build_connect_json_message(event: dict[str, Any], key_columns: list[str]) -> tuple[bytes, bytes]:
    """Build the (key, value) pair for one event as Kafka Connect JSON with
    embedded schemas (`{"schema": ..., "payload": ...}`), for the Debezium
    JDBC sink. Key must be non-null so the sink can resolve the primary key
    from `record_key`; deletes carry the row in `before` and `op` = "d"."""
    row_for_key = event.get("after") or event.get("before") or {}
    field_types = {n: (t, o) for n, t, o in ROW_SCHEMA_FIELDS}
    key_schema = {
        "type": "struct",
        "name": f"{ENVELOPE_SCHEMA_NAME}.Key",
        "optional": False,
        "fields": [{"field": c, "type": field_types[c][0], "optional": False} for c in key_columns],
    }
    key = {"schema": key_schema, "payload": {c: row_for_key.get(c) for c in key_columns}}

    value_schema = {
        "type": "struct",
        "name": f"{ENVELOPE_SCHEMA_NAME}.Envelope",
        "optional": False,
        "fields": [
            {"field": "before", **_connect_row_schema(optional=True)},
            {"field": "after", **_connect_row_schema(optional=True)},
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
    value = {"schema": value_schema, "payload": event}
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


def _produce_to_kafka(events: list[dict[str, Any]], key_columns: list[str]) -> None:
    producer = Producer(_kafka_producer_config())
    for event in events:
        key, value = _build_kafka_message(event, key_columns)
        producer.produce(topic=KAFKA_TOPIC, key=key, value=value)
        jdbc_key, jdbc_value = _build_connect_json_message(event, key_columns)
        producer.produce(topic=KAFKA_JDBC_TOPIC, key=jdbc_key, value=jdbc_value)
    producer.flush()


# Cap on how many produced messages are echoed back into the asset's run
# metadata -- a full backfill can emit far more events than are useful to
# render in the Dagster UI.
MESSAGE_PREVIEW_LIMIT = 50


def _message_previews(
    events: list[dict[str, Any]], key_columns: list[str], limit: int = MESSAGE_PREVIEW_LIMIT
) -> list[dict[str, Any]]:
    """Decode the (key, value) pairs actually sent to Kafka for the first
    `limit` events, so the run's materialization metadata can show real
    produced messages rather than just op counts."""
    previews = []
    for event in events[:limit]:
        key, value = _build_kafka_message(event, key_columns)
        previews.append({
            "key": json.loads(key) if key is not None else None,
            "value": json.loads(value),
        })
    return previews


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


@asset(
    group_name="reverse_etl_poc",
    description=(
        "Create the reverse-ETL CDF POC's source table (Change Data Feed enabled "
        "at creation) and its watermark bookkeeping table in "
        "de_dev.sr_poc_external, if absent. See docs/poc/REVERSE_ETL_CDF_POC_PLAN.md."
    ),
)
def reverse_etl_poc_table_setup(context: AssetExecutionContext):
    _require_databricks_env()
    token = _get_token()

    _run_sql(
        token,
        f"""
        CREATE TABLE IF NOT EXISTS {CATALOG}.{SCHEMA}.{SOURCE_TABLE} (
            id BIGINT NOT NULL,
            value STRING,
            updated_at TIMESTAMP
        ) USING DELTA
        TBLPROPERTIES ('delta.enableChangeDataFeed' = 'true')
        """,
    )
    context.log.info(f"{CATALOG}.{SCHEMA}.{SOURCE_TABLE} ready (CDF enabled)")

    _run_sql(
        token,
        f"""
        CREATE TABLE IF NOT EXISTS {CATALOG}.{SCHEMA}.{STATE_TABLE} (
            sync_name STRING, last_commit_version BIGINT
        )
        """,
    )
    context.log.info(f"{CATALOG}.{SCHEMA}.{STATE_TABLE} ready")

    context.add_output_metadata({
        "source_table": MetadataValue.text(f"{CATALOG}.{SCHEMA}.{SOURCE_TABLE}"),
        "state_table": MetadataValue.text(f"{CATALOG}.{SCHEMA}.{STATE_TABLE}"),
    })


@asset(
    group_name="reverse_etl_poc",
    deps=[reverse_etl_poc_table_setup, kafka_output_topics_setup],
    description=(
        "Batch-read new/changed/deleted rows from the POC source table's Change "
        "Data Feed since the last watermark, and produce them as Debezium-style "
        f"before/after/op events to the {KAFKA_TOPIC} Kafka topic. "
        "See docs/poc/REVERSE_ETL_CDF_POC_PLAN.md."
    ),
)
def reverse_etl_cdf_to_kafka(context: AssetExecutionContext):
    _require_databricks_env()
    token = _get_token()

    last_version = _read_last_version(token, SYNC_NAME)

    if last_version is None:
        backfill_from = _resolve_backfill_version()
        if backfill_from is not None:
            context.log.info(
                f"First run for {SYNC_NAME} -- backfilling {SOURCE_TABLE} "
                f"from explicit override version {backfill_from}"
            )
        else:
            earliest_available = _get_earliest_available_version(token)
            if earliest_available == 0:
                backfill_from = 0
                context.log.info(
                    f"First run for {SYNC_NAME} -- {SOURCE_TABLE}'s full history "
                    "is intact (earliest available version is 0), backfilling "
                    "from the beginning"
                )
            else:
                context.log.info(
                    f"First run for {SYNC_NAME} -- earliest available version "
                    f"is {earliest_available} (history already partially aged "
                    f"out of retention), baselining at {SOURCE_TABLE}'s current "
                    "version with no historical rows synced"
                )

        rows = _read_changes(token, backfill_from) if backfill_from is not None else []
    else:
        # table_changes()'s startingVersion is inclusive, and last_version was
        # already fully processed (it's what the previous run watermarked) --
        # resuming from last_version itself would re-deliver that version's
        # rows a second time. +1 to actually resume from the first
        # un-synced version.
        next_version = last_version + 1
        current_version = _get_current_version(token)
        if next_version > current_version:
            # No commits since the last run -- the common case for a daily
            # batch once caught up. table_changes() errors
            # (DELTA_CDC_START_VERSION_AFTER_LATEST) rather than returning
            # empty if asked for a version beyond the table's latest, so this
            # must be checked before querying rather than relying on an empty
            # result.
            context.log.info(f"No new commits since version {last_version} for {SYNC_NAME}")
            rows = []
        else:
            context.log.info(f"Reading CDF for {SOURCE_TABLE} since version {next_version}")
            rows = _read_changes(token, next_version)

    key_columns = ["id"]
    events = _to_debezium_events(rows, key_columns) if rows else []

    if events:
        _produce_to_kafka(events, key_columns=key_columns)

    if last_version is None and not rows:
        new_version = _get_current_version(token)
    else:
        new_version = _next_watermark(rows, last_version)

    if new_version is not None:
        _write_last_version(token, SYNC_NAME, new_version)

    op_counts = _summarize_ops(events)
    context.log.info(f"Produced {len(events)} event(s) to {KAFKA_TOPIC}: {op_counts}")

    context.add_output_metadata({
        "events_synced": MetadataValue.int(len(events)),
        "op_counts": MetadataValue.json(op_counts),
        "last_commit_version": (
            MetadataValue.int(new_version) if new_version is not None else MetadataValue.text("none")
        ),
        "kafka_topic": MetadataValue.text(KAFKA_TOPIC),
        "kafka_messages": MetadataValue.json(_message_previews(events, key_columns)),
        "kafka_messages_truncated": MetadataValue.bool(len(events) > MESSAGE_PREVIEW_LIMIT),
    })
