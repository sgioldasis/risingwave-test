"""Batch Databricks Change Data Feed (CDF) -> Kafka reverse-ETL POC (APR-233).

See docs/poc/REVERSE_ETL_CDF_POC_PLAN.md for the full design rationale: this
evaluates whether Databricks CDF, read in batch (table_changes(), not
Structured Streaming), can cheaply sync new/changed/deleted rows to Kafka --
a capability drt (the leading OSS reverse-ETL candidate) deliberately does
not provide (ADR 0004 in drt-hub/drt: "drt activates warehouse data; it does
not replicate into the warehouse").

Uses a dedicated POC table + topic in the author's own sandbox schema
(de_dev.sr_poc_external), not any real E&A table.

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
    through the latest available version."""
    response = _run_sql(
        token,
        f"SELECT * FROM table_changes('{CATALOG}.{SCHEMA}.{SOURCE_TABLE}', {since_version})",
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


def _summarize_change_types(rows: list[dict[str, Any]]) -> dict[str, int]:
    """Count rows per CDF `_change_type` value, for run metadata."""
    counts: dict[str, int] = {}
    for row in rows:
        change_type = row.get(CHANGE_TYPE_COLUMN, "unknown")
        counts[change_type] = counts.get(change_type, 0) + 1
    return counts


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


def _build_kafka_message(row: dict[str, Any], key_columns: list[str]) -> tuple[bytes | None, bytes]:
    """Build the (key, value) pair for one CDF row.

    The value carries the full row, including CDF's own `_change_type` /
    `_commit_version` / `_commit_timestamp` columns, so a downstream consumer
    can distinguish inserts/updates/deletes and order by commit.
    """
    key: bytes | None = None
    if key_columns:
        key_value = {col: row.get(col) for col in key_columns}
        key = json.dumps(key_value, default=str, sort_keys=True).encode("utf-8")
    value = json.dumps(row, default=str).encode("utf-8")
    return key, value


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


def _produce_to_kafka(rows: list[dict[str, Any]], key_columns: list[str]) -> None:
    producer = Producer(_kafka_producer_config())
    for row in rows:
        key, value = _build_kafka_message(row, key_columns)
        producer.produce(topic=KAFKA_TOPIC, key=key, value=value)
    producer.flush()


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

    return {
        "source_table": MetadataValue.text(f"{CATALOG}.{SCHEMA}.{SOURCE_TABLE}"),
        "state_table": MetadataValue.text(f"{CATALOG}.{SCHEMA}.{STATE_TABLE}"),
    }


@asset(
    group_name="reverse_etl_poc",
    deps=[reverse_etl_poc_table_setup, kafka_output_topics_setup],
    description=(
        "Batch-read new/changed/deleted rows from the POC source table's Change "
        "Data Feed since the last watermark, and produce them as JSON to the "
        f"{KAFKA_TOPIC} Kafka topic. See docs/poc/REVERSE_ETL_CDF_POC_PLAN.md."
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

    if rows:
        _produce_to_kafka(rows, key_columns=["id"])

    if last_version is None and not rows:
        new_version = _get_current_version(token)
    else:
        new_version = _next_watermark(rows, last_version)

    if new_version is not None:
        _write_last_version(token, SYNC_NAME, new_version)

    change_counts = _summarize_change_types(rows)
    context.log.info(f"Produced {len(rows)} rows to {KAFKA_TOPIC}: {change_counts}")

    return {
        "rows_synced": MetadataValue.int(len(rows)),
        "change_type_counts": MetadataValue.json(change_counts),
        "last_commit_version": (
            MetadataValue.int(new_version) if new_version is not None else MetadataValue.text("none")
        ),
        "kafka_topic": MetadataValue.text(KAFKA_TOPIC),
    }
