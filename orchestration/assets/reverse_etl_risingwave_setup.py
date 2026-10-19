"""RisingWave table sourced from the reverse-ETL CDF POC's Kafka topic (APR-233).

See docs/poc/REVERSE_ETL_CDF_POC_PLAN.md. Ingests the reverse-ETL topic
(reverse_etl_cdf_setup.KAFKA_TOPIC) via FORMAT DEBEZIUM ENCODE JSON -- this is
the payoff of producing Debezium-style before/after/op envelopes (see
reverse_etl_cdf_setup.py's _to_debezium_events): RisingWave applies the
before/after/op stream as real upserts and deletes against this table's own
storage, keyed by `id`, rather than needing a window-function MV to
reconstruct "current state" from an append-only event log. The messages embed
a Kafka Connect schema (for the Debezium JDBC sink); RisingWave unwraps the
payload and reads typed values from it.
"""

import os

import psycopg2
from dagster import AssetExecutionContext, AssetKey, MetadataValue, asset

from .databricks_optimize import _get_token
from .reverse_etl_config import ReverseEtlSyncConfig
from .reverse_etl_cdf_setup import (
    ZONED_TIMESTAMP,
    RowField,
    _register_avro_schemas,
    _require_databricks_env,
    get_table_location_cached,
)
from .reverse_etl_delta_reader import get_row_fields

# Kafka Connect schema type -> RisingWave type. int8 and int16 both map to
# SMALLINT (RisingWave has no 1-byte integer).
_RISINGWAVE_TYPE_BY_CONNECT_TYPE = {
    "int8": "SMALLINT",
    "int16": "SMALLINT",
    "int32": "INT",
    "int64": "BIGINT",
    "float": "REAL",
    "double": "DOUBLE PRECISION",
    "boolean": "BOOLEAN",
    "string": "VARCHAR",
    ZONED_TIMESTAMP: "TIMESTAMPTZ",
}

# information_schema.data_type (lowercased) back to the Avro connect type, for columns whose type the
# sync did not read from the source this run -- a renamed or dropped column still on the table while a
# registration has to keep it (see _registration_fields).
_CONNECT_TYPE_BY_RW_DATA_TYPE = {
    "character varying": "string",
    "varchar": "string",
    "bigint": "int64",
    "integer": "int32",
    "int": "int32",
    "smallint": "int16",
    "double precision": "double",
    "double": "double",
    "real": "float",
    "boolean": "boolean",
}


def _registration_fields(cfg: ReverseEtlSyncConfig, row_fields: list[RowField], existing: dict[str, str]) -> list[RowField]:
    """row_fields plus every live table column the registration would otherwise shed, as RowFields.

    RisingWave rebinds the whole table against the registry's latest schema on every ADD COLUMN and
    rejects the statement while any non-generated table column is missing from that schema. When a
    rename or drop is still pending -- the source already carries the new name / no longer has the
    column, but the targets do -- a registration built from row_fields alone sheds the old name from
    the registry and fails every ADD in the same sync (verified live: 'Column "description" is defined
    in SQL but not found in the source'). The extra columns ride along with their RisingWave type
    mapped back to the Avro connect type; the registration after the rename or drop has landed sheds
    them again. Generated <col>_ts companions are exempt: RisingWave never required them in the
    registry schema."""
    known = {field[0] for field in row_fields}
    extras: list[RowField] = []
    for name, data_type in existing.items():
        if name in known or name.endswith("_ts"):
            continue
        extras.append((name, _CONNECT_TYPE_BY_RW_DATA_TYPE.get(data_type.lower(), "string"), True))
    return list(row_fields) + extras


def _quote(identifier: str) -> str:
    return '"' + identifier.replace('"', '""') + '"'


def _get_risingwave_connection():
    """Same connection convention as risingwave_countries_table.py / risingwave_udfs.py."""
    return psycopg2.connect(
        host=os.environ.get("RISINGWAVE_HOST",     "frontend-node-0"),
        port=int(os.environ.get("RISINGWAVE_PORT", "4566")),
        database=os.environ.get("RISINGWAVE_DB",   "dev"),
        user=os.environ.get("RISINGWAVE_USER",     "root"),
        password=os.environ.get("RISINGWAVE_PASSWORD", ""),
        # A stalled frontend (TCP accepted, handshake never completes) must fail
        # the sync fast instead of hanging the run forever — seen live when the
        # zero-DBU sync ran from outside the container network (2026-10-14).
        connect_timeout=10,
    )


def _kafka_connector_options(cfg: ReverseEtlSyncConfig) -> str:
    """WITH (...) clause options for the Kafka connector, matching
    kafka_topics_setup.py's _admin_client() credential convention (same
    OUTPUT_TOPICS-scoped credentials this topic lives under)."""
    bootstrap = os.environ.get("KAFKA_OUTPUT_BOOTSTRAP", "")
    if not bootstrap:
        raise ValueError("KAFKA_OUTPUT_BOOTSTRAP must be set in .env")

    options = [
        "connector = 'kafka'",
        f"topic = '{cfg.kafka_topic}'",
        f"properties.bootstrap.server = '{bootstrap}'",
        "scan.startup.mode = 'earliest'",
    ]
    username = os.environ.get("KAFKA_OUTPUT_SASL_USERNAME", "")
    if username:
        password = os.environ.get("KAFKA_OUTPUT_SASL_PASSWORD", "")
        mechanism = os.environ.get("KAFKA_OUTPUT_SASL_MECHANISM", "SCRAM-SHA-512")
        options.extend([
            "properties.security.protocol = 'SASL_SSL'",
            f"properties.sasl.mechanism = '{mechanism}'",
            f"properties.sasl.username = '{username}'",
            f"properties.sasl.password = '{password}'",
        ])
    return ",\n    ".join(options)


def _column_defs(cfg: ReverseEtlSyncConfig, field: RowField) -> list[str]:
    """Column definition(s) for one field. With Avro, RisingWave cannot read the ZonedTimestamp string as
    TIMESTAMPTZ: it reads it as VARCHAR, plus a generated TIMESTAMPTZ column named <column>_ts (section 14.1
    of the sink doc)."""
    name, connect_type, _ = field
    if cfg.encoding == "avro" and connect_type == ZONED_TIMESTAMP:
        return [f"{_quote(name)} VARCHAR", f"{_quote(name + '_ts')} TIMESTAMPTZ AS {_quote(name)}::timestamptz"]
    return [f"{_quote(name)} {_RISINGWAVE_TYPE_BY_CONNECT_TYPE[connect_type]}"]


def _create_table_sql(cfg: ReverseEtlSyncConfig, row_fields: list[RowField]) -> str:
    """CREATE TABLE with *all* live columns up front. Creating with only a
    few and adding the rest afterwards would race the table's
    scan.startup.mode='earliest' read of the topic: a column added a moment
    later misses the messages already read (they keep NULL)."""
    column_defs = [
        column_def + (" PRIMARY KEY" if field[0] == cfg.key_column else "")
        for field in row_fields
        for column_def in _column_defs(cfg, field)
    ]
    if cfg.encoding == "avro":
        row_format = f"FORMAT DEBEZIUM ENCODE AVRO (schema.registry = '{cfg.schema_registry_url}')"
    else:
        row_format = "FORMAT DEBEZIUM ENCODE JSON"
    return f"""
        CREATE TABLE IF NOT EXISTS {cfg.risingwave_table} (
            {", ".join(column_defs)}
        )
        WITH (
            {_kafka_connector_options(cfg)}
        )
        {row_format}
    """


def add_missing_columns(cfg: ReverseEtlSyncConfig, row_fields: list[RowField]) -> list[str]:
    """Add any of `row_fields` the table lacks, with the matching RisingWave
    type; returns the names added. A no-op (returns []) if the table doesn't
    exist yet.

    Must run *before* messages carrying a new column are produced: RisingWave
    only fills a column from messages read after it exists, so a row ingested
    earlier keeps NULL. With Avro the new schema version is registered first.
    """
    conn = _get_risingwave_connection()
    try:
        with conn.cursor() as cur:
            cur.execute(
                "SELECT column_name, data_type FROM information_schema.columns "
                "WHERE table_schema = 'public' AND table_name = %s",
                (cfg.risingwave_table,),
            )
            existing = {row[0]: row[1] for row in cur.fetchall()}
            if not existing:
                return []
            missing = [field for field in row_fields if field[0] not in existing]
            if missing and cfg.encoding == "avro":
                # RisingWave checks a new column against the registry schema, so the new version comes
                # first -- and it must keep every column the table still has (a pending rename's old
                # name, a pending drop), see _registration_fields.
                _register_avro_schemas(cfg, [cfg.key_column], _registration_fields(cfg, row_fields, existing))
            for field in missing:
                for column_def in _column_defs(cfg, field):
                    cur.execute(f"ALTER TABLE {cfg.risingwave_table} ADD COLUMN {column_def}")
        conn.commit()
        return [name for name, _, _ in missing]
    finally:
        conn.close()


def drop_columns_from_risingwave(cfg: ReverseEtlSyncConfig, dropped_fields: list[str]) -> list[str]:
    """Drop `dropped_fields` — and their <name>_ts generated companions — from the
    RisingWave table; returns the names actually dropped. A no-op (returns [])
    if the table doesn't exist.

    The caller passes only fields recorded in the state table's last_schema and
    absent from the current source read (see _propagate_column_drops), so this
    can never touch a column the source didn't genuinely drop. The companion
    goes first: RisingWave refuses to drop a column a generated column still
    depends on."""
    conn = _get_risingwave_connection()
    try:
        with conn.cursor() as cur:
            cur.execute(
                "SELECT column_name FROM information_schema.columns "
                "WHERE table_schema = 'public' AND table_name = %s",
                (cfg.risingwave_table,),
            )
            existing = {row[0] for row in cur.fetchall()}
            if not existing:
                return []
            removed: list[str] = []
            for name in dropped_fields:
                for column in (f"{name}_ts", name):
                    if column in existing and column != cfg.key_column:
                        cur.execute(f"ALTER TABLE {cfg.risingwave_table} DROP COLUMN {_quote(column)}")
                        removed.append(column)
        conn.commit()
        return removed
    finally:
        conn.close()


def rename_columns_in_risingwave(cfg: ReverseEtlSyncConfig, renames: list[tuple[str, str]], row_fields: list[RowField]) -> list[str]:
    """Rename columns on the RisingWave target, preserving their data; returns the
    renames applied, as "old -> new".

    RisingWave has no RENAME COLUMN (v3.1 rejects it: 'Feature is not yet
    implemented', verified live), so a rename is: ADD the new column (the
    matching type, plus the <new>_ts generated companion for Avro timestamps),
    UPDATE it from the old column — UPDATE works on connector-ingesting tables,
    verified live — then drop the old companion and the old column. The data
    survives; the column moves to the end of the table.

    Each DDL commits before the next statement runs, keeping a partially failed
    run resumable. Idempotent: a rename already applied (new exists, old gone)
    is skipped, so a retried run cannot re-add a half-built column; and when the
    add path has already created the new column empty (a rename pending while
    events were produced), the UPDATE copy still runs and fills it before the
    old column goes away."""
    conn = _get_risingwave_connection()
    fields_by_name = {field[0]: field for field in row_fields}
    applied: list[str] = []

    def table_columns(cur) -> dict[str, str]:
        cur.execute(
            "SELECT column_name, data_type FROM information_schema.columns "
            "WHERE table_schema = 'public' AND table_name = %s",
            (cfg.risingwave_table,),
        )
        return {row[0]: row[1] for row in cur.fetchall()}

    try:
        with conn.cursor() as cur:
            existing = table_columns(cur)
            if not existing:
                return []
            if any(old in existing for old, _ in renames) and cfg.encoding == "avro":
                # RisingWave rebinds the whole table against the registry's latest schema on ADD, so
                # the registration keeps every column the table still has -- the old names included
                # (see _registration_fields).
                _register_avro_schemas(cfg, [cfg.key_column], _registration_fields(cfg, row_fields, existing))
            for old, new in renames:
                if old not in existing and new in existing:
                    applied.append(f"{old} -> {new}")
                    continue
                if old not in existing:
                    continue
                field = fields_by_name.get(new)
                if field is None:
                    continue
                for column_def in _column_defs(cfg, field):
                    if column_def.split()[0].strip('"') not in existing:
                        cur.execute(f"ALTER TABLE {cfg.risingwave_table} ADD COLUMN {column_def}")
                        conn.commit()
                        existing = table_columns(cur)
                cur.execute(f"UPDATE {cfg.risingwave_table} SET {_quote(new)} = {_quote(old)}")
                conn.commit()
                for gone in (f"{old}_ts", old):
                    if gone in existing and gone != cfg.key_column:
                        cur.execute(f"ALTER TABLE {cfg.risingwave_table} DROP COLUMN {_quote(gone)}")
                        conn.commit()
                        existing = table_columns(cur)
                applied.append(f"{old} -> {new}")
        return applied
    finally:
        conn.close()


def build_risingwave_table_asset(cfg: ReverseEtlSyncConfig):
    @asset(
        name=cfg.risingwave_asset,
        group_name=cfg.group_name,
        # Ordered after the sync asset purely for job-run narrative (confirms
        # messages exist by the time this materializes) -- not a functional
        # requirement: scan.startup.mode='earliest' below reads from the
        # topic's beginning regardless of creation order.
        deps=[AssetKey(cfg.topic_asset), AssetKey(cfg.sync_asset)],
        description=(
            f"Create a RisingWave table ingesting {cfg.kafka_topic} via "
            "FORMAT DEBEZIUM ENCODE JSON -- a live upsert/delete table (keyed by "
            f"{cfg.key_column}), not a derived view, with the source table's current "
            "columns and types. See docs/poc/REVERSE_ETL_CDF_POC_PLAN.md."
        ),
    )
    def risingwave_table(context: AssetExecutionContext):
        _require_databricks_env()
        token = _get_token()
        row_fields = get_row_fields(get_table_location_cached(token, cfg, cfg.source_fqn))
        if cfg.encoding == "avro":
            _register_avro_schemas(cfg, [cfg.key_column], row_fields)

        conn = _get_risingwave_connection()
        try:
            with conn.cursor() as cur:
                cur.execute(_create_table_sql(cfg, row_fields))
            conn.commit()
            context.log.info(f"{cfg.risingwave_table} ready, sourced from {cfg.kafka_topic}")

            with conn.cursor() as cur:
                cur.execute(f"SELECT COUNT(*) FROM {cfg.risingwave_table}")
                count = cur.fetchone()[0]
        finally:
            conn.close()

        return {
            "table": MetadataValue.text(cfg.risingwave_table),
            "kafka_topic": MetadataValue.text(cfg.kafka_topic),
            "row_count_at_setup_time": MetadataValue.int(count),
        }

    return risingwave_table
