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
from dagster import AssetExecutionContext, MetadataValue, asset

from .databricks_optimize import _get_token
from .kafka_topics_setup import kafka_output_topics_setup
from .reverse_etl_cdf_setup import (
    KAFKA_TOPIC,
    KEY_COLUMN,
    RowField,
    _get_row_fields,
    _require_databricks_env,
    reverse_etl_cdf_to_kafka,
)

TABLE_NAME = "reverse_etl_cdf_poc_current"
PRIMARY_KEY_COLUMN = KEY_COLUMN

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
}


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
    )


def _kafka_connector_options() -> str:
    """WITH (...) clause options for the Kafka connector, matching
    kafka_topics_setup.py's _admin_client() credential convention (same
    OUTPUT_TOPICS-scoped credentials this topic lives under)."""
    bootstrap = os.environ.get("KAFKA_OUTPUT_BOOTSTRAP", "")
    if not bootstrap:
        raise ValueError("KAFKA_OUTPUT_BOOTSTRAP must be set in .env")

    options = [
        "connector = 'kafka'",
        f"topic = '{KAFKA_TOPIC}'",
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


def _column_ddl(field: RowField) -> str:
    name, connect_type, _ = field
    return f"{_quote(name)} {_RISINGWAVE_TYPE_BY_CONNECT_TYPE[connect_type]}"


def _create_table_sql(row_fields: list[RowField]) -> str:
    """CREATE TABLE with *all* live columns up front. Creating with only a
    few and adding the rest afterwards would race the table's
    scan.startup.mode='earliest' read of the topic: a column added a moment
    later misses the messages already read (they keep NULL)."""
    column_defs = [
        _column_ddl(field) + (" PRIMARY KEY" if field[0] == PRIMARY_KEY_COLUMN else "")
        for field in row_fields
    ]
    return f"""
        CREATE TABLE IF NOT EXISTS {TABLE_NAME} (
            {", ".join(column_defs)}
        )
        WITH (
            {_kafka_connector_options()}
        )
        FORMAT DEBEZIUM ENCODE JSON
    """


def add_missing_columns(row_fields: list[RowField]) -> list[str]:
    """Add any of `row_fields` the table lacks, with the matching RisingWave
    type; returns the names added. A no-op (returns []) if the table doesn't
    exist yet.

    Must run *before* messages carrying a new column are produced: RisingWave
    only fills a column from messages read after it exists, so a row ingested
    earlier keeps NULL.
    """
    conn = _get_risingwave_connection()
    try:
        with conn.cursor() as cur:
            cur.execute(
                "SELECT column_name FROM information_schema.columns "
                "WHERE table_schema = 'public' AND table_name = %s",
                (TABLE_NAME,),
            )
            existing = {row[0] for row in cur.fetchall()}
            if not existing:
                return []
            missing = [field for field in row_fields if field[0] not in existing]
            for field in missing:
                cur.execute(f"ALTER TABLE {TABLE_NAME} ADD COLUMN {_column_ddl(field)}")
        conn.commit()
        return [name for name, _, _ in missing]
    finally:
        conn.close()


@asset(
    group_name="reverse_etl_poc",
    # Ordered after reverse_etl_cdf_to_kafka purely for job-run narrative
    # (confirms messages exist by the time this materializes) -- not a
    # functional requirement: scan.startup.mode='earliest' below reads from
    # the topic's beginning regardless of creation order.
    deps=[kafka_output_topics_setup, reverse_etl_cdf_to_kafka],
    description=(
        f"Create a RisingWave table ingesting {KAFKA_TOPIC} via "
        "FORMAT DEBEZIUM ENCODE JSON -- a live upsert/delete table (keyed by "
        "id), not a derived view, with the source table's current columns and "
        "types. See docs/poc/REVERSE_ETL_CDF_POC_PLAN.md."
    ),
)
def reverse_etl_cdf_risingwave_table(context: AssetExecutionContext):
    _require_databricks_env()
    row_fields = _get_row_fields(_get_token())

    conn = _get_risingwave_connection()
    try:
        with conn.cursor() as cur:
            cur.execute(_create_table_sql(row_fields))
        conn.commit()
        context.log.info(f"{TABLE_NAME} ready, sourced from {KAFKA_TOPIC}")

        with conn.cursor() as cur:
            cur.execute(f"SELECT COUNT(*) FROM {TABLE_NAME}")
            count = cur.fetchone()[0]
    finally:
        conn.close()

    return {
        "table": MetadataValue.text(TABLE_NAME),
        "kafka_topic": MetadataValue.text(KAFKA_TOPIC),
        "row_count_at_setup_time": MetadataValue.int(count),
    }
