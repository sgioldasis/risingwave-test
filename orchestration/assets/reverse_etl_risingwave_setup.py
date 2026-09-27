"""RisingWave table sourced from the reverse-ETL CDF POC's Kafka topic (APR-233).

See docs/poc/REVERSE_ETL_CDF_POC_PLAN.md. Ingests rw_poc_reverse_etl_cdf_out
via FORMAT DEBEZIUM ENCODE JSON -- this is the payoff of producing
Debezium-style before/after/op envelopes (see reverse_etl_cdf_setup.py's
_to_debezium_events): RisingWave applies the before/after/op stream as real
upserts and deletes against this table's own storage, keyed by `id`, rather
than needing a window-function MV to reconstruct "current state" from an
append-only event log.
"""

import os

import psycopg2
from dagster import AssetExecutionContext, MetadataValue, asset

from .kafka_topics_setup import kafka_output_topics_setup
from .reverse_etl_cdf_setup import reverse_etl_cdf_to_kafka

TABLE_NAME = "reverse_etl_cdf_poc_current"
KAFKA_TOPIC = "rw_poc_reverse_etl_cdf_out"


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


@asset(
    group_name="reverse_etl_poc",
    # Ordered after reverse_etl_cdf_to_kafka purely for job-run narrative
    # (confirms messages exist by the time this materializes) -- not a
    # functional requirement: scan.startup.mode='earliest' below reads from
    # the topic's beginning regardless of creation order.
    deps=[kafka_output_topics_setup, reverse_etl_cdf_to_kafka],
    description=(
        "Create a RisingWave table ingesting rw_poc_reverse_etl_cdf_out via "
        "FORMAT DEBEZIUM ENCODE JSON -- a live upsert/delete table (keyed by "
        "id), not a derived view. See docs/poc/REVERSE_ETL_CDF_POC_PLAN.md."
    ),
)
def reverse_etl_cdf_risingwave_table(context: AssetExecutionContext):
    conn = _get_risingwave_connection()
    try:
        with conn.cursor() as cur:
            cur.execute(f"""
                CREATE TABLE IF NOT EXISTS {TABLE_NAME} (
                    id BIGINT PRIMARY KEY,
                    value VARCHAR,
                    updated_at VARCHAR
                )
                WITH (
                    {_kafka_connector_options()}
                )
                FORMAT DEBEZIUM ENCODE JSON
            """)
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
