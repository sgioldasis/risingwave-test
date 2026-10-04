"""Tear down the reverse-ETL CDF POC so the demo can start again from a known state.

Run reverse_etl_poc_reset_job, then reverse_etl_poc_setup_job, then the seed
script: setup recreates everything with the original schema (rid, id, value,
updated_at), so columns added during a demo do not survive a reset. See
docs/poc/REVERSE_ETL_DEBEZIUM_JDBC_SINK.md, section 14.3.

Destructive, but only against the POC's own named objects (constants imported
from the setup modules, plus the guard below).
"""

import time

import requests
from dagster import In, Nothing, OpExecutionContext, in_process_executor, job, op

from .databricks_optimize import _get_token
from .kafka_topics_setup import _admin_client
from .postgres_sink_setup import get_postgres_connection
from .reverse_etl_config import POC_SYNC
from .reverse_etl_cdf_setup import (
    CATALOG,
    KAFKA_TOPIC,
    SCHEMA,
    SOURCE_TABLE,
    STATE_TABLE,
    _require_databricks_env,
    _run_sql,
)
from .reverse_etl_debezium_sink import CONNECT_URL, CONNECTOR_NAME, TARGET_TABLE
from .reverse_etl_risingwave_setup import TABLE_NAME as RISINGWAVE_TABLE
from .reverse_etl_risingwave_setup import _get_risingwave_connection

CONSUMER_GROUP = POC_SYNC.consumer_group

# The guard below is deliberately independent of the config: it is an allowlist, so
# pointing the config at other names makes the reset refuse, not follow.
_POC_PREFIXES = ("rw_poc_reverse_etl_", "reverse_etl_cdf_")


def _assert_poc_objects() -> None:
    """Refuse to run if any name this job drops stops looking like a POC object
    (for example after a careless rename of a shared constant)."""
    names = [KAFKA_TOPIC, CONNECTOR_NAME, TARGET_TABLE, RISINGWAVE_TABLE, SOURCE_TABLE, STATE_TABLE]
    bad = [n for n in names if not n.startswith(_POC_PREFIXES)]
    if bad or SCHEMA != "sr_poc_external":
        raise RuntimeError(f"Refusing to reset: these do not look like POC objects: {bad}, schema {SCHEMA}")


@op
def delete_connector(context: OpExecutionContext) -> None:
    _assert_poc_objects()
    resp = requests.delete(f"{CONNECT_URL}/connectors/{CONNECTOR_NAME}", timeout=30)
    if resp.status_code == 404:
        context.log.info(f"{CONNECTOR_NAME} did not exist")
    elif resp.ok:
        context.log.info(f"Deleted connector {CONNECTOR_NAME}")
    else:
        raise RuntimeError(f"Deleting {CONNECTOR_NAME} failed ({resp.status_code}): {resp.text[:500]}")


@op(ins={"start": In(Nothing)})
def drop_risingwave_table(context: OpExecutionContext) -> None:
    conn = _get_risingwave_connection()
    try:
        with conn.cursor() as cur:
            cur.execute(f"DROP TABLE IF EXISTS {RISINGWAVE_TABLE}")
        conn.commit()
    finally:
        conn.close()
    context.log.info(f"Dropped RisingWave table {RISINGWAVE_TABLE} (if it existed)")


@op(ins={"start": In(Nothing)})
def drop_postgres_table(context: OpExecutionContext) -> None:
    conn = get_postgres_connection()
    try:
        with conn.cursor() as cur:
            cur.execute(f'DROP TABLE IF EXISTS "{TARGET_TABLE}"')
        conn.commit()
    finally:
        conn.close()
    context.log.info(f"Dropped Postgres table {TARGET_TABLE} (if it existed)")


@op(ins={"start": In(Nothing)})
def delete_kafka_topic_and_group(context: OpExecutionContext) -> None:
    admin = _admin_client()

    # The group only becomes deletable once the connector's consumer has left it.
    for attempt in range(6):
        try:
            admin.delete_consumer_groups([CONSUMER_GROUP])[CONSUMER_GROUP].result()
            context.log.info(f"Deleted consumer group {CONSUMER_GROUP}")
            break
        except Exception as e:  # noqa: BLE001
            text = str(e)
            if "GROUP_ID_NOT_FOUND" in text:
                context.log.info(f"Consumer group {CONSUMER_GROUP} did not exist")
                break
            if attempt == 5:
                raise RuntimeError(f"Could not delete consumer group {CONSUMER_GROUP}: {text[:300]}") from e
            time.sleep(5)

    if KAFKA_TOPIC in admin.list_topics(timeout=20).topics:
        admin.delete_topics([KAFKA_TOPIC], operation_timeout=30)[KAFKA_TOPIC].result()
        context.log.info(f"Delete requested for topic {KAFKA_TOPIC}; waiting for it to disappear")
    # Topic deletes are asynchronous (about 6s observed); the setup job must not
    # try to recreate it while it is still listed.
    deadline = time.monotonic() + 90
    while KAFKA_TOPIC in admin.list_topics(timeout=20).topics:
        if time.monotonic() > deadline:
            raise RuntimeError(f"Topic {KAFKA_TOPIC} still listed after 90s")
        time.sleep(3)
    context.log.info(f"Topic {KAFKA_TOPIC} is gone")


@op(ins={"start": In(Nothing)})
def drop_databricks_tables(context: OpExecutionContext) -> None:
    _require_databricks_env()
    token = _get_token()
    for table in (SOURCE_TABLE, STATE_TABLE):
        _run_sql(token, f"DROP TABLE IF EXISTS {CATALOG}.{SCHEMA}.{table}")
        context.log.info(f"Dropped {CATALOG}.{SCHEMA}.{table} (if it existed)")


@job(
    name="reverse_etl_poc_reset_job",
    description=(
        "Tear down the APR-233 reverse-ETL CDF POC: the Debezium JDBC sink connector, "
        "the RisingWave and Postgres tables, the Kafka topic and consumer group, and the "
        "Databricks source and watermark tables. Then run reverse_etl_poc_setup_job and "
        "the seed script for a fresh demo with the original schema."
    ),
    executor_def=in_process_executor,
)
def reverse_etl_poc_reset_job():
    stopped = delete_connector()
    rw_dropped = drop_risingwave_table(stopped)
    pg_dropped = drop_postgres_table(rw_dropped)
    kafka_gone = delete_kafka_topic_and_group(pg_dropped)
    drop_databricks_tables(kafka_gone)
