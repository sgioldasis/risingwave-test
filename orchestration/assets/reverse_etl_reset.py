"""Tear down a reverse-ETL CDF sync so the demo can start again from a known state.

For the POC: run reverse_etl_poc_reset_job, then reverse_etl_poc_setup_job, then the
seed script. Setup recreates everything with the original schema (rid, id, value,
updated_at), so columns added during a demo do not survive a reset. See
docs/poc/REVERSE_ETL_DEBEZIUM_JDBC_SINK.md, section 14.3.

Destructive, but only against the sync's own named objects, and only after the
guard in _assert_reset_allowed() confirms every name matches the config's
reset_name_prefixes allowlist and reset_schema.
"""

import time

import requests
from dagster import In, Nothing, OpExecutionContext, in_process_executor, job, op

from .databricks_optimize import _get_token
from .kafka_topics_setup import _admin_client
from .postgres_sink_setup import get_postgres_connection
from .reverse_etl_cdf_setup import _require_databricks_env, _run_sql
from .reverse_etl_config import ReverseEtlSyncConfig
from .reverse_etl_debezium_sink import CONNECT_URL
from .reverse_etl_risingwave_setup import _get_risingwave_connection


def _assert_reset_allowed(cfg: ReverseEtlSyncConfig) -> None:
    """Refuse to run if any name this job drops stops looking like it belongs to
    the sync (for example after a careless rename in the config). The allowlist is
    its own config field, so widening it is a deliberate edit."""
    names = [
        cfg.kafka_topic,
        cfg.connector_name,
        cfg.postgres_table,
        cfg.risingwave_table,
        cfg.source_table,
        cfg.state_table,
    ]
    bad = [n for n in names if not n.startswith(cfg.reset_name_prefixes)]
    if bad or cfg.schema != cfg.reset_schema:
        raise RuntimeError(
            f"Refusing to reset {cfg.sync_name}: these do not match {cfg.reset_name_prefixes}: {bad}, "
            f"schema {cfg.schema} (allowed: {cfg.reset_schema})"
        )


def build_reset_job(cfg: ReverseEtlSyncConfig):
    # Dagster requires unique op names across all jobs in a repository, so each
    # sync's ops carry its sync_name.
    prefix = cfg.sync_name

    @op(name=f"{prefix}_delete_connector")
    def delete_connector(context: OpExecutionContext) -> None:
        _assert_reset_allowed(cfg)
        resp = requests.delete(f"{CONNECT_URL}/connectors/{cfg.connector_name}", timeout=30)
        if resp.status_code == 404:
            context.log.info(f"{cfg.connector_name} did not exist")
        elif resp.ok:
            context.log.info(f"Deleted connector {cfg.connector_name}")
        else:
            raise RuntimeError(f"Deleting {cfg.connector_name} failed ({resp.status_code}): {resp.text[:500]}")

    @op(name=f"{prefix}_drop_risingwave_table", ins={"start": In(Nothing)})
    def drop_risingwave_table(context: OpExecutionContext) -> None:
        conn = _get_risingwave_connection()
        try:
            with conn.cursor() as cur:
                cur.execute(f"DROP TABLE IF EXISTS {cfg.risingwave_table}")
            conn.commit()
        finally:
            conn.close()
        context.log.info(f"Dropped RisingWave table {cfg.risingwave_table} (if it existed)")

    @op(name=f"{prefix}_drop_postgres_table", ins={"start": In(Nothing)})
    def drop_postgres_table(context: OpExecutionContext) -> None:
        conn = get_postgres_connection()
        try:
            with conn.cursor() as cur:
                cur.execute(f'DROP TABLE IF EXISTS "{cfg.postgres_table}"')
            conn.commit()
        finally:
            conn.close()
        context.log.info(f"Dropped Postgres table {cfg.postgres_table} (if it existed)")

    @op(name=f"{prefix}_delete_kafka_topic_and_group", ins={"start": In(Nothing)})
    def delete_kafka_topic_and_group(context: OpExecutionContext) -> None:
        admin = _admin_client()
        group = cfg.consumer_group
        topic = cfg.kafka_topic

        # The group only becomes deletable once the connector's consumer has left it.
        for attempt in range(6):
            try:
                admin.delete_consumer_groups([group])[group].result()
                context.log.info(f"Deleted consumer group {group}")
                break
            except Exception as e:  # noqa: BLE001
                text = str(e)
                if "GROUP_ID_NOT_FOUND" in text:
                    context.log.info(f"Consumer group {group} did not exist")
                    break
                if attempt == 5:
                    raise RuntimeError(f"Could not delete consumer group {group}: {text[:300]}") from e
                time.sleep(5)

        if topic in admin.list_topics(timeout=20).topics:
            admin.delete_topics([topic], operation_timeout=30)[topic].result()
            context.log.info(f"Delete requested for topic {topic}; waiting for it to disappear")
        # Topic deletes are asynchronous (about 6s observed); the setup job must not
        # try to recreate it while it is still listed.
        deadline = time.monotonic() + 90
        while topic in admin.list_topics(timeout=20).topics:
            if time.monotonic() > deadline:
                raise RuntimeError(f"Topic {topic} still listed after 90s")
            time.sleep(3)
        context.log.info(f"Topic {topic} is gone")

    @op(name=f"{prefix}_drop_databricks_tables", ins={"start": In(Nothing)})
    def drop_databricks_tables(context: OpExecutionContext) -> None:
        _require_databricks_env()
        token = _get_token()
        for table in (cfg.source_fqn, cfg.state_fqn):
            _run_sql(token, f"DROP TABLE IF EXISTS {table}")
            context.log.info(f"Dropped {table} (if it existed)")

    @job(
        name=cfg.reset_job,
        description=(
            f"Tear down the {cfg.sync_name} reverse-ETL CDF sync: the Debezium JDBC sink connector, "
            "the RisingWave and Postgres tables, the Kafka topic and consumer group, and the "
            f"Databricks source and watermark tables. Then run {cfg.setup_job} and "
            "the seed script for a fresh demo with the original schema."
        ),
        executor_def=in_process_executor,
    )
    def reset_job():
        stopped = delete_connector()
        rw_dropped = drop_risingwave_table(stopped)
        pg_dropped = drop_postgres_table(rw_dropped)
        kafka_gone = delete_kafka_topic_and_group(pg_dropped)
        drop_databricks_tables(kafka_gone)

    return reset_job
