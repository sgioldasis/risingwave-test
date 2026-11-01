"""Local bring-up + inspection for the delta sync's Postgres target: reverse_etl_delta_target.

The Debezium JDBC sink connector never creates its table (the demo's connector config sets no
auto.create), so the target table is the one-time setup the runbook did by hand. The second
asset is the consumer's side of the seam: what the connector reports and what the table holds,
so "is the data there?" is one Dagster materialization instead of three shell commands.
"""

import requests
from dagster import AssetExecutionContext, AssetSelection, asset, define_asset_job

from . import reverse_etl_connections as connections
from .reverse_etl_debezium_sink import CONNECT_URL

# The demo's delta sync (orchestration/defs/reverse_etl_delta/defs.yaml): the source is
# rid BIGINT GENERATED ALWAYS AS IDENTITY (see docs/poc/REVERSE_ETL_DEBEZIUM_JDBC_SINK.md),
# and the sync's ZONED_TIMESTAMP logical type maps to timestamptz in Postgres. The connector
# upserts by the record key (primary.key.fields = rid), so rid is the primary key here.
TARGET_TABLE = "reverse_etl_delta_target"
CONNECTOR_NAME = "reverse_etl_delta_sink"

TARGET_TABLE_DDL = f"""
CREATE TABLE IF NOT EXISTS {TARGET_TABLE} (
    rid BIGINT NOT NULL,
    id BIGINT,
    description TEXT,
    updated_at TIMESTAMPTZ,
    PRIMARY KEY (rid)
);
"""


@asset(
    group_name="reverse_etl",
    compute_kind="python",
    description="Creates the reverse_etl_delta_target table in local PostgreSQL (the JDBC sink never does)",
)
def reverse_etl_delta_postgres_target(context: AssetExecutionContext) -> str:
    """Create the delta sync's Postgres target table, idempotently."""
    conn = connections.schema_postgres().connect()
    try:
        with conn.cursor() as cur:
            cur.execute(TARGET_TABLE_DDL)
            cur.execute(
                "SELECT column_name, data_type FROM information_schema.columns "
                "WHERE table_name = %s ORDER BY ordinal_position;",
                (TARGET_TABLE,),
            )
            columns = cur.fetchall()
        conn.commit()
        context.log.info(f"{TARGET_TABLE} ready with columns: {columns}")
    finally:
        conn.close()
    return f"{TARGET_TABLE} created/verified"


@asset(
    group_name="reverse_etl",
    compute_kind="python",
    description="Reports the sink connector's state and the target table's row count",
    deps=["reverse_etl_delta_postgres_target"],
)
def reverse_etl_delta_consumer_report(context: AssetExecutionContext) -> str:
    """What the consumer sees: the connector's task states and the target's row count.

    A missing connector is reported, not raised — creating it is the inverness
    reverse_etl_delta_jdbc_sink asset's job (or the demo's), not this report's.
    Connect itself being unreachable is raised: the stack is not up.
    """
    try:
        response = requests.get(f"{CONNECT_URL}/connectors/{CONNECTOR_NAME}/status", timeout=10)
    except requests.RequestException as e:
        raise RuntimeError(
            f"Kafka Connect is unreachable at {CONNECT_URL} — start the stack (./bin/1_up.sh) "
            "and set KAFKA_CONNECT_URL=http://localhost:8083 in .env when running from the host"
        ) from e

    if response.status_code == 404:
        context.log.info(f"Connector {CONNECTOR_NAME} does not exist yet — materialize the sink asset first")
        connector_state = "not created"
    else:
        response.raise_for_status()
        status = response.json()
        connector_state = status.get("connector", {}).get("state", "unknown")
        task_states = [task.get("state", "unknown") for task in status.get("tasks", [])]
        context.log.info(f"Connector {CONNECTOR_NAME}: {connector_state}, tasks: {task_states}")
        if connector_state != "RUNNING":
            context.log.warning(f"Connector state is {connector_state}, not RUNNING — check its /status errors")

    conn = connections.schema_postgres().connect()
    try:
        with conn.cursor() as cur:
            cur.execute(f"SELECT COUNT(*) FROM {TARGET_TABLE};")
            (row_count,) = cur.fetchone()
        conn.commit()
    finally:
        conn.close()

    context.log.info(f"{TARGET_TABLE} holds {row_count} rows")
    return f"connector={connector_state}, rows={row_count}"


# The one-click path: create the table, then report. Materialize this after the sync has run
# (reverse_etl_delta_to_kafka over in inverness, or the demo's delta_setup_job here).
reverse_etl_consumer_job = define_asset_job(
    name="reverse_etl_consumer_job",
    selection=AssetSelection.assets(
        reverse_etl_delta_postgres_target, reverse_etl_delta_consumer_report
    ),
    description=(
        "Bring up the delta sync's Postgres target: create reverse_etl_delta_target, then "
        "report the reverse_etl_delta_sink connector's state and the table's row count. "
        "Run after the sync has produced to reverse_etl_delta_topic."
    ),
)
