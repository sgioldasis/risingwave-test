"""Preflight checks for the modern dashboard dependency graph."""

import os

from confluent_kafka import Consumer
from dagster import AssetExecutionContext, MetadataValue, asset
from sqlalchemy import create_engine, text

REQUIRED_CATALOGS = {"databricks_uc", "lakekeeper_local", "risingwave"}
REQUIRED_TOPIC = "funnel"


@asset(
    name="modern_dashboard_preflight",
    group_name="dashboard_setup",
    description=(
        "Validate StarRocks catalogs and dashboard relations plus the live "
        "Kafka funnel topic before dashboard assets execute."
    ),
)
def modern_dashboard_preflight(context: AssetExecutionContext) -> dict:
    """Fail early when a dashboard dependency is unavailable."""
    starrocks_url = os.environ.get(
        "STARROCKS_URL",
        "mysql+pymysql://root@starrocks:9030",
    )
    engine = create_engine(
        starrocks_url,
        connect_args={"connect_timeout": 5},
        pool_pre_ping=True,
    )

    with engine.connect() as connection:
        catalogs = {row[0] for row in connection.execute(text("SHOW CATALOGS"))}
        missing_catalogs = sorted(REQUIRED_CATALOGS - catalogs)
        if missing_catalogs:
            raise RuntimeError(f"Missing StarRocks catalogs: {missing_catalogs}")

        checks = {
            "country_reference": (
                "SELECT country, country_name "
                "FROM lakekeeper_local.public.iceberg_countries LIMIT 1"
            ),
            "databricks_history": (
                "SELECT window_start, country "
                "FROM databricks_uc.sr_poc_external.funnel_summary_historical LIMIT 1"
            ),
            "risingwave_hot": (
                "SELECT window_start, country "
                "FROM risingwave.public.funnel_summary LIMIT 1"
            ),
        }
        for name, query in checks.items():
            connection.execute(text(query)).fetchone()
            context.log.info("Preflight check passed: %s", name)

    kafka_servers = os.environ.get("KAFKA_BOOTSTRAP_SERVERS", "redpanda:9092")
    consumer = Consumer({
        "bootstrap.servers": kafka_servers,
        "group.id": "modern-dashboard-preflight",
        "enable.auto.commit": False,
    })
    try:
        metadata = consumer.list_topics(timeout=5)
    finally:
        consumer.close()
    if REQUIRED_TOPIC not in metadata.topics:
        raise RuntimeError(
            f"Kafka topic {REQUIRED_TOPIC!r} is unavailable at {kafka_servers}"
        )

    context.log.info("Preflight check passed: Kafka topic %s", REQUIRED_TOPIC)
    return {
        "catalogs": MetadataValue.int(len(REQUIRED_CATALOGS)),
        "kafka_topic": MetadataValue.text(REQUIRED_TOPIC),
        "checks": MetadataValue.int(4),
    }
