"""Preflight checks for the modern dashboard dependency graph."""

import os

from confluent_kafka import Consumer
from dagster import AssetExecutionContext, AssetKey, MetadataValue, asset
from sqlalchemy import create_engine, text

REQUIRED_CATALOGS = {"databricks_uc", "lakekeeper_local", "risingwave"}
REQUIRED_TOPIC = "funnel"


@asset(
    name="modern_dashboard_preflight",
    group_name="setup",
    # iceberg_countries and modern_dashboard_databricks_table now create/load
    # their own data (no longer just validate pre-existing state elsewhere),
    # so this preflight must run strictly after both -- without this, Dagster
    # schedules all three in parallel and this asset races the other two's
    # StarRocks queries against tables that may not exist or be populated
    # yet. Confirmed 2026-09-07: failed with "Unknown table
    # 'public.iceberg_countries'" when the race was lost.
    deps=[AssetKey(["iceberg_countries"]), AssetKey(["modern_dashboard_databricks_table"])],
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

        # Note: does NOT check risingwave.public.funnel_summary here. That
        # table is created by realtime_funnel_dbt_assets, which itself
        # depends on this preflight passing first (see CustomDagsterDbtTranslator
        # in orchestration/definitions.py) -- checking for it here is a
        # chicken-and-egg error that can never pass on a fresh RisingWave.
        # Confirmed 2026-09-07: this asset only needs to validate prerequisites
        # that are NOT created by this same job's downstream steps.
        checks = {
            "country_reference": (
                "SELECT country, country_name "
                "FROM databricks_uc.sr_poc_external.iceberg_countries LIMIT 1"
            ),
            "databricks_history": (
                "SELECT window_start, country "
                "FROM databricks_uc.sr_poc_external.funnel_summary_historical LIMIT 1"
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
        "checks": MetadataValue.int(len(checks) + 1),
    }
