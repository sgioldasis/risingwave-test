"""Prerequisite assets for the modern funnel dashboard."""

import os

from dagster import AssetExecutionContext, MetadataValue, asset
from sqlalchemy import create_engine, text

TABLE_FQN = "databricks_uc.sr_poc_external.funnel_summary_historical"


@asset(
    group_name="setup",
    description=(
        "Validate that the already-provisioned Unity Catalog Iceberg table used "
        "by the RisingWave cold sink and StarRocks unified funnel view is readable."
    ),
)
def modern_dashboard_databricks_table(context: AssetExecutionContext) -> dict:
    """Validate the existing historical funnel table through StarRocks."""
    starrocks_url = os.environ.get(
        "STARROCKS_URL",
        "mysql+pymysql://root@starrocks:9030",
    )
    context.log.info("Validating %s through StarRocks", TABLE_FQN)

    engine = create_engine(
        starrocks_url,
        connect_args={"connect_timeout": 5},
        pool_pre_ping=True,
    )
    with engine.connect() as connection:
        connection.execute(text(f"""
            SELECT window_start, window_end, country, viewers, carters, purchasers
            FROM {TABLE_FQN}
            LIMIT 0
        """))

    context.log.info("Validated %s through StarRocks", TABLE_FQN)

    return {
        "table": MetadataValue.text(TABLE_FQN),
        "validated_through": MetadataValue.text("StarRocks databricks_uc catalog"),
        "required_columns": MetadataValue.int(6),
    }