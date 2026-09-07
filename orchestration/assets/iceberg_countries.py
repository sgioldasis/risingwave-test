"""Validate the Iceberg country reference table through StarRocks.

Moved to Databricks Unity Catalog (de_dev.sr_poc_external.iceberg_countries)
on 2026-09-07, created and loaded once via Databricks SQL as a Managed
Iceberg table (USING ICEBERG, catalogManaged feature dropped) matching the
format/properties of the existing funnel_summary_historical table. Read here
through the same databricks_uc StarRocks catalog already used for that
table -- no new catalog needed.

This table previously lived in the local lakekeeper_local Iceberg catalog
(MinIO-backed), which meant it was wiped by every full stack teardown and
had to be recreated (a ~3 minute one-time cost per teardown -- see
docs/SR_POC_UNIFIED_PLAN.md). Since Databricks is external/persistent like
the historical funnel table, this table now survives teardowns permanently
and this asset goes back to pure validation, same pattern as
modern_dashboard_setup.py's modern_dashboard_databricks_table.
"""

import os

from dagster import AssetExecutionContext, MetadataValue, asset
from sqlalchemy import create_engine, text

TABLE_FQN = "databricks_uc.sr_poc_external.iceberg_countries"


@asset(
    name="iceberg_countries",
    group_name="datalake",
    description=(
        "Validate the existing Databricks Iceberg country reference table "
        "through StarRocks."
    ),
)
def iceberg_countries(context: AssetExecutionContext) -> dict:
    """Validate the country reference table used by the dashboard and RisingWave."""
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
        columns = connection.execute(text(f"""
            SELECT country, country_name
            FROM {TABLE_FQN} LIMIT 0
        """)).keys()
        count = connection.execute(text(f"SELECT COUNT(*) FROM {TABLE_FQN}")).scalar()

    required_columns = {"country", "country_name"}
    missing_columns = required_columns - set(columns)
    if missing_columns:
        raise RuntimeError(
            f"{TABLE_FQN} is missing required columns: {sorted(missing_columns)}"
        )
    if not count:
        raise RuntimeError(f"{TABLE_FQN} exists but contains no country rows")

    context.log.info("Validated %s with %s rows", TABLE_FQN, count)
    return {
        "table": MetadataValue.text(TABLE_FQN),
        "validated_through": MetadataValue.text("StarRocks databricks_uc catalog"),
        "country_count": MetadataValue.int(count),
    }
