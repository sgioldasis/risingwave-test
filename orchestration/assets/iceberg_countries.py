"""Validate the existing Iceberg country reference table through StarRocks."""

import os

from dagster import AssetExecutionContext, MetadataValue, asset
from sqlalchemy import create_engine, text

TABLE_FQN = "lakekeeper_local.public.iceberg_countries"


@asset(
    name="iceberg_countries",
    group_name="datalake",
    description=(
        "Validate the existing Lakekeeper Iceberg country reference table "
        "through StarRocks."
    ),
)
def iceberg_countries(context: AssetExecutionContext) -> dict:
    """Validate the country reference table used by the dashboard and RisingWave."""
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
        columns = connection.execute(text(f"""
            SELECT country, country_name
            FROM {TABLE_FQN}
            LIMIT 0
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
        "validated_through": MetadataValue.text("StarRocks lakekeeper_local catalog"),
        "country_count": MetadataValue.int(count),
    }
