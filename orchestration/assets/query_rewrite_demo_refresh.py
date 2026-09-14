"""One-shot warm-up for the StarRocks Query Rewrite Demo.

`mv_funnel_daily_country_rollup` (dbt_starrocks/models/mv_funnel_daily_country_rollup.sql)
is deliberately `refresh_method`-default (MANUAL) -- see
docs/SR_POC_QUERY_REWRITE_DEMO.md: a demo showing StarRocks transparently
redirecting a query to a pre-aggregated MV wants that MV to be static and
reproducible for the whole session, not silently refreshing mid-demo. But
that means it's genuinely empty right after dbt creates it, and the
"Rewrite ON" chart shows nothing until someone runs `REFRESH MATERIALIZED
VIEW ... WITH SYNC MODE` by hand -- previously a manual step documented in
SR_POC_SUPERSET_DEMOS_RUNBOOK.md. This asset does it automatically instead,
so a single Dagster job actually finishes with the whole Query Rewrite
Demo ready to view.
"""

import os

from dagster import AssetExecutionContext, AssetKey, MetadataValue, asset
from sqlalchemy import create_engine, text

SCHEMA = "sr_local_db_sr_local_db"
MV_NAME = "mv_funnel_daily_country_rollup"


@asset(
    name="refresh_mv_funnel_daily_country_rollup",
    group_name="starrocks",
    deps=[AssetKey(["sr_local_db", MV_NAME])],
    description=(
        "One-shot REFRESH MATERIALIZED VIEW ... WITH SYNC MODE for the "
        "Query Rewrite Demo's MANUAL-refresh MV, so the demo is fully "
        "populated with no separate manual step. See "
        "docs/SR_POC_QUERY_REWRITE_DEMO.md."
    ),
)
def refresh_mv_funnel_daily_country_rollup(context: AssetExecutionContext) -> dict:
    starrocks_url = os.environ.get(
        "STARROCKS_URL",
        "mysql+pymysql://root@starrocks:9030",
    )
    engine = create_engine(
        starrocks_url,
        connect_args={"connect_timeout": 5},
        pool_pre_ping=True,
        isolation_level="AUTOCOMMIT",
    )

    with engine.connect() as connection:
        connection.execute(
            text(f"REFRESH MATERIALIZED VIEW {SCHEMA}.{MV_NAME} WITH SYNC MODE")
        )
        context.log.info("Refreshed %s.%s (SYNC)", SCHEMA, MV_NAME)

        count = connection.execute(
            text(f"SELECT COUNT(*) FROM {SCHEMA}.{MV_NAME}")
        ).scalar()

    return {
        "mv": MetadataValue.text(f"{SCHEMA}.{MV_NAME}"),
        "row_count_after_refresh": MetadataValue.int(count or 0),
    }
