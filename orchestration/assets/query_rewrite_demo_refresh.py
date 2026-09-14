"""One-shot warm-up for the StarRocks Query Rewrite Demo.

`mv_funnel_daily_country_rollup` (dbt_starrocks/models/mv_funnel_daily_country_rollup.sql)
was originally `refresh_method`-default (MANUAL), deliberately, so a demo
showing StarRocks transparently redirecting a query to a pre-aggregated MV
stayed static and reproducible for the whole session. Changed to
`REFRESH SCHEDULE EVERY (INTERVAL 5 MINUTE)` on 2026-09-14 after live use
showed the tradeoff cutting the other way: over a long-running session, the
"Rewrite OFF" chart (a live raw-table scan) kept advancing while "Rewrite
ON" (this MV) stayed frozen at whatever it looked like when last refreshed
by hand, so the two visibly diverged in row count, not just latency --
read as a bug, not the intended contrast. Briefly set to 1 minute the same
day, then lengthened to 5 minutes after profiling showed each refresh
takes 20-46s regardless of tuning, so 1 minute let live queries land
inside a slow refresh often enough to make "Rewrite ON" look as slow as
"Rewrite OFF". See the model file's own comment for the full tradeoff.

This asset still matters even with the periodic schedule: dbt-starrocks's
`materialized_view` materialization can skip rebuilding the model (and
therefore skip its `post_hook`'s sync refresh) if it detects no change
since the last run -- this asset forces a synchronous refresh unconditionally
as part of the Dagster job, independent of whether dbt decided to touch the
model that run, so the demo is guaranteed fresh at the end of every job
run regardless.
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
        "Forces an immediate synchronous REFRESH MATERIALIZED VIEW for the "
        "Query Rewrite Demo's MV (which also auto-refreshes every 5 minutes "
        "on its own schedule), guaranteeing it's current at the end of "
        "this job regardless of whether dbt decided to rebuild it. See "
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
