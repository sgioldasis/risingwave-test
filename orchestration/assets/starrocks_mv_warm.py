"""Force a synchronous refresh of the StarRocks unified funnel MV.

dashboard_funnel_serving reads the live 3-minute RisingWave window directly,
but anything older comes from mv_unified_funnel_summary, which only
self-refreshes on its own async schedule (widened to 5 minutes in
docs/SR_POC_UNIFIED_PLAN.md). A window that just aged past the 3-minute
boundary can be briefly invisible to the SQL query endpoints until that
refresh catches up. Running this asset forces the refresh synchronously so
that gap is closed on demand -- e.g. right before a live demo -- without
issuing ad hoc SQL by hand.
"""

import os

from dagster import AssetDep, AssetExecutionContext, AssetKey, MetadataValue, asset
from sqlalchemy import create_engine, text

MV_RELATION = "sr_local_db_sr_local_db.mv_unified_funnel_summary"


@asset(
    name="starrocks_mv_warm",
    group_name="dashboard_setup",
    deps=[AssetDep(asset=AssetKey(["sr_local_db_sr_local_db", "mv_unified_funnel_summary"]))],
    description=(
        "Synchronously refresh mv_unified_funnel_summary so the SQL query "
        "endpoints have no hot/cold boundary gap immediately after this runs."
    ),
)
def starrocks_mv_warm(context: AssetExecutionContext) -> dict:
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
        connection.execute(text(f"REFRESH MATERIALIZED VIEW {MV_RELATION} WITH SYNC MODE"))
    context.log.info("Synchronously refreshed %s", MV_RELATION)
    return {"relation": MetadataValue.text(MV_RELATION)}
