"""Direct Kafka -> StarRocks ingestion for the Funnel Dashboard's hot path.

Second leg of the "hot-only" contrast on the Funnel Dashboard: the
existing funnel_summary_hot_direct dataset reads
risingwave.public.funnel_summary through StarRocks's JDBC catalog (needs
RisingWave live), while this asset feeds the SAME traffic straight from
the three funnel Kafka topics (page_views / cart_events / purchases) via
Routine Load jobs into DUPLICATE KEY tables -- no RisingWave in the loop
at all, same pattern as wallet_direct_kafka_setup.py.

A VIEW (funnel_summary_direct, a dbt model since 2026-11-17 -- see the
note above HOT_DIRECT_VIEW_SQL) recomputes funnel_summary's 1-minute
window aggregation over the raw events at query time. Deliberately a
VIEW, not a scheduled MV: the chart's story is zero-lag, and at demo
volume (~tens of thousands of rows) the recompute is milliseconds.
Recomputing from raw events also sidesteps the revision double-counting
risk that RisingWave's window output carries (see the sync-rollup notes
in docs/poc/STARROCKS_SERVING_LAYER.md) -- one message per real event,
no revisions concept.

Recreation caveat: these tables are append-only with no unique key (the
producer emits no event id), so a Routine Load recreation re-reads the
topic from OFFSET_BEGINNING and double-counts everything it re-reads.
Routine Load only recreates when its latest run is inactive (stopped or
cancelled), which should not happen in normal operation; if it does,
truncate the three tables right after recreating.
"""

import os

from dagster import AssetExecutionContext, AssetKey, asset
from sqlalchemy import create_engine, text

# Reuse the wallet asset's idempotent Routine Load machinery verbatim --
# same engine, same inactive-state detection, same recreate semantics.
from .sql_loader import load_sql
from .wallet_direct_kafka_setup import _ensure_routine_load

SCHEMA = "sr_local_db"

# (topic, table, payload column DDL, payload field names) -- event_time is
# parsed from the producer's ISO 8601 UTC string at load time, same
# str_to_date(SUBSTR...) trick as the wallet loader (SUBSTR drops the
# fixed 6-char "+00:00" suffix).
_TABLES = [
    ("page_views", "page_views_direct",
     "user_id BIGINT, page_id VARCHAR(32)", ["user_id", "page_id"]),
    ("cart_events", "cart_events_direct",
     "user_id BIGINT, item_id VARCHAR(32)", ["user_id", "item_id"]),
    ("purchases", "purchases_direct",
     "user_id BIGINT, amount DECIMAL(38,9)", ["user_id", "amount"]),
]


# The Routine Load template lives in sql/starrocks/funnel_events_routine_
# load.sql (extracted 2026-11-17) -- it also carries the max_batch_interval
# = 5 floor note (the _direct tables trail a RisingWave-fed twin by up to
# ~5s -- expected pipeline latency) and the recreation double-count caveat.
def _create_routine_load_sql(table: str, fields: list[str]) -> str:
    cols = ", ".join(fields) + ", event_time_raw"
    # Escaped quotes inside the StarRocks SQL string literal, matching the
    # wallet loader's jsonpaths spelling exactly.
    jsonpaths = "[" + ",".join(f'\\"$.{f}\\"' for f in fields + ["event_time"]) + "]"
    return load_sql(
        "funnel_events_routine_load",
        schema=SCHEMA,
        table=table,
        cols=cols,
        jsonpaths=jsonpaths,
        topic=table.removesuffix("_direct"),
    )


# funnel_summary_direct and funnel_summary_direct_20s (the minute- and
# 20-second-grain funnel views over the direct-Kafka tables) are dbt
# models since 2026-11-17 (dbt_starrocks/models/funnel_summary_direct*.sql,
# with ref() ordering tables -> views -> dashboard_funnel_serving_cached_
# direct) -- this asset creates neither. What remains here is the RW-only
# twin's dataset view (below), which reads the risingwave JDBC catalog and
# therefore can't be a dbt model of this project (dbt runs would fail with
# RW down -- see the 2026-11-14 note on it).

# The RW-only twin's dataset view (chart "Funnel Over Time (Hot-Only)
# [RisingWave only]"). Defined here because it was originally created ad-hoc
# with no repo source -- a dbt run executed while RisingWave was down
# (2026-11-14) drop-swapped it away silently, and nothing recreated it.
# IF NOT EXISTS keeps an existing view untouched, and the guarded execution
# keeps THIS asset's mission (RW-free ingestion) intact when the risingwave
# JDBC catalog can't validate the reference because RW is down.
HOT_DIRECT_VIEW_SQL = f"""
CREATE VIEW IF NOT EXISTS {SCHEMA}.funnel_summary_hot_direct AS
SELECT window_start, window_end, country, viewers, carters, purchasers
FROM risingwave.public.funnel_summary
"""

# User dimension for the multi-table MV rewrite demo (mv_funnel_country_daily,
# dim_users (the user_id -> country lookup the MVs join, since the raw
# topics carry no country) is a dbt model since 2026-11-17
# (dbt_starrocks/models/dim_users.sql) -- same deterministic skewed
# mapping (GR plurality ~40%, then RO/CY/PT/ES/IT), 1M rows via
# generate_series -- but built by the dbt build step, not here.


@asset(
    name="funnel_events_direct_kafka",
    group_name="starrocks_unified_funnel",
    # The tables themselves (page_views_direct, cart_events_direct,
    # purchases_direct) are dbt-owned shells since 2026-11-17
    # (dbt_starrocks/models/*_direct.sql, on_table_exists='ignore') -- this
    # asset only wires the Routine Loads into them, so it depends on the
    # dbt models materializing first on a fresh cluster.
    deps=[
        AssetKey(["page_views_direct"]),
        AssetKey(["cart_events_direct"]),
        AssetKey(["purchases_direct"]),
    ],
    description=(
        "Raw funnel Kafka topics -> Routine Load jobs into the dbt-managed "
        "DUPLICATE KEY shell tables (dbt_starrocks/models/*_direct.sql; "
        "this asset wires the loads, the tables and the funnel_summary_"
        "direct views over them are dbt models). Powers the "
        "'Direct-from-Kafka' hot-path chart on the Funnel Dashboard, side "
        "by side with the RisingWave-JDBC chart."
    ),
)
def funnel_events_direct_kafka(context: AssetExecutionContext) -> dict:
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

    row = {}
    with engine.connect() as connection:
        for topic, table, _cols, fields in _TABLES:  # noqa: F841 -- cols kept in _TABLES for reference; DDL now dbt-owned
            # Table DDL is dbt-owned (dbt_starrocks/models/<table>.sql,
            # on_table_exists='ignore') since 2026-11-17 -- this loop only
            # wires the Routine Load into the existing table.
            _ensure_routine_load(
                connection, context, f"{table}_load",
                _create_routine_load_sql(table, fields),
            )
            context.log.info("Routine Load %s.%s ensured", SCHEMA, table)

        # funnel_summary_direct + _20s are dbt models now (see the note
        # above the HOT_DIRECT_VIEW_SQL constant) -- created by the dbt
        # build step, not here.

        try:
            connection.execute(text(HOT_DIRECT_VIEW_SQL))
            context.log.info("View %s.funnel_summary_hot_direct ready", SCHEMA)
        except Exception as exc:
            context.log.warning(
                "funnel_summary_hot_direct not ensured (RisingWave down?): %s", exc
            )

        # dim_users is a dbt model now (see the module note) -- built by
        # the dbt build step, not here.

        for _, table, _, _ in _TABLES:
            row[table] = connection.execute(text(
                f"SELECT COUNT(*) FROM {SCHEMA}.{table}"
            )).scalar()
        context.log.info("Direct-Kafka funnel rows: %s", row)

    return {
        "table_rows": row,
        "routine_loads": [f"{t}_load" for _, t, _, _ in _TABLES],
        "view": "funnel_summary_direct",
    }
