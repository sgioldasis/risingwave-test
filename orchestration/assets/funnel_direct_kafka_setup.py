"""Direct Kafka -> StarRocks ingestion for the Funnel Dashboard's hot path.

Second leg of the "hot-only" contrast on the Funnel Dashboard: the
existing funnel_summary_hot_direct dataset reads
risingwave.public.funnel_summary through StarRocks's JDBC catalog (needs
RisingWave live), while this asset feeds the SAME traffic straight from
the three funnel Kafka topics (page_views / cart_events / purchases) via
Routine Load jobs into DUPLICATE KEY tables -- no RisingWave in the loop
at all, same pattern as wallet_direct_kafka_setup.py.

A VIEW (funnel_summary_direct) recomputes funnel_summary's 1-minute
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

from dagster import AssetExecutionContext, asset
from sqlalchemy import create_engine, text

# Reuse the wallet asset's idempotent Routine Load machinery verbatim --
# same engine, same inactive-state detection, same recreate semantics.
from .wallet_direct_kafka_setup import _ensure_routine_load

SCHEMA = "sr_local_db_sr_local_db"

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


def _create_routine_load_sql(table: str, fields: list[str]) -> str:
    cols = ", ".join(fields) + ", event_time_raw"
    # Escaped quotes inside the StarRocks SQL string literal, matching the
    # wallet loader's jsonpaths spelling exactly.
    jsonpaths = "[" + ",".join(f'\\"$.{f}\\"' for f in fields + ["event_time"]) + "]"
    return f"""
CREATE ROUTINE LOAD {SCHEMA}.{table}_load ON {table}
COLUMNS({cols}, event_time = str_to_date(substr(event_time_raw, 1, 26), '%Y-%m-%dT%H:%i:%s.%f'))
PROPERTIES (
    "format" = "json",
    "jsonpaths" = "{jsonpaths}",
    "desired_concurrent_number" = "1",
    "max_batch_interval" = "5"
)
FROM KAFKA (
    "kafka_broker_list" = "redpanda:9092",
    "kafka_topic" = "{table.removesuffix('_direct')}",
    "property.kafka_default_offsets" = "OFFSET_BEGINNING"
)
"""
# max_batch_interval = 5 is the StarRocks-enforced floor ("3" is rejected
# with "max_batch_interval should >= 5", same as the wallet loaders -- see
# orchestration/assets/wallet_direct_kafka_setup.py). Consequence for any
# side-by-side comparison against a RisingWave-fed table: the RW sink
# lands rows per checkpoint while this Routine Load commits only every 5s,
# so the _direct tables trail by up to ~5s / a few rows at any instant --
# expected pipeline latency, not missing data (verified 2026-11-14 on the
# wallet twin of this comparison: exactly 5 rows / 5.0s behind at 1 TPS,
# converging).


# Same window/funnel semantics as dbt/models/funnel_summary.sql (1-minute
# tumbling windows, distinct users per window, country hardcoded 'GR',
# same conversion rates), computed over the direct-Kafka tables instead
# of RisingWave's sources. A LEFT JOIN chain over per-window distinct
# user sets, identical shape.
CREATE_VIEW_SQL = f"""
CREATE VIEW IF NOT EXISTS {SCHEMA}.funnel_summary_direct AS
WITH p AS (
    SELECT date_trunc('minute', event_time) AS window_start, user_id
    FROM {SCHEMA}.page_views_direct GROUP BY 1, 2
),
c AS (
    SELECT date_trunc('minute', event_time) AS window_start, user_id
    FROM {SCHEMA}.cart_events_direct GROUP BY 1, 2
),
pur AS (
    SELECT date_trunc('minute', event_time) AS window_start, user_id
    FROM {SCHEMA}.purchases_direct GROUP BY 1, 2
),
stats AS (
    SELECT
        p.window_start,
        count(DISTINCT p.user_id)   AS viewers,
        count(DISTINCT c.user_id)   AS carters,
        count(DISTINCT pur.user_id) AS purchasers
    FROM p
    LEFT JOIN c   ON p.window_start = c.window_start   AND p.user_id = c.user_id
    LEFT JOIN pur ON p.window_start = pur.window_start AND p.user_id = pur.user_id
    GROUP BY p.window_start
)
SELECT
    window_start,
    date_add(window_start, INTERVAL 1 MINUTE) AS window_end,
    'GR' AS country,
    viewers,
    carters,
    purchasers,
    round(coalesce(carters / nullif(viewers, 0), 0), 2)   AS view_to_cart_rate,
    round(coalesce(purchasers / nullif(carters, 0), 0), 2) AS cart_to_buy_rate
FROM stats
"""


# 20-second-grain twin of funnel_summary_direct (time_slice, not
# date_trunc -- date_trunc bottoms out at minute). The original RW-fed
# funnel topic pushed 20-second windows, so the dashboard chart and the
# online learner were built for that cadence; at minute grain both visibly
# slowed down (2026-11-14: new chart point and new training record once a
# minute instead of three times). The RW-free serving view's hot branch
# (dashboard_funnel_serving_cached_direct) reads THIS view, not the
# minute-grain one -- the frozen MV cutoff sits in September, so every
# post-cutoff 20-second window is served exactly once.
CREATE_VIEW_20S_SQL = f"""
CREATE VIEW IF NOT EXISTS {SCHEMA}.funnel_summary_direct_20s AS
WITH p AS (
    SELECT time_slice(event_time, INTERVAL 20 SECOND) AS window_start, user_id
    FROM {SCHEMA}.page_views_direct GROUP BY 1, 2
),
c AS (
    SELECT time_slice(event_time, INTERVAL 20 SECOND) AS window_start, user_id
    FROM {SCHEMA}.cart_events_direct GROUP BY 1, 2
),
pur AS (
    SELECT time_slice(event_time, INTERVAL 20 SECOND) AS window_start, user_id
    FROM {SCHEMA}.purchases_direct GROUP BY 1, 2
),
stats AS (
    SELECT
        p.window_start,
        count(DISTINCT p.user_id)   AS viewers,
        count(DISTINCT c.user_id)   AS carters,
        count(DISTINCT pur.user_id) AS purchasers
    FROM p
    LEFT JOIN c   ON p.window_start = c.window_start   AND p.user_id = c.user_id
    LEFT JOIN pur ON p.window_start = pur.window_start AND p.user_id = pur.user_id
    GROUP BY p.window_start
)
SELECT
    window_start,
    date_add(window_start, INTERVAL 20 SECOND) AS window_end,
    'GR' AS country,
    viewers,
    carters,
    purchasers,
    round(coalesce(carters / nullif(viewers, 0), 0), 2)   AS view_to_cart_rate,
    round(coalesce(purchasers / nullif(carters, 0), 0), 2) AS cart_to_buy_rate
FROM stats
"""


@asset(
    name="funnel_events_direct_kafka",
    group_name="starrocks_unified_funnel",
    description=(
        "Raw funnel Kafka topics -> StarRocks DUPLICATE KEY tables via "
        "Routine Load (no RisingWave), plus funnel_summary_direct -- a "
        "VIEW recomputing funnel_summary's 1-minute window aggregation "
        "over them. Powers the 'Direct-from-Kafka' hot-path chart on the "
        "Funnel Dashboard, side by side with the RisingWave-JDBC chart."
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
        for topic, table, cols, fields in _TABLES:
            connection.execute(text(
                f"CREATE TABLE IF NOT EXISTS {SCHEMA}.{table} ("
                f"{cols}, event_time DATETIME"
                ") DISTRIBUTED BY HASH(user_id)"
            ))
            _ensure_routine_load(
                connection, context, f"{table}_load",
                _create_routine_load_sql(table, fields),
            )
            context.log.info("Routine Load %s.%s ensured", SCHEMA, table)

        connection.execute(text(CREATE_VIEW_SQL))
        context.log.info("View %s.funnel_summary_direct ready", SCHEMA)

        connection.execute(text(CREATE_VIEW_20S_SQL))
        context.log.info("View %s.funnel_summary_direct_20s ready", SCHEMA)

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
