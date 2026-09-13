"""AGGREGATE KEY table demo for the funnel pipeline.

Add-on comparison for docs/SR_POC_FUNNEL_AGGREGATE_KEY_DEMO.md -- suggestion
#5 from the StarRocks-skill-driven review of this project: AGGREGATE KEY
(and UNIQUE KEY) table models were never demonstrated anywhere in this
project. This asset builds the AGGREGATE KEY half, applied to the funnel
use case (viewers/carters/purchasers) instead of the wallet demo, since
those are pure additive counts with no reversal/upsert semantics to fight
(unlike the wallet's SUM-by-type attempt, which produced numbers that
could never match the Primary Key table's -- see
docs/SR_POC_WALLET_UPSERT_DEMO.md's AGGREGATE KEY section for that
finding).

NOT sourced from the `funnel` Kafka topic (the pre-aggregated
funnel_summary output) or the `funnel` materialized view -- confirmed live
2026-09-13 that both continuously re-emit a GROWING snapshot for the same
still-open window (viewers climbing 1 -> 11 -> 21 -> ... within one
window_start, not a single final row), despite funnel_summary.sql's own
comment claiming "EMIT ON WINDOW CLOSE finalises rows" -- that comment
describes aspirational/intended behavior, not what the SQL (which has no
actual EMIT ON WINDOW CLOSE clause) does. Naively summing those revisions
would massively over-count.

Sourced instead from the three RAW event Kafka topics (page_views,
cart_events, purchases) -- genuinely append-only, one message per real
occurrence, confirmed live to have no revision/update semantics at all.
Three independent Routine Load jobs, each incrementing only its own
counter column (via explicit 0/1 literals for the columns it doesn't own)
into the SAME AGGREGATE KEY table row -- confirmed live on a throwaway
table before building this that two jobs on two different topics merge
correctly into the same key's row without clobbering each other's column.

dbt-starrocks CANNOT build any of this -- checked the installed adapter's
actual macro source (`starrocks__olap_table` in
dbt/include/starrocks/macros/adapters/relation_helpers.sql): `table_type`
only accepts DUPLICATE/PRIMARY/UNIQUE, raising a compiler error for
anything else, so AGGREGATE isn't reachable via dbt's table
materialization at all. There's also no dbt materialization for Routine
Load anywhere in the adapter. Hence this is a plain Dagster/SQLAlchemy
asset, same pattern as every other Routine-Load-based table built for the
wallet demo.
"""

import os

from dagster import AssetExecutionContext, MetadataValue, asset
from sqlalchemy import create_engine, text

SCHEMA = "sr_local_db_sr_local_db"
TABLE = "funnel_daily_totals_agg"

CREATE_TABLE_SQL = f"""
CREATE TABLE IF NOT EXISTS {SCHEMA}.{TABLE} (
    day DATE,
    country VARCHAR(2),
    viewers BIGINT SUM,
    carters BIGINT SUM,
    purchasers BIGINT SUM
)
AGGREGATE KEY (day, country)
DISTRIBUTED BY HASH (day, country)
"""

# Each job explicitly sets ALL three counter columns (1 for the one it
# owns, 0 literal for the other two) rather than omitting columns and
# relying on a DEFAULT -- simpler and more certain to work, confirmed live.
# `day` is derived from the raw ISO 8601 event_time via LEFT(...,10) (the
# date portion is always the first 10 characters regardless of what
# follows) then STR_TO_DATE.
_LOAD_JOBS = {
    "funnel_viewers_agg_load": {
        "topic": "page_views",
        "jsonpaths": ["$.event_time"],
        "columns": (
            "event_time_raw, "
            "day = str_to_date(left(event_time_raw, 10), '%Y-%m-%d'), "
            "country = 'GR', viewers = 1, carters = 0, purchasers = 0"
        ),
    },
    "funnel_carters_agg_load": {
        "topic": "cart_events",
        "jsonpaths": ["$.event_time"],
        "columns": (
            "event_time_raw, "
            "day = str_to_date(left(event_time_raw, 10), '%Y-%m-%d'), "
            "country = 'GR', viewers = 0, carters = 1, purchasers = 0"
        ),
    },
    "funnel_purchasers_agg_load": {
        "topic": "purchases",
        "jsonpaths": ["$.event_time"],
        "columns": (
            "event_time_raw, "
            "day = str_to_date(left(event_time_raw, 10), '%Y-%m-%d'), "
            "country = 'GR', viewers = 0, carters = 0, purchasers = 1"
        ),
    },
}

INACTIVE_ROUTINE_LOAD_STATES = {"STOPPED", "CANCELLED"}


def _create_routine_load_sql(job_name: str, spec: dict) -> str:
    jsonpaths = ",".join(f'\\"{p}\\"' for p in spec["jsonpaths"])
    return f"""
CREATE ROUTINE LOAD {SCHEMA}.{job_name} ON {TABLE}
COLUMNS({spec["columns"]})
PROPERTIES (
    "format" = "json",
    "jsonpaths" = "[{jsonpaths}]",
    "desired_concurrent_number" = "1",
    "max_batch_interval" = "5"
)
FROM KAFKA (
    "kafka_broker_list" = "redpanda:9092",
    "kafka_topic" = "{spec["topic"]}",
    "property.kafka_default_offsets" = "OFFSET_BEGINNING"
)
"""


def _ensure_routine_load(connection, context, job_name: str, create_sql: str) -> None:
    existing = connection.execute(
        text(
            "SELECT STATE FROM information_schema.routine_load_jobs "
            "WHERE NAME = :name AND DB_NAME = :schema "
            "ORDER BY CREATE_TIME DESC LIMIT 1"
        ),
        {"name": job_name, "schema": SCHEMA},
    ).fetchone()

    if existing is None or existing[0] in INACTIVE_ROUTINE_LOAD_STATES:
        connection.execute(text(create_sql))
        context.log.info("Created/recreated Routine Load job %s.%s", SCHEMA, job_name)
    else:
        context.log.info(
            "Routine Load job %s.%s already %s, leaving it running",
            SCHEMA, job_name, existing[0],
        )


@asset(
    name="funnel_daily_totals_agg",
    group_name="starrocks",
    description=(
        "StarRocks AGGREGATE KEY table, one row per (day, country), kept "
        "continuously summed at the storage layer by three independent "
        "Routine Load jobs reading the raw page_views/cart_events/"
        "purchases Kafka topics -- no MV, no query-time GROUP BY. "
        "Table-model coverage add-on, see "
        "docs/SR_POC_FUNNEL_AGGREGATE_KEY_DEMO.md."
    ),
)
def funnel_daily_totals_agg(context: AssetExecutionContext) -> dict:
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
        connection.execute(text(CREATE_TABLE_SQL))
        context.log.info("Table %s.%s ready", SCHEMA, TABLE)

        for job_name, spec in _LOAD_JOBS.items():
            create_sql = _create_routine_load_sql(job_name, spec)
            _ensure_routine_load(connection, context, job_name, create_sql)

        count = connection.execute(
            text(f"SELECT COUNT(*) FROM {SCHEMA}.{TABLE}")
        ).scalar()

    return {
        "table": MetadataValue.text(f"{SCHEMA}.{TABLE}"),
        "routine_load_jobs": MetadataValue.text(", ".join(_LOAD_JOBS.keys())),
        "row_count_at_setup_time": MetadataValue.int(count or 0),
    }
