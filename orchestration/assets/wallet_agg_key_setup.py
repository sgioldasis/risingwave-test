"""AGGREGATE KEY table demo for the wallet upsert pipeline.

Add-on comparison for docs/SR_POC_WALLET_UPSERT_DEMO.md -- suggestion #5
from the StarRocks-skill-driven review of this project: AGGREGATE KEY (and
UNIQUE KEY) table models were never demonstrated anywhere in this project.
This asset builds the AGGREGATE KEY half.

The mechanism is genuinely different from everything else built for this
demo: on a Primary Key table, a second row with the same key REPLACES the
first (whole-row). On an Aggregate Key table, a second row with the same
key MERGES into it via each column's declared aggregate function (SUM,
REPLACE, MAX, MIN, ...) -- confirmed live on a throwaway table before
building this: two rows for the same key summed correctly (SUM columns)
and correctly kept only the latest value (a REPLACE column), with no MV
and no query-time GROUP BY needed at all -- COUNT(*) and a plain SELECT *
already reflect the merged, one-row-per-key state.

This is a THIRD way (alongside the ad-hoc aggregate query over
wallet_transactions and the synchronous rollup MV over
wallet_transactions_log) to answer the same question -- "total amount and
event count per type" -- each via a structurally different mechanism:
query-time aggregation, a query-time-rewritten rollup index, and now
storage-level incremental aggregation on ingest.
"""

import os

from dagster import AssetExecutionContext, MetadataValue, asset
from sqlalchemy import create_engine, text

SCHEMA = "sr_local_db_sr_local_db"
TABLE = "wallet_type_totals_agg"
LOAD_JOB = "wallet_type_totals_agg_load"

CREATE_TABLE_SQL = f"""
CREATE TABLE IF NOT EXISTS {SCHEMA}.{TABLE} (
    type VARCHAR(16),
    total_amount DECIMAL(38,9) SUM,
    event_count BIGINT SUM,
    last_event_time DATETIME REPLACE
)
AGGREGATE KEY (type)
DISTRIBUTED BY HASH (type)
"""

# `event_count = 1` is a constant per-row contribution -- summed on merge,
# giving a running count without a separate COUNT aggregate (StarRocks
# Aggregate tables only support one declared aggregate function per
# column, applied uniformly; a literal-1 SUM column is the standard way to
# get a count). `last_event_time` uses REPLACE (keep the latest value on
# merge) rather than SUM, deliberately mixing aggregate functions in one
# table to show they're independent per column -- confirmed live on a
# throwaway table: SUM columns summed correctly, the REPLACE column
# correctly kept only the most recent event_time, not summed or dropped.
CREATE_ROUTINE_LOAD_SQL = f"""
CREATE ROUTINE LOAD {SCHEMA}.{LOAD_JOB} ON {TABLE}
COLUMNS(type, amount, event_time_raw, total_amount = amount, event_count = 1, last_event_time = str_to_date(substr(event_time_raw, 1, 26), '%Y-%m-%dT%H:%i:%s.%f'))
PROPERTIES (
    "format" = "json",
    "jsonpaths" = "[\\"$.type\\",\\"$.amount\\",\\"$.event_time\\"]",
    "desired_concurrent_number" = "1",
    "max_batch_interval" = "5"
)
FROM KAFKA (
    "kafka_broker_list" = "redpanda:9092",
    "kafka_topic" = "wallet_transactions",
    "property.kafka_default_offsets" = "OFFSET_BEGINNING"
)
"""

INACTIVE_ROUTINE_LOAD_STATES = {"STOPPED", "CANCELLED"}


@asset(
    name="wallet_type_totals_agg",
    group_name="wallet",
    description=(
        "StarRocks AGGREGATE KEY table, one row per `type`, kept "
        "continuously summed at the storage layer by a Routine Load job "
        "reading the wallet_transactions Kafka topic -- no MV, no "
        "query-time GROUP BY. Add-on comparison (table-model coverage) "
        "for docs/SR_POC_WALLET_UPSERT_DEMO.md."
    ),
)
def wallet_type_totals_agg(context: AssetExecutionContext) -> dict:
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

        existing = connection.execute(
            text(
                "SELECT STATE FROM information_schema.routine_load_jobs "
                "WHERE NAME = :name AND DB_NAME = :schema "
                "ORDER BY CREATE_TIME DESC LIMIT 1"
            ),
            {"name": LOAD_JOB, "schema": SCHEMA},
        ).fetchone()

        if existing is None or existing[0] in INACTIVE_ROUTINE_LOAD_STATES:
            connection.execute(text(CREATE_ROUTINE_LOAD_SQL))
            context.log.info("Created/recreated Routine Load job %s.%s", SCHEMA, LOAD_JOB)
        else:
            context.log.info(
                "Routine Load job %s.%s already %s, leaving it running",
                SCHEMA, LOAD_JOB, existing[0],
            )

        count = connection.execute(
            text(f"SELECT COUNT(*) FROM {SCHEMA}.{TABLE}")
        ).scalar()

    return {
        "table": MetadataValue.text(f"{SCHEMA}.{TABLE}"),
        "routine_load_job": MetadataValue.text(f"{SCHEMA}.{LOAD_JOB}"),
        "row_count_at_setup_time": MetadataValue.int(count or 0),
    }
