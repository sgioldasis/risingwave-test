"""Synchronous (rollup) materialized view demo for the wallet upsert pipeline.

Add-on comparison for docs/SR_POC_WALLET_UPSERT_DEMO.md: every MV
elsewhere in this project (mv_unified_funnel_summary,
mv_funnel_daily_country_rollup, mv_iceberg_countries_cache) is
*asynchronous* -- it needs an explicit REFRESH to pick up new data, which
is exactly why this project spent so much effort on staleness/refresh
handling. A synchronous MV (a "rollup") is different: it's maintained
directly by the storage engine on a single base table, updates in lockstep
with every write, and has no REFRESH statement at all.

Constraint that rules out attaching this to any existing table in this
project: synchronous MVs only work on Duplicate Key or Aggregate Key base
tables (confirmed against StarRocks docs before building this) -- NOT
Primary Key (both wallet_transactions and wallet_transactions_direct_kafka
are Primary Key) and not any table backed by an external catalog (the
Funnel Dashboard's tables all are). So this needs its own base table:
wallet_transactions_log, a Duplicate Key table that -- unlike the two PK
tables -- keeps every event as a separate row with no upsert/collapsing,
fed by yet another Routine Load job reading the same wallet_transactions
Kafka topic (a third independent consumer of it, alongside RisingWave and
the direct-Kafka PK table's own Routine Load job).
"""

import os

from dagster import AssetExecutionContext, MetadataValue, asset
from sqlalchemy import create_engine, text

SCHEMA = "sr_local_db_sr_local_db"
TABLE = "wallet_transactions_log"
LOAD_JOB = "wallet_log_kafka_load"
MV_NAME = "mv_wallet_type_rollup"

CREATE_TABLE_SQL = f"""
CREATE TABLE IF NOT EXISTS {SCHEMA}.{TABLE} (
    transaction_id VARCHAR(64),
    account_id VARCHAR(32),
    type VARCHAR(16),
    amount DECIMAL(38,9),
    status VARCHAR(16),
    event_time DATETIME
)
DUPLICATE KEY (transaction_id)
DISTRIBUTED BY HASH (transaction_id)
"""

# event_time parsed to a real DATETIME at load time, same approach and same
# reasoning as wallet_transactions_direct_kafka (see
# orchestration/assets/wallet_direct_kafka_setup.py) -- consistent display
# across all wallet tables on the same Superset dashboard.
CREATE_ROUTINE_LOAD_SQL = f"""
CREATE ROUTINE LOAD {SCHEMA}.{LOAD_JOB} ON {TABLE}
COLUMNS(transaction_id, account_id, type, amount, status, event_time_raw, event_time = str_to_date(substr(event_time_raw, 1, 26), '%Y-%m-%dT%H:%i:%s.%f'))
PROPERTIES (
    "format" = "json",
    "jsonpaths" = "[\\"$.transaction_id\\",\\"$.account_id\\",\\"$.type\\",\\"$.amount\\",\\"$.status\\",\\"$.event_time\\"]",
    "desired_concurrent_number" = "1",
    "max_batch_interval" = "5"
)
FROM KAFKA (
    "kafka_broker_list" = "redpanda:9092",
    "kafka_topic" = "wallet_transactions",
    "property.kafka_default_offsets" = "OFFSET_BEGINNING"
)
"""

# COUNT(*) is rejected by synchronous MV creation -- confirmed live:
# "The materialized view currently does not support const expr in select
# statement: {}. Please use Asynchronous Materialized View instead."
# COUNT(transaction_id) (a real column) works fine.
CREATE_MV_SQL = f"""
CREATE MATERIALIZED VIEW {SCHEMA}.{MV_NAME} AS
SELECT type, SUM(amount) AS total_amount, COUNT(transaction_id) AS event_count
FROM {SCHEMA}.{TABLE}
GROUP BY type
"""

INACTIVE_ROUTINE_LOAD_STATES = {"STOPPED", "CANCELLED"}


def _ensure_routine_load(connection, context, job_name: str, create_sql: str) -> None:
    existing = connection.execute(
        text(
            "SELECT STATE FROM information_schema.routine_load_jobs "
            "WHERE NAME = :name AND DB_NAME = :schema "
            "ORDER BY CREATE_TIME DESC LIMIT 1"
        ),
        {"name": job_name, "schema": SCHEMA},
    ).fetchone()

    if existing is None:
        connection.execute(text(create_sql))
        context.log.info("Created Routine Load job %s.%s", SCHEMA, job_name)
    elif existing[0] in INACTIVE_ROUTINE_LOAD_STATES:
        connection.execute(text(create_sql))
        context.log.info(
            "Routine Load job %s.%s's latest run was %s -- recreated",
            SCHEMA, job_name, existing[0],
        )
    else:
        context.log.info(
            "Routine Load job %s.%s already %s, leaving it running",
            SCHEMA, job_name, existing[0],
        )


@asset(
    name="wallet_transactions_log",
    group_name="wallet",
    description=(
        "Duplicate Key StarRocks table (keeps every event, no upsert/"
        "collapsing) fed by its own Routine Load job reading the "
        "wallet_transactions Kafka topic, plus a synchronous rollup MV "
        "(mv_wallet_type_rollup) demonstrating zero-lag, no-REFRESH-ever "
        "aggregation -- in contrast to every async MV elsewhere in this "
        "project. See docs/SR_POC_WALLET_UPSERT_DEMO.md."
    ),
)
def wallet_transactions_log(context: AssetExecutionContext) -> dict:
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

        _ensure_routine_load(connection, context, LOAD_JOB, CREATE_ROUTINE_LOAD_SQL)

        # `CREATE MATERIALIZED VIEW IF NOT EXISTS` does NOT suppress the
        # "already exists" error for synchronous MVs -- confirmed live, so
        # existence has to be checked manually (there's no
        # information_schema view for sync MVs the way there is for async
        # ones). `SHOW ALTER TABLE ROLLUP` looked like the natural check but
        # is WRONG for this: it's job *history*, which survives a
        # DROP TABLE + recreate under the same name -- confirmed live
        # 2026-09-13, this caused the asset to see a stale FINISHED record
        # from the dropped table and skip recreating the rollup on the new
        # one, silently leaving queries unaccelerated (EXPLAIN showed the
        # base table being scanned, not the rollup). `DESC <table> ALL`
        # instead reflects the table's actual CURRENT indexes.
        index_rows = connection.execute(
            text(f"DESC {SCHEMA}.{TABLE} ALL")
        ).fetchall()
        mv_exists = any(row[0] == MV_NAME for row in index_rows)
        if not mv_exists:
            connection.execute(text(CREATE_MV_SQL))
            context.log.info("Created synchronous rollup MV %s.%s", SCHEMA, MV_NAME)
        else:
            context.log.info("Synchronous rollup MV %s.%s already exists", SCHEMA, MV_NAME)

        count = connection.execute(
            text(f"SELECT COUNT(*) FROM {SCHEMA}.{TABLE}")
        ).scalar()

    return {
        "table": MetadataValue.text(f"{SCHEMA}.{TABLE}"),
        "routine_load_job": MetadataValue.text(f"{SCHEMA}.{LOAD_JOB}"),
        "rollup_mv": MetadataValue.text(f"{SCHEMA}.{MV_NAME}"),
        "row_count_at_setup_time": MetadataValue.int(count or 0),
    }
