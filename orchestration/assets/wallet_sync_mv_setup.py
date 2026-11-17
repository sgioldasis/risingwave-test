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

from dagster import AssetExecutionContext, AssetKey, MetadataValue, asset
from sqlalchemy import create_engine, text

from .sql_loader import load_sql

SCHEMA = "sr_local_db"
TABLE = "wallet_transactions_log"
LOAD_JOB = "wallet_log_kafka_load"
MV_NAME = "mv_wallet_type_rollup"

# Table DDL is dbt-owned since 2026-11-17: dbt_starrocks/models/
# wallet_transactions_log.sql (on_table_exists='ignore') creates the table
# on a fresh cluster; this asset only wires the Routine Load and the
# synchronous rollup MV in (deps on the dbt model key enforce ordering).
#
# The Routine Load and MV DDL live in sql/starrocks/ (extracted
# 2026-11-17) -- wallet_transactions_log_load.sql (event_time str_to_date
# parsing, the 5s max_batch_interval floor / ~5s trail) and
# mv_wallet_type_rollup.sql (the COUNT(*) rejection gotcha).
CREATE_ROUTINE_LOAD_SQL = load_sql("wallet_transactions_log_load", schema=SCHEMA)
CREATE_MV_SQL = load_sql("mv_wallet_type_rollup", schema=SCHEMA)

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
    elif existing[0] == "PAUSED":
        # StarRocks pauses a Routine Load on ANY single consumer error (e.g.
        # 'BE is not alive' during a CN restart) and leaves it paused forever
        # -- verified 2026-11-17. RESUME keeps the committed offsets: the
        # backlog replays, nothing is lost or re-created.
        connection.execute(text(f"RESUME ROUTINE LOAD FOR {SCHEMA}.{job_name}"))
        context.log.info("Routine Load job %s.%s was PAUSED -- resumed", SCHEMA, job_name)
    else:
        context.log.info(
            "Routine Load job %s.%s already %s, leaving it running",
            SCHEMA, job_name, existing[0],
        )


@asset(
    name="wallet_transactions_log_setup",
    group_name="wallet",
    # Named ..._setup because the dbt model of the same table name IS the
    # asset with that key now -- the two would collide in the asset graph.
    # This asset depends on the dbt model (table shell) and owns only the
    # Routine Load + synchronous rollup MV wiring.
    deps=[AssetKey(["wallet_transactions_log"])],
    description=(
        "Duplicate Key StarRocks table (keeps every event, no upsert/"
        "collapsing) fed by its own Routine Load job reading the "
        "wallet_transactions Kafka topic, plus a synchronous rollup MV "
        "(mv_wallet_type_rollup) demonstrating zero-lag, no-REFRESH-ever "
        "aggregation -- in contrast to every async MV elsewhere in this "
        "project. See docs/SR_POC_WALLET_UPSERT_DEMO.md."
    ),
)
def wallet_transactions_log_setup(context: AssetExecutionContext) -> dict:
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
