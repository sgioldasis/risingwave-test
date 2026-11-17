"""Direct Kafka -> StarRocks ingestion for the wallet upsert demo.

Add-on comparison for docs/SR_POC_WALLET_UPSERT_DEMO.md, motivated by the
same internal PAM Operational Query Layer proposal that motivated the
original demo: the proposal specifically argues for StarRocks to serve
upsert-heavy financial reads directly from Kafka, not via an intermediate
stream processor. This asset builds exactly that path -- a StarRocks
Routine Load job consuming the same `wallet_transactions` Kafka topic the
RisingWave-mediated path also reads -- so the two can be shown side by
side with the same live traffic.

Independent of src_wallet_transactions/sink_wallet_transactions_to_starrocks
on purpose: this table's whole point is that it does NOT need RisingWave in
the loop at all, only the Kafka topic (already created by redpanda-init)
and StarRocks itself.
"""

import os

from dagster import AssetExecutionContext, AssetKey, MetadataValue, asset
from sqlalchemy import create_engine, text

from .sql_loader import load_sql

SCHEMA = "sr_local_db"
TABLE = "wallet_transactions_direct_kafka"
LOAD_JOB = "wallet_direct_kafka_load"
STATUS_LOAD_JOB = "wallet_status_update_load"

# Table DDL is dbt-owned since 2026-11-17: dbt_starrocks/models/
# wallet_transactions_direct_kafka.sql (on_table_exists='ignore') creates
# the table on a fresh cluster; this asset only wires the Routine Load in
# (deps on the dbt model key enforce the ordering).
#
# The Routine Load DDL lives in sql/starrocks/ (extracted 2026-11-17) --
# wallet_direct_kafka_load.sql and wallet_status_update_load.sql carry the
# full reasoning: the event_time str_to_date parsing (SUBSTR dropping the
# "+00:00" suffix, the earlier VARCHAR experiment), the 5s
# max_batch_interval floor and the resulting ~5s trail behind the
# RisingWave path, and the partial_update semantics of the status job.
CREATE_ROUTINE_LOAD_SQL = load_sql("wallet_direct_kafka_load", schema=SCHEMA)
CREATE_STATUS_LOAD_SQL = load_sql("wallet_status_update_load", schema=SCHEMA)

# Confirmed live: StarRocks keeps a row per CREATE in
# information_schema.routine_load_jobs, all sharing the same NAME --
# STOPPED/CANCELLED rows are historical, not currently-claimed names, so
# `CREATE ROUTINE LOAD` with that same name succeeds again with no need to
# (and no way to -- `STOP ROUTINE LOAD` on an already-stopped job errors
# with "not found when checking privilege") re-stop it first.
INACTIVE_ROUTINE_LOAD_STATES = {"STOPPED", "CANCELLED"}


def _ensure_routine_load(connection, context, job_name: str, create_sql: str) -> None:
    """Idempotently create a Routine Load job by name, recreating it if the
    latest job with that name is no longer active."""
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
        context.log.info(
            "Routine Load job %s.%s's latest run is %s -- recreating "
            "(StarRocks allows reusing the name once the prior job "
            "isn't active)",
            SCHEMA, job_name, existing[0],
        )
        connection.execute(text(create_sql))
        context.log.info("Recreated Routine Load job %s.%s", SCHEMA, job_name)
    elif existing[0] == "PAUSED":
        # StarRocks pauses a Routine Load on ANY single consumer error (e.g.
        # 'BE is not alive' during a CN restart) and leaves it paused forever
        # -- verified 2026-11-17: a CN blip paused all 10 loads silently and
        # the funnel dashboard went stale. RESUME keeps the committed
        # offsets: the backlog replays, nothing is lost or re-created.
        connection.execute(text(f"RESUME ROUTINE LOAD FOR {SCHEMA}.{job_name}"))
        context.log.info("Routine Load job %s.%s was PAUSED -- resumed", SCHEMA, job_name)
    else:
        context.log.info(
            "Routine Load job %s.%s already %s, leaving it running",
            SCHEMA, job_name, existing[0],
        )


@asset(
    name="wallet_transactions_direct_kafka_setup",
    group_name="wallet",
    # Named ..._setup (not ..._direct_kafka) because the dbt model of the
    # same table name IS the asset with that key now -- the two would
    # collide in the asset graph. This asset depends on the dbt model
    # (table shell) and owns only the Routine Load wiring.
    deps=[AssetKey(["wallet_transactions_direct_kafka"])],
    description=(
        "Routine Load job feeding the dbt-managed wallet_transactions_"
        "direct_kafka Primary Key table from the wallet_transactions "
        "Kafka topic -- no RisingWave in the loop. Add-on comparison "
        "against the RisingWave-mediated path "
        "(sink_wallet_transactions_to_starrocks -> wallet_transactions), "
        "see docs/SR_POC_WALLET_UPSERT_DEMO.md."
    ),
)
def wallet_transactions_direct_kafka_setup(context: AssetExecutionContext) -> dict:
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

        count = connection.execute(
            text(f"SELECT COUNT(*) FROM {SCHEMA}.{TABLE}")
        ).scalar()

    return {
        "table": MetadataValue.text(f"{SCHEMA}.{TABLE}"),
        "routine_load_job": MetadataValue.text(f"{SCHEMA}.{LOAD_JOB}"),
        "row_count_at_setup_time": MetadataValue.int(count or 0),
    }


@asset(
    name="wallet_status_update_load_job",
    group_name="wallet",
    deps=[AssetKey(["wallet_transactions_direct_kafka_setup"])],
    description=(
        "Second Routine Load job on wallet_transactions_direct_kafka, "
        "reading an independent 'wallet_status_updates' Kafka topic and "
        "writing ONLY the `status` column via StarRocks partial_update -- "
        "demonstrates a genuinely different write pattern than the "
        "full-row upsert used elsewhere: multiple independent writers, "
        "each with partial knowledge of a row, converging on the same "
        "table with no RisingWave involved. See "
        "docs/SR_POC_WALLET_UPSERT_DEMO.md."
    ),
)
def wallet_status_update_load_job(context: AssetExecutionContext) -> dict:
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
        _ensure_routine_load(connection, context, STATUS_LOAD_JOB, CREATE_STATUS_LOAD_SQL)

    return {
        "table": MetadataValue.text(f"{SCHEMA}.{TABLE}"),
        "routine_load_job": MetadataValue.text(f"{SCHEMA}.{STATUS_LOAD_JOB}"),
    }
