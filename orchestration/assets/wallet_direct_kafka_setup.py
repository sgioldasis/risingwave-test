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

from dagster import AssetExecutionContext, MetadataValue, asset
from sqlalchemy import create_engine, text

SCHEMA = "sr_local_db_sr_local_db"
TABLE = "wallet_transactions_direct_kafka"
LOAD_JOB = "wallet_direct_kafka_load"

CREATE_TABLE_SQL = f"""
CREATE TABLE IF NOT EXISTS {SCHEMA}.{TABLE} (
    transaction_id VARCHAR(64),
    account_id VARCHAR(32),
    type VARCHAR(16),
    amount DECIMAL(38,9),
    status VARCHAR(16),
    event_time VARCHAR(64)
)
PRIMARY KEY (transaction_id)
DISTRIBUTED BY HASH (transaction_id)
"""

# event_time kept as VARCHAR (raw ISO 8601 string from the producer, e.g.
# "2026-09-12T09:00:41.585742+00:00") rather than parsed into DATETIME at
# load time -- ISO 8601's lexicographic order already matches chronological
# order, so sorting/MAX() work correctly without a STR_TO_DATE expression in
# the Routine Load COLUMNS clause. Keeps the load job simple; a real
# production version would parse it, but for this latency-comparison demo
# it's not worth the extra failure surface.
CREATE_ROUTINE_LOAD_SQL = f"""
CREATE ROUTINE LOAD {SCHEMA}.{LOAD_JOB} ON {TABLE}
COLUMNS(transaction_id, account_id, type, amount, status, event_time)
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

# max_batch_interval has a hard floor of 5s -- confirmed live, "3" is
# rejected with "max_batch_interval should >= 5". 5s is still a fair
# comparison against the RisingWave path's ~2-3s tuned latency.

# Confirmed live: StarRocks keeps a row per CREATE in
# information_schema.routine_load_jobs, all sharing the same NAME --
# STOPPED/CANCELLED rows are historical, not currently-claimed names, so
# `CREATE ROUTINE LOAD` with that same name succeeds again with no need to
# (and no way to -- `STOP ROUTINE LOAD` on an already-stopped job errors
# with "not found when checking privilege") re-stop it first.
INACTIVE_ROUTINE_LOAD_STATES = {"STOPPED", "CANCELLED"}


@asset(
    name="wallet_transactions_direct_kafka",
    group_name="wallet",
    description=(
        "StarRocks Primary Key table fed directly by a Routine Load job "
        "reading the wallet_transactions Kafka topic -- no RisingWave in "
        "the loop. Add-on comparison against the RisingWave-mediated path "
        "(sink_wallet_transactions_to_starrocks -> wallet_transactions), "
        "see docs/SR_POC_WALLET_UPSERT_DEMO.md."
    ),
)
def wallet_transactions_direct_kafka(context: AssetExecutionContext) -> dict:
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

        if existing is None:
            connection.execute(text(CREATE_ROUTINE_LOAD_SQL))
            context.log.info("Created Routine Load job %s.%s", SCHEMA, LOAD_JOB)
        elif existing[0] in INACTIVE_ROUTINE_LOAD_STATES:
            context.log.info(
                "Routine Load job %s.%s's latest run is %s -- recreating "
                "(StarRocks allows reusing the name once the prior job "
                "isn't active)",
                SCHEMA, LOAD_JOB, existing[0],
            )
            connection.execute(text(CREATE_ROUTINE_LOAD_SQL))
            context.log.info("Recreated Routine Load job %s.%s", SCHEMA, LOAD_JOB)
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
