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

SCHEMA = "sr_local_db_sr_local_db"
TABLE = "wallet_transactions_direct_kafka"
LOAD_JOB = "wallet_direct_kafka_load"
STATUS_LOAD_JOB = "wallet_status_update_load"

CREATE_TABLE_SQL = f"""
CREATE TABLE IF NOT EXISTS {SCHEMA}.{TABLE} (
    transaction_id VARCHAR(64),
    account_id VARCHAR(32),
    type VARCHAR(16),
    amount DECIMAL(38,9),
    status VARCHAR(16),
    event_time DATETIME
)
PRIMARY KEY (transaction_id)
DISTRIBUTED BY HASH (transaction_id)
"""

# event_time is parsed to a real DATETIME at load time via a computed
# column in the Routine Load COLUMNS clause -- the producer emits ISO 8601
# (e.g. "2026-09-12T09:00:41.585742+00:00", always UTC per
# datetime.now(timezone.utc).isoformat()), which StarRocks can't parse
# directly as a column-to-column mapping. `event_time_raw` is a temp column
# (positionally mapped from jsonpaths, same as any other field); the actual
# `event_time` column is a derived expression over it. Confirmed live on a
# throwaway table before applying here: SUBSTR(...,1,26) drops the fixed
# 6-char "+00:00" suffix (always UTC, so always exactly this length),
# leaving "2026-09-13T03:42:13.609764" for str_to_date's '%Y-%m-%dT%H:%i:%s.%f'
# to parse, correctly preserving microsecond precision.
#
# An earlier version of this table kept event_time as VARCHAR (the raw
# ISO 8601 string) instead, reasoning that its lexicographic order already
# matches chronological order so sorting/MAX() would work without this
# parsing step. That was true, but it meant this table displayed
# differently from the RisingWave-mediated wallet_transactions table
# (which does `CAST(event_time AS TIMESTAMP)` in its own sink) side by side
# on the same Superset dashboard, which was confusing on inspection -- not
# worth the display inconsistency for demo purposes.
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

# max_batch_interval has a hard floor of 5s -- confirmed live, "3" is
# rejected with "max_batch_interval should >= 5". 5s is still a fair
# comparison against the RisingWave path's ~2-3s tuned latency.

# Second Routine Load job on the SAME table: reads an independent
# 'wallet_status_updates' topic carrying only {transaction_id, status} (a
# simulated fraud-review service that never sees amount/type/account_id at
# all -- see scripts/wallet_producer.py) and writes ONLY the `status`
# column via `partial_update`, confirmed live against a throwaway table
# before building this: other columns on the row are left untouched.
CREATE_STATUS_LOAD_SQL = f"""
CREATE ROUTINE LOAD {SCHEMA}.{STATUS_LOAD_JOB} ON {TABLE}
COLUMNS(transaction_id, status)
PROPERTIES (
    "format" = "json",
    "jsonpaths" = "[\\"$.transaction_id\\",\\"$.status\\"]",
    "partial_update" = "true",
    "desired_concurrent_number" = "1",
    "max_batch_interval" = "5"
)
FROM KAFKA (
    "kafka_broker_list" = "redpanda:9092",
    "kafka_topic" = "wallet_status_updates",
    "property.kafka_default_offsets" = "OFFSET_BEGINNING"
)
"""

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
    else:
        context.log.info(
            "Routine Load job %s.%s already %s, leaving it running",
            SCHEMA, job_name, existing[0],
        )


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
    deps=[AssetKey(["wallet_transactions_direct_kafka"])],
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
