"""RW-free updater for funnel_summary_historical (micro-batch watermark).

Third leg of the "no RisingWave" path: the RW-free serving stack already
ingests the raw funnel topics into StarRocks (funnel_events_direct_kafka)
and recomputes the 1-minute window aggregates at query time
(funnel_summary_direct VIEW). This asset closes the last gap the RW Iceberg
sink used to cover -- landing those windows into
databricks_uc.sr_poc_external.funnel_summary_historical -- with a scheduled
micro-batch INSERT instead of a streaming sink.

Design decisions (validated live 2026-11-15, see the insert guard test):

- Append-only INSERT, never DELETE: UC rejects Iceberg delete files from
  external engines (same constraint that forced the RW sink to be
  append-only). Idempotency comes from a correlated NOT EXISTS per
  (window_start, country) plus a MAX(window_start) watermark, so re-runs
  and retries are no-ops -- verified: the identical statement inserted 1
  row on first run and 0 on the second.

- First-value-wins per window. The source view recomputes counts from the
  raw event tables, so once a window is closed its value is final; the
  3-minute cutoff (same number the cold branch's
  mv_unified_funnel_summary uses) only ever exposes windows old enough
  that all their events have long since arrived. A late backfill would
  not be picked up for an already-inserted window -- accepted for the
  demo, and strictly no worse than the RW sink's dedup-by-MAX contract.

- Schedule runs every minute (the historical table is 3-min stale by
  design; the hot charts never read it). Unlike the reverse-ETL health
  schedules this one defaults STARTED: it IS the data path for the
  historical table once the RW sink is retired, so a stack rebuild
  should re-enable it. Pause it in the Dagster UI (Automation) to stop.

The scratch probe used to validate the write path lives at
databricks_uc.sr_poc_external.starrocks_write_probe (drop at leisure).
"""

import os

from dagster import (
    AssetExecutionContext,
    DefaultScheduleStatus,
    ScheduleDefinition,
    asset,
    define_asset_job,
    in_process_executor,
)
from sqlalchemy import create_engine, text

from .funnel_direct_kafka_setup import funnel_events_direct_kafka

TARGET = "databricks_uc.sr_poc_external.funnel_summary_historical"
SOURCE_VIEW = "sr_local_db_sr_local_db.funnel_summary_direct"

# One statement, idempotent by construction: the watermark skips everything
# already covered, the NOT EXISTS additionally guards any window the
# watermark logic could double-count after a retry between MAX and INSERT.
INSERT_CLOSED_WINDOWS_SQL = f"""
INSERT INTO {TARGET}
SELECT
    v.window_start,
    v.window_end,
    v.country,
    v.viewers,
    v.carters,
    v.purchasers,
    v.view_to_cart_rate,
    v.cart_to_buy_rate,
    DATE(v.window_start) AS window_date
FROM {SOURCE_VIEW} v
WHERE v.window_start > COALESCE(
        (SELECT MAX(window_start) FROM {TARGET}),
        TIMESTAMP('1970-01-01 00:00:00'))
  AND v.window_start < DATE_SUB(CURRENT_TIMESTAMP(), INTERVAL 3 MINUTE)
  AND NOT EXISTS (
        SELECT 1 FROM {TARGET} t
        WHERE t.window_start = v.window_start AND t.country = v.country)
"""


@asset(
    name="funnel_historical_watermark",
    group_name="starrocks_unified_funnel",
    description=(
        "Micro-batch updater for funnel_summary_historical (Databricks UC "
        "Iceberg): inserts closed 1-minute funnel windows recomputed from "
        "the direct-Kafka raw tables, guarded by a MAX(window_start) "
        "watermark and a per-window NOT EXISTS. Replaces the RisingWave "
        "Iceberg sink with no RisingWave in the loop."
    ),
    deps=[funnel_events_direct_kafka],
)
def funnel_historical_watermark(context: AssetExecutionContext) -> dict:
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
        # NOTE: positional access, not `.count` -- Row shadows a column named
        # "count" with the tuple's count() method (TypeError: int - function).
        before = connection.execute(text(
            f"SELECT COUNT(*), COALESCE(MAX(window_start), '1970-01-01') FROM {TARGET}"
        )).one()
        connection.execute(text(INSERT_CLOSED_WINDOWS_SQL))
        after_count, watermark = connection.execute(text(
            f"SELECT COUNT(*), COALESCE(MAX(window_start), '1970-01-01') FROM {TARGET}"
        )).one()

    inserted = after_count - before[0]
    context.log.info(
        "funnel_summary_historical: inserted %d closed window(s) "
        "(%d -> %d rows), watermark %s",
        inserted, before[0], after_count, watermark,
    )

    return {
        "rows_inserted": inserted,
        "rows_total": after_count,
        "watermark": str(watermark),
    }


# Every-minute schedule; runs the asset alone so a run is one small SQL
# round-trip (in_process keeps it out of the worker pool).
funnel_historical_watermark_job = define_asset_job(
    name="funnel_historical_watermark_job",
    selection=[funnel_historical_watermark],
    description=(
        "Insert newly closed funnel windows into funnel_summary_historical "
        "(RW-free micro-batch replacement for the RW Iceberg sink)."
    ),
    executor_def=in_process_executor,
)

funnel_historical_watermark_schedule = ScheduleDefinition(
    name="funnel_historical_watermark_schedule",
    job=funnel_historical_watermark_job,
    cron_schedule="* * * * *",
    default_status=DefaultScheduleStatus.RUNNING,
)
