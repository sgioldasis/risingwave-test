-- WHAT: the Routine Load feeding wallet_transactions_log, a Duplicate Key
--   table that keeps every wallet event as its own row (no upsert/collapse).
-- FEATURE: Routine Load -- StarRocks's continuous Kafka consumer (JSON
--   batches committed every 5s here).
-- WHY: the table exists because synchronous (rollup) MVs only attach to
--   Duplicate/Aggregate Key base tables -- this is the event-history base
--   the sync-MV demo needed, fed by a third independent consumer of the
--   wallet_transactions topic (besides RisingWave and the direct-Kafka PK
--   table's own load).
-- WHERE: Wallet Upsert Demo dashboard; wired by the
--   wallet_transactions_log_setup Dagster asset; the table shell is a dbt
--   model.

-- Routine Load feeding wallet_transactions_log, a Duplicate Key table that
-- keeps every event as a separate row with no upsert/collapsing (unlike the
-- Primary Key tables). It exists because synchronous (rollup) MVs only work
-- on Duplicate Key or Aggregate Key base tables -- NOT Primary Key
-- (wallet_transactions and wallet_transactions_direct_kafka are Primary
-- Key) and not on external-catalog-backed tables. Third independent
-- consumer of the wallet_transactions topic, alongside RisingWave and the
-- direct-Kafka PK table's own Routine Load job.
--
-- Table DDL is dbt-owned since 2026-11-17
-- (dbt_starrocks/models/wallet_transactions_log.sql,
-- on_table_exists='ignore'); this job is wired by
-- orchestration/assets/wallet_sync_mv_setup.py. event_time parsed to a real
-- DATETIME at load time, same approach as wallet_transactions_direct_kafka
-- (see wallet_direct_kafka_load.sql) -- consistent display across all
-- wallet tables on the same Superset dashboard. Same 5s max_batch_interval
-- floor, so this table trails the RisingWave-sink table by up to ~5s --
-- expected pipeline latency, not missing data.
CREATE ROUTINE LOAD {schema}.wallet_log_kafka_load ON wallet_transactions_log
COLUMNS(transaction_id, account_id, type, amount, status, event_time_raw, event_time = str_to_date(substr(event_time_raw, 1, 26), '%Y-%m-%dT%H:%i:%s.%f'))
PROPERTIES (
    "format" = "json",
    "jsonpaths" = "[\"$.transaction_id\",\"$.account_id\",\"$.type\",\"$.amount\",\"$.status\",\"$.event_time\"]",
    "desired_concurrent_number" = "1",
    "max_batch_interval" = "5"
)
FROM KAFKA (
    "kafka_broker_list" = "redpanda:9092",
    "kafka_topic" = "wallet_transactions",
    "property.kafka_default_offsets" = "OFFSET_BEGINNING"
)
