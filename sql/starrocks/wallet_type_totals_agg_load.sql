-- WHAT: the Routine Load feeding wallet_type_totals_agg from the
--   wallet_transactions Kafka topic, one merged contribution per message.
-- FEATURE: Routine Load -- StarRocks's continuous Kafka consumer (JSON
--   batches committed every 5s here).
-- WHY: without a writer the AGGREGATE table stays empty -- this job is what
--   makes the storage-level aggregation live: every incoming event is
--   summed into its type's row at ingest, no MV and no query-time GROUP BY.
-- WHERE: created by the wallet_type_totals_agg Dagster asset alongside the
--   table. Story: docs/SR_POC_WALLET_UPSERT_DEMO.md.

-- Routine Load feeding wallet_type_totals_agg from the wallet_transactions
-- Kafka topic (see wallet_type_totals_agg.sql for the AGGREGATE KEY merge
-- story). event_time is parsed to a real DATETIME at load time: the
-- producer emits ISO 8601 UTC (always ending in a fixed 6-char "+00:00"),
-- which StarRocks can't map column-to-column directly, so event_time_raw is
-- a temp column and the real last_event_time is a derived expression
-- (SUBSTR(...,1,26) drops the suffix; microsecond precision preserved).
--
-- max_batch_interval = 5 is the StarRocks-enforced floor ("3" is rejected
-- with "max_batch_interval should >= 5"); this table therefore trails the
-- RisingWave-sink wallet_transactions by up to ~5s at any instant --
-- expected pipeline latency on the side-by-side dashboard, not missing data.
-- Owned by orchestration/assets/wallet_agg_key_setup.py.
CREATE ROUTINE LOAD {schema}.wallet_type_totals_agg_load ON wallet_type_totals_agg
COLUMNS(type, amount, event_time_raw, total_amount = amount, event_count = 1, last_event_time = str_to_date(substr(event_time_raw, 1, 26), '%Y-%m-%dT%H:%i:%s.%f'))
PROPERTIES (
    "format" = "json",
    "jsonpaths" = "[\"$.type\",\"$.amount\",\"$.event_time\"]",
    "desired_concurrent_number" = "1",
    "max_batch_interval" = "5"
)
FROM KAFKA (
    "kafka_broker_list" = "redpanda:9092",
    "kafka_topic" = "wallet_transactions",
    "property.kafka_default_offsets" = "OFFSET_BEGINNING"
)
