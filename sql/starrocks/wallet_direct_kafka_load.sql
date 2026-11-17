-- WHAT: the direct Kafka -> StarRocks wallet path -- a Routine Load feeding
--   the wallet_transactions_direct_kafka Primary Key table straight from
--   the wallet_transactions topic.
-- FEATURE: Routine Load -- StarRocks's continuous Kafka consumer (JSON
--   batches committed every 5s here).
-- WHY: the "hot-only" contrast of the wallet demo: the same live traffic
--   served with no RisingWave in the loop at all, side by side with the
--   RW-mediated path (sink_wallet_transactions_to_starrocks ->
--   wallet_transactions) -- motivated by the PAM Operational Query Layer
--   proposal's argument for StarRocks serving upsert-heavy financial reads
--   directly from Kafka.
-- WHERE: Wallet Upsert Demo dashboard (RW vs direct charts); wired by the
--   wallet_transactions_direct_kafka_setup Dagster asset; the table shell
--   is a dbt model. Story: docs/SR_POC_WALLET_UPSERT_DEMO.md.

-- Routine Load feeding the wallet_transactions_direct_kafka Primary Key
-- table straight from the wallet_transactions Kafka topic -- no RisingWave
-- in the loop; the RW-mediated path reads the same topic side by side.
-- The table itself is dbt-owned since 2026-11-17
-- (dbt_starrocks/models/wallet_transactions_direct_kafka.sql,
-- on_table_exists='ignore'); this job is wired by
-- orchestration/assets/wallet_direct_kafka_setup.py.
--
-- event_time is parsed to a real DATETIME at load time via a computed
-- column in the COLUMNS clause: the producer emits ISO 8601 UTC (e.g.
-- "2026-09-12T09:00:41.585742+00:00"), which StarRocks can't parse directly
-- as a column-to-column mapping. `event_time_raw` is a temp column
-- (positionally mapped from jsonpaths like any other field); the real
-- `event_time` column is a derived expression over it. Confirmed live on a
-- throwaway table before applying: SUBSTR(...,1,26) drops the fixed 6-char
-- "+00:00" suffix (always UTC, so always exactly this length), leaving
-- "2026-09-13T03:42:13.609764" for str_to_date's '%Y-%m-%dT%H:%i:%s.%f' to
-- parse, correctly preserving microsecond precision.
--
-- An earlier version of the table kept event_time as VARCHAR (the raw ISO
-- 8601 string), reasoning that lexicographic order already matches
-- chronological order. That was true, but it displayed differently from the
-- RisingWave-mediated wallet_transactions table side by side on the same
-- Superset dashboard -- not worth the inconsistency.
--
-- max_batch_interval has a hard floor of 5s ("3" is rejected with
-- "max_batch_interval should >= 5"). Consequence for the side-by-side demo:
-- the RisingWave upsert sink lands rows row-by-row (per checkpoint) while
-- this job commits only every max_batch_interval, so this table trails
-- wallet_transactions by up to ~5s / a handful of rows at any instant.
-- Verified 2026-11-14 at 1 TPS: exactly 5 rows / 5.0s behind, converging as
-- the next batch lands -- expected pipeline latency, not missing data.
CREATE ROUTINE LOAD {schema}.wallet_direct_kafka_load ON wallet_transactions_direct_kafka
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
