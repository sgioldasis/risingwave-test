-- WHAT: a second, independent Routine Load writing ONLY the status column
--   of wallet_transactions_direct_kafka, from the wallet_status_updates
--   topic (partial_update).
-- FEATURE: Routine Load partial update -- writing only a subset of a
--   Primary Key table's columns per batch ("partial_update" = "true").
-- WHY: demonstrates a genuinely different write pattern than the full-row
--   upsert used elsewhere: multiple independent writers, each with partial
--   knowledge of a row (a simulated fraud-review service that never sees
--   amount/type/account_id), converging on the same table with no
--   RisingWave involved.
-- WHERE: Wallet Upsert Demo dashboard (status point-lookup charts); wired
--   by the wallet_status_update_load_job Dagster asset.

-- Second Routine Load job on the SAME wallet_transactions_direct_kafka
-- table (see wallet_direct_kafka_load.sql): reads an independent
-- 'wallet_status_updates' topic carrying only transaction_id + status (a
-- simulated fraud-review service that never sees amount/type/account_id at
-- all -- see scripts/wallet_producer.py) and writes ONLY the `status`
-- column via `partial_update`, confirmed live against a throwaway table
-- before building this: other columns on the row are left untouched.
-- Multiple independent writers, each with partial knowledge of a row,
-- converging on the same table with no RisingWave involved.
-- Owned by orchestration/assets/wallet_direct_kafka_setup.py.
CREATE ROUTINE LOAD {schema}.wallet_status_update_load ON wallet_transactions_direct_kafka
COLUMNS(transaction_id, status)
PROPERTIES (
    "format" = "json",
    "jsonpaths" = "[\"$.transaction_id\",\"$.status\"]",
    "partial_update" = "true",
    "desired_concurrent_number" = "1",
    "max_batch_interval" = "5"
)
FROM KAFKA (
    "kafka_broker_list" = "redpanda:9092",
    "kafka_topic" = "wallet_status_updates",
    "property.kafka_default_offsets" = "OFFSET_BEGINNING"
)
