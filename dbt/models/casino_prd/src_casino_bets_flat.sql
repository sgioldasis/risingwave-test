{{ config(
    materialized='kafka_table',
    tags=['casino_uc1'],
    topic='casino_bets_flat',
    pre_hook=['DROP TABLE IF EXISTS ' ~ this ~ ' CASCADE']
) }}

-- Round-trip leg 2 (§14): re-ingest the flat bet stream as a TABLE with a real watermark on
-- the now top-level transaction_created_at. No UNNEST downstream -> the watermark survives ->
-- TUMBLE + EMIT ON WINDOW CLOSE works (unlike reading mv_casino_transactions directly, §14).
-- scan.startup.mode='earliest' so the table doesn't miss sink output (our own low-history topic).
CREATE TABLE IF NOT EXISTS {{ this }} (
    customer_id            INT,
    currency_id            INT,
    transaction_created_at TIMESTAMPTZ,
    amount_abs             NUMERIC,
    WATERMARK FOR transaction_created_at AS transaction_created_at - INTERVAL '5' MINUTE
)
APPEND ONLY
WITH (
    connector                   = 'kafka',
    topic                       = 'casino_bets_flat',
    properties.bootstrap.server = 'redpanda:9092',
    scan.startup.mode           = 'earliest'
)
FORMAT PLAIN ENCODE JSON
