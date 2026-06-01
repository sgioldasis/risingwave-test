{{ config(
    materialized='sink',
    tags=['casino_uc1']
) }}

-- Round-trip leg 1 (§14): publish the flat UC1 bet stream to Redpanda so it can be
-- re-ingested as a watermarked table (src_casino_bets_flat). force_append_only because
-- mv_casino_bets_flat is append-only (one row per bet event).
CREATE SINK IF NOT EXISTS sink_casino_bets_flat_kafka
FROM {{ ref('mv_casino_bets_flat') }}
WITH (
    connector                     = 'kafka',
    properties.bootstrap.server   = 'redpanda:9092',
    topic                         = 'casino_bets_flat'
)
FORMAT PLAIN ENCODE JSON (
    force_append_only = 'true'
)
