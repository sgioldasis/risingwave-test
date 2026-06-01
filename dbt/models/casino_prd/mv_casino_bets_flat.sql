{{ config(
    materialized='materialized_view',
    tags=['casino_uc1']
) }}

-- First-level flat stream for the UC1 tumbling pipeline (§14 Kafka round-trip).
-- Filters the UC1 bet rows out of the already-unnested mv_casino_transactions and projects
-- only the columns the TUMBLE needs. This is sunk to Kafka and re-ingested with a watermark
-- (sink_casino_bets_flat_kafka -> src_casino_bets_flat) so transaction_created_at can anchor
-- EMIT ON WINDOW CLOSE: UNNEST strips watermarks, so the round-trip re-establishes one.
SELECT
    customer_id,
    currency_id,
    transaction_created_at,
    amount_abs
FROM {{ ref('mv_casino_transactions') }}
WHERE message_type_id = 1
  AND account_id      = 1
  AND amount_abs IS NOT NULL   -- §17: NULL iff Amount was empty/null (amount_raw pruned)
