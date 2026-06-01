{{ config(
    materialized='materialized_view',
    tags=['casino_uc1']
) }}

-- UC1 — decoupled rolling window (the chosen config; see BRAZIL_WORKLOAD_TUNING.md §15).
-- Reads the re-ingested src_casino_bets_flat (Kafka round-trip), NOT mv_casino_transactions.
-- The Kafka buffer takes this window off src_casino_prd's backpressure path (the decoupling that
-- lifts throughput ~120/s -> ~300/s). Continuous emit-on-update rolling sum (no TUMBLE/EOWC latency).
-- src_casino_bets_flat is already filtered to UC1 bets, so no WHERE needed here.
SELECT
    customer_id,
    currency_id,
    transaction_created_at                                AS event_ts,
    SUM(amount_abs) OVER (
        PARTITION BY customer_id, currency_id
        ORDER BY transaction_created_at
        -- 5-min demo window (see §2/§13). Column kept as rolling_1d_real_bet_amount for
        -- dashboard/Trino-view compatibility (historical misnomer).
        RANGE BETWEEN INTERVAL '300 SECONDS' PRECEDING AND CURRENT ROW
    )                                                     AS rolling_1d_real_bet_amount
FROM {{ ref('src_casino_bets_flat') }}
