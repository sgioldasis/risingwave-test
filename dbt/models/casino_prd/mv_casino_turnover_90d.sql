{{ config(
    materialized='materialized_view',
    tags=['casino_uc2']
) }}

SELECT
    customer_id,
    transaction_created_at                                AS event_ts,
    SUM(amount_abs) OVER (
        PARTITION BY customer_id
        ORDER BY transaction_created_at
        -- Demo window: 300s (5 min). Shrunk from 7 days so the rolling-window
        -- state evicts within a short demo run and throughput stays bounded.
        -- See BRAZIL_WORKLOAD_TUNING.md §2 / §13.
        RANGE BETWEEN INTERVAL '300 SECONDS' PRECEDING AND CURRENT ROW
    )                                                     AS rolling_7d_turnover
FROM {{ ref('mv_casino_transactions_full') }}
WHERE message_type_id = 2
  AND account_id      IN (1, 4)
  AND amount_abs IS NOT NULL   -- §17: NULL iff Amount was empty/null (amount_raw pruned)
