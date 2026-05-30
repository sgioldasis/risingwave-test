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
        RANGE BETWEEN INTERVAL '7776000 SECONDS' PRECEDING AND CURRENT ROW
    )                                                     AS rolling_90d_turnover
FROM {{ ref('mv_casino_transactions') }}
WHERE message_type_id = 2
  AND account_id      IN (1, 4)
  AND amount_raw IS NOT NULL
  AND amount_raw <> ''
