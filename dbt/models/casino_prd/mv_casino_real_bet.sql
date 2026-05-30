{{ config(
    materialized='materialized_view',
    tags=['casino_uc1']
) }}

SELECT
    customer_id,
    currency_id,
    transaction_created_at                                AS event_ts,
    SUM(amount_abs) OVER (
        PARTITION BY customer_id, currency_id
        ORDER BY transaction_created_at
        RANGE BETWEEN INTERVAL '1209600 SECONDS' PRECEDING AND CURRENT ROW
    )                                                     AS rolling_14d_real_bet_amount
FROM {{ ref('mv_casino_transactions') }}
WHERE message_type_id = 1
  AND account_id      = 1
  AND amount_raw IS NOT NULL
  AND amount_raw <> ''
