{{ config(
    materialized='materialized_view',
    tags=['casino_uc2']
) }}

SELECT
    customer_id,
    currency_id,
    amount_abs               AS turnover,
    transaction_created_at   AS event_ts
FROM {{ ref('mv_casino_transactions') }}
WHERE message_type_id = 2
  AND account_id      IN (1, 4)
  AND amount_raw IS NOT NULL
  AND amount_raw <> ''
