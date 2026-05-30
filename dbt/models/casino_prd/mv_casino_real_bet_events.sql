{{ config(
    materialized='materialized_view',
    tags=['casino_uc1']
) }}

SELECT
    customer_id,
    transaction_id,
    currency_id,
    game_id,
    game_type,
    is_live,
    company_id,
    amount_abs                AS real_bet_amount,
    transaction_created_at    AS event_ts
FROM {{ ref('mv_casino_transactions') }}
WHERE message_type_id = 1
  AND account_id      = 1
  AND amount_raw IS NOT NULL
  AND amount_raw <> ''
