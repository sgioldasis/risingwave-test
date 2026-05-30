{{ config(
    materialized='materialized_view',
    tags=['casino_uc1']
) }}

SELECT
    customer_id,
    currency_id,
    event_ts,
    SUM(real_bet_amount) OVER (
        PARTITION BY customer_id, currency_id
        ORDER BY event_ts
        RANGE BETWEEN INTERVAL '1209600 SECONDS' PRECEDING AND CURRENT ROW
    ) AS rolling_14d_real_bet_amount
FROM {{ ref('mv_casino_real_bet_events') }}
