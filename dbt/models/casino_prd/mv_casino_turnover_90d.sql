{{ config(
    materialized='materialized_view',
    tags=['casino_uc2']
) }}

SELECT
    customer_id,
    event_ts,
    SUM(turnover) OVER (
        PARTITION BY customer_id
        ORDER BY event_ts
        RANGE BETWEEN INTERVAL '7776000 SECONDS' PRECEDING AND CURRENT ROW
    ) AS rolling_90d_turnover
FROM {{ ref('mv_casino_turnover_events') }}
