{{ config(
    materialized='materialized_view',
    tags=['casino_uc2']
) }}

SELECT customer_id, sportsbook_turnover, event_ts
FROM (
    SELECT
        customer_id,
        rolling_7d_turnover                                                      AS sportsbook_turnover,
        event_ts,
        ROW_NUMBER() OVER (PARTITION BY customer_id ORDER BY event_ts DESC)      AS rn
    FROM {{ ref('mv_sportsbook_turnover_90d') }}
) t
WHERE rn = 1
