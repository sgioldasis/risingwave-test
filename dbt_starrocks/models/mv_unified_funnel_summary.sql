{{
  config(
    materialized='materialized_view',
    refresh_method='ASYNC EVERY (INTERVAL 1 MINUTE)',
    partition_by='window_start',
    properties={'query_rewrite_consistency': 'loose'}
  )
}}

SELECT
    window_start,
    window_end,
    country,
    viewers,
    carters,
    purchasers,
    view_to_cart_rate,
    cart_to_buy_rate
FROM {{ ref('hot_funnel_summary') }}
WHERE window_start >= DATE_SUB(CURRENT_TIMESTAMP(), INTERVAL 1 DAY)

UNION ALL

SELECT
    window_start,
    window_end,
    country,
    viewers,
    carters,
    purchasers,
    view_to_cart_rate,
    cart_to_buy_rate
FROM {{ source('databricks_uc', 'funnel_summary_historical') }}
WHERE window_start < DATE_SUB(CURRENT_TIMESTAMP(), INTERVAL 1 DAY)
