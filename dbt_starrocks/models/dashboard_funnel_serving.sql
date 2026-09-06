{{
  config(
    materialized='view',
    alias='dashboard_funnel_serving'
  )
}}

-- Single dashboard serving surface at (window_start, country) grain. Recent
-- windows are read directly from RisingWave through StarRocks JDBC; older
-- windows come from the deduplicated cold MV.
WITH unified_funnel AS (
  SELECT
    window_start,
    window_end,
    country,
    viewers,
    carters,
    purchasers,
    view_to_cart_rate,
    cart_to_buy_rate
  FROM {{ ref('mv_unified_funnel_summary') }}
  WHERE window_start < DATE_SUB(CURRENT_TIMESTAMP(), INTERVAL 3 MINUTE)

  UNION ALL

  SELECT
    window_start,
    window_end,
    country,
    viewers,
    carters,
    purchasers,
    CAST(carters AS DOUBLE) / NULLIF(viewers, 0) AS view_to_cart_rate,
    CAST(purchasers AS DOUBLE) / NULLIF(carters, 0) AS cart_to_buy_rate
  FROM {{ source('risingwave', 'funnel_summary') }}
  WHERE window_start >= DATE_SUB(CURRENT_TIMESTAMP(), INTERVAL 3 MINUTE)
)

SELECT *
FROM unified_funnel