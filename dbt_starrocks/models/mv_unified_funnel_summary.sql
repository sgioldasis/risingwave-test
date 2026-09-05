{{
  config(
    materialized='materialized_view',
    refresh_method='ASYNC EVERY (INTERVAL 5 MINUTE)',
    distributed_by=['window_start'],
    properties={'query_rewrite_consistency': 'loose'}
  )
}}

-- PILOT B: Cold-path-only unified view (hot path deferred pending JDBC catalog fix)
-- Reads historical funnel summary from Databricks Unity Catalog Managed Iceberg table.
-- 
-- Once the RisingWave JDBC external catalog is working, this view will be updated to
-- UNION ALL the hot path (most recent windows from risingwave.public.funnel_summary)
-- with the cold path below.

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

