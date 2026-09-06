{{
  config(
    materialized='materialized_view',
    refresh_method='ASYNC EVERY (INTERVAL 5 MINUTE)',
    distributed_by=['window_start'],
    properties={'query_rewrite_consistency': 'loose'}
  )
}}

-- Federated demo view: RisingWave owns the newest three minutes while Databricks
-- UC owns older windows. The append-only cold source can contain multiple
-- snapshots for one window, so the cold branch collapses those snapshots first.
--
-- Partitioning this MV (e.g. by window_start) was attempted and rejected by
-- StarRocks: "Materialized view partition column in partition exp must be
-- base table partition column" -- neither base table (hot_funnel_summary via
-- JDBC, funnel_summary_historical as an unpartitioned external Iceberg table)
-- is itself partitioned, so partition-level incremental refresh isn't
-- available here without first partitioning those base tables. Until that's
-- done, this stays unpartitioned and relies on a longer refresh interval
-- (5 min, widened from 1 min) to bound full-rebuild frequency -- cold data is
-- immutable past the 3-minute hot boundary and doesn't need minute-level
-- freshness; the dashboard already reads the live 3-minute hot window
-- directly from RisingWave regardless of MV freshness.

WITH cold_deduplicated AS (
  SELECT
    window_start,
    MAX(window_end) AS window_end,
    country,
    MAX(viewers) AS viewers,
    MAX(carters) AS carters,
    MAX(purchasers) AS purchasers
  FROM {{ source('databricks_uc', 'funnel_summary_historical') }}
  WHERE window_start < DATE_SUB(CURRENT_TIMESTAMP(), INTERVAL 3 MINUTE)
  GROUP BY window_start, country
)

SELECT
    window_start,
    window_end,
    country,
    viewers,
    carters,
    purchasers,
  ROUND(CAST(carters AS DOUBLE) / NULLIF(viewers, 0), 2) AS view_to_cart_rate,
  ROUND(CAST(purchasers AS DOUBLE) / NULLIF(carters, 0), 2) AS cart_to_buy_rate
FROM cold_deduplicated

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
FROM {{ ref('hot_funnel_summary') }}
WHERE window_start >= DATE_SUB(CURRENT_TIMESTAMP(), INTERVAL 3 MINUTE)

