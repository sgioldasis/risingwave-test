{{
  config(
    materialized='materialized_view',
    refresh_method='MANUAL',
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
-- done, this stays unpartitioned; a full rebuild costs 13s-117s depending on
-- Databricks-side latency (confirmed 2026-09-07 -- see
-- docs/SR_POC_ICEBERG_COUNTRIES_MIGRATION.md), which is exactly why this is
-- MANUAL rather than a scheduled ASYNC EVERY interval: a periodic full
-- rebuild that size was found to overlap itself and compete with every
-- foreground dashboard query on the same FE, not just its own runtime. The
-- dashboard's hot path (dashboard_funnel_serving) doesn't depend on this MV
-- being fresh at all -- it reads risingwave.public.funnel_summary live via
-- JDBC for the current 3-minute window (see that model's own comment for
-- the full history of what else was tried there and why it was reverted
-- back to this). This MV only needs to run once per stack startup (already handled
-- by the starrocks_mv_warm Dagster asset in the standard setup job) to pick
-- up whatever's accumulated in Databricks since the last run, plus on
-- demand (re-run that same asset, or
-- `REFRESH MATERIALIZED VIEW sr_local_db_sr_local_db.mv_unified_funnel_summary WITH SYNC MODE;`)
-- if demonstrating the live hot-to-cold aging transition with the producer
-- running.

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

