{{
  config(
    materialized='view',
    alias='dashboard_funnel_serving'
  )
}}

-- Single dashboard serving surface at (window_start, country) grain. Recent
-- windows are read directly from RisingWave through StarRocks JDBC; older
-- windows come from the deduplicated cold MV.
--
-- Reverted 2026-09-07 to this original, simplest form after a same-day
-- chain of attempts to remove the live JDBC touch: a 10s-refresh local MV
-- mirror (mv_hot_funnel_cache), then a StarRocks Routine Load job off
-- RisingWave's `funnel` Kafka topic (hard 5s minimum batch interval,
-- StarRocks-enforced), then a native RisingWave StarRocks sink (Stream
-- Load-based, no floor, but every-checkpoint flushing overloaded
-- hot_funnel_kafka's compaction and made query latency worse, not better).
-- Each step traded the known, bounded ~500-900ms JDBC planning-time tax
-- (see docs/SR_POC_ICEBERG_COUNTRIES_MIGRATION.md finding #3 -- a StarRocks
-- bug with no config fix) for a different, less predictable failure mode.
-- Reverted rather than continue chasing it -- see that doc's final section
-- for the full history of what was tried and why each step was undone.
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
