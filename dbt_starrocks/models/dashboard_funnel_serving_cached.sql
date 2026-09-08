{{
  config(
    materialized='view',
    alias='dashboard_funnel_serving_cached'
  )
}}

-- Created 2026-09-08 as the "cached" side of a side-by-side demo comparing
-- both architectures this project tried, alongside the current default
-- (zero-copy) dashboard_funnel_serving.sql. This is that file's pre-zero-copy
-- form, restored verbatim under a new name -- not a new design, a snapshot
-- of a real prior state kept alive on purpose. See
-- docs/SR_POC_ICEBERG_COUNTRIES_MIGRATION.md's twelfth follow-up for why
-- the project moved off this design as its default, and the "Demo: compare
-- both architectures" section for how to use both side by side.
--
-- Single dashboard serving surface at (window_start, country) grain. Recent
-- windows are read directly from RisingWave through StarRocks JDBC; older
-- windows come from the deduplicated cold MV (mv_unified_funnel_summary,
-- REFRESH MANUAL -- run
-- `REFRESH MATERIALIZED VIEW sr_local_db_sr_local_db.mv_unified_funnel_summary WITH SYNC MODE;`
-- to pick up new archived data; no Dagster asset wraps this one, unlike
-- the retired starrocks_mv_warm).
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
