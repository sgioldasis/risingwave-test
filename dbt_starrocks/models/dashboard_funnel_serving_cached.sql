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
--
-- Cutoff (2026-09-10): switched from the original fixed "3 minutes ago"
-- rolling boundary to a self-adjusting one, matching the fix already
-- applied to dashboard_funnel_serving.sql for the exact same reason -- a
-- fixed relative-time cutoff leaves a dead zone whenever RisingWave has
-- any downtime (producer stopped, `bin/6_down.sh`, etc). If RisingWave's
-- hot data is empty or lagging, "3 minutes ago" can fall AFTER the MV's
-- last-refreshed data and BEFORE RisingWave's earliest current row,
-- meaning that window is served by neither branch -- confirmed live: with
-- the MV frozen at a past refresh and the producer having had downtime,
-- data was missing between the two. The self-adjusting version instead
-- uses RisingWave's own current MIN(window_start) as the boundary: the
-- live branch is unconditional (shows everything RisingWave currently
-- has), and the MV branch only contributes rows strictly older than that
-- -- so the two branches always meet with no gap, self-healing after any
-- restart. This does NOT make the MV itself any less stale (it's still
-- only as fresh as its last rebuild) -- it just removes the extra,
-- avoidable gap the fixed 3-minute line used to create on top of that.
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
  WHERE window_start < COALESCE(
    (SELECT MIN(window_start) FROM {{ source('risingwave', 'funnel_summary') }}),
    CAST('9999-12-31 00:00:00' AS DATETIME)
  )

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
)

SELECT *
FROM unified_funnel
