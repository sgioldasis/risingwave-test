{{
  config(
    materialized='table',
    distributed_by=['window_start']
  )
}}

-- TABLE, not VIEW (2026-11-14): as a view over risingwave.public.funnel_summary
-- (JDBC catalog), this relation sat in mv_unified_funnel_summary's base-table
-- chain. That MV is INVALID for rewrite (UNION + volatile predicates), but
-- StarRocks' rewrite PREPARATION still resolves every candidate MV's base
-- tables -- so any query touching funnel_summary_historical (e.g. the Query
-- Rewrite demo's "Rewrite ON" chart, which matches BOTH that MV and
-- mv_funnel_daily_country_rollup) needed a live RisingWave JDBC connection
-- just to plan. With RW unreachable the prep threw "base-table dropped:
-- funnel_summary" (fe.log), rewrite was abandoned entirely, and the ON chart
-- fell back to a raw Iceberg scan -- breaking the rewrite demo's "no
-- streaming layer in the loop" claim. As a local TABLE, the MV's base tables
-- (funnel_summary_historical + this) resolve without RisingWave, and the
-- Rewrite demo is genuinely RW-independent.
--
-- Freshness trade-off: the MV's hot branch now freezes at the last dbt build
-- of this table (each starrocks_demo_setup_job run), not at MV-refresh time.
-- In practice both happen in the same setup run; the RW-free cached view
-- (dashboard_funnel_serving_cached_direct) overlays newer windows via its
-- direct branch, so no chart loses coverage.

SELECT
  CAST(window_start AS DATETIME) AS window_start,
  CAST(window_end AS DATETIME) AS window_end,
  country,
  viewers,
  carters,
  purchasers,
  CAST(view_to_cart_rate AS DOUBLE) AS view_to_cart_rate,
  CAST(cart_to_buy_rate AS DOUBLE) AS cart_to_buy_rate
FROM {{ source('risingwave', 'funnel_summary') }}
