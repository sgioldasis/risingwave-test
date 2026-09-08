{{
  config(
    materialized='view',
    alias='dashboard_funnel_serving'
  )
}}

-- Zero-copy version (2026-09-08). No local materialized copies of anything
-- Databricks-sourced -- both branches read live at query time:
--   hot:  risingwave.public.funnel_summary directly via JDBC (unchanged --
--         this was already zero-copy before this change)
--   cold: databricks_uc.sr_poc_external.funnel_summary_historical directly
--         via Iceberg REST (previously mv_unified_funnel_summary, a
--         REFRESH MANUAL local copy -- dropped)
--
-- Superseded a chain of caching attempts for the hot branch specifically
-- (10s-refresh MV -> Kafka Routine Load -> native Stream Load sink) that
-- all got reverted; see docs/SR_POC_ICEBERG_COUNTRIES_MIGRATION.md for that
-- history. This zero-copy version is a deliberate, separate decision made
-- after measuring the real cost: a live three-way query (hot JDBC + cold
-- Iceberg + country-name Iceberg join, done in the API layer) settles at a
-- consistent ~2.0-2.1s per query, vs ~500ms-1s with the caching layers this
-- replaces. Traded query speed for zero staleness by construction -- no
-- refresh schedule, no "why didn't my edit show up" question is possible
-- anymore, because nothing is ever cached.
--
-- Partition pruning note: funnel_summary_historical is partitioned on
-- window_date, a plain column (not a hidden day(window_start) transform --
-- Databricks' managed-Iceberg DDL rejected that syntax, see the migration
-- doc's tenth follow-up), and window_date isn't exposed by this view. That
-- means queries against this view can't get true partition-level pruning
-- (which would need an explicit window_date filter) -- but filtering on
-- window_start/window_end alone (what the API layer already does) still
-- gets a real ~5x pruning benefit via per-file Iceberg column statistics
-- (confirmed: 456ms vs 2.38s scan time for the full table). Exposing
-- window_date as an output column for the stronger benefit was considered
-- and skipped for now, to avoid touching the API response shape.
WITH unified_funnel AS (
  SELECT
    window_start,
    MAX(window_end) AS window_end,
    country,
    MAX(viewers) AS viewers,
    MAX(carters) AS carters,
    MAX(purchasers) AS purchasers,
    ROUND(CAST(MAX(carters) AS DOUBLE) / NULLIF(MAX(viewers), 0), 2) AS view_to_cart_rate,
    ROUND(CAST(MAX(purchasers) AS DOUBLE) / NULLIF(MAX(carters), 0), 2) AS cart_to_buy_rate
  FROM {{ source('databricks_uc', 'funnel_summary_historical') }}
  WHERE window_start < DATE_SUB(CURRENT_TIMESTAMP(), INTERVAL 3 MINUTE)
  GROUP BY window_start, country

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
