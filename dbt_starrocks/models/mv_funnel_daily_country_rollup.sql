{{
  config(
    materialized='materialized_view',
    distributed_by=['day'],
    properties={'query_rewrite_consistency': 'loose'},
    post_hook="REFRESH MATERIALIZED VIEW {{ this }} WITH SYNC MODE;"
  )
}}

-- The post_hook forces a SYNCHRONOUS refresh right after CREATE, so the
-- Dagster job doesn't report success until this MV is actually populated
-- -- see mv_unified_funnel_summary.sql for the full explanation of the
-- CREATE-returns-before-population gap this closes.

-- Daily pre-aggregation over the Databricks cold history table, built
-- specifically to demonstrate StarRocks' transparent query rewrite: a
-- query against the raw base table below, grouped/filtered compatibly
-- with this MV's shape, gets silently redirected here by the optimizer.
--
-- Deliberately NOT built from mv_unified_funnel_summary or hot_funnel_summary
-- -- those involve CURRENT_TIMESTAMP()-relative filters and a UNION across
-- a JDBC source, which StarRocks reports as QUERY_REWRITE_STATUS=INVALID /
-- UNSUPPORTED_DEFINITION (confirmed 2026-09-06). This MV is a plain SPJG
-- rollup with no volatile predicates and a single external-Iceberg source,
-- which is the pattern StarRocks' rewrite optimizer actually supports.
--
-- No refresh_method specified -- dbt-starrocks defaults to MANUAL, which is
-- deliberate: trigger a refresh once before a demo so results stay static
-- and reproducible for the whole session.
SELECT
  date_trunc('day', window_start) AS day,
  country,
  SUM(viewers) AS viewers,
  SUM(carters) AS carters,
  SUM(purchasers) AS purchasers
FROM {{ source('databricks_uc', 'funnel_summary_historical') }}
GROUP BY day, country
