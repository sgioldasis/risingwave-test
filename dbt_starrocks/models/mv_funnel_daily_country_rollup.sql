{{
  config(
    materialized='materialized_view',
    distributed_by=['day'],
    properties={'query_rewrite_consistency': 'loose'},
    refresh_method="ASYNC EVERY (INTERVAL 5 MINUTE)",
    post_hook="REFRESH MATERIALIZED VIEW {{ this }} WITH SYNC MODE;"
  )
}}

-- The post_hook forces a SYNCHRONOUS refresh right after CREATE, so the
-- Dagster job doesn't report success until this MV is actually populated
-- -- see mv_unified_funnel_summary.sql for the full explanation of the
-- CREATE-returns-before-population gap this closes. Confirmed live
-- (2026-09-14) that combining this with `refresh_method` below causes no
-- conflict -- the explicit sync refresh and the periodic schedule
-- coexist fine.
--
-- refresh_method="ASYNC EVERY (INTERVAL 1 MINUTE)" (2026-09-14): changed
-- from the previous MANUAL default after live use showed "Rewrite ON"
-- (this MV) and "Rewrite OFF" (the raw table) drifting apart over a long
-- session -- the raw scan always reflects current Databricks state, but
-- this MV was frozen at whatever it looked like when last refreshed by
-- hand, so a long-running demo could show the two diverging in row count,
-- not just latency, which read as a bug rather than the intended
-- rewrite-vs-raw contrast. 1 minute roughly matches
-- sink_funnel_to_databricks's own ~60s commit cadence
-- (commit_checkpoint_interval = 15, ~4s checkpoints in this project's
-- tuned config) -- refreshing much more often would just re-scan the
-- external Iceberg table before it could possibly have new data.
-- Confirmed live: a scheduled refresh both populates immediately on
-- CREATE (no need to wait for the first interval tick) and picks up
-- newly-landed base-table rows automatically on the next tick, no manual
-- REFRESH needed for either.
--
-- Lengthened to 5 minutes (still 2026-09-14, same day): the refresh
-- itself is a full, non-incremental `INSERT OVERWRITE` (this MV has one
-- unpartitioned partition, so StarRocks' partition-change-tracking always
-- rebuilds the whole thing) and, confirmed via profiling, that task
-- consistently takes 20-46s regardless of resource-group CPU weight
-- (tried 1%, 20%, 80% -- no meaningful difference) even though the
-- equivalent SELECT alone takes ~3s and an equivalent INSERT OVERWRITE
-- into a plain table takes ~6s. The extra cost is specific to StarRocks'
-- MV-refresh task machinery (locking/coordination, not CPU or Iceberg
-- metadata size -- both ruled out separately). At a 1-minute schedule
-- that meant a third to most of every window was spent in a slow
-- refresh, during which "Rewrite ON" queries could be caught behind it
-- and run as slow as "Rewrite OFF". 5 minutes keeps the periodic-refresh
-- benefit (no manual step, no long-session drift) while cutting how often
-- a live query can land inside that window.
--
-- Tradeoff, stated explicitly: this MV is no longer static for a whole
-- demo session the way MANUAL was -- numbers can now shift between two
-- queries several minutes apart. Accepted deliberately in exchange for
-- "Rewrite ON" staying in sync with "Rewrite OFF" without a manual step.

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
SELECT
  date_trunc('day', window_start) AS day,
  country,
  SUM(viewers) AS viewers,
  SUM(carters) AS carters,
  SUM(purchasers) AS purchasers
FROM {{ source('databricks_uc', 'funnel_summary_historical') }}
GROUP BY day, country
