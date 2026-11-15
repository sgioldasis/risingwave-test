{{
  config(
    materialized='materialized_view',
    distributed_by=['day'],
    properties={'query_rewrite_consistency': 'loose',
                'partition_refresh_strategy': 'force'},
    refresh_method="ASYNC EVERY (INTERVAL 1 MINUTE)",
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
--
-- partition_refresh_strategy="force" (2026-11-15): the scheduled refresh
-- stopped picking up new rows -- every 5-minute tick reported SUCCESS with
-- refreshMode=PCT and basePartitionsToRefreshMap={} (zero partitions
-- selected), so "Rewrite ON" froze at its last full refresh while the raw
-- scan kept moving. PCT had worked when this comment's 2026-09-14 notes
-- were written; something upstream changed (table recreated RW-style,
-- Databricks-side snapshot churn) and change detection now never fires
-- for this UC REST Iceberg base table. The "force" strategy bypasses PCT:
-- every scheduled tick does a full rebuild (same 20-46s cost as before --
-- the no-op ticks were already spending that on metadata walks). Applied
-- live via ALTER MATERIALIZED VIEW ... SET and verified: next tick ran
-- with forceRefresh=true and the MV jumped to current. This config entry
-- makes it survive rebuilds. (SUBMIT TASK with a REFRESH body is NOT an
-- alternative -- the task-body grammar only accepts INSERT/CREATE/
-- EXPLAIN, and CREATE TASK/CREATE JOB don't parse at all on 4.1.7.)
--
-- Back to 1 minute (2026-11-15, same day, user request): with the force
-- strategy above the scheduled refresh is useful every tick instead of a
-- no-op, so the 5-minute spacing is no longer buying much -- the original
-- 1-minute concern (live queries landing inside the 20-46s rebuild) is
-- re-accepted deliberately: "Rewrite ON" may briefly run at raw-scan
-- speed while a refresh is in flight, and the chart can be up to ~2
-- minutes behind the base table (schedule interval + rebuild time).

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
  -- DATE(window_start), NOT date_trunc('day', window_start): Superset runs
  -- every dataset SQL through sqlglot with the MySQL dialect before sending
  -- it, and sqlglot transpiles date_trunc('day', x) into DATE(x) -- so the
  -- SQL that actually reaches StarRocks from the Rewrite-demo charts uses
  -- DATE(), whatever the dataset YAML says. StarRocks' rewrite requires the
  -- query's day expression to match the MV's exactly (CAST(x AS DATE) is
  -- not unified either -- tested 2026-11-13), so with a date_trunc MV the
  -- "Rewrite ON" chart silently scanned raw Iceberg exactly like the "OFF"
  -- chart (confirmed via fe.audit.log: identical ScanBytes/ScanRows, 6-7s
  -- both). Building the MV on DATE() makes the transpiled chart SQL
  -- rewrite-eligible. Verified via a throwaway MV before this change.
  DATE(window_start) AS day,
  country,
  SUM(viewers) AS viewers,
  SUM(carters) AS carters,
  SUM(purchasers) AS purchasers
FROM {{ source('databricks_uc', 'funnel_summary_historical') }}
GROUP BY day, country
