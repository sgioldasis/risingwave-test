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
--
-- Hot/cold cutoff: NOT a calendar date. Cold (Databricks) serves anything
-- strictly older than RisingWave's own oldest currently-retained row; hot
-- (RisingWave) serves everything it currently has, unconditionally.
--
-- History: started as a rolling 3-minute cutoff, changed 2026-09-10 to a
-- CURRENT_DATE cutoff after confirming sink_funnel_to_databricks commits in
-- batches (Iceberg sinks don't flush per-row), so a row less than ~3
-- minutes old that hadn't landed in Databricks yet was invisible to this
-- view entirely once it aged past the hot window -- observed live as the
-- funnel's total count visibly *decreasing* over time even under steady
-- traffic. CURRENT_DATE fixed that specific gap, but had its own: RisingWave's
-- own storage is wiped by `bin/6_down.sh` (`docker compose down --volumes`),
-- so a mid-day restart loses that day's pre-restart data from RisingWave --
-- and a calendar-date cutoff would then hide it from Databricks too, even
-- though the sink had already replicated it there, until midnight rolled
-- the cutoff over. Fixed by tracking RisingWave's *actual* retained range
-- instead of a calendar boundary -- this self-heals after any restart,
-- partial-day or full, as long as the sink has kept up (confirmed working
-- as of 2026-09-10, see docs/SR_POC_ICEBERG_COUNTRIES_MIGRATION.md for the
-- catalogManaged root cause that had silently broken it before that).
--
-- Caught by the restart test itself: the naive `SELECT MIN(window_start)
-- FROM {{ source('risingwave', 'funnel_summary') }}` (or even a plain
-- `ORDER BY window_start ASC/DESC LIMIT 1`, no aggregate) intermittently
-- returned `2000-01-01` instead of the real minimum -- confirmed via direct
-- comparison against RisingWave itself (correct) vs. through StarRocks's
-- JDBC catalog (wrong), and NOT specific to MIN()/ORDER-BY-LIMIT shapes --
-- plain row counts through the same catalog fluctuated too. Root-caused to
-- StarRocks CN's native JDBC scanner intermittently misdecoding
-- RisingWave's Postgres binary-protocol timestamps as 2000-01-01 00:00:00
-- (Postgres's own internal epoch -- zeroed/stale native buffer memory, not
-- a StarRocks default), reproducing deterministically after a small fixed
-- number of queries per StarRocks CN process lifetime. An earlier revision
-- of this file added `WHERE viewers >= 0` here as a workaround, on the
-- mistaken theory that it forced a real scan past a stats-cache shortcut --
-- that was never the actual mechanism (confirmed via EXPLAIN: this was
-- always a real scan) and did not reliably prevent the corruption. The
-- real fix is at the catalog level: `binaryTransfer=false` on the
-- `risingwave` JDBC catalog's connection string (starrocks/init_catalog.sh)
-- forces the Postgres JDBC driver to text protocol, bypassing the binary
-- decode path entirely -- confirmed clean across 100+ repeated queries.
-- Upgrading StarRocks 4.0.14 -> 4.1.4 was tried first and did NOT fix this
-- (a different, real bug in that version range was fixed by the upgrade --
-- JDBC-catalog row-count instability -- but not this one). See
-- docs/SR_POC_ICEBERG_COUNTRIES_MIGRATION.md for the full investigation.
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
  WHERE window_start < COALESCE(
    (SELECT MIN(window_start) FROM {{ source('risingwave', 'funnel_summary') }}),
    CAST('9999-12-31 00:00:00' AS DATETIME)
  )
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
)

SELECT *
FROM unified_funnel
