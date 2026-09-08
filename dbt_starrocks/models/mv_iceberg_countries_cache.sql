{{
  config(
    materialized='materialized_view',
    refresh_method='ASYNC EVERY (INTERVAL 20 SECOND)',
    properties={'query_rewrite_consistency': 'loose'}
  )
}}

-- Restored 2026-09-08 (was dropped as part of the zero-copy migration --
-- see docs/SR_POC_ICEBERG_COUNTRIES_MIGRATION.md's twelfth follow-up) to
-- support a side-by-side demo of both architectures. Feeds
-- dashboard_funnel_serving_cached, a second view alongside the current
-- default (zero-copy) dashboard_funnel_serving, so both can be queried and
-- compared directly via /api/query/funnel (live) vs
-- /api/query/funnel/cached (this).
--
-- Local mirror of the tiny Databricks iceberg_countries reference table.
-- Dashboard queries used to LEFT JOIN databricks_uc.sr_poc_external.iceberg_countries
-- live, on every request -- confirmed via EXPLAIN ANALYZE (2026-09-07) that
-- every query touching an external catalog during planning pays a large
-- (500ms-1.5s+) StarRocks CBO stats-cache-lookup tax (see
-- docs/SR_POC_ICEBERG_COUNTRIES_MIGRATION.md), and this join was hitting that
-- cost *in addition to* the same cost already paid for the risingwave JDBC
-- catalog, roughly doubling total query time (~2.1-2.3s observed).
--
-- Mirroring the table locally and joining against this instead removes the
-- external-catalog touch from the hot query path entirely (join becomes
-- local-to-local). Trade-off: a Databricks-side edit (e.g. a country rename)
-- now takes up to ~20s to appear instead of being visible on the very next
-- query -- an explicit, accepted trade of instant freshness for query speed
-- on this specific lookup, chosen because the table is tiny and rarely
-- edited outside of demos.
--
-- History: 10s (2026-09-07) -> each refresh cycle does 3 separate Iceberg
-- snapshot-metadata scans against Databricks (StarRocks's own "does this
-- non-partitioned MV need a refresh" check), each costing 0.5-4s of real
-- network round-trip time regardless of iceberg_meta_cache_ttl_sec (tried
-- raising that from 0 to 8 specifically to reduce this -- no effect,
-- reverted); this collided with foreground dashboard queries roughly every
-- 10s, causing periodic 2.7-5.5s spikes (see
-- docs/SR_POC_ICEBERG_COUNTRIES_MIGRATION.md) -> widened to 60s, then back
-- down to 20s (same day) as a middle ground -- confirmed via a 30-run
-- timing test spanning ~1-2 refresh cycles: max 1.03s, mostly 440-980ms,
-- nowhere near the 2.7-5.5s spikes seen at 10s. If similar spikes ever
-- reappear at 20s under heavier load, that's the same root cause, and 60s
-- is the last known-good fallback. materialized_view_min_refresh_interval
-- stays
-- lowered to 10 in starrocks/docker-entrypoint.sh regardless, in case a
-- future need justifies a faster interval again.
SELECT
  country,
  country_name
FROM {{ source('databricks_uc', 'iceberg_countries') }}
