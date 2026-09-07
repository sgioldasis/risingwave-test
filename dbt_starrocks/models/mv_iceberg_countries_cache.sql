{{
  config(
    materialized='materialized_view',
    refresh_method='ASYNC EVERY (INTERVAL 60 SECOND)',
    properties={'query_rewrite_consistency': 'loose'}
  )
}}

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
-- now takes up to ~60s to appear instead of being visible on the very next
-- query -- an explicit, accepted trade of instant freshness for query speed
-- on this specific lookup, chosen because the table is tiny and rarely
-- edited outside of demos.
--
-- Widened 10s -> 60s on 2026-09-07 after testing with the producer
-- generating real traffic: each refresh cycle does 3 separate Iceberg
-- snapshot-metadata scans against Databricks (StarRocks's own "does this
-- non-partitioned MV need a refresh" check), each costing 0.5-4s of real
-- network round-trip time regardless of iceberg_meta_cache_ttl_sec (tried
-- raising that from 0 to 8 specifically to reduce this -- no effect,
-- reverted). At a 10s interval this collided with foreground dashboard
-- queries roughly every 10s, causing periodic 2.7-5.5s spikes (see
-- docs/SR_POC_ICEBERG_COUNTRIES_MIGRATION.md). No config fix was found;
-- 60s simply reduces collision frequency 6x. materialized_view_min_refresh_interval
-- stays lowered to 10 in starrocks/docker-entrypoint.sh regardless, in case
-- a future need justifies a faster interval again.
SELECT
  country,
  country_name
FROM {{ source('databricks_uc', 'iceberg_countries') }}
