{{
  config(
    materialized='materialized_view',
    refresh_method='ASYNC EVERY (INTERVAL 10 SECOND)',
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
-- now takes up to ~10s to appear instead of being visible on the very next
-- query -- an explicit, accepted trade of instant freshness for query speed
-- on this specific lookup, chosen because the table is tiny and rarely
-- edited outside of demos. 10s requires lowering the FE's
-- materialized_view_min_refresh_interval config from its 60s default (see
-- starrocks/docker-entrypoint.sh) -- that config is global, so it also
-- lowers the floor for any other async MV added to this project later.
SELECT
  country,
  country_name
FROM {{ source('databricks_uc', 'iceberg_countries') }}
