{{
  config(
    materialized='table',
    distributed_by=['user_id']
  )
}}

-- Demo dimension table: 1M synthetic users with a fixed country
-- distribution (GR plurality ~40%, then RO, CY, PT, ES, IT). Migrated from
-- orchestration/assets/funnel_direct_kafka_setup.py (2026-11-17) because
-- dbt models read it (mv_funnel_country_daily joins it) and on a fresh
-- cluster dbt now builds before the Dagster asset that used to create it
-- -- the asset's old `WHERE (SELECT COUNT(*) ...) = 0` guard is gone
-- since dbt's table materialization recreates deterministically. Also
-- read by the bloom-filter benchmark (bin/5_starrocks_bloom_filter_
-- benchmark.sh) and the serving-features demo (country_scores) by name.

SELECT
    t.generate_series AS user_id,
    CASE t.generate_series % 10
        WHEN 0 THEN 'GR' WHEN 1 THEN 'GR' WHEN 2 THEN 'GR' WHEN 3 THEN 'GR'
        WHEN 4 THEN 'RO' WHEN 5 THEN 'RO'
        WHEN 6 THEN 'CY' WHEN 7 THEN 'PT' WHEN 8 THEN 'ES' ELSE 'IT'
    END AS country
FROM TABLE(generate_series(1, 1000000)) t
