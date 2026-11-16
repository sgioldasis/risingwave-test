{{
  config(
    materialized='materialized_view',
    distributed_by=['day'],
    refresh_method="ASYNC EVERY (INTERVAL 1 MINUTE)",
    post_hook="REFRESH MATERIALIZED VIEW {{ this }} WITH SYNC MODE;"
  )
}}

-- Approximate-aggregates demo, storage side (2026-11-16): daily distinct
-- viewers of the direct-Kafka hot path, carried as MERGEABLE SKETCHES.
--
-- Why sketches: COUNT(DISTINCT user_id) cannot be rolled up -- the distinct
-- count of a day is not the sum of its minutes, and recomputing it means
-- rescanning every raw event. BITMAP and HLL columns store a compressed
-- summary of the distinct set that CAN be merged, so this MV pre-aggregates
-- once and every later query merges fixed-size sketches instead of scanning
-- page_views_direct.
--
--   BITMAP_UNION(to_bitmap(user_id))  -- exact distinct, memory ~ cardinality
--   HLL_UNION(hll_hash(user_id))      -- ~1% error, fixed few-KB sketch
--
-- Query side (funnel_distinct_comparison): BITMAP_UNION_COUNT(viewers_bitmap)
-- and HLL_UNION_AGG(viewers_hll). BITMAP agrees with exact COUNT DISTINCT to
-- the digit (it IS exact); HLL lands within ~1% -- that agreement pair is
-- the demo's punchline.
--
-- Base tables are INTERNAL (Routine Load feeds page_views_direct), so PCT
-- change detection works natively here -- no partition_refresh_strategy=
-- "force" needed (that workaround was for the UC REST Iceberg base table
-- whose change detection broke, see mv_funnel_daily_country_rollup.sql).
-- The post_hook forces a synchronous first refresh so the dbt/Dagster step
-- doesn't report success against an empty MV.
SELECT
  DATE(event_time) AS day,
  BITMAP_UNION(to_bitmap(user_id)) AS viewers_bitmap,
  HLL_UNION(hll_hash(user_id))     AS viewers_hll
FROM {{ source('sr_local_db', 'page_views_direct') }}
GROUP BY day
