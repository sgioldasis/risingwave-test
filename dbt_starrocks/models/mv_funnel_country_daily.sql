{{
  config(
    materialized='materialized_view',
    distributed_by=['day'],
    properties={'query_rewrite_consistency': 'loose'},
    refresh_method="ASYNC EVERY (INTERVAL 1 MINUTE)",
    post_hook="REFRESH MATERIALIZED VIEW {{ this }} WITH SYNC MODE;"
  )
}}

-- Multi-table MV rewrite demo (2026-11-16): a JOIN materialized view --
-- page_views_direct joined to dim_users, aggregated per day x country.
-- This is one step beyond the single-table rollup the Query Rewrite
-- dashboard demos (mv_funnel_daily_country_rollup): transparent rewrite
-- also fires for queries whose shape matches a JOIN MV, not just plain
-- single-table aggregates.
--
-- The join is semantically real for the demo: the raw topics carry no
-- country, and the funnel views hardcode 'GR'. With the dim, country
-- becomes data (GR is the plurality ~40%, not a constant). dim_users is
-- created and seeded by the funnel_events_direct_kafka asset
-- (orchestration/assets/funnel_direct_kafka_setup.py) -- 1M deterministic
-- synthetic rows covering the producer's whole user_id range.
--
-- Rewrite-eligibility notes, same lessons as mv_funnel_daily_country_rollup:
-- - DATE(event_time), not date_trunc: Superset's sqlglot transpiles
--   date_trunc('day', x) into DATE(x), and rewrite needs the expressions
--   to match exactly.
-- - Internal base tables, so PCT change detection works natively (no
--   partition_refresh_strategy='force' -- that workaround was for the UC
--   REST Iceberg base table).
-- - query_rewrite_consistency='loose' is REQUIRED here (confirmed live
--   2026-11-16): with DEFAULT consistency the optimizer refuses to rewrite
--   while the MV is stale relative to its base tables -- and Routine Load
--   commits to page_views_direct every ~5s, so the MV is essentially
--   always behind and rewrite NEVER fired (EXPLAIN kept scanning the base
--   tables even for the MV's own SQL). 'loose' trades that freshness
--   guarantee for rewrite availability -- same setting as
--   mv_funnel_daily_country_rollup, whose external-Iceberg base table
--   made the need less obvious. Verified: with 'loose', EXPLAIN scans
--   this MV; without, it scans page_views_direct + dim_users.
-- - The 1-minute rebuild scans the 1M-row dim each tick; negligible at
--   demo volume (the dim is static data).
-- Verify rewrite with: EXPLAIN SELECT DATE(p.event_time), u.country,
-- COUNT(*) FROM page_views_direct p JOIN dim_users u ON ... GROUP BY 1, 2
-- -- plan should scan this MV, not the base tables.
SELECT
  DATE(p.event_time) AS day,
  u.country,
  COUNT(*) AS page_views
FROM {{ ref('page_views_direct') }} p
JOIN {{ ref('dim_users') }} u
  ON p.user_id = u.user_id
GROUP BY day, country
