{{
  config(materialized='view')
}}

-- Approximate-aggregates demo, query side (2026-11-16): one row, three
-- ways to count the SAME distinct viewers of the direct-Kafka hot path,
-- all read from the same raw vintage so the only differences are the
-- counting mechanisms themselves:
--
--   viewers_exact  -- COUNT(DISTINCT user_id): the baseline exact count
--   viewers_bitmap -- BITMAP_UNION_COUNT(to_bitmap(user_id)): EXACT like
--                     COUNT DISTINCT (bitmap = the exact-distinct sketch),
--                     agree with viewers_exact to the digit
--   viewers_hll    -- HLL_UNION_AGG(hll_hash(user_id)): within ~1-2% of
--                     exact at a fixed few-KB sketch per value
--
-- Why sketch at all when the exact scan is milliseconds at demo volume?
-- The scaling story lives in mv_funnel_distinct_daily: COUNT(DISTINCT)
-- cannot be pre-aggregated and rolled up (distinct of a day != sum of its
-- minutes), while HLL_UNION/BITMAP_UNION columns in that MV merge
-- fixed-size sketches per day -- the mechanism that keeps distinct
-- counting O(1) in sketch size as event volume grows. The Superset
-- markdown explains the pair.
SELECT
  e.viewers_exact,
  b.viewers_bitmap,
  h.viewers_hll,
  ROUND(ABS(h.viewers_hll - e.viewers_exact) / NULLIF(e.viewers_exact, 0) * 100, 2)
    AS hll_error_pct
FROM (
  SELECT COUNT(DISTINCT user_id) AS viewers_exact
  FROM {{ ref('page_views_direct') }}
) e
CROSS JOIN (
  SELECT BITMAP_UNION_COUNT(to_bitmap(user_id)) AS viewers_bitmap
  FROM {{ ref('page_views_direct') }}
) b
CROSS JOIN (
  SELECT HLL_UNION_AGG(hll_hash(user_id)) AS viewers_hll
  FROM {{ ref('page_views_direct') }}
) h
