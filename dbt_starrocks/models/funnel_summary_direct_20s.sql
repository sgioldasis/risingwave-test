{{
  config(
    materialized='view'
  )
}}

-- 20-second-grain twin of funnel_summary_direct (time_slice, not
-- date_trunc -- date_trunc bottoms out at minute; migrated from
-- orchestration/assets/funnel_direct_kafka_setup.py, 2026-11-17). The
-- original RW-fed funnel topic pushed 20-second windows, so the dashboard
-- chart and the online learner were built for that cadence; at minute
-- grain both visibly slowed down (2026-11-14: new chart point and new
-- training record once a minute instead of three times). The RW-free
-- serving view's hot branch (dashboard_funnel_serving_cached_direct)
-- reads the MINUTE view; the cadence-sensitive consumer of THIS one is
-- the RW-free online learner (ml/serving/kafka_river_predictor.py via
-- ML_RECENT_WINDOWS_TABLE), which keeps its 3x/minute cadence.

WITH p AS (
    SELECT time_slice(event_time, INTERVAL 20 SECOND) AS window_start, user_id
    FROM {{ ref('page_views_direct') }} GROUP BY 1, 2
),
c AS (
    SELECT time_slice(event_time, INTERVAL 20 SECOND) AS window_start, user_id
    FROM {{ ref('cart_events_direct') }} GROUP BY 1, 2
),
pur AS (
    SELECT time_slice(event_time, INTERVAL 20 SECOND) AS window_start, user_id
    FROM {{ ref('purchases_direct') }} GROUP BY 1, 2
),
stats AS (
    SELECT
        p.window_start,
        count(DISTINCT p.user_id)   AS viewers,
        count(DISTINCT c.user_id)   AS carters,
        count(DISTINCT pur.user_id) AS purchasers
    FROM p
    LEFT JOIN c   ON p.window_start = c.window_start   AND p.user_id = c.user_id
    LEFT JOIN pur ON p.window_start = pur.window_start AND p.user_id = pur.user_id
    GROUP BY p.window_start
)
SELECT
    window_start,
    date_add(window_start, INTERVAL 20 SECOND) AS window_end,
    'GR' AS country,
    viewers,
    carters,
    purchasers,
    round(coalesce(carters / nullif(viewers, 0), 0), 2)   AS view_to_cart_rate,
    round(coalesce(purchasers / nullif(carters, 0), 0), 2) AS cart_to_buy_rate
FROM stats
