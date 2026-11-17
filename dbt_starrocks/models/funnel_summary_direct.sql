{{
  config(
    materialized='view'
  )
}}

-- RW-free 1-minute funnel aggregation over the Routine-Load-fed raw tables
-- (migrated from orchestration/assets/funnel_direct_kafka_setup.py,
-- 2026-11-17, together with the table models it reads). Same window
-- semantics as dbt/models/funnel_summary.sql (1-minute tumbling windows,
-- distinct users per window, country hardcoded 'GR'), computed over the
-- direct-Kafka tables instead of RisingWave's sources. A LEFT JOIN chain
-- over per-window distinct user sets, identical shape.
--
-- The views over the now-dbt-managed tables live in dbt because they are
-- derived relations (the same dividing line that puts the tables here);
-- being dbt models also gives this dashboard-facing chain its ordering:
-- tables (ref) -> this view -> dashboard_funnel_serving_cached_direct
-- (ref) -- which replaced the Dagster-side cross-asset dep that used to
-- exist for exactly this purpose and, once the tables became dbt models
-- too, formed a dependency cycle (fresh-stack failure 2026-11-17: dbt
-- built the reading model before the asset had created the view).

WITH p AS (
    SELECT date_trunc('minute', event_time) AS window_start, user_id
    FROM {{ ref('page_views_direct') }} GROUP BY 1, 2
),
c AS (
    SELECT date_trunc('minute', event_time) AS window_start, user_id
    FROM {{ ref('cart_events_direct') }} GROUP BY 1, 2
),
pur AS (
    SELECT date_trunc('minute', event_time) AS window_start, user_id
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
    date_add(window_start, INTERVAL 1 MINUTE) AS window_end,
    'GR' AS country,
    viewers,
    carters,
    purchasers,
    round(coalesce(carters / nullif(viewers, 0), 0), 2)   AS view_to_cart_rate,
    round(coalesce(purchasers / nullif(carters, 0), 0), 2) AS cart_to_buy_rate
FROM stats
