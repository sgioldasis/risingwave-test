{{ config(materialized='materialized_view') }}

-- 1-minute tumbling windows on each source independently, then joined
-- by (window_start, user_id). EMIT ON WINDOW CLOSE finalises rows once
-- watermarks pass window_end so downstream sinks see immutable rows.

WITH p AS (
    SELECT window_start, window_end, user_id
    FROM TUMBLE({{ ref('src_page') }}, event_time, INTERVAL '1 MINUTE')
    GROUP BY window_start, window_end, user_id
),
c AS (
    SELECT window_start, window_end, user_id
    FROM TUMBLE({{ ref('src_cart') }}, event_time, INTERVAL '1 MINUTE')
    GROUP BY window_start, window_end, user_id
),
pur AS (
    SELECT window_start, window_end, user_id
    FROM TUMBLE({{ ref('src_purchase') }}, event_time, INTERVAL '1 MINUTE')
    GROUP BY window_start, window_end, user_id
),
stats AS (
    SELECT
        p.window_start,
        p.window_end,
        count(distinct p.user_id)   as viewers,
        count(distinct c.user_id)   as carters,
        count(distinct pur.user_id) as purchasers
    FROM p
    LEFT JOIN c
        ON p.window_start = c.window_start
       AND p.user_id     = c.user_id
    LEFT JOIN pur
        ON p.window_start = pur.window_start
       AND p.user_id     = pur.user_id
    GROUP BY p.window_start, p.window_end
)

SELECT
    window_start,
    window_end,
    'GR'::varchar as country,
    viewers,
    carters,
    purchasers,
    round(coalesce(carters::numeric    / nullif(viewers, 0), 0), 2) as view_to_cart_rate,
    round(coalesce(purchasers::numeric / nullif(carters, 0), 0), 2) as cart_to_buy_rate
FROM stats
