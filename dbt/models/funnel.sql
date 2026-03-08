{{ config(materialized='materialized_view') }}

WITH stats AS (
    SELECT
        -- Create 20-second tumbling windows for faster prediction updates
        window_start,
        window_end,
        count(distinct p.user_id) as viewers,
        count(distinct c.user_id) as carters,
        count(distinct pur.user_id) as purchasers
    FROM TUMBLE({{ ref('src_page') }}, event_time, INTERVAL '20 SECOND') p
    -- Join Cart events
    LEFT JOIN {{ ref('src_cart') }} c
        ON p.user_id = c.user_id
        AND c.event_time BETWEEN p.window_start AND p.window_end
    -- Join Purchase events
    LEFT JOIN {{ ref('src_purchase') }} pur
        ON p.user_id = pur.user_id
        AND pur.event_time BETWEEN p.window_start AND p.window_end
    GROUP BY window_start, window_end
)

SELECT
    window_start,
    window_end,
    viewers,
    carters,
    purchasers,
    -- Use COALESCE with NULLIF instead of CASE for Trino compatibility
    round(coalesce(carters::numeric / nullif(viewers, 0), 0), 2) as view_to_cart_rate,
    round(coalesce(purchasers::numeric / nullif(carters, 0), 0), 2) as cart_to_buy_rate
FROM stats