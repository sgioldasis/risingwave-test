{{ config(materialized='materialized_view') }}

{#
  Funnel from Tables (Modifiable Data)

  This materialized view aggregates funnel metrics from the tbl_* tables
  instead of src_* sources. Key differences:

  1. Data can be modified via UPDATE/DELETE on the source tables
  2. Changes will cascade to this MV in real-time
  3. Great for demonstrating data correction scenarios

  Comparison Query:
    SELECT
        s.window_start,
        s.viewers as source_viewers,
        t.viewers as table_viewers,
        s.carters as source_carters,
        t.carters as table_carters
    FROM funnel s
    FULL OUTER JOIN hermes_funnel t
        ON s.window_start = t.window_start
    ORDER BY COALESCE(s.window_start, t.window_start) DESC
    LIMIT 10;
#}

WITH interval_10_seconds AS (
    SELECT
        window_start,
        window_end,
        count(distinct p.user_id) as viewers,
        count(distinct c.user_id) as carters,
        count(distinct pur.user_id) as purchasers,
        '10 SECONDS' as time_interval,
        date_trunc('minute', window_start) as minute_start
    FROM TUMBLE({{ ref('hermes_page') }}, event_time, INTERVAL '10 SECONDS') p
    -- Join Cart events
    LEFT JOIN {{ ref('hermes_cart') }} c
        ON p.user_id = c.user_id
        AND c.event_time BETWEEN p.window_start AND p.window_end
    -- Join Purchase events
    LEFT JOIN {{ ref('hermes_purchase') }} pur
        ON p.user_id = pur.user_id
        AND pur.event_time BETWEEN p.window_start AND p.window_end
    GROUP BY window_start, window_end
),
interval_30_seconds AS (
    SELECT
        window_start,
        window_end,
        count(distinct p.user_id) as viewers,
        count(distinct c.user_id) as carters,
        count(distinct pur.user_id) as purchasers,
        '30 SECONDS' as time_interval,
        date_trunc('minute', window_start) as minute_start
    FROM TUMBLE({{ ref('hermes_page') }}, event_time, INTERVAL '30 SECONDS') p
    -- Join Cart events
    LEFT JOIN {{ ref('hermes_cart') }} c
        ON p.user_id = c.user_id
        AND c.event_time BETWEEN p.window_start AND p.window_end
    -- Join Purchase events
    LEFT JOIN {{ ref('hermes_purchase') }} pur
        ON p.user_id = pur.user_id
        AND pur.event_time BETWEEN p.window_start AND p.window_end
    GROUP BY window_start, window_end
),
interval_1_minute AS (
    SELECT
        window_start,
        window_end,
        count(distinct p.user_id) as viewers,
        count(distinct c.user_id) as carters,
        count(distinct pur.user_id) as purchasers,
        '60 SECONDS' as time_interval,
        date_trunc('minute', window_start) as minute_start
    FROM TUMBLE({{ ref('hermes_page') }}, event_time, INTERVAL '1 MINUTE') p
    -- Join Cart events
    LEFT JOIN {{ ref('hermes_cart') }} c
        ON p.user_id = c.user_id
        AND c.event_time BETWEEN p.window_start AND p.window_end
    -- Join Purchase events
    LEFT JOIN {{ ref('hermes_purchase') }} pur
        ON p.user_id = pur.user_id
        AND pur.event_time BETWEEN p.window_start AND p.window_end
    GROUP BY window_start, window_end
),
stats AS (
    SELECT * FROM interval_10_seconds
    UNION ALL
    SELECT * FROM interval_30_seconds
    UNION ALL
    SELECT * FROM interval_1_minute
)

SELECT
    window_start,
    window_end,
    time_interval,
    minute_start,
    viewers,
    carters,
    purchasers,
    -- Use COALESCE with NULLIF instead of CASE for Trino compatibility
    round(coalesce(carters::numeric / nullif(viewers, 0), 0), 2) as view_to_cart_rate,
    round(coalesce(purchasers::numeric / nullif(carters, 0), 0), 2) as cart_to_buy_rate
FROM stats
