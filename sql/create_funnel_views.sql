-- Create funnel views directly in RisingWave
-- Run this if dbt fails to create the views

-- First, create the source tables if they don't exist
CREATE SOURCE IF NOT EXISTS src_page (
    user_id int,
    page_id varchar,
    event_time timestamp
) WITH (
    connector = 'kafka',
    topic = 'page_views',
    properties.bootstrap.server = 'redpanda:9092',
    scan.startup.mode = 'earliest'
) FORMAT PLAIN ENCODE JSON;

CREATE SOURCE IF NOT EXISTS src_cart (
    user_id int,
    item_id varchar,
    event_time timestamp
) WITH (
    connector = 'kafka',
    topic = 'cart_events',
    properties.bootstrap.server = 'redpanda:9092',
    scan.startup.mode = 'earliest'
) FORMAT PLAIN ENCODE JSON;

CREATE SOURCE IF NOT EXISTS src_purchase (
    user_id int,
    amount decimal,
    event_time timestamp
) WITH (
    connector = 'kafka',
    topic = 'purchases',
    properties.bootstrap.server = 'redpanda:9092',
    scan.startup.mode = 'earliest'
) FORMAT PLAIN ENCODE JSON;

-- Create the funnel materialized view
CREATE MATERIALIZED VIEW IF NOT EXISTS funnel AS
WITH stats AS (
    SELECT
        window_start,
        window_end,
        count(distinct p.user_id) as viewers,
        count(distinct c.user_id) as carters,
        count(distinct pur.user_id) as purchasers
    FROM TUMBLE(src_page, event_time, INTERVAL '1 MINUTE') p
    LEFT JOIN src_cart c 
        ON p.user_id = c.user_id 
        AND c.event_time BETWEEN p.window_start AND p.window_end
    LEFT JOIN src_purchase pur 
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
    case when viewers > 0 then round(carters::numeric / viewers, 2) else 0 end as view_to_cart_rate,
    case when carters > 0 then round(purchasers::numeric / carters, 2) else 0 end as cart_to_buy_rate
FROM stats;

-- Create the funnel_training materialized view with features for ML
CREATE MATERIALIZED VIEW IF NOT EXISTS funnel_training AS
WITH funnel_base AS (
    SELECT
        window_start,
        window_end,
        viewers,
        carters,
        purchasers,
        view_to_cart_rate,
        cart_to_buy_rate
    FROM funnel
),
time_features AS (
    SELECT
        *,
        EXTRACT(HOUR FROM window_start) AS hour_of_day,
        EXTRACT(MINUTE FROM window_start) AS minute_of_hour,
        EXTRACT(DOW FROM window_start) AS day_of_week,
        EXTRACT(EPOCH FROM window_start) / 60 AS minute_sequence
    FROM funnel_base
),
lag_features AS (
    SELECT
        *,
        LAG(viewers, 1) OVER (ORDER BY window_start) AS viewers_lag_1,
        LAG(viewers, 2) OVER (ORDER BY window_start) AS viewers_lag_2,
        LAG(carters, 1) OVER (ORDER BY window_start) AS carters_lag_1,
        LAG(carters, 2) OVER (ORDER BY window_start) AS carters_lag_2,
        LAG(purchasers, 1) OVER (ORDER BY window_start) AS purchasers_lag_1,
        LAG(purchasers, 2) OVER (ORDER BY window_start) AS purchasers_lag_2,
        AVG(viewers) OVER (
            ORDER BY window_start 
            ROWS BETWEEN 4 PRECEDING AND CURRENT ROW
        ) AS viewers_ma_5,
        AVG(carters) OVER (
            ORDER BY window_start 
            ROWS BETWEEN 4 PRECEDING AND CURRENT ROW
        ) AS carters_ma_5
    FROM time_features
)
SELECT
    window_start,
    window_end,
    viewers,
    carters,
    purchasers,
    view_to_cart_rate,
    cart_to_buy_rate,
    hour_of_day,
    minute_of_hour,
    day_of_week,
    minute_sequence,
    viewers_lag_1,
    viewers_lag_2,
    carters_lag_1,
    carters_lag_2,
    purchasers_lag_1,
    purchasers_lag_2,
    viewers_ma_5,
    carters_ma_5,
    CASE 
        WHEN viewers_lag_1 IS NOT NULL AND viewers_lag_1 > 0 
        THEN (viewers - viewers_lag_1)::numeric / viewers_lag_1 
        ELSE 0 
    END AS viewers_trend,
    CASE 
        WHEN carters_lag_1 IS NOT NULL AND carters_lag_1 > 0 
        THEN (carters - carters_lag_1)::numeric / carters_lag_1 
        ELSE 0 
    END AS carters_trend
FROM lag_features;
