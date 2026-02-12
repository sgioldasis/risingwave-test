-- ==========================================================
-- AUTO-GENERATED: Combined dbt models for RisingWave
-- All Jinja2 templates resolved - ready to run via psql
-- ==========================================================

-- Reduce barrier interval for faster processing
ALTER SYSTEM SET barrier_interval_ms = '250';

-- ==========================================================
-- STEP 1: Create Iceberg Connection
-- ==========================================================

CREATE CONNECTION IF NOT EXISTS lakekeeper_catalog_conn
WITH (
    type = 'iceberg',
    catalog.type = 'rest',
    catalog.uri = 'http://lakekeeper:8181/catalog/',
    warehouse.path = 'risingwave-warehouse',
    s3.access.key = 'hummockadmin',
    s3.secret.key = 'hummockadmin',
    s3.path.style.access = 'true',
    s3.endpoint = 'http://minio-0:9301',
    s3.region = 'us-east-1'
);

SET iceberg_engine_connection = 'public.lakekeeper_catalog_conn';

-- ==========================================================
-- STEP 2: Create Kafka Sources
-- ==========================================================

-- ----- Model: src_page -----
CREATE SOURCE src_page (
    user_id int,
    page_id varchar,
    event_time timestamp
) WITH (
    connector = 'kafka',
    topic = 'page_views',
    properties.bootstrap.server = 'redpanda:9092',
    scan.startup.mode = 'latest'
) FORMAT PLAIN ENCODE JSON;

-- ----- Model: src_cart -----
CREATE SOURCE src_cart (
    user_id int,
    item_id varchar,
    event_time timestamp
) WITH (
    connector = 'kafka',
    topic = 'cart_events',
    properties.bootstrap.server = 'redpanda:9092',
    scan.startup.mode = 'latest'
) FORMAT PLAIN ENCODE JSON;

-- ----- Model: src_purchase -----
CREATE SOURCE src_purchase (
    user_id int,
    amount numeric,
    event_time timestamp
) WITH (
    connector = 'kafka',
    topic = 'purchases',
    properties.bootstrap.server = 'redpanda:9092',
    scan.startup.mode = 'latest'
) FORMAT PLAIN ENCODE JSON;

-- ==========================================================
-- STEP 3: Create Materialized View (Funnel Analysis)
-- ==========================================================

-- ----- Model: funnel -----
CREATE MATERIALIZED VIEW funnel AS
WITH stats AS (
    SELECT
        -- Create 1-minute tumbling windows
        window_start,
        window_end,
        count(distinct p.user_id) as viewers,
        count(distinct c.user_id) as carters,
        count(distinct pur.user_id) as purchasers
    FROM TUMBLE(src_page, event_time, INTERVAL '1 MINUTE') p
    -- Join Cart events
    LEFT JOIN src_cart c
        ON p.user_id = c.user_id
        AND c.event_time BETWEEN p.window_start AND p.window_end
    -- Join Purchase events
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
    -- Calculate live conversion rates
    case when viewers > 0 then round(carters::numeric / viewers, 2) else 0 end as view_to_cart_rate,
    case when carters > 0 then round(purchasers::numeric / carters, 2) else 0 end as cart_to_buy_rate
FROM stats;

-- ==========================================================
-- STEP 4: Create Iceberg Tables
-- ==========================================================

-- ----- Model: iceberg_cart_events -----
CREATE TABLE IF NOT EXISTS iceberg_cart_events (
    user_id INT,
    item_id VARCHAR,
    event_time TIMESTAMP
) ENGINE = iceberg;

-- ----- Model: iceberg_page_views -----
CREATE TABLE IF NOT EXISTS iceberg_page_views (
    user_id INT,
    page_id VARCHAR,
    event_time TIMESTAMP
) ENGINE = iceberg;

-- ----- Model: iceberg_purchases -----
CREATE TABLE IF NOT EXISTS iceberg_purchases (
    user_id INT,
    amount NUMERIC,
    event_time TIMESTAMP
) ENGINE = iceberg;

-- ==========================================================
-- STEP 5: Create Sinks to Iceberg
-- ==========================================================

-- ----- Model: sink_cart_events_to_iceberg -----
CREATE SINK IF NOT EXISTS iceberg_cart_events_sink
INTO iceberg_cart_events
FROM src_cart
WITH (
    type = 'append-only',
    commit_checkpoint_interval = 1,
    sink_decouple = false
);

-- ----- Model: sink_page_views_to_iceberg -----
CREATE SINK IF NOT EXISTS iceberg_page_views_sink
INTO iceberg_page_views
FROM src_page
WITH (
    type = 'append-only',
    commit_checkpoint_interval = 1,
    sink_decouple = false
);

-- ----- Model: sink_purchases_to_iceberg -----
CREATE SINK IF NOT EXISTS iceberg_purchases_sink
INTO iceberg_purchases
FROM src_purchase
WITH (
    type = 'append-only',
    commit_checkpoint_interval = 1,
    sink_decouple = false
);

-- ==========================================================
-- END OF AUTO-GENERATED SCRIPT
-- ==========================================================
