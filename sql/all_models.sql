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
-- STEP 2: Create Kafka Sources (Original Funnel)
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
    amount DOUBLE,
    event_time timestamp
) WITH (
    connector = 'kafka',
    topic = 'purchases',
    properties.bootstrap.server = 'redpanda:9092',
    scan.startup.mode = 'latest'
) FORMAT PLAIN ENCODE JSON;

-- ==========================================================
-- STEP 3: Create Rich Clickstream Tables (Direct Producer)
-- ==========================================================

-- ----- Model: users -----
CREATE TABLE users (
    user_id BIGINT,
    full_name VARCHAR,
    email VARCHAR,
    country VARCHAR,
    signup_time VARCHAR,
    marketing_opt_in BOOLEAN,
    ingested_at VARCHAR
);

-- ----- Model: sessions -----
CREATE TABLE sessions (
    session_id VARCHAR,
    user_id BIGINT,
    device_id VARCHAR,
    session_start VARCHAR,
    ip_address VARCHAR,
    geo_city VARCHAR,
    geo_region VARCHAR,
    ingested_at VARCHAR
);

-- ----- Model: devices -----
CREATE TABLE devices (
    device_id VARCHAR,
    device_type VARCHAR,
    os VARCHAR,
    browser VARCHAR,
    user_agent VARCHAR,
    ingested_at VARCHAR
);

-- ----- Model: campaigns -----
CREATE TABLE campaigns (
    campaign_id VARCHAR,
    source VARCHAR,
    medium VARCHAR,
    campaign VARCHAR,
    content VARCHAR,
    term VARCHAR,
    ingested_at VARCHAR
);

-- ----- Model: page_catalog -----
CREATE TABLE page_catalog (
    page_url VARCHAR,
    page_category VARCHAR,
    product_id VARCHAR,
    product_category VARCHAR,
    ingested_at VARCHAR
);

-- ----- Model: clickstream_events -----
CREATE TABLE clickstream_events (
    event_id VARCHAR,
    user_id BIGINT,
    session_id VARCHAR,
    event_type VARCHAR,
    page_url VARCHAR,
    element_id VARCHAR,
    event_time VARCHAR,
    referrer VARCHAR,
    campaign_id VARCHAR,
    revenue_usd DOUBLE PRECISION,
    ingested_at VARCHAR
);

-- ==========================================================
-- STEP 4: Create Materialized View (Funnel Analysis)
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
-- STEP 5: Create Original Iceberg Tables (Funnel)
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
    amount DOUBLE,
    event_time TIMESTAMP
) ENGINE = iceberg;

-- ==========================================================
-- STEP 6: Create New Clickstream Iceberg Tables
-- ==========================================================

-- ----- Model: iceberg_users -----
CREATE TABLE IF NOT EXISTS iceberg_users (
    user_id BIGINT,
    full_name VARCHAR,
    email VARCHAR,
    country VARCHAR,
    signup_time VARCHAR,
    marketing_opt_in BOOLEAN,
    ingested_at VARCHAR
) ENGINE = iceberg;

-- ----- Model: iceberg_sessions -----
CREATE TABLE IF NOT EXISTS iceberg_sessions (
    session_id VARCHAR,
    user_id BIGINT,
    device_id VARCHAR,
    session_start VARCHAR,
    ip_address VARCHAR,
    geo_city VARCHAR,
    geo_region VARCHAR,
    ingested_at VARCHAR
) ENGINE = iceberg;

-- ----- Model: iceberg_devices -----
CREATE TABLE IF NOT EXISTS iceberg_devices (
    device_id VARCHAR,
    device_type VARCHAR,
    os VARCHAR,
    browser VARCHAR,
    user_agent VARCHAR,
    ingested_at VARCHAR
) ENGINE = iceberg;

-- ----- Model: iceberg_campaigns -----
CREATE TABLE IF NOT EXISTS iceberg_campaigns (
    campaign_id VARCHAR,
    source VARCHAR,
    medium VARCHAR,
    campaign VARCHAR,
    content VARCHAR,
    term VARCHAR,
    ingested_at VARCHAR
) ENGINE = iceberg;

-- ----- Model: iceberg_page_catalog -----
CREATE TABLE IF NOT EXISTS iceberg_page_catalog (
    page_url VARCHAR,
    page_category VARCHAR,
    product_id VARCHAR,
    product_category VARCHAR,
    ingested_at VARCHAR
) ENGINE = iceberg;

-- ----- Model: iceberg_clickstream_events -----
CREATE TABLE IF NOT EXISTS iceberg_clickstream_events (
    event_id VARCHAR,
    user_id BIGINT,
    session_id VARCHAR,
    event_type VARCHAR,
    page_url VARCHAR,
    element_id VARCHAR,
    event_time VARCHAR,
    referrer VARCHAR,
    campaign_id VARCHAR,
    revenue_usd DOUBLE PRECISION,
    ingested_at VARCHAR
) ENGINE = iceberg;

-- ==========================================================
-- STEP 7: Create Original Sinks to Iceberg (Funnel)
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
-- STEP 8: Create New Clickstream Sinks to Iceberg
-- ==========================================================

-- ----- Model: sink_users_to_iceberg -----
CREATE SINK IF NOT EXISTS iceberg_users_sink
INTO iceberg_users
FROM users
WITH (
    type = 'append-only',
    force_append_only = 'true',
    commit_checkpoint_interval = 1,
    sink_decouple = false
);

-- ----- Model: sink_sessions_to_iceberg -----
CREATE SINK IF NOT EXISTS iceberg_sessions_sink
INTO iceberg_sessions
FROM sessions
WITH (
    type = 'append-only',
    force_append_only = 'true',
    commit_checkpoint_interval = 1,
    sink_decouple = false
);

-- ----- Model: sink_devices_to_iceberg -----
CREATE SINK IF NOT EXISTS iceberg_devices_sink
INTO iceberg_devices
FROM devices
WITH (
    type = 'append-only',
    force_append_only = 'true',
    commit_checkpoint_interval = 1,
    sink_decouple = false
);

-- ----- Model: sink_campaigns_to_iceberg -----
CREATE SINK IF NOT EXISTS iceberg_campaigns_sink
INTO iceberg_campaigns
FROM campaigns
WITH (
    type = 'append-only',
    force_append_only = 'true',
    commit_checkpoint_interval = 1,
    sink_decouple = false
);

-- ----- Model: sink_page_catalog_to_iceberg -----
CREATE SINK IF NOT EXISTS iceberg_page_catalog_sink
INTO iceberg_page_catalog
FROM page_catalog
WITH (
    type = 'append-only',
    force_append_only = 'true',
    commit_checkpoint_interval = 1,
    sink_decouple = false
);

-- ----- Model: sink_clickstream_to_iceberg -----
CREATE SINK IF NOT EXISTS iceberg_clickstream_events_sink
INTO iceberg_clickstream_events
FROM clickstream_events
WITH (
    type = 'append-only',
    force_append_only = 'true',
    commit_checkpoint_interval = 1,
    sink_decouple = false
);

-- ==========================================================
-- END OF AUTO-GENERATED SCRIPT
-- ==========================================================
