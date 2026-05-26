-- =============================================================================
-- RisingWave PROTOBUF + nested-data demo
--
-- Prereqs (handled by bin/3_run_protobuf_demo.sh):
--   1. Redpanda + Schema Registry running (docker-compose).
--   2. Topic `orders` exists.
--   3. Producer has emitted at least one message so the schema is registered
--      under subject `orders-value`.
--
-- Run interactively:
--   psql -h localhost -p 4566 -d dev -U root -f sql/protobuf_demo.sql
-- =============================================================================

DROP SOURCE IF EXISTS src_orders_proto CASCADE;

-- RisingWave fetches the protobuf descriptor from the Schema Registry; no
-- local .proto file or message-name argument needed when using SR.
CREATE SOURCE src_orders_proto
WITH (
    connector = 'kafka',
    topic = 'orders',
    properties.bootstrap.server = 'redpanda:9092',
    scan.startup.mode = 'earliest'
) FORMAT PLAIN ENCODE PROTOBUF (
    schema.registry = 'http://redpanda:8081',
    message = 'demo.Order'
);

-- -----------------------------------------------------------------------------
-- Sanity: top-level + nested struct field access.
-- RisingWave (Postgres dialect) parses `customer.id` as <table>.<column>, so
-- accessing a struct field requires parenthesizing the column: `(customer).id`.
-- -----------------------------------------------------------------------------
\echo '=== top-level + nested customer.address ==='
SELECT
    order_id,
    status,
    (customer).id                  AS customer_id,
    (customer).email               AS customer_email,
    ((customer).address).city      AS city,
    ((customer).address).country   AS country,
    total,
    currency
FROM src_orders_proto
LIMIT 5;

-- -----------------------------------------------------------------------------
-- Repeated nested message -> flatten with UNNEST.
-- RisingWave's UNNEST of a struct[] expands the inner struct fields into
-- separate top-level columns (here: product, quantity, unit_price, discount_pct).
-- -----------------------------------------------------------------------------
\echo ''
\echo '=== exploded line items (UNNEST repeated nested struct) ==='
SELECT
    order_id,
    (product).id        AS sku,
    (product).category  AS category,
    (product).name      AS product_name,
    quantity            AS qty,
    unit_price,
    discount_pct,
    ROUND((quantity * unit_price * (1 - discount_pct / 100.0))::numeric, 2) AS line_total
FROM src_orders_proto, UNNEST(items)
LIMIT 10;

-- -----------------------------------------------------------------------------
-- map<string, string> -> map[] indexing.
-- -----------------------------------------------------------------------------
\echo ''
\echo '=== map<string,string> attributes via map[key] ==='
SELECT
    order_id,
    attributes['source']   AS source_channel,
    attributes['campaign'] AS campaign,
    attributes['tier']     AS vip_tier
FROM src_orders_proto
LIMIT 10;

-- -----------------------------------------------------------------------------
-- `oneof payment` -> RisingWave materializes each variant as a nullable struct
-- column; exactly one is non-null per row.
-- -----------------------------------------------------------------------------
\echo ''
\echo '=== oneof payment variants (only one non-null per row) ==='
SELECT
    order_id,
    CASE
        WHEN card   IS NOT NULL THEN 'card:'   || (card).brand   || ' **** ' || (card).last4
        WHEN wallet IS NOT NULL THEN 'wallet:' || (wallet).provider
        WHEN crypto IS NOT NULL THEN 'crypto:' || (crypto).chain || ' ' || substr((crypto).tx_hash, 1, 12) || '...'
        ELSE 'unknown'
    END AS payment_method,
    total,
    currency
FROM src_orders_proto
LIMIT 10;

-- -----------------------------------------------------------------------------
-- Real-time aggregate: revenue per (country, category) materialized view.
-- Demonstrates nested-field GROUP BY across struct + repeated boundaries.
-- -----------------------------------------------------------------------------
DROP MATERIALIZED VIEW IF EXISTS mv_revenue_by_country_category;

CREATE MATERIALIZED VIEW mv_revenue_by_country_category AS
SELECT
    ((customer).address).country   AS country,
    (product).category             AS category,
    COUNT(DISTINCT order_id)       AS orders,
    SUM(quantity)                  AS units,
    ROUND(
        SUM(quantity * unit_price * (1 - discount_pct / 100.0))::numeric,
        2
    )                              AS revenue
FROM src_orders_proto, UNNEST(items)
WHERE status IN ('ORDER_STATUS_PAID', 'ORDER_STATUS_SHIPPED')
GROUP BY 1, 2;

\echo ''
\echo '=== materialized rollup ==='
SELECT * FROM mv_revenue_by_country_category ORDER BY revenue DESC LIMIT 15;

-- =============================================================================
-- Managed-Iceberg sinks (Lakekeeper REST catalog + MinIO storage)
-- =============================================================================
-- 1. Idempotent connection + session binding.
CREATE CONNECTION IF NOT EXISTS lakekeeper_catalog_conn WITH (
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

-- 2. Flattened orders MV — can't sink directly from a SOURCE in upsert mode.
--    google.protobuf.Timestamp becomes a struct<seconds, nanos> in RW, so we
--    convert it to TIMESTAMPTZ here for the iceberg table.
DROP SINK IF EXISTS rw_managed_proto_orders_sink;
DROP TABLE IF EXISTS rw_managed_proto_orders;
DROP MATERIALIZED VIEW IF EXISTS mv_proto_orders_flat;

CREATE MATERIALIZED VIEW mv_proto_orders_flat AS
SELECT
    order_id,
    status,
    to_timestamp((event_time).seconds + (event_time).nanos / 1e9) AS event_time,
    (customer).id                  AS customer_id,
    (customer).email               AS customer_email,
    ((customer).address).city      AS city,
    ((customer).address).country   AS country,
    total,
    currency,
    CASE
        WHEN card   IS NOT NULL THEN 'card'
        WHEN wallet IS NOT NULL THEN 'wallet'
        WHEN crypto IS NOT NULL THEN 'crypto'
        ELSE 'unknown'
    END                            AS payment_method
FROM src_orders_proto;

-- 3. Managed iceberg table #1 — per-order fact table (upsert by order_id).
CREATE TABLE rw_managed_proto_orders (
    order_id        VARCHAR,
    status          VARCHAR,
    event_time      TIMESTAMPTZ,
    customer_id     BIGINT,
    customer_email  VARCHAR,
    city            VARCHAR,
    country         VARCHAR,
    total           DOUBLE,
    currency        VARCHAR,
    payment_method  VARCHAR,
    PRIMARY KEY (order_id)
) ENGINE = iceberg;

CREATE SINK rw_managed_proto_orders_sink
INTO rw_managed_proto_orders
FROM mv_proto_orders_flat
WITH (
    type = 'upsert',
    primary_key = 'order_id',
    enable_compaction = 'true',
    compaction_interval_sec = '60',
    enable_snapshot_expiration = 'true',
    commit_checkpoint_interval = 5
);

-- 4. Managed iceberg table #2 — (country, category) revenue rollup.
DROP SINK IF EXISTS rw_managed_proto_revenue_sink;
DROP TABLE IF EXISTS rw_managed_proto_revenue;

CREATE TABLE rw_managed_proto_revenue (
    country   VARCHAR,
    category  VARCHAR,
    orders    BIGINT,
    units     BIGINT,
    revenue   NUMERIC,
    PRIMARY KEY (country, category)
) ENGINE = iceberg;

CREATE SINK rw_managed_proto_revenue_sink
INTO rw_managed_proto_revenue
FROM mv_revenue_by_country_category
WITH (
    type = 'upsert',
    primary_key = 'country,category',
    enable_compaction = 'true',
    compaction_interval_sec = '60',
    enable_snapshot_expiration = 'true',
    commit_checkpoint_interval = 5
);

\echo ''
\echo '=== managed iceberg sinks created ==='
SELECT name, connector, sink_type
FROM rw_catalog.rw_sinks
WHERE name IN ('rw_managed_proto_orders_sink', 'rw_managed_proto_revenue_sink');
\echo ''
\echo 'Rows land in the iceberg tables after the first commit (~5s).'
\echo 'The runner waits and prints counts; or query manually later:'
\echo '   SELECT count(*) FROM rw_managed_proto_orders;'
\echo '   SELECT count(*) FROM rw_managed_proto_revenue;'
