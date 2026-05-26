-- =============================================================================
-- RisingWave PROTOBUF demo — schema from FileDescriptorSet (.pb), NO Schema
-- Registry. Companion to sql/protobuf_demo.sql which uses SR.
--
-- Prereqs (handled by bin/3_run_protobuf_demo_filedesc.sh):
--   1. Redpanda running (docker-compose).
--   2. Topic `orders_filedesc` exists.
--   3. ./proto/events.pb (FileDescriptorSet, --include_imports) exists; the
--      file is bind-mounted into the RisingWave containers at /proto.
--   4. Producer has emitted raw protobuf messages (no SR magic byte) to the
--      topic.
--
-- Run interactively:
--   psql -h localhost -p 4566 -d dev -U root -f sql/protobuf_demo_filedesc.sql
-- =============================================================================

DROP SOURCE IF EXISTS src_orders_proto_filedesc CASCADE;

-- schema.location points at the compiled FileDescriptorSet that was bind-
-- mounted into frontend/meta/compute at /proto/events.pb.
CREATE SOURCE src_orders_proto_filedesc
WITH (
    connector = 'kafka',
    topic = 'orders_filedesc',
    properties.bootstrap.server = 'redpanda:9092',
    scan.startup.mode = 'earliest'
) FORMAT PLAIN ENCODE PROTOBUF (
    schema.location = 'file:///proto/events.pb',
    message = 'demo.Order'
);

\echo '=== top-level + nested customer.address (file-descriptor source) ==='
SELECT
    order_id,
    status,
    (customer).id                  AS customer_id,
    (customer).email               AS customer_email,
    ((customer).address).city      AS city,
    ((customer).address).country   AS country,
    total,
    currency
FROM src_orders_proto_filedesc
LIMIT 5;

\echo ''
\echo '=== exploded line items (UNNEST repeated nested struct) ==='
SELECT
    order_id,
    (product).id        AS sku,
    (product).category  AS category,
    quantity            AS qty,
    unit_price,
    discount_pct
FROM src_orders_proto_filedesc, UNNEST(items)
LIMIT 10;

\echo ''
\echo '=== oneof payment variants (only one non-null per row) ==='
SELECT
    order_id,
    CASE
        WHEN card   IS NOT NULL THEN 'card:'   || (card).brand   || ' **** ' || (card).last4
        WHEN wallet IS NOT NULL THEN 'wallet:' || (wallet).provider
        WHEN crypto IS NOT NULL THEN 'crypto:' || (crypto).chain
        ELSE 'unknown'
    END AS payment_method,
    total,
    currency
FROM src_orders_proto_filedesc
LIMIT 10;

DROP MATERIALIZED VIEW IF EXISTS mv_revenue_by_country_category_filedesc;

CREATE MATERIALIZED VIEW mv_revenue_by_country_category_filedesc AS
SELECT
    ((customer).address).country   AS country,
    (product).category             AS category,
    COUNT(DISTINCT order_id)       AS orders,
    SUM(quantity)                  AS units,
    ROUND(
        SUM(quantity * unit_price * (1 - discount_pct / 100.0))::numeric,
        2
    )                              AS revenue
FROM src_orders_proto_filedesc, UNNEST(items)
WHERE status IN ('ORDER_STATUS_PAID', 'ORDER_STATUS_SHIPPED')
GROUP BY 1, 2;

\echo ''
\echo '=== materialized rollup (file-descriptor source) ==='
SELECT * FROM mv_revenue_by_country_category_filedesc
ORDER BY revenue DESC
LIMIT 15;

-- =============================================================================
-- Managed-Iceberg sinks (Lakekeeper REST catalog + MinIO storage)
-- =============================================================================
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

-- Flattened orders MV — upsert sinks need a primary key in the upstream.
-- google.protobuf.Timestamp becomes struct<seconds, nanos> in RW; convert
-- to TIMESTAMPTZ for the iceberg table.
DROP SINK IF EXISTS rw_managed_proto_fd_orders_sink;
DROP TABLE IF EXISTS rw_managed_proto_fd_orders;
DROP MATERIALIZED VIEW IF EXISTS mv_proto_fd_orders_flat;

CREATE MATERIALIZED VIEW mv_proto_fd_orders_flat AS
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
FROM src_orders_proto_filedesc;

CREATE TABLE rw_managed_proto_fd_orders (
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

CREATE SINK rw_managed_proto_fd_orders_sink
INTO rw_managed_proto_fd_orders
FROM mv_proto_fd_orders_flat
WITH (
    type = 'upsert',
    primary_key = 'order_id',
    enable_compaction = 'true',
    compaction_interval_sec = '60',
    enable_snapshot_expiration = 'true',
    commit_checkpoint_interval = 5
);

DROP SINK IF EXISTS rw_managed_proto_fd_revenue_sink;
DROP TABLE IF EXISTS rw_managed_proto_fd_revenue;

CREATE TABLE rw_managed_proto_fd_revenue (
    country   VARCHAR,
    category  VARCHAR,
    orders    BIGINT,
    units     BIGINT,
    revenue   NUMERIC,
    PRIMARY KEY (country, category)
) ENGINE = iceberg;

CREATE SINK rw_managed_proto_fd_revenue_sink
INTO rw_managed_proto_fd_revenue
FROM mv_revenue_by_country_category_filedesc
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
WHERE name IN ('rw_managed_proto_fd_orders_sink', 'rw_managed_proto_fd_revenue_sink');
\echo ''
\echo 'Rows land in the iceberg tables after the first commit (~5s).'
\echo 'The runner waits and prints counts; or query manually later:'
\echo '   SELECT count(*) FROM rw_managed_proto_fd_orders;'
\echo '   SELECT count(*) FROM rw_managed_proto_fd_revenue;'
