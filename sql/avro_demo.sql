-- =============================================================================
-- RisingWave AVRO + nested-data demo
--
-- Sister demo to sql/protobuf_demo.sql — same logical payload, Avro on the
-- wire. Demonstrates the same nested-access patterns (struct, array, map,
-- union-as-struct) so you can compare encodings side by side.
--
-- Prereqs (handled by bin/3_run_avro_demo.sh):
--   1. Redpanda + Schema Registry running (docker-compose).
--   2. Topic `orders_avro` exists.
--   3. Producer has emitted at least one message so the schema is registered
--      under subject `orders_avro-value`.
--
-- Run interactively:
--   psql -h localhost -p 4566 -d dev -U root -f sql/avro_demo.sql
-- =============================================================================

DROP SOURCE IF EXISTS src_orders_avro CASCADE;

CREATE SOURCE src_orders_avro
WITH (
    connector = 'kafka',
    topic = 'orders_avro',
    properties.bootstrap.server = 'redpanda:9092',
    scan.startup.mode = 'earliest'
) FORMAT PLAIN ENCODE AVRO (
    schema.registry = 'http://redpanda:8081'
);

-- -----------------------------------------------------------------------------
-- Sanity: top-level + nested struct field access.
-- Same parenthesization rule as protobuf: `(customer).id`, not `customer.id`.
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
FROM src_orders_avro
LIMIT 5;

-- -----------------------------------------------------------------------------
-- Avro array<record> -> UNNEST. Inner record fields are projected as
-- separate top-level columns, identical to RisingWave's protobuf behaviour.
-- -----------------------------------------------------------------------------
\echo ''
\echo '=== exploded line items (UNNEST array<record>) ==='
SELECT
    order_id,
    (product).id        AS sku,
    (product).category  AS category,
    (product).name      AS product_name,
    quantity            AS qty,
    unit_price,
    discount_pct,
    ROUND((quantity * unit_price * (1 - discount_pct / 100.0))::numeric, 2) AS line_total
FROM src_orders_avro, UNNEST(items)
LIMIT 10;

-- -----------------------------------------------------------------------------
-- Avro map<string, string> -> RisingWave map[key].
-- -----------------------------------------------------------------------------
\echo ''
\echo '=== map<string,string> attributes via map[key] ==='
SELECT
    order_id,
    attributes['source']   AS source_channel,
    attributes['campaign'] AS campaign,
    attributes['tier']     AS vip_tier
FROM src_orders_avro
LIMIT 10;

-- -----------------------------------------------------------------------------
-- Payment variants: a `Payment` record with three nullable sub-records (the
-- workaround for RisingWave's not-yet-implemented Avro union<record> support,
-- issue #17632). Exactly one sub-field is non-null per row — same shape as
-- the protobuf oneof demo.
-- -----------------------------------------------------------------------------
\echo ''
\echo '=== payment variants (only one non-null per row) ==='
SELECT
    order_id,
    CASE
        WHEN (payment).card   IS NOT NULL
            THEN 'card:'   || ((payment).card).brand   || ' **** ' || ((payment).card).last4
        WHEN (payment).wallet IS NOT NULL
            THEN 'wallet:' || ((payment).wallet).provider
        WHEN (payment).crypto IS NOT NULL
            THEN 'crypto:' || ((payment).crypto).chain || ' ' || substr(((payment).crypto).tx_hash, 1, 12) || '...'
        ELSE 'unknown'
    END AS payment_method,
    total,
    currency
FROM src_orders_avro
LIMIT 10;

-- -----------------------------------------------------------------------------
-- Real-time aggregate: revenue per (country, category) materialized view.
-- Same shape as the protobuf demo — proves the SQL is encoding-agnostic.
-- -----------------------------------------------------------------------------
DROP MATERIALIZED VIEW IF EXISTS mv_avro_revenue_by_country_category;

CREATE MATERIALIZED VIEW mv_avro_revenue_by_country_category AS
SELECT
    ((customer).address).country   AS country,
    (product).category             AS category,
    COUNT(DISTINCT order_id)       AS orders,
    SUM(quantity)                  AS units,
    ROUND(
        SUM(quantity * unit_price * (1 - discount_pct / 100.0))::numeric,
        2
    )                              AS revenue
FROM src_orders_avro, UNNEST(items)
WHERE status IN ('ORDER_STATUS_PAID', 'ORDER_STATUS_SHIPPED')
GROUP BY 1, 2;

\echo ''
\echo '=== materialized rollup ==='
SELECT * FROM mv_avro_revenue_by_country_category ORDER BY revenue DESC LIMIT 15;

-- =============================================================================
-- Sink the demo into RisingWave-managed Iceberg tables.
--
-- This section creates two `ENGINE = iceberg` tables in the Lakekeeper
-- catalog (MinIO storage) and continuously streams the Avro pipeline into
-- them. After the first commit (~60s) the same tables are queryable from
-- DuckDB / Spark / Trino via the 🦆 / 🔥 / 🧊 tiles.
--
-- For the API contract / gotchas see /memories/repo/risingwave-iceberg-sink.md.
-- =============================================================================

-- 1. Iceberg catalog connection (idempotent).
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

-- Tells the iceberg engine which catalog to use for `ENGINE = iceberg` tables.
SET iceberg_engine_connection = 'public.lakekeeper_catalog_conn';

-- 2. Flattened orders MV — can't sink directly from a SOURCE in upsert mode.
DROP SINK IF EXISTS rw_managed_avro_orders_sink;
DROP TABLE IF EXISTS rw_managed_avro_orders;
DROP MATERIALIZED VIEW IF EXISTS mv_avro_orders_flat;

CREATE MATERIALIZED VIEW mv_avro_orders_flat AS
SELECT
    order_id,
    status,
    event_time,
    (customer).id                  AS customer_id,
    (customer).email               AS customer_email,
    ((customer).address).city      AS city,
    ((customer).address).country   AS country,
    total,
    currency,
    CASE
        WHEN (payment).card   IS NOT NULL THEN 'card'
        WHEN (payment).wallet IS NOT NULL THEN 'wallet'
        WHEN (payment).crypto IS NOT NULL THEN 'crypto'
        ELSE 'unknown'
    END                            AS payment_method
FROM src_orders_avro;

-- 3. Managed iceberg table #1 — per-order fact table (upsert by order_id).
CREATE TABLE rw_managed_avro_orders (
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

-- When sinking INTO a managed iceberg table, RW infers
-- connector/connection/database/table from the target. Only `type` +
-- `primary_key` (+ maintenance opts) belong here.
CREATE SINK rw_managed_avro_orders_sink
INTO rw_managed_avro_orders
FROM mv_avro_orders_flat
WITH (
    type = 'upsert',
    primary_key = 'order_id',
    enable_compaction = 'true',
    compaction_interval_sec = '60',
    enable_snapshot_expiration = 'true',
    commit_checkpoint_interval = 5
);

-- 4. Managed iceberg table #2 — (country, category) revenue rollup.
DROP SINK IF EXISTS rw_managed_avro_revenue_sink;
DROP TABLE IF EXISTS rw_managed_avro_revenue;

CREATE TABLE rw_managed_avro_revenue (
    country   VARCHAR,
    category  VARCHAR,
    orders    BIGINT,
    units     BIGINT,
    revenue   NUMERIC,
    PRIMARY KEY (country, category)
) ENGINE = iceberg;

CREATE SINK rw_managed_avro_revenue_sink
INTO rw_managed_avro_revenue
FROM mv_avro_revenue_by_country_category
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
WHERE name IN ('rw_managed_avro_orders_sink', 'rw_managed_avro_revenue_sink');
\echo ''
\echo 'Rows land in the iceberg tables after the first commit (~5s).'
\echo 'The runner waits and prints counts; or query manually later:'
\echo '   SELECT count(*) FROM rw_managed_avro_orders;'
\echo '   SELECT count(*) FROM rw_managed_avro_revenue;'
