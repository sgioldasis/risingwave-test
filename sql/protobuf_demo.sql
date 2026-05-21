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
