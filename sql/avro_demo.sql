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
