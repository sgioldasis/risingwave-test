# RisingWave Protobuf + Nested Data Demo

End-to-end demo showing how RisingWave ingests Protobuf messages from Kafka
(Redpanda) via Schema Registry and treats deeply nested fields — structs,
repeated messages, maps, and `oneof` — as first-class SQL.

## What gets exercised

The schema in [proto/events.proto](../proto/events.proto) is a single
`demo.Order` message that intentionally uses every nested feature:

| Protobuf construct        | Where in `Order`            | RisingWave SQL access            |
| ------------------------- | --------------------------- | -------------------------------- |
| nested message            | `customer`, `customer.address` | `((customer).address).country` |
| `repeated` nested message | `items[]`                   | `UNNEST(items)` (struct fields become top-level columns) |
| `map<string,string>`      | `attributes`                | `attributes['campaign']`         |
| `oneof`                   | `payment {card\|wallet\|crypto}` | three nullable struct columns |
| `enum`                    | `status`                    | rendered as text (`ORDER_STATUS_PAID`) |
| `google.protobuf.Timestamp` | `event_time`              | `struct<seconds bigint, nanos integer>` |

## Prereqs

The standard stack must be up:

```bash
./bin/1_up.sh
```

That brings up Redpanda (`localhost:19092`), Schema Registry
(`localhost:8081`), the Redpanda Console (`localhost:9090`), and RisingWave
on `localhost:4566`.

## Run it

```bash
./bin/3_run_protobuf_demo.sh
```

Optional knobs:

```bash
COUNT=1000 TPS=50 ./bin/3_run_protobuf_demo.sh
```

What the script does:

1. Creates the `orders` topic in Redpanda if missing.
2. Auto-compiles `proto/events.proto` to `scripts/_pb/events_pb2.py`
   (uses `grpc_tools.protoc`).
3. Runs `scripts/produce_protobuf_orders.py`, which uses Confluent's
   `ProtobufSerializer` to **register the schema under subject
   `orders-value`** and emit nested Order messages.
4. Executes `sql/protobuf_demo.sql` against RisingWave — this creates the
   source, prints the four nested-access query results, and builds a
   materialized view rolling up revenue by `(country, category)`.

## The RisingWave bit

The key DDL is:

```sql
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
```

No column list — RisingWave reads the descriptor straight from Schema
Registry and reflects the full nested type into the catalog. Verify with:

```sql
-- \d hits a non-C-collation pg_catalog query that RW doesn't implement.
-- Use information_schema instead:
SELECT column_name, data_type
FROM information_schema.columns
WHERE table_name = 'src_orders_proto'
ORDER BY ordinal_position;
```

## Validate it yourself

Open `psql` against RisingWave:

```bash
psql -h localhost -p 4566 -d dev -U root
```

### 1. Source row count (grows while producer runs)

```sql
SELECT COUNT(*) AS orders_ingested FROM src_orders_proto;
```

### 2. Top-level + nested struct access

```sql
SELECT
    order_id,
    status,
    (customer).id                  AS customer_id,
    (customer).email               AS email,
    ((customer).address).city      AS city,
    ((customer).address).country   AS country,
    total, currency
FROM src_orders_proto
LIMIT 10;
```

### 3. Filter / group by deeply nested fields

```sql
SELECT
    ((customer).address).country AS country,
    COUNT(*)                     AS orders,
    SUM(total)                   AS gross
FROM src_orders_proto
WHERE (customer).is_vip
GROUP BY 1
ORDER BY gross DESC;
```

### 4. `repeated` LineItem — flatten with UNNEST

RisingWave's `UNNEST(struct[])` expands the inner struct fields into
separate top-level columns (here: `product`, `quantity`, `unit_price`,
`discount_pct`).

```sql
SELECT
    order_id,
    (product).id        AS sku,
    (product).category  AS category,
    quantity,
    unit_price,
    discount_pct,
    ROUND((quantity * unit_price * (1 - discount_pct/100.0))::numeric, 2) AS line_total
FROM src_orders_proto, UNNEST(items)
LIMIT 20;

-- Items per order
SELECT order_id, COUNT(*) AS n_lines, SUM(quantity) AS n_units
FROM src_orders_proto, UNNEST(items)
GROUP BY order_id
ORDER BY n_units DESC
LIMIT 10;

-- Array length without unnesting
SELECT order_id, array_length(items) AS n_lines
FROM src_orders_proto
LIMIT 10;
```

### 5. `map<string,string>` access

```sql
-- Indexed lookups
SELECT order_id,
       attributes['source']   AS channel,
       attributes['campaign'] AS campaign,
       attributes['tier']     AS vip_tier
FROM src_orders_proto
LIMIT 10;

-- Filter by map value
SELECT COUNT(*) FROM src_orders_proto WHERE attributes['campaign'] = 'spring_sale';

-- Map keys / values
SELECT order_id, map_keys(attributes) AS keys, map_values(attributes) AS vals
FROM src_orders_proto LIMIT 5;
```

### 6. `oneof payment` — exactly one variant non-null

```sql
-- Sanity: every row has exactly one payment variant set
SELECT
    SUM((card   IS NOT NULL)::int) AS card_rows,
    SUM((wallet IS NOT NULL)::int) AS wallet_rows,
    SUM((crypto IS NOT NULL)::int) AS crypto_rows,
    SUM(((card IS NOT NULL)::int + (wallet IS NOT NULL)::int + (crypto IS NOT NULL)::int)) AS total_non_null,
    COUNT(*) AS rows
FROM src_orders_proto;

-- Revenue per payment method
SELECT
    CASE WHEN card   IS NOT NULL THEN 'card:'   || (card).brand
         WHEN wallet IS NOT NULL THEN 'wallet:' || (wallet).provider
         WHEN crypto IS NOT NULL THEN 'crypto:' || (crypto).chain
    END AS method,
    COUNT(*) AS n,
    ROUND(SUM(total)::numeric, 2) AS revenue
FROM src_orders_proto
GROUP BY 1
ORDER BY revenue DESC;
```

### 7. Enum + `google.protobuf.Timestamp`

```sql
-- Enum is exposed as text
SELECT status, COUNT(*) FROM src_orders_proto GROUP BY 1 ORDER BY 2 DESC;

-- Timestamp comes through as struct<seconds bigint, nanos integer>; convert it:
SELECT
    order_id,
    to_timestamp((event_time).seconds + (event_time).nanos / 1e9) AS event_ts
FROM src_orders_proto
ORDER BY event_ts DESC
LIMIT 5;
```

### 8. Streaming MV — confirm it updates live

```sql
SELECT * FROM mv_revenue_by_country_category ORDER BY revenue DESC LIMIT 10;
```

In another terminal:

```bash
uv run python scripts/produce_protobuf_orders.py --count 200 --tps 50
```

Re-run the `SELECT` — `orders`, `units`, `revenue` will increment without
re-running the MV definition.

### 9. Internal catalog peek (RisingWave-specific)

```sql
-- All your sources / MVs in this DB
SELECT name, owner FROM rw_catalog.rw_sources WHERE name = 'src_orders_proto';
SELECT name FROM rw_catalog.rw_materialized_views WHERE name LIKE 'mv_%';

-- Throughput stats for the source
SELECT * FROM rw_catalog.rw_source_throughput
WHERE source_name = 'src_orders_proto';
```

### 10. Schema-evolution sanity (optional)

After adding a new optional field to `proto/events.proto` and re-running
the producer:

```sql
DROP SOURCE src_orders_proto CASCADE;
\i sql/protobuf_demo.sql
SELECT column_name FROM information_schema.columns WHERE table_name='src_orders_proto';
```

The new field appears automatically — RisingWave re-pulls the descriptor
from the Schema Registry.

> Tip: if any psql output line-wraps awkwardly, run
> `\pset format wrapped` or `\pset format unaligned`.

## Talking points for the demo

- **Schema-on-read for protobuf is real**: zero column DDL, full struct
  reflection. Compare with `src_purchase.sql` (JSON) which lists every
  column by hand.
- **Nested access via parenthesized struct**: `((customer).address).country`
  works in `SELECT`, `WHERE`, and `GROUP BY` (Postgres dialect — bare
  `customer.id` would be parsed as `table.column`). See
  `mv_revenue_by_country_category`.
- **`repeated` is an array of structs**: use `UNNEST` to fan out, then
  treat each row like a regular join input. The MV groups across the
  unnested boundary.
- **`map<K,V>` is a real SQL map**: `attributes['campaign']` returns
  `NULL` for missing keys, no JSON path gymnastics.
- **`oneof` becomes N nullable columns**, exactly one set per row — the
  payment-method `CASE` shows the idiomatic pattern.
- **Schema evolution**: add a new optional field to `events.proto`, bump
  the producer, re-register — existing queries keep working, new field
  becomes available on the source automatically (re-create source to pick
  it up).

## Inspect the registered schema

Open <http://localhost:9090> (Redpanda Console) → **Schema Registry** →
subject `orders-value` to see the `.proto` definition that RisingWave is
pulling from.

## Cleanup

```bash
psql -h localhost -p 4566 -d dev -U root -c \
    "DROP MATERIALIZED VIEW IF EXISTS mv_revenue_by_country_category;
     DROP SOURCE IF EXISTS src_orders_proto CASCADE;"
docker exec redpanda rpk topic delete orders
```
