# Protobuf File-Descriptor Demo — Step-by-Step Walkthrough

End-to-end walkthrough for the **FileDescriptorSet (`.pb`) variant** of the
RisingWave protobuf demo. Same nested `demo.Order` payloads as the Schema
Registry demo, but RisingWave learns the schema from a compiled descriptor
file mounted into the containers at `/proto/events.pb` — no Schema Registry
involved.

If you want the SR variant instead, see
[PROTOBUF_NESTED_DEMO.md](PROTOBUF_NESTED_DEMO.md).

## What you'll do

1. Start the stack.
2. Launch the demo from the script-runner UI.
3. Run the validation queries in psql.
4. Evolve the protobuf schema (add a field + an enum value), refresh the
   source in place, and verify with new queries.

---

## 0. Prereqs

The compose stack must be up:

```bash
./bin/1_up.sh
```

That brings up Redpanda (`localhost:19092`), Redpanda Console
(`localhost:9090`), and RisingWave on `localhost:4566`.

> The compose file bind-mounts `./proto` read-only into
> `frontend-node-0`, `compute-node-0`, and `meta-node-0` at `/proto`. If
> your stack predates that change the runner will fail fast and tell you
> to recreate those three services:
>
> ```bash
> docker compose up -d --force-recreate frontend-node-0 compute-node-0 meta-node-0
> ```

---

## 1. Run the demo from the script runner

Start the runner (idempotent — kills any prior instance):

```bash
./bin/0_script_runner.sh
```

Open the UI: <http://localhost:4001>

Click **🧬 Protobuf Demo (FileDescriptorSet)**. That tile is wired to
[bin/3_run_protobuf_demo_filedesc.sh](../bin/3_run_protobuf_demo_filedesc.sh)
which performs five steps:

| Step | What happens |
| ---- | ------------ |
| 1    | Verifies `/proto` is mounted inside `frontend-node-0`, `compute-node-0`, `meta-node-0`. |
| 2    | Creates Kafka topic `orders_filedesc` if missing. |
| 3    | Runs [scripts/produce_protobuf_orders_filedesc.py](../scripts/produce_protobuf_orders_filedesc.py) — compiles `proto/events.proto` to both `events_pb2.py` (Python) and `proto/events.pb` (FileDescriptorSet with `--include_imports`), then sends raw protobuf bytes to Kafka. **No** Confluent magic-byte prefix. |
| 4    | Applies [sql/protobuf_demo_filedesc.sql](../sql/protobuf_demo_filedesc.sql) against RisingWave — creates `src_orders_proto_filedesc`, `mv_revenue_by_country_category_filedesc`, a flattening MV `mv_proto_fd_orders_flat`, two `ENGINE = iceberg` tables (`rw_managed_proto_fd_orders`, `rw_managed_proto_fd_revenue`), and the upsert sinks that feed them. |
| 5    | Waits `ICEBERG_WAIT` seconds (default 10) for the first iceberg commit, then prints row counts from both managed tables. Set `ICEBERG_WAIT=0` to skip. |

Watch the streamed output in the UI; it ends with `=== done ===`.

Defaults: `COUNT=200`, `TPS=0` (no rate limit), `TOPIC=orders_filedesc`,
`ICEBERG_WAIT=10`. Override via the runner's env-input box or by setting
them before running the shell script directly.

---

## 2. Validate it in psql

Open a SQL shell against RisingWave:

```bash
psql -h localhost -p 4566 -d dev -U root
```

### 2a. Confirm the source exists and reflected the descriptor

```sql
-- \d is blocked by a non-C-collation pg_catalog query in RW. Use IS instead.
SELECT column_name, data_type
FROM information_schema.columns
WHERE table_name = 'src_orders_proto_filedesc'
ORDER BY ordinal_position;
```

You should see `order_id`, `customer` (struct), `items` (struct[]),
`attributes` (map), `status` (text), `event_time` (struct), `total`,
`currency`, plus three nullable oneof columns `card`, `wallet`, `crypto`.

### 2b. Top-level + nested struct access

```sql
SELECT
    order_id,
    status,
    (customer).id                  AS customer_id,
    (customer).email               AS email,
    ((customer).address).city      AS city,
    ((customer).address).country   AS country,
    total, currency
FROM src_orders_proto_filedesc
LIMIT 10;
```

Nested protobuf access uses **parenthesized** struct syntax —
`((customer).address).country` — because `customer.country` would be
parsed as `<table>.<column>` in Postgres dialect.

### 2c. Exploded line items via `UNNEST`

```sql
SELECT
    order_id,
    (product).id        AS sku,
    (product).category  AS category,
    quantity            AS qty,
    unit_price,
    discount_pct,
    ROUND((quantity * unit_price * (1 - discount_pct / 100.0))::numeric, 2)
        AS line_total
FROM src_orders_proto_filedesc, UNNEST(items)
LIMIT 20;
```

`UNNEST(struct[])` lifts the inner struct fields (`product`, `quantity`,
`unit_price`, `discount_pct`) to top-level columns — no struct alias.

### 2d. `map<string,string>` lookup

```sql
SELECT order_id,
       attributes['source']   AS channel,
       attributes['campaign'] AS campaign
FROM src_orders_proto_filedesc
LIMIT 10;

SELECT COUNT(*)
FROM src_orders_proto_filedesc
WHERE attributes['campaign'] = 'spring_sale';
```

### 2e. `oneof payment` — exactly one variant non-null

```sql
SELECT
    SUM((card   IS NOT NULL)::int) AS card_rows,
    SUM((wallet IS NOT NULL)::int) AS wallet_rows,
    SUM((crypto IS NOT NULL)::int) AS crypto_rows,
    COUNT(*)                       AS rows
FROM src_orders_proto_filedesc;

SELECT
    CASE WHEN card   IS NOT NULL THEN 'card:'   || (card).brand
         WHEN wallet IS NOT NULL THEN 'wallet:' || (wallet).provider
         WHEN crypto IS NOT NULL THEN 'crypto:' || (crypto).chain
    END AS method,
    COUNT(*)                     AS n,
    ROUND(SUM(total)::numeric, 2) AS revenue
FROM src_orders_proto_filedesc
GROUP BY 1
ORDER BY revenue DESC;
```

### 2f. Enum + `google.protobuf.Timestamp`

```sql
SELECT status, COUNT(*) FROM src_orders_proto_filedesc GROUP BY 1 ORDER BY 2 DESC;

SELECT
    order_id,
    to_timestamp((event_time).seconds + (event_time).nanos / 1e9) AS event_ts
FROM src_orders_proto_filedesc
ORDER BY event_ts DESC
LIMIT 5;
```

### 2g. The streaming MV

```sql
SELECT * FROM mv_revenue_by_country_category_filedesc
ORDER BY revenue DESC
LIMIT 10;
```

Re-run the producer tile (or `COUNT=500 ./bin/3_run_protobuf_demo_filedesc.sh`)
and re-`SELECT` — the rollup updates live without redefining the MV.

### 2h. Peek at raw payloads (no SR envelope)

```bash
docker exec redpanda rpk topic consume orders_filedesc -n 1 -o oldest
```

The value is plain protobuf wire-format bytes; there is no leading
`\x00` + 4-byte schema-id prefix that the SR variant carries.

### 2i. Managed Iceberg sinks

The SQL step also created two `ENGINE = iceberg` tables in the
Lakekeeper-managed warehouse and the upsert sinks that feed them. After
step [5/5] of the runner finishes, both tables already have data:

```sql
SELECT count(*) FROM rw_managed_proto_fd_orders;    -- e.g. 200
SELECT count(*) FROM rw_managed_proto_fd_revenue;   -- e.g. 20

SELECT name, connector, sink_type
FROM rw_catalog.rw_sinks
WHERE name IN ('rw_managed_proto_fd_orders_sink',
               'rw_managed_proto_fd_revenue_sink');
```

Both sinks use `commit_checkpoint_interval = 5`, so new RisingWave rows
appear in iceberg snapshots within ~5 s. When sinking **into** a managed
iceberg table the `WITH` clause must not include `connector`,
`connection`, `database.name`, `table.name`, or
`create_table_if_not_exists` — RisingWave infers them from the target.
Only `type`, `primary_key`, and maintenance opts belong there.

`google.protobuf.Timestamp` is exposed as `struct<seconds, nanos>`, so
the flattening MV converts it to `TIMESTAMPTZ` before sinking:

```sql
to_timestamp((event_time).seconds + (event_time).nanos / 1e9)
    AS event_time
```

The same iceberg tables are visible through the other tiles in the
runner:

- 🦆 **DuckDB Iceberg** — [bin/5_duckdb_iceberg.sh](../bin/5_duckdb_iceberg.sh)
- 🔥 **Spark Iceberg** — [bin/5_spark_iceberg.sh](../bin/5_spark_iceberg.sh)
- 🧊 **Trino / Marimo** — [bin/5_marimo_risingwave.sh](../bin/5_marimo_risingwave.sh)

---

## 3. Schema evolution

Goal: ship a backward-compatible change to `demo.Order`, update the
producer, then bring it into RisingWave **without dropping the source
or its dependent MV**.

We'll add:

- A new optional top-level field `shipping_method` (field number `9`).
- A new enum value `ORDER_STATUS_REFUNDED = 5`.

Both are safe: old consumers ignore the new field/enum value; old
rows on the topic are still readable (missing field → empty string).

### 3a. Update `proto/events.proto`

Apply this diff to [proto/events.proto](../proto/events.proto):

```diff
 enum OrderStatus {
   ORDER_STATUS_UNSPECIFIED = 0;
   ORDER_STATUS_PENDING     = 1;
   ORDER_STATUS_PAID        = 2;
   ORDER_STATUS_SHIPPED     = 3;
   ORDER_STATUS_CANCELLED   = 4;
+  ORDER_STATUS_REFUNDED    = 5;
 }
@@
 message Order {
   string                       order_id   = 1;
   Customer                     customer   = 2;
   repeated LineItem            items      = 3;
   map<string, string>          attributes = 4;
   OrderStatus                  status     = 5;
   google.protobuf.Timestamp    event_time = 6;
   double                       total      = 7;
   string                       currency   = 8;
+  string                       shipping_method = 9;  // "standard" | "express" | "pickup"

   oneof payment {
     CardPayment   card   = 20;
     WalletPayment wallet = 21;
     CryptoPayment crypto = 22;
   }
 }
```

> Rule of thumb: **only add** with fresh field numbers, **never** change
> or reuse existing numbers/types. Don't remove or renumber `oneof`
> arms.

### 3b. Update the producer to populate the new field

In [scripts/produce_protobuf_orders.py](../scripts/produce_protobuf_orders.py),
inside `build_order()`, populate the new field and use the new enum
value occasionally:

```diff
     order = pb.Order(
         order_id=f"ord-{int(time.time()*1000)}-{i:05d}",
         customer=customer,
         items=items,
         status=random.choice(
             [
                 pb.ORDER_STATUS_PENDING,
                 pb.ORDER_STATUS_PAID,
                 pb.ORDER_STATUS_PAID,
                 pb.ORDER_STATUS_SHIPPED,
                 pb.ORDER_STATUS_CANCELLED,
+                pb.ORDER_STATUS_REFUNDED,
             ]
         ),
         event_time=ts,
         total=total,
         currency=random.choice(["USD", "EUR", "JPY"]),
+        shipping_method=random.choice(["standard", "standard", "express", "pickup"]),
     )
```

The file-descriptor producer reuses this same `build_order`, so no
changes are needed in `produce_protobuf_orders_filedesc.py`.

### 3c. Regenerate `events.pb` and produce new messages

Just re-run the tile **🧬 Protobuf Demo (FileDescriptorSet)** in the
runner. The producer detects that `events.proto` is newer than
`events_pb2.py` / `events.pb` and rebuilds both, then emits a fresh
batch of orders that include `shipping_method` and the new enum value.

Because `./proto` is bind-mounted into the RisingWave containers, the
new `events.pb` is immediately visible inside them — but the existing
source still has the **old** catalog schema cached.

> Heads-up: the runner also re-applies `sql/protobuf_demo_filedesc.sql`
> which begins with `DROP SOURCE IF EXISTS … CASCADE`. If you want to
> preserve MV state through the evolution, run **just** the producer
> directly instead and skip step 4 of the script:
>
> ```bash
> uv run python scripts/produce_protobuf_orders_filedesc.py --count 200 --tps 0
> ```

### 3d. Refresh the source schema (no DROP)

```sql
ALTER SOURCE src_orders_proto_filedesc REFRESH SCHEMA;
```

RisingWave re-reads `/proto/events.pb`, picks up the new field and
enum value, and updates the catalog in place. Dependent MVs keep
running.

Confirm:

```sql
SELECT column_name, data_type
FROM information_schema.columns
WHERE table_name = 'src_orders_proto_filedesc'
  AND column_name = 'shipping_method';
```

You should see one row: `shipping_method | character varying`.

### 3e. Query the new field

Old rows have `shipping_method = ''` (proto3 default), new rows have
real values:

```sql
-- How many rows actually carry the new field populated?
SELECT
    shipping_method,
    COUNT(*) AS n
FROM src_orders_proto_filedesc
GROUP BY 1
ORDER BY n DESC;
```

```sql
-- Revenue split by shipping method, ignoring old (empty) rows.
SELECT
    shipping_method,
    COUNT(*)                          AS orders,
    ROUND(SUM(total)::numeric, 2)     AS revenue
FROM src_orders_proto_filedesc
WHERE shipping_method <> ''
GROUP BY 1
ORDER BY revenue DESC;
```

```sql
-- Express orders going abroad (nested field + new field together).
SELECT
    order_id,
    ((customer).address).country AS country,
    total, currency
FROM src_orders_proto_filedesc
WHERE shipping_method = 'express'
  AND ((customer).address).country <> 'US'
ORDER BY total DESC
LIMIT 20;
```

### 3f. Query the new enum value

```sql
-- Refunded orders only appear after the evolution.
SELECT status, COUNT(*)
FROM src_orders_proto_filedesc
GROUP BY 1
ORDER BY 2 DESC;

SELECT
    order_id,
    ((customer).address).country  AS country,
    total, currency,
    shipping_method
FROM src_orders_proto_filedesc
WHERE status = 'ORDER_STATUS_REFUNDED'
ORDER BY total DESC
LIMIT 10;
```

### 3g. Build a new MV on the evolved schema

The existing `mv_revenue_by_country_category_filedesc` keeps working
unchanged. You can also create a new MV that uses the new field:

```sql
CREATE MATERIALIZED VIEW mv_revenue_by_shipping AS
SELECT
    shipping_method,
    ((customer).address).country AS country,
    COUNT(*)                     AS orders,
    ROUND(SUM(total)::numeric, 2) AS revenue
FROM src_orders_proto_filedesc
WHERE shipping_method <> ''
  AND status IN ('ORDER_STATUS_PAID', 'ORDER_STATUS_SHIPPED')
GROUP BY 1, 2;

SELECT * FROM mv_revenue_by_shipping
ORDER BY revenue DESC
LIMIT 10;
```

Produce more rows and re-`SELECT`; the new MV updates incrementally.

---

## 4. When `REFRESH SCHEMA` is not enough

`ALTER SOURCE … REFRESH SCHEMA` only handles **backward-compatible**
diffs. If you removed a field, renumbered something, or changed a
type, you have to drop and re-create:

```sql
DROP SINK            IF EXISTS rw_managed_proto_fd_orders_sink;
DROP SINK            IF EXISTS rw_managed_proto_fd_revenue_sink;
DROP TABLE           IF EXISTS rw_managed_proto_fd_orders;
DROP TABLE           IF EXISTS rw_managed_proto_fd_revenue;
DROP MATERIALIZED VIEW IF EXISTS mv_proto_fd_orders_flat;
DROP MATERIALIZED VIEW IF EXISTS mv_revenue_by_shipping;
DROP MATERIALIZED VIEW IF EXISTS mv_revenue_by_country_category_filedesc;
DROP SOURCE IF EXISTS src_orders_proto_filedesc CASCADE;
\i sql/protobuf_demo_filedesc.sql
```

You'll lose any incremental state on the dropped MVs, so prefer
`REFRESH SCHEMA` whenever the diff allows it.

---

## 5. Cleanup

```sql
DROP SINK            IF EXISTS rw_managed_proto_fd_orders_sink;
DROP SINK            IF EXISTS rw_managed_proto_fd_revenue_sink;
DROP TABLE           IF EXISTS rw_managed_proto_fd_orders;
DROP TABLE           IF EXISTS rw_managed_proto_fd_revenue;
DROP MATERIALIZED VIEW IF EXISTS mv_proto_fd_orders_flat;
DROP MATERIALIZED VIEW IF EXISTS mv_revenue_by_shipping;
DROP MATERIALIZED VIEW IF EXISTS mv_revenue_by_country_category_filedesc;
DROP SOURCE IF EXISTS src_orders_proto_filedesc CASCADE;
```

```bash
docker exec redpanda rpk topic delete orders_filedesc
```

---

## Quick reference

| Task                                       | Command                                                                                  |
| ------------------------------------------ | ---------------------------------------------------------------------------------------- |
| Start stack                                | `./bin/1_up.sh`                                                                          |
| Start runner UI                            | `./bin/0_script_runner.sh` → <http://localhost:4001>                                     |
| Run demo                                   | Click **🧬 Protobuf Demo (FileDescriptorSet)** (or `./bin/3_run_protobuf_demo_filedesc.sh`) |
| Produce only (preserve MV state)           | `uv run python scripts/produce_protobuf_orders_filedesc.py --count 200 --tps 0`          |
| Inspect raw payload                        | `docker exec redpanda rpk topic consume orders_filedesc -n 1 -o oldest`                  |
| List columns                               | `SELECT column_name, data_type FROM information_schema.columns WHERE table_name='src_orders_proto_filedesc';` |
| Refresh descriptor after `.proto` edit     | `ALTER SOURCE src_orders_proto_filedesc REFRESH SCHEMA;`                                 |
| Full reset                                 | `DROP SOURCE … CASCADE;` then re-run the runner tile                                     |
