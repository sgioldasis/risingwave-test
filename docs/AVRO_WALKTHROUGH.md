# Avro Demo — Step-by-Step Walkthrough

End-to-end walkthrough for the Avro variant of the RisingWave nested-data
demo. Same logical `Order` payload as the protobuf demos, but Avro on the
wire and the schema lives as JSON in [avro/orders.avsc](../avro/orders.avsc).
The producer registers it with Redpanda's Schema Registry; RisingWave fetches
it via `schema.registry = '...'` — no local descriptor file, no proto compiler.

Sister docs:

- [PROTOBUF_NESTED_DEMO.md](PROTOBUF_NESTED_DEMO.md) — protobuf + Schema Registry.
- [PROTOBUF_FILEDESC_WALKTHROUGH.md](PROTOBUF_FILEDESC_WALKTHROUGH.md) — protobuf via FileDescriptorSet, no SR.

## What you'll do

1. Start the stack.
2. Launch the demo from the script-runner UI.
3. Run the validation queries in psql.
4. Evolve the Avro schema (add a field + an enum symbol), refresh the
   source in place, and verify with new queries.

---

## 0. Prereqs

The compose stack must be up:

```bash
./bin/1_up.sh
```

That brings up Redpanda (Kafka on `localhost:19092`, Schema Registry on
`localhost:8081`), Redpanda Console (`localhost:9090`), and RisingWave on
`localhost:4566`.

No bind mounts are needed for this variant — RisingWave talks to the
Schema Registry over HTTP from inside the compose network
(`http://redpanda:8081`).

---

## 1. Run the demo from the script runner

Start the runner (idempotent — kills any prior instance):

```bash
./bin/0_script_runner.sh
```

Open the UI: <http://localhost:4001>

Click **🅰️  Avro Demo**. That tile is wired to
[bin/3_run_avro_demo.sh](../bin/3_run_avro_demo.sh) which performs three steps:

| Step | What happens |
| ---- | ------------ |
| 1    | **Resets** Kafka topic `orders_avro` (delete + recreate) and clears the SR subject `orders_avro-value` (soft + hard delete). Idempotent. |
| 2    | Runs [scripts/produce_avro_orders.py](../scripts/produce_avro_orders.py) — loads `avro/orders.avsc`, registers it under subject `orders_avro-value`, and sends Confluent-framed Avro messages (magic byte `\x00` + 4-byte schema id + Avro body) to Kafka. |
| 3    | Applies [sql/avro_demo.sql](../sql/avro_demo.sql) against RisingWave — creates `src_orders_avro` and `mv_avro_revenue_by_country_category`. |

> **Why the aggressive reset in step 1?** The SR subject reset prevents
> BACKWARD-compatibility 409s from a previously-registered schema. The
> topic reset is just as important: if a prior run left messages on the
> topic that reference now-deleted schema ids, the RisingWave source
> stalls indefinitely on the first SELECT — psql sits there after
> `CREATE_SOURCE` with no rows ever returned. Wiping both keeps the
> demo reproducible.

Watch the streamed output in the UI; it ends with `=== done ===`.

Defaults: `COUNT=200`, `TPS=20`, `TOPIC=orders_avro`. Override via the
runner's env-input box or by setting them before running the shell script
directly.

You can confirm the schema landed in Schema Registry from the host:

```bash
curl -s http://localhost:8081/subjects | jq
curl -s http://localhost:8081/subjects/orders_avro-value/versions/latest | jq
```

---

## 2. Validate it in psql

Open a SQL shell against RisingWave:

```bash
psql -h localhost -p 4566 -d dev -U root
```

### 2a. Confirm the source exists and reflected the Avro schema

```sql
-- \d is blocked by a non-C-collation pg_catalog query in RW. Use IS instead.
SELECT column_name, data_type
FROM information_schema.columns
WHERE table_name = 'src_orders_avro'
ORDER BY ordinal_position;
```

You should see top-level columns: `order_id`, `customer` (struct),
`items` (struct[]), `attributes` (map), `status` (text), `event_time`
(timestamptz — Avro logicalType `timestamp-millis`), `total`, `currency`,
plus `payment` — a **struct** with three nullable sub-fields `card`,
`wallet`, `crypto`.

> **Why a record-of-nullable-records and not a true Avro union of
> records?** RisingWave does not yet support Avro `union` whose branches
> are named record types ("Avro named type used in Union type" /
> [risingwavelabs/risingwave#17632](https://github.com/risingwavelabs/risingwave/issues/17632)).
> A nullable union with a single record (`["null", SomeRecord]`) is fine
> — that's the standard Avro nullable pattern — so we model the three
> payment variants as three sibling nullable fields inside a `Payment`
> record. The on-the-wire encoding is virtually identical (one zero byte
> per null branch) and the SQL shape matches the protobuf `oneof` demo.

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
FROM src_orders_avro
LIMIT 10;
```

Nested struct access uses **parenthesized** syntax —
`((customer).address).country` — because `customer.country` would be
parsed as `<table>.<column>` in Postgres dialect. Exactly the same rule
as the protobuf demos.

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
FROM src_orders_avro, UNNEST(items)
LIMIT 20;
```

`UNNEST(struct[])` lifts the inner record fields (`product`, `quantity`,
`unit_price`, `discount_pct`) to top-level columns — no struct alias.

### 2d. `map<string,string>` lookup

```sql
SELECT order_id,
       attributes['source']   AS channel,
       attributes['campaign'] AS campaign
FROM src_orders_avro
LIMIT 10;

SELECT COUNT(*)
FROM src_orders_avro
WHERE attributes['campaign'] = 'spring_sale';
```

Avro `map<string>` maps directly to RisingWave's `map<varchar, varchar>`,
indexed with `[key]` (returns `NULL` for missing keys).

### 2e. Payment variants — exactly one branch non-null

`payment` is a struct with three nullable sub-fields (`card`, `wallet`,
`crypto`). The producer sets exactly one per row.

```sql
SELECT
    SUM(((payment).card   IS NOT NULL)::int) AS card_rows,
    SUM(((payment).wallet IS NOT NULL)::int) AS wallet_rows,
    SUM(((payment).crypto IS NOT NULL)::int) AS crypto_rows,
    COUNT(*)                                 AS rows
FROM src_orders_avro;

SELECT
    CASE
        WHEN (payment).card   IS NOT NULL THEN 'card:'   || ((payment).card).brand
        WHEN (payment).wallet IS NOT NULL THEN 'wallet:' || ((payment).wallet).provider
        WHEN (payment).crypto IS NOT NULL THEN 'crypto:' || ((payment).crypto).chain
    END AS method,
    COUNT(*)                      AS n,
    ROUND(SUM(total)::numeric, 2) AS revenue
FROM src_orders_avro
GROUP BY 1
ORDER BY revenue DESC;
```

### 2f. Enum + logical-type timestamp

```sql
SELECT status, COUNT(*) FROM src_orders_avro GROUP BY 1 ORDER BY 2 DESC;

-- event_time is a real timestamptz thanks to logicalType=timestamp-millis,
-- so date/interval math just works — no seconds/nanos struct gymnastics
-- like the protobuf well-known Timestamp.
SELECT
    order_id,
    event_time,
    now() - event_time AS age
FROM src_orders_avro
ORDER BY event_time DESC
LIMIT 5;
```

### 2g. The streaming MV

```sql
SELECT * FROM mv_avro_revenue_by_country_category
ORDER BY revenue DESC
LIMIT 10;
```

Re-run the producer tile (or `COUNT=500 TPS=50 ./bin/3_run_avro_demo.sh`)
and re-`SELECT` — the rollup updates live without redefining the MV.

### 2h. Peek at raw payloads

```bash
docker exec redpanda rpk topic consume orders_avro -n 1 -o oldest
```

The value starts with `\x00` (Confluent magic byte) followed by a 4-byte
big-endian schema id, then the Avro-binary-encoded record. Compare with
the protobuf-FileDescriptor demo's raw payload, which has no magic-byte
prefix.

---

## 3. Schema evolution

Goal: ship a backward-compatible change to the Avro schema, update the
producer, then bring it into RisingWave **without dropping the source
or its dependent MV**.

We'll add:

- A new top-level field `shipping_method` (string, with a default so old
  rows remain readable).
- A new enum symbol `ORDER_STATUS_REFUNDED`. Avro requires the field that
  references the enum to carry a `default` so old readers can handle
  unknown symbols — we already have a default-eligible setup because the
  field is required from new producers only.

Avro compatibility rules (default Schema Registry policy is `BACKWARD`):

- **Adding a field** → must have a `default` so consumers reading old
  data without the field can still resolve.
- **Adding an enum symbol** → backward-compatible as long as the
  containing field has a default (so old data missing the new symbol
  isn't actually missing — it carries the default).
- **Removing / renaming / type-changing** → not compatible; needs a new
  subject or compatibility-mode override.

### 3a. Update `avro/orders.avsc`

Apply this diff to [avro/orders.avsc](../avro/orders.avsc):

```diff
     {
       "name": "status",
       "type": {
         "type": "enum",
         "name": "OrderStatus",
         "symbols": [
           "ORDER_STATUS_UNSPECIFIED",
           "ORDER_STATUS_PENDING",
           "ORDER_STATUS_PAID",
           "ORDER_STATUS_SHIPPED",
-          "ORDER_STATUS_CANCELLED"
+          "ORDER_STATUS_CANCELLED",
+          "ORDER_STATUS_REFUNDED"
-        ]
+        ],
+        "default": "ORDER_STATUS_UNSPECIFIED"
       }
     },
@@
     { "name": "total",    "type": "double" },
     { "name": "currency", "type": "string" },
+    { "name": "shipping_method", "type": "string", "default": "" },
     {
       "name": "payment",
```

### 3b. Update the producer to populate the new field

In [scripts/produce_avro_orders.py](../scripts/produce_avro_orders.py),
inside `build_order()`, add the new field and use the new enum value
occasionally:

```diff
         "status": random.choice(
             [
                 "ORDER_STATUS_PENDING",
                 "ORDER_STATUS_PAID",
                 "ORDER_STATUS_PAID",
                 "ORDER_STATUS_SHIPPED",
                 "ORDER_STATUS_CANCELLED",
+                "ORDER_STATUS_REFUNDED",
             ]
         ),
         "event_time": datetime.now(timezone.utc),
         "total": total,
         "currency": random.choice(["USD", "EUR", "JPY"]),
+        "shipping_method": random.choice(["standard", "standard", "express", "pickup"]),
         "payment": payment,
     }
```

### 3c. Register the new schema version and emit new messages

Just re-run the tile **🅰️  Avro Demo** in the runner. The producer
loads the updated `.avsc` and `AvroSerializer` automatically registers
it as a new version under `orders_avro-value` (Schema Registry assigns
the next schema id and validates BACKWARD compatibility).

If you'd rather keep MV state through the evolution — the SQL step in
the runner begins with `DROP SOURCE IF EXISTS … CASCADE` — run **just**
the producer:

```bash
uv run python scripts/produce_avro_orders.py --count 200 --tps 20
```

Verify both versions are now registered:

```bash
curl -s http://localhost:8081/subjects/orders_avro-value/versions | jq
# -> [1, 2]
curl -s http://localhost:8081/subjects/orders_avro-value/versions/latest | jq .schema | jq -r . | jq .
```

### 3d. Refresh the source schema (no DROP)

```sql
ALTER SOURCE src_orders_avro REFRESH SCHEMA;
```

RisingWave re-fetches the latest schema from the registry, picks up
the new field and enum symbol, and updates the catalog in place.
Dependent MVs keep running.

Confirm:

```sql
SELECT column_name, data_type
FROM information_schema.columns
WHERE table_name = 'src_orders_avro'
  AND column_name = 'shipping_method';
```

You should see one row: `shipping_method | character varying`.

### 3e. Query the new field

Old rows resolve to the default (`shipping_method = ''`), new rows
have real values:

```sql
SELECT
    shipping_method,
    COUNT(*) AS n
FROM src_orders_avro
GROUP BY 1
ORDER BY n DESC;
```

```sql
-- Revenue split by shipping method, ignoring old (default) rows.
SELECT
    shipping_method,
    COUNT(*)                       AS orders,
    ROUND(SUM(total)::numeric, 2)  AS revenue
FROM src_orders_avro
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
FROM src_orders_avro
WHERE shipping_method = 'express'
  AND ((customer).address).country <> 'US'
ORDER BY total DESC
LIMIT 20;
```

### 3f. Query the new enum value

```sql
SELECT status, COUNT(*)
FROM src_orders_avro
GROUP BY 1
ORDER BY 2 DESC;

SELECT
    order_id,
    ((customer).address).country  AS country,
    total, currency,
    shipping_method
FROM src_orders_avro
WHERE status = 'ORDER_STATUS_REFUNDED'
ORDER BY total DESC
LIMIT 10;
```

### 3g. Build a new MV on the evolved schema

The existing `mv_avro_revenue_by_country_category` keeps working
unchanged. You can also create a new MV that uses the new field:

```sql
CREATE MATERIALIZED VIEW mv_avro_revenue_by_shipping AS
SELECT
    shipping_method,
    ((customer).address).country AS country,
    COUNT(*)                      AS orders,
    ROUND(SUM(total)::numeric, 2) AS revenue
FROM src_orders_avro
WHERE shipping_method <> ''
  AND status IN ('ORDER_STATUS_PAID', 'ORDER_STATUS_SHIPPED')
GROUP BY 1, 2;

SELECT * FROM mv_avro_revenue_by_shipping
ORDER BY revenue DESC
LIMIT 10;
```

Produce more rows and re-`SELECT`; the new MV updates incrementally.

---

## 4. When `REFRESH SCHEMA` is not enough

`ALTER SOURCE … REFRESH SCHEMA` only handles **backward-compatible**
diffs (the same diffs Schema Registry will accept in BACKWARD mode).
If you remove a field, rename one, or change a type, you have to drop
and re-create — and the registry will likely reject the new version
unless you change compatibility mode first:

```bash
# Loosen compatibility for a one-off breaking change.
curl -X PUT http://localhost:8081/config/orders_avro-value \
     -H 'Content-Type: application/json' \
     -d '{"compatibility": "NONE"}'
```

```sql
DROP MATERIALIZED VIEW IF EXISTS mv_avro_revenue_by_shipping;
DROP MATERIALIZED VIEW IF EXISTS mv_avro_revenue_by_country_category;
DROP SOURCE IF EXISTS src_orders_avro CASCADE;
\i sql/avro_demo.sql
```

You'll lose any incremental state on the dropped MVs, so prefer
`REFRESH SCHEMA` whenever the diff allows it. Restore the
compatibility mode afterwards:

```bash
curl -X PUT http://localhost:8081/config/orders_avro-value \
     -H 'Content-Type: application/json' \
     -d '{"compatibility": "BACKWARD"}'
```

---

## 5. Cleanup

```sql
DROP MATERIALIZED VIEW IF EXISTS mv_avro_revenue_by_shipping;
DROP MATERIALIZED VIEW IF EXISTS mv_avro_revenue_by_country_category;
DROP SOURCE IF EXISTS src_orders_avro CASCADE;
```

```bash
docker exec redpanda rpk topic delete orders_avro
# Optional — drop all schema versions under the subject:
curl -X DELETE http://localhost:8081/subjects/orders_avro-value
curl -X DELETE 'http://localhost:8081/subjects/orders_avro-value?permanent=true'
```

---

## 6. Known limitations & gotchas

These are the rough edges hit (and worked around) while building the
demo. All verified against the current compose stack on 2026-05-26.

### 6.1 `union<record>` is not implemented yet

```
Feature is not yet implemented: Avro named type used in Union type:
Record(RecordSchema { name: Name { name: "CardPayment", ... } })
```

Tracking issue: [risingwavelabs/risingwave#17632](https://github.com/risingwavelabs/risingwave/issues/17632).

A union of two or more **named record types** (the natural Avro
analogue of a protobuf `oneof`) makes `CREATE SOURCE` succeed but the
first SELECT fail with the message above.

Workaround used in this demo: model the variants as a record with one
nullable sub-record per branch. Each sub-field is a **nullable union of
exactly one record** (`["null", CardPayment]`) — which Avro and
RisingWave both support — and the producer sets exactly one branch
non-null per row. SQL accessor: `(payment).card`, `((payment).card).brand`.

### 6.2 SR + Kafka must be reset together, or the source stalls

Symptom: psql sits forever after `CREATE_SOURCE`, never returning the
first row of `SELECT … LIMIT 5`. No error in the SQL output, no obvious
log line.

Root cause: the topic still holds messages produced under an earlier
schema id that's been deleted from Schema Registry (typical after a
breaking schema change). When the RisingWave source reads from
`earliest`, it can't resolve those ids and gets wedged before reaching
the well-formed messages.

Fix (already baked into [bin/3_run_avro_demo.sh](../bin/3_run_avro_demo.sh)):
**delete and recreate the topic in the same step that resets the SR
subject.** Keeping just one of the two is not enough.

```bash
docker exec redpanda rpk topic delete orders_avro
docker exec redpanda rpk topic create orders_avro --partitions 3
curl -X DELETE "http://localhost:8081/subjects/orders_avro-value"
curl -X DELETE "http://localhost:8081/subjects/orders_avro-value?permanent=true"
```

### 6.3 BACKWARD compatibility 409s

If you bump the schema in a way Schema Registry considers incompatible
(removing a field, changing a type), the producer's
`AvroSerializer.register_schema()` returns HTTP 409. Either:

- Make the change backward-compatible (add fields with `default`, add
  enum symbols only when the containing field has a default), **or**
- Relax compatibility for the subject and accept a hard reset:
  ```bash
  curl -X PUT http://localhost:8081/config/orders_avro-value \
       -H 'Content-Type: application/json' \
       -d '{"compatibility": "NONE"}'
  ```

### 6.4 Benign `NOTICE`s you can ignore

- `Neither wildcard ("*") nor regular ... columns appear in the
  user-defined schema from SQL.` — same NOTICE the protobuf demos emit;
  the source still reflects every Avro field, the notice is just
  suggesting cosmetic SQL.
- `snapshot backfill disabled due to using shared source` — emitted
  when creating an MV on a source-backed query. Expected; backfill is
  unnecessary because the source already replays from `earliest`.

### 6.5 `\d` doesn't work in psql against RisingWave

`psql`'s `\d table` issues a pg_catalog query that uses a non-C
collation, which RisingWave doesn't implement. Use `information_schema`
instead — the listed-columns query in §2a works.

---

## 7. Why there is no `schema.location` Avro variant

The protobuf demos come in two flavours — one Schema-Registry backed
([PROTOBUF_NESTED_DEMO.md](PROTOBUF_NESTED_DEMO.md)) and one using a
local FileDescriptorSet
([PROTOBUF_FILEDESC_WALKTHROUGH.md](PROTOBUF_FILEDESC_WALKTHROUGH.md)
with `schema.location = 'file:///proto/events.pb'`). The natural
mirror for Avro would be
`ENCODE AVRO (schema.location = 'file:///avro/orders.avsc')`, but
**RisingWave does not currently support Avro without Schema Registry**:

```
ERROR:  Failed to run the query
Caused by:
  Feature is not yet implemented: avro without schema registry
  Tracking issue: https://github.com/risingwavelabs/risingwave/issues/12871
```

Until [risingwavelabs/risingwave#12871](https://github.com/risingwavelabs/risingwave/issues/12871)
lands, the only supported Avro ingestion path is the SR-backed flow in
§1. If you need a registry-less option today, use the protobuf
file-descriptor demo instead.

---

## Avro vs protobuf — at a glance

| Aspect                          | Avro (this demo)                                | Protobuf + SR                                | Protobuf + FileDescriptor                |
| ------------------------------- | ----------------------------------------------- | -------------------------------------------- | ---------------------------------------- |
| Schema source of truth          | `avro/orders.avsc` (JSON)                        | `proto/events.proto` → SR                    | `proto/events.proto` → `events.pb`       |
| RisingWave config               | `ENCODE AVRO (schema.registry = ...)`            | `ENCODE PROTOBUF (schema.registry = ..., message = 'demo.Order')` | `ENCODE PROTOBUF (schema.location = 'file:///proto/events.pb', message = 'demo.Order')` |
| Wire format                     | `\x00` + 4-byte schema id + Avro body            | `\x00` + schema id + protobuf body           | raw protobuf bytes (no envelope)         |
| Schema Registry required        | Yes (no `schema.location` support — [#12871](https://github.com/risingwavelabs/risingwave/issues/12871)) | Yes                                          | No                                       |
| Union / `oneof`                 | A `Payment` record with one nullable sub-record per branch (true Avro `union<record>` is [not yet supported](https://github.com/risingwavelabs/risingwave/issues/17632) by RisingWave) | Multiple top-level nullable struct columns | Same as SR variant                       |
| Timestamp ergonomics            | Native `timestamptz` (logicalType)               | Struct `<seconds, nanos>` (well-known type)  | Same as SR variant                       |
| Schema evolution                | Add fields with `default`; SR enforces BACKWARD | Add fields with new numbers; SR enforces compat | Add fields with new numbers; no SR check |
| In-place refresh                | `ALTER SOURCE … REFRESH SCHEMA`                  | `ALTER SOURCE … REFRESH SCHEMA`              | `ALTER SOURCE … REFRESH SCHEMA`          |

---

## Quick reference

| Task                                    | Command                                                                          |
| --------------------------------------- | -------------------------------------------------------------------------------- |
| Start stack                             | `./bin/1_up.sh`                                                                  |
| Start runner UI                         | `./bin/0_script_runner.sh` → <http://localhost:4001>                             |
| Run demo                                | Click **🅰️  Avro Demo** (or `./bin/3_run_avro_demo.sh`)                          |
| Produce only (preserve MV state)        | `uv run python scripts/produce_avro_orders.py --count 200 --tps 20`              |
| Inspect raw payload                     | `docker exec redpanda rpk topic consume orders_avro -n 1 -o oldest`              |
| List columns                            | `SELECT column_name, data_type FROM information_schema.columns WHERE table_name='src_orders_avro';` |
| List schema versions                    | `curl -s http://localhost:8081/subjects/orders_avro-value/versions`              |
| Refresh schema after `.avsc` edit       | `ALTER SOURCE src_orders_avro REFRESH SCHEMA;`                                   |
| Full reset                              | `DROP SOURCE … CASCADE;` then re-run the runner tile                             |
