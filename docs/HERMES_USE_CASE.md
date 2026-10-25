# Hermes Use Case — Mutable Streaming Funnel with Iceberg Propagation

## What is Hermes?

Hermes demonstrates that RisingWave can handle **data corrections on streaming data**.
The main casino pipeline is append-only (Kafka → SOURCE → MV). Hermes shows the alternative:
Kafka data ingested into **TABLEs** (not SOURCEs), which support `UPDATE` and `DELETE`.
Any change to the source tables propagates through the MV in real time and upserts into Iceberg.

**Core question answered:** *"What happens in Iceberg when I correct streaming source data?"*

---

## Architecture

```
Kafka Topics
    │
    ├── page_views   ──▶ tbl_hermes_page     ─┐
    ├── cart_events  ──▶ tbl_hermes_cart     ─┼──▶ hermes_features (MV) ──▶ sink_hermes_features_to_iceberg
    └── purchases    ──▶ tbl_hermes_purchase ─┘              │                        │
                              │                              │                        ▼
                         UPDATE/DELETE                  multi-interval          iceberg_hermes_features
                              │                         tumble windows          (Lakekeeper / Iceberg)
                              └──────────────────────────────▶ cascades to MV and Iceberg automatically
```

### Key difference vs casino pipeline

| | Casino pipeline | Hermes |
|---|---|---|
| Ingest object | `CREATE SOURCE` | `CREATE TABLE ... WITH (connector='kafka')` |
| Supports UPDATE/DELETE | No — append-only | Yes |
| Internal storage | No (reads Kafka directly) | Yes (data stored in RisingWave) |
| Primary key | No | Yes (enables upserts) |
| Use case | High-throughput real-time analytics | Data corrections, auditing |

---

## Data Models

### Source tables (`tbl_*`)

All three tables back onto Kafka topics and use `user_id` as primary key (upsert on duplicate).

| Model | Topic | Primary Key |
|---|---|---|
| `tbl_hermes_page` | `page_views` | `user_id` |
| `tbl_hermes_cart` | `cart_events` | `user_id` |
| `tbl_hermes_purchase` | `purchases` | `user_id` |

Schema (all three tables):
```sql
user_id      INT
page_id /
item_id /
amount       VARCHAR / DOUBLE
event_time   TIMESTAMPTZ
produced_at  TIMESTAMPTZ
PRIMARY KEY (user_id)
```

### `hermes_features` (MV)

Joins the three tables and computes funnel metrics at **three window sizes simultaneously**:

| `time_interval` | Window |
|---|---|
| `10 SECONDS` | TUMBLE 10s |
| `30 SECONDS` | TUMBLE 30s |
| `60 SECONDS` | TUMBLE 1min |

Output columns: `window_start`, `window_end`, `time_interval`, `minute_start`, `viewers`, `carters`, `purchasers`, `view_to_cart_rate`, `cart_to_buy_rate`

Composite primary key for the Iceberg sink: `(window_start, time_interval)`.

### `sink_hermes_features_to_iceberg`

Upsert sink → `iceberg_hermes_features` table in Lakekeeper.

- Type: `upsert`
- Primary key: `window_start, time_interval`
- RisingWave-managed compaction enabled (`compaction_interval_sec = 60`)
- Snapshot expiration enabled
- Compression: `zstd`

> **Local stack only** — Lakekeeper is not available in STG.

---

## Running the Demo

### 1. Start the stack and create models

```bash
./bin/1_up.sh
cd dbt && dbt run --select tbl_hermes_page tbl_hermes_cart tbl_hermes_purchase hermes_features sink_hermes_features_to_iceberg
```

### 2. Connect to RisingWave

```bash
psql -h localhost -p 4566 -d dev -U root
```

### 3. Verify data is flowing

```bash
# Check Kafka topics have data
kcat -b localhost:19092 -t page_views -C -o beginning -c 5
```

```sql
-- Check table row counts
SELECT 'page' as tbl, COUNT(*) FROM tbl_hermes_page
UNION ALL SELECT 'cart', COUNT(*) FROM tbl_hermes_cart
UNION ALL SELECT 'purchase', COUNT(*) FROM tbl_hermes_purchase;

-- Check the MV
SELECT time_interval, COUNT(*) as windows FROM hermes_features GROUP BY time_interval;
```

---

## Demo Scenarios

### Scenario 1 — Correct a data error

```sql
-- Fix a purchase recorded with the wrong amount
UPDATE tbl_hermes_purchase
SET amount = 15.99
WHERE user_id = 456 AND amount = 1599.00;

-- Observe the MV update within seconds
SELECT * FROM hermes_features ORDER BY window_start DESC LIMIT 5;
```

### Scenario 2 — Delete a user's data (GDPR erasure)

```sql
-- Record a timestamp before deletion (for time travel later)
SELECT NOW() AS before_delete;

-- Erase user
DELETE FROM tbl_hermes_page     WHERE user_id = 123;
DELETE FROM tbl_hermes_cart     WHERE user_id = 123;
DELETE FROM tbl_hermes_purchase WHERE user_id = 123;

-- Verify MV updated
SELECT viewers, carters, purchasers FROM hermes_features
ORDER BY window_start DESC LIMIT 5;
```

### Scenario 3 — Insert a manual record

```sql
INSERT INTO tbl_hermes_page (user_id, page_id, event_time)
VALUES (777, 'demo-landing-page', NOW());

INSERT INTO tbl_hermes_cart (user_id, item_id, event_time)
VALUES (777, 'demo-product', NOW());

INSERT INTO tbl_hermes_purchase (user_id, amount, event_time)
VALUES (777, 49.99, NOW());
```

### Scenario 4 — Compare multiple window sizes

```sql
-- See how the same event stream looks at 10s, 30s, and 60s windows
SELECT time_interval, window_start, viewers, carters, purchasers,
       view_to_cart_rate, cart_to_buy_rate
FROM hermes_features
ORDER BY window_start DESC, time_interval
LIMIT 15;
```

### Scenario 5 — Time travel in Iceberg (local stack only)

After deleting a user, query the Iceberg table as it was before the deletion via Trino:

```sql
-- List available snapshots
SELECT snapshot_id, committed_at, operation
FROM datalake.public."iceberg_hermes_features$snapshots"
ORDER BY committed_at DESC;

-- Query by timestamp (before deletion)
SELECT * FROM datalake.public.iceberg_hermes_features
FOR TIMESTAMP AS OF TIMESTAMP '2026-06-19 17:20:00'
ORDER BY window_start DESC LIMIT 10;

-- Query by snapshot ID
SELECT * FROM datalake.public.iceberg_hermes_features
FOR VERSION AS OF 3023402865675048688
ORDER BY window_start DESC LIMIT 10;
```

> **Note:** Trino time travel requires no license. RisingWave time travel (`FOR SYSTEM_TIME AS OF`) requires a valid RisingWave license.

---

## Troubleshooting

### Tables already exist

```sql
DROP TABLE IF EXISTS tbl_hermes_page CASCADE;
DROP TABLE IF EXISTS tbl_hermes_cart CASCADE;
DROP TABLE IF EXISTS tbl_hermes_purchase CASCADE;
DROP MATERIALIZED VIEW IF EXISTS hermes_features CASCADE;
```

Then re-run dbt.

### UPDATE not reflecting in MV

```sql
-- Check that the MV is running
SELECT name, definition FROM rw_catalog.rw_materialized_views WHERE name = 'hermes_features';

-- Force a flush
FLUSH;
```

### No data in tables

The tables consume from `earliest` offset by default. If empty, verify the Kafka topics have data:

```bash
kcat -b localhost:19092 -t page_views -C -o beginning -c 1
```
