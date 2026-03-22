# Kafka Tables for UPDATE/DELETE Operations

This guide explains how to use the new Kafka TABLE models to perform UPDATE and DELETE operations on streaming data in RisingWave, with propagation to Iceberg.

## Overview

Your dbt project now includes two approaches for consuming Kafka data:

| Approach | Models | Characteristics |
|----------|--------|-----------------|
| **Sources** | `src_cart`, `src_page`, `src_purchase` | Append-only, read-only, lightweight |
| **Tables** | `tbl_cart`, `tbl_page`, `tbl_purchase` | Modifiable, supports UPDATE/DELETE, stores data internally |

## Architecture

```
Kafka Topics
    │
    ├──▶ src_* (SOURCE) ──▶ funnel (MV)
    │
    └──▶ tbl_* (TABLE) ───▶ funnel_from_tables (MV)
                │
                ├── UPDATE ▶ MV updates
                └── DELETE ▶ MV updates
```

## Quick Start

### 1. Run dbt to Create All Models

```bash
cd dbt
dbt run
```

This creates:
- Original sources: `src_cart`, `src_page`, `src_purchase`
- New tables: `tbl_cart`, `tbl_page`, `tbl_purchase`
- Original funnel: `funnel` (from sources)
- New funnel: `funnel_from_tables` (from tables)

### 2. Connect to RisingWave

```bash
psql -h localhost -p 4566 -d dev -U root
```

### 3. Try UPDATE Operations

```sql
-- Update a user's cart item
UPDATE tbl_cart
SET item_id = 'premium-widget', event_time = NOW()
WHERE user_id = 123;

-- Check the effect on the funnel
SELECT * FROM funnel_from_tables
ORDER BY window_start DESC
LIMIT 5;
```

### 4. Try DELETE Operations

```sql
-- Remove test data
DELETE FROM tbl_cart WHERE user_id = 99999;
DELETE FROM tbl_page WHERE user_id = 99999;
DELETE FROM tbl_purchase WHERE user_id = 99999;

-- View updated counts
SELECT 'page_views' as table_name, count(*) as record_count FROM tbl_page
UNION ALL
SELECT 'cart_events', count(*) FROM tbl_cart
UNION ALL
SELECT 'purchases', count(*) FROM tbl_purchase;
```

## Demo Scenarios

| Scenario | Description | Learn |
|----------|-------------|-------|
| **1. Correct Data** | Fix erroneous purchase amounts | UPDATE propagation |
| **2. Add Records** | Insert manual test records | INSERT behavior |
| **3. Batch Cleanup** | Remove test user data in bulk | DELETE operations |
| **4. Compare Approaches** | Side-by-side source vs table comparison | Feature differences |
| **5. Time Travel** | Query historical data after deletion | Iceberg snapshots + time travel |

### Scenario 1: Correct Erroneous Data

```sql
-- Fix a purchase that was recorded with wrong amount
UPDATE tbl_purchase
SET amount = 15.99
WHERE user_id = 456 AND amount = 1599.00;

-- Verify the fix
SELECT * FROM funnel_from_tables
ORDER BY window_start DESC
LIMIT 3;
```

### Scenario 2: Add Manual Records

```sql
-- Insert a manual page view for testing
INSERT INTO tbl_page (user_id, page_id, event_time)
VALUES (777, 'demo-landing-page', NOW());

-- Insert a manual cart event
INSERT INTO tbl_cart (user_id, item_id, event_time)
VALUES (777, 'demo-product', NOW());

-- Insert a manual purchase
INSERT INTO tbl_purchase (user_id, amount, event_time)
VALUES (777, 49.99, NOW());
```

### Scenario 3: Batch Cleanup

```sql
-- Remove all test user data in one go
DELETE FROM tbl_cart WHERE user_id >= 90000;
DELETE FROM tbl_page WHERE user_id >= 90000;
DELETE FROM tbl_purchase WHERE user_id >= 90000;
```

### Scenario 4: Compare Source vs Table Funnels

```sql
-- Compare funnel metrics side-by-side
SELECT
    COALESCE(s.window_start, t.window_start) as window_start,
    s.viewers as source_viewers,
    t.viewers as table_viewers,
    s.carters as source_carters,
    t.carters as table_carters,
    s.purchasers as source_purchasers,
    t.purchasers as table_purchasers
FROM funnel s
FULL OUTER JOIN funnel_from_tables t
    ON s.window_start = t.window_start
ORDER BY COALESCE(s.window_start, t.window_start) DESC
LIMIT 10;
```

## Key Differences: SOURCE vs TABLE

| Feature | CREATE SOURCE | CREATE TABLE WITH connector |
|---------|---------------|----------------------------|
| **Storage** | No internal storage | Stores data internally |
| **Modifiable** | ❌ No - read-only | ✅ Yes - UPDATE/DELETE |
| **PRIMARY KEY** | ❌ No | ✅ Yes - enables upserts |
| **Query Speed** | Reads from Kafka | Reads from internal storage |
| **Resource Usage** | Lower | Higher (stores data) |
| **Use Case** | Raw ingestion | Processing, corrections |

## Model Reference

### Kafka Table Models

| Model | File | Topic | Primary Key |
|-------|------|-------|-------------|
| `tbl_cart` | `dbt/models/tbl_cart.sql` | `cart_events` | `user_id` |
| `tbl_page` | `dbt/models/tbl_page.sql` | `page_views` | `user_id` |
| `tbl_purchase` | `dbt/models/tbl_purchase.sql` | `purchases` | `user_id` |

### Materialization Macro

The custom materialization [`dbt/macros/materializations/kafka_table.sql`](dbt/macros/materializations/kafka_table.sql) handles:
- CREATE TABLE IF NOT EXISTS
- Full refresh support (DROP + CREATE)
- Configuration validation

### Demo Operations Model

The [`dbt/models/demo_operations.sql`](dbt/models/demo_operations.sql) model contains documented SQL examples for all UPDATE/DELETE operations.

## Configuration

### dbt_project.yml Updates

```yaml
vars:
  # Kafka configuration
  kafka_bootstrap_servers: 'redpanda:9092'

# Model execution order updated:
# 1. src_*.sql (sources) - Kafka sources (append-only)
# 2. tbl_*.sql (kafka_table) - Kafka tables (modifiable)
# 3. funnel.sql (from sources)
# 4. funnel_from_tables.sql (from tables)
```

## Troubleshooting

### Table Already Exists

If you get "table already exists" errors during dbt run:

```sql
-- Drop tables manually
DROP TABLE IF EXISTS tbl_cart CASCADE;
DROP TABLE IF EXISTS tbl_page CASCADE;
DROP TABLE IF EXISTS tbl_purchase CASCADE;
DROP MATERIALIZED VIEW IF EXISTS funnel_from_tables CASCADE;
```

Then re-run dbt:
```bash
dbt run
```

### No Data in Tables

Tables start consuming from `earliest` offset by default. If no data appears:

```sql
-- Check if Kafka has data
SELECT count(*) FROM src_cart;  -- Should show data from source
SELECT count(*) FROM tbl_cart;  -- Should show data from table
```

If the source has data but table doesn't, check the Kafka consumer group.

### UPDATE Not Reflecting in Funnel

```sql
-- Check if materialized view is updating
SELECT * FROM funnel_from_tables
ORDER BY window_start DESC
LIMIT 5;

-- Force a flush (if needed)
FLUSH;
```

## Iceberg Sink Demo

The `sink_funnel_from_tables_to_iceberg` model creates an upsert sink from `funnel_from_tables` to Iceberg, demonstrating how UPDATE/DELETE operations propagate to your data lake.

### Architecture

```
tbl_* tables ──▶ funnel_from_tables MV ──▶ Iceberg Sink ──▶ Iceberg Table
      │                                            │
      └── UPDATE/DELETE ──▶ Updates ───────────────▶ Updates Iceberg
```

### Demo Steps

**Step 1: Check current state in RisingWave**
```sql
SELECT * FROM funnel_from_tables ORDER BY window_start DESC LIMIT 5;
```

**Step 2: Check current state in Iceberg (via Trino)**
```bash
psql -h localhost -p 8080 -d risingwave
```
```sql
SELECT * FROM datalake.public.funnel_from_tables ORDER BY window_start DESC LIMIT 5;
```

**Step 3: Modify source data in RisingWave**
```sql
UPDATE tbl_purchase SET amount = 999.99 WHERE user_id = 100;
```

**Step 4: Watch funnel_from_tables update**
```sql
SELECT * FROM funnel_from_tables ORDER BY window_start DESC LIMIT 5;
```

**Step 5: Watch Iceberg update (in Trino)**
```sql
SELECT * FROM datalake.public.funnel_from_tables ORDER BY window_start DESC LIMIT 5;
```

The upsert sink ensures Iceberg stays in sync with the MV changes!

### How It Works

- **UPDATE on tbl_*** → Updates `funnel_from_tables` → Upserts to Iceberg (updates existing rows)
- **DELETE on tbl_*** → Updates `funnel_from_tables` → Deletes from Iceberg (removes rows)
- **INSERT on tbl_*** → Updates `funnel_from_tables` → Inserts to Iceberg (new rows)

The sink uses `window_start` as the primary key for upsert semantics.

---

## Scenario 5: Time Travel Queries (RisingWave + Iceberg)

This scenario demonstrates **time travel queries** - a powerful feature where you can query historical data at a specific point in time, even after records have been deleted or updated.

### What is Time Travel?

Time travel queries allow you to:
- View data as it existed at a specific timestamp
- Audit changes over time
- Recover accidentally deleted data
- Compare historical vs current state

> **Note:** This scenario primarily demonstrates **Iceberg time travel via Trino**, which is available without license restrictions. RisingWave also supports time travel on internal tables, but it requires a valid license.
>
> | Engine | Timestamp Syntax | Version Syntax | License Required |
> |--------|------------------|----------------|------------------|
> | **RisingWave** | `FOR SYSTEM_TIME AS OF '...'` | `FOR SYSTEM_VERSION AS OF <id>` | ✅ Yes |
> | **Trino** | `FOR TIMESTAMP AS OF TIMESTAMP '...'` | `FOR VERSION AS OF <id>` | ❌ No |
>
> Iceberg time travel works via both RisingWave (as an Iceberg source) and Trino.

### Architecture

```
┌─────────────────────────────────────────────────────────────────┐
│                      CURRENT STATE                              │
│  ┌─────────────┐    ┌──────────────┐    ┌─────────────────┐    │
│  │ tbl_page    │───▶│ funnel_from  │───▶│ Iceberg Table   │    │
│  │ (current)   │    │ _tables      │    │ (current)       │    │
│  └─────────────┘    └──────────────┘    └─────────────────┘    │
│         │                                                       │
│         │ DELETE user_id=123                                    │
│         ▼                                                       │
│  ┌─────────────┐    ┌──────────────┐    ┌─────────────────┐    │
│  │ tbl_page    │───▶│ funnel_from  │───▶│ Iceberg Table   │    │
│  │ (minus      │    │ _tables      │    │ (minus deleted) │    │
│  │ deleted)    │    │ (updated)    │    │                 │    │
│  └─────────────┘    └──────────────┘    └─────────────────┘    │
│                                                                 │
└─────────────────────────────────────────────────────────────────┘
                              │
                              │ TIME TRAVEL (Snapshot from before DELETE)
                              ▼
┌─────────────────────────────────────────────────────────────────┐
│                    HISTORICAL STATE                             │
│  ┌─────────────────────────────────────────────────────────┐   │
│  │  Iceberg Table @ Timestamp T1 (before deletion)         │   │
│  │  - Still contains user_id=123 records                   │   │
│  │  - Query via: SELECT * FROM table FOR SYSTEM_TIME AS    │   │
│  │    OF TIMESTAMP 'T1'                                    │   │
│  └─────────────────────────────────────────────────────────┘   │
└─────────────────────────────────────────────────────────────────┘
```

### Demo Steps

**Step 1: Identify a user with data to delete**

```sql
-- Find a user_id that has data in all three tables
SELECT user_id, COUNT(*) as page_views
FROM tbl_page
GROUP BY user_id
HAVING COUNT(*) > 0
ORDER BY page_views DESC
LIMIT 5;
```

Make note of a user_id (e.g., `123`).

**Step 2: Check current state in RisingWave before deletion**

```sql
-- Record the current timestamp (we'll need this for time travel)
SELECT NOW() as before_delete_timestamp;

-- View current state of this user's data in RisingWave funnel
SELECT * FROM funnel_from_tables
WHERE viewers >= (
    SELECT COUNT(DISTINCT user_id)
    FROM tbl_page
    WHERE user_id = 123
)
ORDER BY window_start DESC
LIMIT 5;

-- Count how many page views this user has
SELECT COUNT(*) as user_page_views FROM tbl_page WHERE user_id = 123;
SELECT COUNT(*) as user_cart_events FROM tbl_cart WHERE user_id = 123;
SELECT COUNT(*) as user_purchases FROM tbl_purchase WHERE user_id = 123;
```

**Step 3: Check current state in Iceberg before deletion**

Connect via Trino:
```bash
psql -h localhost -p 8080 -d risingwave
```

```sql
-- Check the Iceberg table current snapshot
SELECT COUNT(*) as total_windows FROM datalake.public.funnel_from_tables;

-- Get the latest snapshot timestamp
SELECT * FROM datalake.public.funnel_from_tables
ORDER BY window_start DESC
LIMIT 5;
```

> **Note:** If `USE datalake.public` doesn't work in your client, use fully qualified names like `datalake.public.funnel_from_tables`.

**Step 4: Delete the user's data from RisingWave tables**

```sql
-- Delete from all three tables (this simulates removing a user from analytics)
DELETE FROM tbl_page WHERE user_id = 123;
DELETE FROM tbl_cart WHERE user_id = 123;
DELETE FROM tbl_purchase WHERE user_id = 123;

-- Record timestamp after deletion
SELECT NOW() as after_delete_timestamp;
```

**Step 5: Verify deletion in RisingWave**

```sql
-- Confirm data is gone from source tables
SELECT COUNT(*) as remaining_page_views FROM tbl_page WHERE user_id = 123;
-- Should return 0

-- Check that funnel_from_tables has been updated
SELECT * FROM funnel_from_tables
ORDER BY window_start DESC
LIMIT 10;

-- Compare total counts before/after
SELECT
    'tbl_page' as table_name,
    COUNT(*) as current_count
FROM tbl_page
UNION ALL
SELECT 'tbl_cart', COUNT(*) FROM tbl_cart
UNION ALL
SELECT 'tbl_purchase', COUNT(*) FROM tbl_purchase;
```

**Step 6: Verify deletion propagated to Iceberg**

Connect via Trino:
```bash
psql -h localhost -p 8080 -d risingwave
```

Set the schema:
```sql
USE datalake.public;
```

Then query:
```sql
-- Check that the funnel metrics have changed
SELECT * FROM funnel_from_tables
ORDER BY window_start DESC
LIMIT 10;

-- The viewer counts should reflect the deletion
```

**Step 7: Time Travel - Query historical state before deletion**

You can query the historical state using Trino (recommended) or RisingWave (requires license):

**Option A: Time Travel in Trino (Iceberg) - Recommended**

This is the primary method for time travel in this demo - it works without any license restrictions.

```sql
-- First, set the catalog and schema
USE datalake.public;

-- Query as of a specific timestamp
SELECT * FROM funnel_from_tables
FOR TIMESTAMP AS OF TIMESTAMP '2026-03-22 17:20:13'
ORDER BY window_start DESC
LIMIT 10;

-- Get available snapshots first
SELECT * FROM "funnel_from_tables$snapshots";

-- Query by snapshot ID (use BIGINT format)
SELECT * FROM funnel_from_tables
FOR VERSION AS OF 1234567890123456789
ORDER BY window_start DESC
LIMIT 10;
```

**Option B: Time Travel in RisingWave (Requires License)**

> ⚠️ **License Required:** RisingWave's time travel feature requires a valid license key. Without it, you'll get an error: `feature TimeTravel is not available due to license error`.

If you have a valid license, you can query historical state in RisingWave:

```sql
-- Query tbl_page as it existed before deletion
SELECT * FROM tbl_page
FOR SYSTEM_TIME AS OF '2026-03-22 17:20:13'
WHERE user_id = 123;

-- Query funnel_from_tables at a specific timestamp
SELECT * FROM funnel_from_tables
FOR SYSTEM_TIME AS OF '2026-03-22 17:20:13'
ORDER BY window_start DESC
LIMIT 10;

-- Query by snapshot ID
SELECT * FROM funnel_from_tables
FOR SYSTEM_VERSION AS OF 1234567890
ORDER BY window_start DESC
LIMIT 10;
```

**Step 8: Compare current vs historical state**

**In Trino (Recommended):**
```sql
USE datalake.public;

-- Current state (after deletion)
SELECT window_start, viewers, carters, purchasers
FROM funnel_from_tables
WHERE window_start >= TIMESTAMP '2026-03-22 17:00:00'
ORDER BY window_start DESC
LIMIT 5;

-- Historical state (before deletion)
SELECT window_start, viewers, carters, purchasers
FROM funnel_from_tables
FOR TIMESTAMP AS OF TIMESTAMP '2026-03-22 17:20:13'
WHERE window_start >= TIMESTAMP '2026-03-22 17:00:00'
ORDER BY window_start DESC
LIMIT 5;
```

**Expected Result:**
The historical query will show higher viewer counts than the current query, because user `123`'s data was still present before the deletion.

### Key Observations

1. **RisingWave tables are mutable but versioned** - DELETE operations immediately remove data from `tbl_page`, but historical versions are accessible via time travel queries

2. **Iceberg is immutable with snapshots** - DELETE operations create new snapshots, but old data remains accessible

3. **Both support time travel** - RisingWave via `FOR SYSTEM_TIME AS OF`, Iceberg via both RisingWave and Trino

4. **Snapshot isolation** - Each change creates a new snapshot, preserving full history in Iceberg; RisingWave maintains versions based on retention settings

### Time Travel Query Patterns

#### Trino/Iceberg Syntax (No License Required)

```sql
-- Pattern 1: Query as of a specific timestamp
SELECT * FROM datalake.public.funnel_from_tables
FOR TIMESTAMP AS OF TIMESTAMP '2024-01-15 10:30:00';

-- Pattern 2: Query as of a specific snapshot ID (BIGINT required)
SELECT * FROM datalake.public.funnel_from_tables
FOR VERSION AS OF 3023402865675048688;

-- Pattern 3: Compare two historical points
SELECT
    current.viewers as viewers_now,
    historical.viewers as viewers_then,
    current.viewers - historical.viewers as viewers_diff
FROM datalake.public.funnel_from_tables current
JOIN datalake.public.funnel_from_tables
    FOR TIMESTAMP AS OF TIMESTAMP '2024-01-15 10:30:00' historical
    ON current.window_start = historical.window_start
WHERE current.window_start >= TIMESTAMP '2024-01-15 10:00:00';

-- Pattern 4: List all available snapshots (quoted for special characters)
SELECT snapshot_id, committed_at, operation
FROM "datalake.public.funnel_from_tables$snapshots"
ORDER BY committed_at DESC;
```

#### RisingWave Syntax (License Required)

> ⚠️ **License Required:** These queries require a valid RisingWave license.

```sql
-- Pattern 1: Query as of a specific timestamp
SELECT * FROM funnel_from_tables
FOR SYSTEM_TIME AS OF '2024-01-15 10:30:00';

-- Pattern 2: Query as of a specific snapshot ID
SELECT * FROM funnel_from_tables
FOR SYSTEM_VERSION AS OF 1234567890;

-- Pattern 3: Query relative time (e.g., 5 minutes ago)
SELECT * FROM tbl_page
FOR SYSTEM_TIME AS OF NOW() - INTERVAL '5 minutes';

-- Pattern 4: Compare current vs historical in one query
SELECT
    (SELECT COUNT(*) FROM funnel_from_tables) as current_count,
    (SELECT COUNT(*) FROM funnel_from_tables
     FOR SYSTEM_TIME AS OF NOW() - INTERVAL '1 hour') as count_1_hour_ago;
```

First, set the schema:
```sql
USE datalake.public;
```

Then run your queries:
```sql
-- Pattern 1: Query as of a specific timestamp
SELECT * FROM funnel_from_tables
FOR TIMESTAMP AS OF TIMESTAMP '2024-01-15 10:30:00';

-- Pattern 2: Query as of a specific snapshot ID
SELECT * FROM funnel_from_tables
FOR VERSION AS OF 1234567890;

-- Pattern 3: Compare two historical points
SELECT
    current.viewers as viewers_now,
    historical.viewers as viewers_then,
    current.viewers - historical.viewers as viewers_diff
FROM funnel_from_tables current
JOIN funnel_from_tables
    FOR TIMESTAMP AS OF TIMESTAMP '2024-01-15 10:30:00' historical
    ON current.window_start = historical.window_start
WHERE current.window_start >= TIMESTAMP '2024-01-15 10:00:00';

-- Pattern 4: List all available snapshots (quoted for special characters)
SELECT snapshot_id, committed_at, operation
FROM "funnel_from_tables$snapshots"
ORDER BY committed_at DESC;
```

### Practical Use Cases

| Use Case | RisingWave | Trino/Iceberg |
|----------|------------|---------------|
| **Audit compliance** | Query historical snapshots via `FOR SYSTEM_TIME AS OF` | Query `"funnel_from_tables$snapshots"` |
| **Data recovery** | `SELECT * FROM tbl_page FOR SYSTEM_TIME AS OF '<timestamp>'` | `SELECT * FROM funnel_from_tables FOR TIMESTAMP AS OF TIMESTAMP '...'` |
| **Trend analysis** | Compare `NOW()` vs `NOW() - INTERVAL '1 hour'` | Join current vs historical snapshots |
| **Debugging** | Query MV state at incident time | Query Iceberg at specific snapshot |
| **Regulatory reporting** | Generate reports as of fiscal quarter end | Same, with Iceberg as source of truth |

### Supported Time Formats

| Format | RisingWave | Trino |
|--------|------------|-------|
| ISO 8601 datetime | ✅ `'2024-01-15T10:30:00'` | ✅ `TIMESTAMP '2024-01-15 10:30:00'` |
| With timezone | ✅ `'2024-01-15T10:30:00+00:00'` | ✅ `TIMESTAMP '2024-01-15 10:30:00+00:00'` |
| Unix timestamp | ✅ `1705315800` | ❌ |
| Relative time | ✅ `NOW() - INTERVAL '5 minutes'` | ❌ |

### Summary

This scenario demonstrates the **powerful time travel capabilities** of both RisingWave and Iceberg:

| Feature | RisingWave | Iceberg (via Trino) |
|---------|------------|---------------------|
| **Time Travel Syntax** | `FOR SYSTEM_TIME AS OF` | `FOR TIMESTAMP AS OF` |
| **Version Syntax** | `FOR SYSTEM_VERSION AS OF` | `FOR VERSION AS OF` |
| **Relative Time** | ✅ `NOW() - INTERVAL` | ❌ |
| **Unix Timestamp** | ✅ Supported | ❌ |
| **Snapshot Metadata** | Via system tables | `"$snapshots"` table |

**Key Insight**: You can query historical data at multiple levels:
1. **RisingWave tables** (`tbl_page`, `funnel_from_tables`) - for recent changes within retention window
2. **Iceberg via RisingWave source** - for Iceberg tables with time travel
3. **Iceberg via Trino** - for data lake analytics with full snapshot history

Together, they provide a complete solution for operational flexibility (modify data in real-time) and auditability (query any historical state).

---

## Best Practices

1. **Use PRIMARY KEY wisely**: The tables use `user_id` as PRIMARY KEY, meaning inserts with the same user_id will upsert (update existing or insert new).

2. **Monitor storage**: Tables store data internally, unlike sources. Monitor disk usage in production.

3. **Test changes**: Always test UPDATE/DELETE operations in a non-production environment first.

4. **Backup before bulk operations**: For large updates, consider the impact on downstream MVs.

## Next Steps

1. Start the stack: `./bin/1_up.sh`
2. Run dbt: `cd dbt && dbt run`
3. Start the producer: `./bin/3_run_producer.sh`
4. Try the UPDATE/DELETE examples in this guide
5. Watch the `funnel_from_tables` MV update in real-time
6. **Try the Time Travel demo** - Delete data and query historical snapshots via Trino
