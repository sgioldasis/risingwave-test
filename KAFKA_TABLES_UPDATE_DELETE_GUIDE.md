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
SELECT * FROM iceberg.public.funnel_from_tables ORDER BY window_start DESC LIMIT 5;
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
SELECT * FROM iceberg.public.funnel_from_tables ORDER BY window_start DESC LIMIT 5;
```

The upsert sink ensures Iceberg stays in sync with the MV changes!

### How It Works

- **UPDATE on tbl_*** → Updates `funnel_from_tables` → Upserts to Iceberg (updates existing rows)
- **DELETE on tbl_*** → Updates `funnel_from_tables` → Deletes from Iceberg (removes rows)
- **INSERT on tbl_*** → Updates `funnel_from_tables` → Inserts to Iceberg (new rows)

The sink uses `window_start` as the primary key for upsert semantics.

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
