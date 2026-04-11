# RisingWave Backfill Control

Manage how RisingWave backfills historical data when creating materialized views.

## Background DDL

Run CREATE MATERIALIZED VIEW without blocking:

```sql
-- Enable background execution
SET BACKGROUND_DDL = true;

-- Create view (returns immediately)
CREATE MATERIALIZED VIEW mv_large_table AS
SELECT * FROM large_table WHERE ...;

-- Check backfill progress
SELECT * FROM rw_catalog.rw_fragment_backfill_progress;
```

## Backfill Order

Control the sequence of backfilling multiple upstream tables:

```sql
CREATE MATERIALIZED VIEW mv_complex
WITH (
    backfill_order = FIXED(
        table1 -> table2,  -- table1 finishes before table2
        table2 -> table3   -- table2 finishes before table3
    )
)
AS SELECT * FROM table1 
   UNION ALL 
   SELECT * FROM table2 
   UNION ALL 
   SELECT * FROM table3;
```

## Serverless Backfill

Offload backfill to dedicated backfiller nodes (RisingWave Cloud):

```sql
-- For current session
SET enable_serverless_backfill = true;

-- Or for single statement
CREATE MATERIALIZED VIEW mv_name
WITH (cloud.serverless_backfill_enabled = true)
AS SELECT ...;
```

**Note**: Not supported for databases in non-default resource groups.

## Snapshot Backfill

Enabled by default for isolation between backfill and streaming:

```sql
-- Disable if needed
SET streaming_use_snapshot_backfill = false;
```

## Monitoring Backfill

```sql
-- View progress
SELECT 
    fragment_id,
    table_name,
    backfill_progress,
    total_rows,
    processed_rows
FROM rw_catalog.rw_fragment_backfill_progress;

-- Inspect fragment structure
DESCRIBE FRAGMENTS <mv_name>;

-- Visualize backfill order
EXPLAIN (backfill, format dot) 
CREATE MATERIALIZED VIEW ...;
```

## WITH Clause Options

| Option | Description | Example |
|--------|-------------|---------|
| `backfill_order` | Control backfill sequence | `FIXED(t1 -> t2)` |
| `source_rate_limit` | Throttle ingestion | `1000` (rows/sec) |
| `cloud.serverless_backfill_enabled` | Use backfiller nodes | `true` |

## Examples

### Throttle Backfill
```sql
CREATE MATERIALIZED VIEW mv_throttled
WITH (source_rate_limit = 10000)  -- 10k rows/sec max
AS SELECT * FROM high_volume_source;
```

### Complex Pipeline with Dependencies
```sql
CREATE MATERIALIZED VIEW mv_pipeline
WITH (
    backfill_order = FIXED(
        raw_events -> cleaned_events,
        cleaned_events -> aggregated_metrics
    )
)
AS SELECT ... FROM aggregated_metrics ...;
```

## Troubleshooting

### Backfill Too Slow
1. Check `source_rate_limit` - may be throttling
2. Enable serverless backfill for more resources
3. Reduce data with filters in the query

### Out of Memory During Backfill
1. Reduce `source_rate_limit`
2. Add time-based filters
3. Process in smaller chunks

### Backfill Order Recovery
If a backfilling job restarts, specified order is lost and all tables backfill concurrently. For critical ordering, monitor and restart manually if needed.

## Related Skills

- `risingwave/materialized-view` - Creating views with backfill
- `risingwave/cascading-materialized-views` - Multi-table backfill
