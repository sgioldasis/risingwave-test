# RisingWave Cascading Materialized Views

Create pipelines of materialized views where one view references another, enabling complex data transformation workflows.

## Concept

```
Source Table
     ↓
MV Level 1 (Raw aggregations)
     ↓
MV Level 2 (Filtered/transformed)
     ↓
MV Level 3 (Business metrics)
```

RisingWave automatically maintains the entire chain - when source data changes, all dependent views update incrementally.

## Usage

```sql
-- Level 1: Base aggregation
CREATE MATERIALIZED VIEW mv_level_1 AS
SELECT ... FROM source_table GROUP BY ...;

-- Level 2: Reference Level 1
CREATE MATERIALIZED VIEW mv_level_2 AS
SELECT ... FROM mv_level_1 WHERE ...;

-- Level 3: Reference Level 2
CREATE MATERIALIZED VIEW mv_level_3 AS
SELECT ... FROM mv_level_2 ...;
```

## Examples

### E-Commerce Analytics Pipeline

```sql
-- Level 1: Order summary by status
CREATE MATERIALIZED VIEW mv_order_summary AS
SELECT 
    status,
    COUNT(*) AS order_count,
    SUM(amount) AS total_amount
FROM orders
GROUP BY status;

-- Level 2: Revenue by region (filters completed orders)
CREATE MATERIALIZED VIEW mv_revenue_by_region AS
SELECT 
    region,
    COUNT(*) AS completed_orders,
    SUM(amount) AS revenue
FROM orders
WHERE status = 'completed'
GROUP BY region;

-- Level 3: Top performing region
CREATE MATERIALIZED VIEW mv_top_region AS
SELECT 
    region, 
    revenue
FROM mv_revenue_by_region
ORDER BY revenue DESC
LIMIT 1;
```

### Customer Tier Analysis Pipeline

```sql
-- Source: Orders and Customers tables
CREATE TABLE orders (
    order_id INT,
    customer_id INT,
    amount DECIMAL,
    status VARCHAR,
    created_at TIMESTAMP
);

CREATE TABLE customers (
    customer_id INT,
    name VARCHAR,
    tier VARCHAR  -- gold, silver, bronze
);

-- Level 1: Join orders with customers
CREATE MATERIALIZED VIEW mv_customer_orders AS
SELECT 
    o.*,
    c.name,
    c.tier
FROM orders o
JOIN customers c ON o.customer_id = c.customer_id;

-- Level 2: Spending by tier
CREATE MATERIALIZED VIEW mv_spending_by_tier AS
SELECT 
    tier,
    COUNT(*) AS order_count,
    SUM(amount) AS total_spent
FROM mv_customer_orders
GROUP BY tier;

-- Level 3: Tier performance ranking
CREATE MATERIALIZED VIEW mv_tier_ranking AS
SELECT 
    tier,
    total_spent,
    RANK() OVER (ORDER BY total_spent DESC) as revenue_rank
FROM mv_spending_by_tier;
```

## Best Practices

1. **Keep chains shallow** - 2-3 levels typical, deeper chains add overhead
2. **Filter early** - Reduce data volume at each level
3. **Monitor dependencies** - Use `DESCRIBE FRAGMENTS <mv_name>` to inspect
4. **Backfill control** - Use `backfill_order` for complex pipelines:

```sql
CREATE MATERIALIZED VIEW mv_pipeline
WITH (backfill_order = FIXED(orders -> customers -> mv_pipeline))
AS SELECT ...;
```

## Performance Considerations

| Factor | Impact |
|--------|--------|
| Number of groups | High cardinality = more memory |
| Join state | Both sides kept in memory |
| Cascading depth | Each level adds processing overhead |
| Filter placement | Filter early to reduce downstream state |

## Monitoring

```sql
-- Check backfill progress
SELECT * FROM rw_catalog.rw_fragment_backfill_progress;

-- Inspect view structure
DESCRIBE FRAGMENTS <mv_name>;

-- Visualize backfill order
EXPLAIN (backfill, format dot) CREATE MATERIALIZED VIEW ...;
```

## Related Skills

- `risingwave/materialized-view` - Base materialized views
- `risingwave/kafka-source` - Data ingestion
- `risingwave/streaming-joins` - Join operations in pipelines
