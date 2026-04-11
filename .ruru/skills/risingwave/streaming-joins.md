# RisingWave Streaming Joins

Perform real-time joins between streams and tables with automatic incremental maintenance.

## Supported Join Types

| Join Type | Syntax | Use Case |
|-----------|--------|----------|
| Inner Join | `JOIN` | Matching records only |
| Left Join | `LEFT JOIN` | All left side, matched right |
| Right Join | `RIGHT JOIN` | All right side, matched left |
| Full Outer | `FULL OUTER JOIN` | All records both sides |
| Cross Join | `CROSS JOIN` | Cartesian product |

## Time-Constrained Joins

Join streams with time window constraints for state cleanup:

```sql
SELECT 
    a.*, 
    b.*
FROM stream_a a
JOIN stream_b b
    ON a.user_id = b.user_id
    AND b.event_time BETWEEN a.event_time - INTERVAL '5 MINUTE' 
                         AND a.event_time + INTERVAL '5 MINUTE';
```

## Examples

### Basic Stream-Table Join
```sql
-- Join orders with customer reference data
CREATE MATERIALIZED VIEW enriched_orders AS
SELECT 
    o.order_id,
    o.amount,
    o.customer_id,
    c.name AS customer_name,
    c.tier AS customer_tier
FROM orders o
JOIN customers c ON o.customer_id = c.customer_id;
```

### Stream-Stream Join (Time Windowed)
```sql
-- Match page views with cart events within 1 minute
CREATE MATERIALIZED VIEW page_to_cart_funnel AS
SELECT 
    p.user_id,
    p.page_id,
    c.item_id,
    p.event_time as view_time,
    c.event_time as cart_time
FROM page_views p
LEFT JOIN cart_events c
    ON p.user_id = c.user_id
    AND c.event_time BETWEEN p.event_time 
                         AND p.event_time + INTERVAL '1 MINUTE';
```

### Multi-Way Join
```sql
-- Conversion funnel: views → cart → purchases
CREATE MATERIALIZED VIEW conversion_funnel AS
SELECT 
    p.window_start,
    p.window_end,
    COUNT(DISTINCT p.user_id) as viewers,
    COUNT(DISTINCT c.user_id) as carters,
    COUNT(DISTINCT pur.user_id) as purchasers
FROM TUMBLE(page_views, event_time, INTERVAL '1 MINUTE') p
LEFT JOIN cart_events c
    ON p.user_id = c.user_id
    AND c.event_time BETWEEN p.window_start AND p.window_end
LEFT JOIN purchases pur
    ON p.user_id = pur.user_id
    AND pur.event_time BETWEEN p.window_start AND p.window_end
GROUP BY p.window_start, p.window_end;
```

### Temporal Join (AS OF)
```sql
-- Join with slowly changing dimension
SELECT 
    o.*,
    r.region_name,
    r.tax_rate
FROM orders o
JOIN regions FOR SYSTEM_TIME AS OF o.proc_time r
    ON o.region_code = r.code;
```

## Join State Management

RisingWave maintains join state in memory for efficient processing:

```
┌──────────────┐      Join State       ┌──────────────┐
│  Left Side   │◄─────────────────────►│  Right Side  │
│   (orders)   │    (hash indexes)     │  (customers) │
└──────┬───────┘                       └───────┬──────┘
       │                                       │
       └──────────────┬────────────────────────┘
                      ▼
               Join Result
```

## Performance Optimization

1. **Filter before join** - Reduce state size:
   ```sql
   -- Good: Filter first
   SELECT * FROM (SELECT * FROM orders WHERE status = 'completed') o
   JOIN customers c ON o.customer_id = c.customer_id;
   ```

2. **Use appropriate time windows** - Clean up old state:
   ```sql
   -- State expires after 10 minutes
   AND b.ts BETWEEN a.ts - INTERVAL '10 MINUTE' AND a.ts
   ```

3. **Minimize join keys** - Simpler keys = faster lookups

4. **Monitor state size**:
   ```sql
   SELECT * FROM rw_catalog.rw_join_state_size;
   ```

## Common Patterns

### Enrichment Pattern
```sql
-- Enrich events with reference data
CREATE MATERIALIZED VIEW enriched_events AS
SELECT 
    e.*,
    u.name,
    u.department
FROM events e
LEFT JOIN users u ON e.user_id = u.user_id;
```

### Deduplication Pattern
```sql
-- Keep only latest record per key
CREATE MATERIALIZED VIEW deduplicated AS
SELECT 
    user_id,
    MAX_BY(status, event_time) as latest_status,
    MAX_BY(amount, event_time) as latest_amount
FROM events
GROUP BY user_id;
```

### Sessionization Pattern
```sql
-- Group events into sessions
CREATE MATERIALIZED VIEW sessions AS
SELECT 
    user_id,
    session_id,
    MIN(event_time) as session_start,
    MAX(event_time) as session_end,
    COUNT(*) as event_count
FROM (
    SELECT *,
        SUM(new_session_flag) OVER (PARTITION BY user_id ORDER BY event_time) as session_id
    FROM (
        SELECT *,
            CASE WHEN event_time - LAG(event_time) OVER (PARTITION BY user_id ORDER BY event_time) 
                 > INTERVAL '30 MINUTE' 
                 THEN 1 ELSE 0 END as new_session_flag
        FROM events
    )
)
GROUP BY user_id, session_id;
```

## Related Skills

- `risingwave/materialized-view` - Store join results
- `risingwave/cascading-materialized-views` - Multi-stage joins
- `risingwave/tumble-window` - Time-based joins
