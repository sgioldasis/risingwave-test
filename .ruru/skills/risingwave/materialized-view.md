# RisingWave Materialized View

Create a materialized view in RisingWave for real-time, incrementally updated query results.

## Usage

```sql
CREATE MATERIALIZED VIEW <view_name> AS
SELECT <columns>
FROM <source_or_table>
[WHERE <conditions>]
[GROUP BY <grouping_columns>];
```

## Description

Materialized views in RisingWave are automatically updated in real-time as new data arrives. Unlike traditional databases, RisingWave maintains the view incrementally without full recomputation.

## Examples

### Basic Materialized View
```sql
CREATE MATERIALIZED VIEW user_counts AS
SELECT 
    user_id,
    COUNT(*) as event_count
FROM src_page
GROUP BY user_id;
```

### Conversion Funnel View
```sql
CREATE MATERIALIZED VIEW funnel AS
WITH stats AS (
    SELECT
        window_start,
        window_end,
        count(distinct p.user_id) as viewers,
        count(distinct c.user_id) as carters,
        count(distinct pur.user_id) as purchasers
    FROM TUMBLE(src_page, event_time, INTERVAL '1 MINUTE') p
    LEFT JOIN src_cart c
        ON p.user_id = c.user_id
        AND c.event_time BETWEEN p.window_start AND p.window_end
    LEFT JOIN src_purchase pur
        ON p.user_id = pur.user_id
        AND pur.event_time BETWEEN p.window_start AND p.window_end
    GROUP BY window_start, window_end
)
SELECT
    window_start,
    window_end,
    viewers,
    carters,
    purchasers,
    round(carters::numeric / nullif(viewers, 0), 2) as view_to_cart_rate,
    round(purchasers::numeric / nullif(carters, 0), 2) as cart_to_buy_rate
FROM stats;
```

### Aggregated Metrics View
```sql
CREATE MATERIALIZED VIEW funnel_summary AS
SELECT
    window_start,
    window_end,
    count(*) as total_events,
    sum(amount) as total_revenue
FROM src_purchase
GROUP BY window_start, window_end;
```

## Key Features

1. **Automatic Updates** - Views update in real-time as source data changes
2. **Incremental Computation** - Only changed data is processed
3. **Queryable** - Materialized views can be queried like tables
4. **Composable** - Views can reference other views

## Performance Tips

- Use appropriate window sizes for your use case (smaller windows = more frequent updates)
- Index key columns for faster lookups
- Use `EMIT ON WINDOW CLOSE` for watermarked streams

## Related Skills

- `risingwave/kafka-source` - Input data for views
- `risingwave/tumble-window` - Time-based windowing
- `risingwave/kafka-sink` - Output view results to Kafka
- `risingwave/iceberg-sink` - Persist views to Iceberg
