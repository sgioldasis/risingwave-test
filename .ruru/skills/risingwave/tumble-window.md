# RisingWave TUMBLE Window

Use TUMBLE windows for time-based aggregations over fixed, non-overlapping time intervals.

## Usage

```sql
SELECT
    window_start,
    window_end,
    <aggregations>
FROM TUMBLE(<source>, <timestamp_column>, INTERVAL '<duration>')
GROUP BY window_start, window_end;
```

## Parameters

| Parameter | Description | Example |
|-----------|-------------|---------|
| `source` | Input source or table | `src_page` |
| `timestamp_column` | Timestamp column for windowing | `event_time` |
| `duration` | Window size interval | `'1 MINUTE'`, `'20 SECOND'` |

## Window Functions

- `TUMBLE()` - Fixed-size, non-overlapping windows
- `HOP()` - Sliding windows with hop size
- `SESSION()` - Session windows (gaps of inactivity)

## Examples

### Basic TUMBLE Window (1 minute)
```sql
SELECT
    window_start,
    window_end,
    count(*) as event_count,
    count(distinct user_id) as unique_users
FROM TUMBLE(src_page, event_time, INTERVAL '1 MINUTE')
GROUP BY window_start, window_end;
```

### TUMBLE with Joins (Conversion Funnel)
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

### Multiple Window Sizes
```sql
-- 20-second windows for fast updates
SELECT
    window_start,
    window_end,
    count(*) as events
FROM TUMBLE(src_page, event_time, INTERVAL '20 SECOND')
GROUP BY window_start, window_end;

-- 5-minute windows for trend analysis
SELECT
    window_start,
    window_end,
    avg(amount) as avg_amount
FROM TUMBLE(src_purchase, event_time, INTERVAL '5 MINUTE')
GROUP BY window_start, window_end;
```

## Window Behavior

```
Time →  00:00  00:01  00:02  00:03  00:04  00:05
        ├──────┤      ├──────┤      ├──────┤
Window  │  W1  │      │  W2  │      │  W3  │
Events    ●  ●    ●      ●  ●  ●      ●
```

- Windows are **non-overlapping**
- Each event belongs to exactly **one window**
- Windows are based on **event time** (not processing time)

## Window Columns

When using `TUMBLE()`, these columns are automatically available:
- `window_start` - Start timestamp of the window
- `window_end` - End timestamp of the window (exclusive)

## Best Practices

1. **Choose appropriate window sizes**:
   - Small windows (10-30s) for real-time dashboards
   - Large windows (1-5min) for trend analysis

2. **Handle late data** with watermarks:
   ```sql
   -- Set watermark for late event handling
   CREATE SOURCE src_page (
       user_id int,
       event_time timestamp,
       WATERMARK FOR event_time AS event_time - INTERVAL '5 SECOND'
   ) ...
   ```

3. **Use `nullif()` for safe division** to avoid division by zero:
   ```sql
   round(carters::numeric / nullif(viewers, 0), 2) as conversion_rate
   ```

## Related Skills

- `risingwave/kafka-source` - Data input for windowing
- `risingwave/materialized-view` - Store windowed results
- `risingwave/kafka-sink` - Stream window results
