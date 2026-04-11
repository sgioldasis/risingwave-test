# RisingWave Kafka Sink

Create a Kafka sink to publish RisingWave query results to Kafka topics in real-time.

## Usage

```sql
CREATE SINK IF NOT EXISTS <sink_name>
FROM <source_or_view>
WITH (
    connector = 'kafka',
    properties.bootstrap.server = '<broker_host>:<port>',
    topic = '<topic_name>'
)
FORMAT PLAIN ENCODE JSON (
    force_append_only = '<boolean>'
);
```

## Parameters

| Parameter | Description | Example |
|-----------|-------------|---------|
| `connector` | Must be 'kafka' | `'kafka'` |
| `properties.bootstrap.server` | Kafka broker address | `'redpanda:9092'` |
| `topic` | Target Kafka topic | `'funnel'` |
| `force_append_only` | Force append-only output | `'true'` or `'false'` |

## Examples

### Basic Kafka Sink
```sql
CREATE SINK IF NOT EXISTS funnel_kafka_sink
FROM funnel_summary
WITH (
    connector = 'kafka',
    properties.bootstrap.server = 'redpanda:9092',
    topic = 'funnel'
)
FORMAT PLAIN ENCODE JSON (
    force_append_only = 'true'
);
```

### Sink with Upsert (for changelog streams)
```sql
CREATE SINK IF NOT EXISTS user_updates_sink
FROM user_materialized_view
WITH (
    connector = 'kafka',
    properties.bootstrap.server = 'redpanda:9092',
    topic = 'user_updates'
)
FORMAT UPSERT ENCODE JSON (
    force_append_only = 'false'
);
```

## force_append_only Option

- `'true'` - Only emit insert operations (no retractions)
- `'false'` - Emit all changes including retractions and updates

Use `'true'` when downstream consumers expect append-only streams (e.g., time-series data).

## Verification

Check if sink is running:
```sql
SHOW SINKS;
```

Drop and recreate a sink:
```sql
DROP SINK funnel_kafka_sink;
-- Then recreate with CREATE SINK
```

## Architecture Pattern

```
┌─────────────────┐     ┌─────────────────┐     ┌─────────────────┐
│   Kafka Sources │────▶│    RisingWave   │     │                 │
│                 │     │                 │     │                 │
│  (page_views,   │     │  Materialized   │────▶│  Kafka Sink     │
│   cart_events)  │     │     View        │     │                 │
└─────────────────┘     └─────────────────┘     └────────┬────────┘
                                                         │
                              ┌──────────────────────────┘
                              ▼
                    ┌─────────────────┐
                    │  Dashboard/     │
                    │  Microservice   │
                    │  (Consumer)     │
                    └─────────────────┘
```

## Benefits

1. **Push Architecture** - Data flows without polling
2. **Decoupling** - Consumers don't need RisingWave credentials
3. **Scalability** - Multiple independent consumers
4. **Resilience** - Kafka buffers during outages

## Related Skills

- `risingwave/kafka-source` - Read from Kafka
- `risingwave/materialized-view` - Data to sink
- `risingwave/iceberg-sink` - Alternative sink to Iceberg
