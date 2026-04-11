# RisingWave Kafka Source

Create a Kafka source in RisingWave to ingest streaming data from Kafka topics.

## Usage

```sql
CREATE SOURCE IF NOT EXISTS <source_name> (
    <column_name> <data_type>,
    ...
) WITH (
    connector = 'kafka',
    topic = '<kafka_topic>',
    properties.bootstrap.server = '<broker_host>:<port>',
    scan.startup.mode = '<mode>'
) FORMAT PLAIN ENCODE <format>;
```

## Parameters

| Parameter | Description | Example |
|-----------|-------------|---------|
| `connector` | Must be 'kafka' | `'kafka'` |
| `topic` | Kafka topic name | `'page_views'` |
| `properties.bootstrap.server` | Kafka broker address | `'redpanda:9092'` or `'localhost:19092'` |
| `scan.startup.mode` | Where to start reading | `'latest'`, `'earliest'`, `'timestamp'` |

## Format Options

- `ENCODE JSON` - JSON formatted messages
- `ENCODE AVRO` - Avro formatted messages
- `ENCODE PROTOBUF` - Protocol Buffer messages
- `ENCODE CSV` - CSV formatted messages

## Examples

### Basic Kafka Source (JSON)
```sql
CREATE SOURCE src_page (
    user_id int,
    page_id varchar,
    event_time timestamp
) WITH (
    connector = 'kafka',
    topic = 'page_views',
    properties.bootstrap.server = 'redpanda:9092',
    scan.startup.mode = 'latest'
) FORMAT PLAIN ENCODE JSON;
```

### Cart Events Source
```sql
CREATE SOURCE src_cart (
    user_id int,
    item_id varchar,
    event_time timestamp
) WITH (
    connector = 'kafka',
    topic = 'cart_events',
    properties.bootstrap.server = 'redpanda:9092',
    scan.startup.mode = 'latest'
) FORMAT PLAIN ENCODE JSON;
```

### Purchase Events Source
```sql
CREATE SOURCE src_purchase (
    user_id int,
    amount numeric,
    event_time timestamp
) WITH (
    connector = 'kafka',
    topic = 'purchases',
    properties.bootstrap.server = 'redpanda:9092',
    scan.startup.mode = 'latest'
) FORMAT PLAIN ENCODE JSON;
```

## Scan Startup Modes

- `'earliest'` - Read from the beginning of the topic
- `'latest'` - Read only new messages (recommended for production)
- `'timestamp'` - Start from a specific timestamp (use with `scan.startup.timestamp.millis`)

## Best Practices

1. **Use `latest` mode for production** to avoid reprocessing historical data
2. **Use internal hostnames** (`redpanda:9092`) when running inside Docker network
3. **Use external hostnames** (`localhost:19092`) when connecting from host machine
4. **Define timestamp columns** for time-based windowing operations

## Related Skills

- `risingwave/materialized-view` - Process source data
- `risingwave/tumble-window` - Time-based aggregations
- `risingwave/kafka-sink` - Output to Kafka topics
