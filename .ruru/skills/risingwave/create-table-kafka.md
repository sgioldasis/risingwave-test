# RisingWave CREATE TABLE with Kafka

Create a table with Kafka connector to persist streaming data directly in RisingWave storage.

## Usage

```sql
CREATE TABLE <table_name> (
    <column_name> <data_type>,
    ...
    PRIMARY KEY (<key_columns>)
) WITH (
    connector = 'kafka',
    topic = '<kafka_topic>',
    properties.bootstrap.server = '<broker_host>:<port>'
) FORMAT <format> ENCODE <encoding>;
```

## Difference: CREATE TABLE vs CREATE SOURCE

| Feature | CREATE TABLE | CREATE SOURCE |
|---------|--------------|---------------|
| Data Storage | ✅ Data stored in RisingWave | ❌ Data not stored |
| Primary Key | ✅ Supported | ❌ Not supported |
| Query Performance | ✅ Fast (local storage) | ⚠️ Reads from Kafka |
| Use Case | Materialized analytics | Passthrough streaming |

## Examples

### Basic Kafka Table
```sql
CREATE TABLE kafka_orders (
    order_id INT,
    customer_id INT,
    amount DECIMAL,
    created_at TIMESTAMP,
    PRIMARY KEY (order_id)
) WITH (
    connector = 'kafka',
    topic = 'orders',
    properties.bootstrap.server = 'redpanda:9092'
) FORMAT PLAIN ENCODE JSON;
```

### Table with Metadata Columns
```sql
CREATE TABLE kafka_events_with_metadata (
    user_id INT,
    event_type VARCHAR,
    timestamp TIMESTAMP,
    PRIMARY KEY (user_id)
) 
INCLUDE key AS kafka_key
INCLUDE partition AS kafka_partition
INCLUDE offset AS kafka_offset
INCLUDE timestamp AS kafka_timestamp
WITH (
    connector = 'kafka',
    topic = 'user_events',
    properties.bootstrap.server = 'broker:9092'
) FORMAT PLAIN ENCODE JSON;
```

### Upsert Table (Changelog Stream)
```sql
CREATE TABLE user_profiles (
    user_id INT,
    name VARCHAR,
    email VARCHAR,
    updated_at TIMESTAMP,
    PRIMARY KEY (user_id)
) 
INCLUDE key AS kafka_key
WITH (
    connector = 'kafka',
    topic = 'user_profiles',
    properties.bootstrap.server = 'broker:9092'
) FORMAT UPSERT ENCODE JSON;
```

## Supported Metadata Fields

| Field | Type | Description |
|-------|------|-------------|
| `key` | BYTEA | Kafka message key |
| `timestamp` | TIMESTAMP WITH TIME ZONE | Message creation time |
| `partition` | VARCHAR | Kafka partition |
| `offset` | VARCHAR | Message offset |
| `header` | STRUCT<VARCHAR, BYTEA>[] | Message headers |
| `payload` | JSON | Raw message content |

## When to Use CREATE TABLE

1. **Need Primary Keys** - For upsert/changelog semantics
2. **Fast Queries** - Data cached locally in RisingWave
3. **Complex Joins** - Better performance for multi-table joins
4. **Historical Analysis** - Access to all data, not just stream

## When to Use CREATE SOURCE

1. **Passthrough** - Just passing data through to sinks
2. **Large Volumes** - Don't want to store all data
3. **Simple Transformations** - Basic filtering/projection only

## Related Skills

- `risingwave/kafka-source` - Non-persisted Kafka connection
- `risingwave/kafka-sink` - Output to Kafka
- `risingwave/materialized-view` - Process table data
