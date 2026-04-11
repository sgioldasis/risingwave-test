# RisingWave Iceberg Sink

Create an Iceberg sink to persist RisingWave data to Apache Iceberg tables for long-term storage and analytics.

## Usage

```sql
CREATE SINK IF NOT EXISTS <sink_name>
INTO <target_table>
FROM <source_or_view>
WITH (
    connector = 'iceberg',
    type = 'upsert',
    primary_key = '<primary_key_column>',
    database.name = '<database>',
    table.name = '<table_name>',
    connection = <connection_name>,
    create_table_if_not_exists = '<boolean>',
    commit_checkpoint_interval = <seconds>
);
```

## Parameters

| Parameter | Description | Example |
|-----------|-------------|---------|
| `connector` | Must be 'iceberg' | `'iceberg'` |
| `type` | Sink type | `'upsert'` or `'append-only'` |
| `primary_key` | Primary key for upserts | `'window_start'` |
| `database.name` | Target database | `'public'` |
| `table.name` | Target table name | `'iceberg_funnel'` |
| `connection` | Iceberg connection reference | `lakekeeper_catalog_conn` |
| `create_table_if_not_exists` | Auto-create table | `'true'` |
| `commit_checkpoint_interval` | Commit frequency (seconds) | `60` |

## Prerequisites

First, create an Iceberg connection:
```sql
CREATE CONNECTION IF NOT EXISTS lakekeeper_catalog_conn
WITH (
    type = 'iceberg',
    catalog.type = 'rest',
    catalog.uri = 'http://lakekeeper:8181/catalog/',
    warehouse.path = 'risingwave-warehouse',
    s3.access.key = 'hummockadmin',
    s3.secret.key = 'hummockadmin',
    s3.path.style.access = 'true',
    s3.endpoint = 'http://minio-0:9301',
    s3.region = 'us-east-1'
);

SET iceberg_engine_connection = 'public.lakekeeper_catalog_conn';
```

## Examples

### Create Iceberg Target Table
```sql
CREATE TABLE IF NOT EXISTS iceberg_funnel (
    window_start TIMESTAMP,
    window_end TIMESTAMP,
    viewers BIGINT,
    carters BIGINT,
    purchasers BIGINT,
    view_to_cart_rate DOUBLE,
    cart_to_buy_rate DOUBLE,
    PRIMARY KEY (window_start)
) ENGINE = iceberg;
```

### Create Iceberg Sink
```sql
CREATE SINK IF NOT EXISTS iceberg_funnel_sink
INTO iceberg_funnel
FROM funnel
WITH (
    connector = 'iceberg',
    type = 'upsert',
    primary_key = 'window_start',
    database.name = 'public',
    table.name = 'iceberg_funnel',
    connection = lakekeeper_catalog_conn,
    create_table_if_not_exists = 'true',
    commit_checkpoint_interval = 60
);
```

## Sink Types

### Upsert Sink
- Updates existing rows based on primary key
- Inserts new rows
- Use for time-series data with updates

### Append-Only Sink
- Only inserts new rows
- No updates to existing data
- Use for immutable event streams

## Architecture Pattern

```
┌─────────────────┐     ┌─────────────────┐     ┌─────────────────┐
│   Kafka Sources │────▶│    RisingWave   │     │                 │
│                 │     │                 │     │                 │
│                 │     │  Materialized   │────▶│  Iceberg Sink   │
│                 │     │     View        │     │                 │
└─────────────────┘     └─────────────────┘     └────────┬────────┘
                                                         │
                              ┌──────────────────────────┘
                              ▼
                    ┌─────────────────┐
                    │  Apache Iceberg │
                    │  (Lakekeeper)   │
                    └────────┬────────┘
                             │
              ┌──────────────┼──────────────┐
              ▼              ▼              ▼
        ┌─────────┐    ┌─────────┐    ┌─────────┐
        │ DuckDB  │    │  Spark  │    │  Trino  │
        └─────────┘    └─────────┘    └─────────┘
```

## Querying Iceberg Data

### Via DuckDB
```bash
duckdb -c "ATTACH 's3://risingwave-warehouse/risingwave-warehouse' AS iceberg (TYPE ICEBERG); SELECT * FROM iceberg.public.iceberg_funnel;"
```

### Via Trino
```bash
trino --server http://localhost:9080 --catalog datalake --schema public --execute "SELECT * FROM iceberg_funnel"
```

### Via Spark
```python
spark.read.format("iceberg").load("datalake.public.iceberg_funnel").show()
```

## Benefits

1. **Persistent Storage** - Data survives RisingWave restarts
2. **Open Format** - Query with any Iceberg-compatible tool
3. **Time Travel** - Access historical snapshots
4. **Schema Evolution** - Safe schema changes

## Related Skills

- `risingwave/iceberg-source` - Read from Iceberg tables
- `risingwave/iceberg-connection` - Create Iceberg connections
- `risingwave/materialized-view` - Data to sink
