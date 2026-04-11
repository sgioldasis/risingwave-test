# RisingWave Iceberg Source

Create a native Iceberg source in RisingWave to read data from Apache Iceberg tables with automatic synchronization.

## Usage

```sql
CREATE SOURCE IF NOT EXISTS <source_name>
WITH (
    connector = 'iceberg',
    catalog.type = 'rest',
    catalog.uri = 'http://<catalog_host>:<port>/catalog',
    warehouse = '<warehouse_name>',
    database.name = '<database>',
    table.name = '<table_name>'
);
```

## Parameters

| Parameter | Description | Example |
|-----------|-------------|---------|
| `connector` | Must be 'iceberg' | `'iceberg'` |
| `catalog.type` | Catalog type | `'rest'`, `'hive'`, `'glue'` |
| `catalog.uri` | REST catalog endpoint | `'http://lakekeeper:8181/catalog'` |
| `warehouse` | Warehouse name | `'risingwave-warehouse'` |
| `database.name` | Database/schema name | `'public'` |
| `table.name` | Iceberg table name | `'iceberg_countries'` |

## Examples

### Iceberg Source with Lakekeeper REST Catalog
```sql
CREATE SOURCE IF NOT EXISTS src_iceberg_countries
WITH (
    connector = 'iceberg',
    catalog.type = 'rest',
    catalog.uri = 'http://lakekeeper:8181/catalog',
    warehouse = 'risingwave-warehouse',
    database.name = 'public',
    table.name = 'iceberg_countries'
);
```

### Create View Over Iceberg Source
```sql
CREATE VIEW rw_countries AS
SELECT
    country::varchar as country,
    country_name::varchar as country_name
FROM src_iceberg_countries;
```

## Architecture

```
┌─────────────────────────────────────────────────────────────────┐
│                         WRITE PATH                               │
├─────────────────────────────────────────────────────────────────┤
│  Trino/Dagster ──▶ Iceberg Table ──▶ Lakekeeper REST Catalog   │
└─────────────────────────────────────────────────────────────────┘
                              │
                              ▼ REST API
┌─────────────────────────────────────────────────────────────────┐
│                         READ PATH                                │
├─────────────────────────────────────────────────────────────────┤
│  RisingWave SOURCE ◀── Iceberg Connector ◀── Lakekeeper         │
│       │                                                          │
│       ▼                                                          │
│  Materialized View (with changelog deduplication)                │
└─────────────────────────────────────────────────────────────────┘
```

## Key Features

1. **Automatic Refresh** - Changes in Iceberg are immediately visible in RisingWave
2. **Changelog Data** - Returns all changes including retractions
3. **No Polling** - Uses REST API to detect metadata changes
4. **Native Integration** - Built-in Iceberg connector

## Behavior Notes

- Iceberg source returns **changelog data** (all changes including retractions)
- For deduplicated data (latest snapshot only), create a view or materialized view
- Updates made via Trino are immediately reflected in RisingWave

## Verification

Query the source:
```sql
SELECT * FROM src_iceberg_countries LIMIT 10;
```

Check for latest values only:
```sql
SELECT * FROM rw_countries WHERE country = 'GR';
```

## Configuration Tips

1. **Warehouse Path** - Use the warehouse name, not the S3 path:
   - ✅ `warehouse = 'risingwave-warehouse'`
   - ❌ `warehouse = 's3://risingwave-warehouse/risingwave-warehouse'`

2. **Auto-refresh** - No additional configuration needed; automatic via REST API

## Related Skills

- `risingwave/iceberg-sink` - Write to Iceberg tables
- `risingwave/iceberg-connection` - Create reusable Iceberg connections
- `risingwave/materialized-view` - Process Iceberg changelog data
