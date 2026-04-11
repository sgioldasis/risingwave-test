# RisingWave Iceberg Connection

Create a reusable Iceberg connection for multiple sinks and sources.

## Usage

```sql
CREATE CONNECTION IF NOT EXISTS <connection_name>
WITH (
    type = 'iceberg',
    catalog.type = '<catalog_type>',
    catalog.uri = '<catalog_url>',
    warehouse.path = '<warehouse_name>',
    s3.access.key = '<access_key>',
    s3.secret.key = '<secret_key>',
    s3.path.style.access = '<boolean>',
    s3.endpoint = '<s3_endpoint>',
    s3.region = '<region>'
);

SET iceberg_engine_connection = '<schema>.<connection_name>';
```

## Parameters

| Parameter | Description | Example |
|-----------|-------------|---------|
| `type` | Must be 'iceberg' | `'iceberg'` |
| `catalog.type` | Catalog implementation | `'rest'`, `'hive'`, `'glue'` |
| `catalog.uri` | Catalog REST endpoint | `'http://lakekeeper:8181/catalog/'` |
| `warehouse.path` | Warehouse name | `'risingwave-warehouse'` |
| `s3.access.key` | S3 access key | `'hummockadmin'` |
| `s3.secret.key` | S3 secret key | `'hummockadmin'` |
| `s3.path.style.access` | Path-style S3 URLs | `'true'` |
| `s3.endpoint` | S3 endpoint URL | `'http://minio-0:9301'` |
| `s3.region` | S3 region | `'us-east-1'` |

## Examples

### Lakekeeper REST Catalog Connection
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

### AWS Glue Catalog Connection
```sql
CREATE CONNECTION IF NOT EXISTS glue_catalog_conn
WITH (
    type = 'iceberg',
    catalog.type = 'glue',
    warehouse.path = 's3://my-bucket/warehouse',
    s3.access.key = 'AKIA...',
    s3.secret.key = 'secret...',
    s3.region = 'us-east-1'
);
```

## Important Configuration

### Warehouse Path
Use the **warehouse name**, not the full S3 path:

- ✅ **Correct**: `warehouse.path = 'risingwave-warehouse'`
- ❌ **Wrong**: `warehouse.path = 's3://risingwave-warehouse/risingwave-warehouse'`

The warehouse path refers to the logical warehouse name registered in the catalog, not the physical S3 location.

## Using the Connection

### In Iceberg Sinks
```sql
CREATE SINK IF NOT EXISTS my_sink
INTO target_table
FROM source_view
WITH (
    connector = 'iceberg',
    connection = lakekeeper_catalog_conn,
    ...
);
```

### In Iceberg Sources
```sql
CREATE SOURCE IF NOT EXISTS my_source
WITH (
    connector = 'iceberg',
    catalog.uri = 'http://lakekeeper:8181/catalog',
    warehouse = 'risingwave-warehouse',
    ...
);
```

## Verification

List connections:
```sql
SHOW CONNECTIONS;
```

Check connection details:
```sql
DESCRIBE CONNECTION lakekeeper_catalog_conn;
```

## Related Skills

- `risingwave/iceberg-source` - Read from Iceberg
- `risingwave/iceberg-sink` - Write to Iceberg
- `risingwave/materialized-view` - Data transformation
