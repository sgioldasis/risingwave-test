# Iceberg Compaction Options for RisingWave Sinks

## Problem
RisingWave is writing to Iceberg very frequently (`commit_checkpoint_interval = 1`), creating many small files. This causes Spark to create thousands of stages when reading.

## Option 1: Increase Commit Interval (Recommended)

Modify the sink in `dbt/models/sink_funnel_to_iceberg.sql`:

```sql
CREATE SINK IF NOT EXISTS iceberg_funnel_sink
FROM {{ ref('funnel_for_iceberg') }}
WITH (
    connector = 'iceberg',
    type = 'upsert',
    primary_key = 'window_start',
    database.name = 'public',
    table.name = 'iceberg_funnel',
    connection = lakekeeper_catalog_conn,
    create_table_if_not_exists = 'true',
    commit_checkpoint_interval = 60  -- Changed from 1 to 60
)
```

This will commit every 60 checkpoints instead of every checkpoint, reducing file creation rate.

## Option 2: Add Scheduled Compaction Job

Create a Python script that runs periodically to compact the Iceberg table:

```python
# scripts/compact_iceberg.py
from pyspark.sql import SparkSession

spark = SparkSession.builder.appName("IcebergCompaction").getOrCreate()

# Compact the funnel table
spark.sql("""
    CALL lakekeeper.system.rewrite_data_files(
        table => 'lakekeeper.public.iceberg_funnel',
        options => map('target-file-size-bytes', '134217728')
    )
""")
```

Run this every few minutes via cron or a scheduled job.

## Option 3: Query with Trino Instead of Spark

Trino handles many small files much better than Spark. Update the notebook to use Trino instead:

```python
import trino
conn = trino.dbapi.connect(host='localhost', port=9080, catalog='datalake', schema='public')
```

## Option 4: Use DuckDB for Local Queries

DuckDB also handles small files efficiently and doesn't create stages like Spark.

## Recommended Approach

For immediate relief:
1. Increase `commit_checkpoint_interval` to 30 or 60
2. Use Trino or DuckDB for dashboard queries instead of Spark
3. Add a periodic compaction job for long-term maintenance

The Spark notebook is best for batch analytics, not real-time dashboards with frequently updated tables.