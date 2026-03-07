# Iceberg to RisingWave Sync Guide

## Problem

When modifying data in the Iceberg table (e.g., deleting rows like India or Spain), the changes were not reflected in RisingWave views. Specifically:

1. `rw_countries` was showing stale data (deleted rows still appeared)
2. `funnel_summary_with_country` was showing duplicate rows (e.g., Greece appearing multiple times with different names)

## Root Cause

RisingWave's Iceberg source (`CREATE SOURCE ... WITH connector = 'iceberg'`) is a **batch source** that:
1. Reads a point-in-time snapshot from Iceberg when created
2. **Caches the data** - it does NOT auto-refresh when Iceberg changes
3. Does NOT emit changelog events (no `_op` column for deletes)

Additionally, **MATERIALIZED VIEWs** in RisingWave cache their results at creation time and don't automatically refresh when source data changes.

### Available Columns in Iceberg Source

```sql
DESCRIBE src_iceberg_countries;
```

| Column | Type | Hidden | Description |
|--------|------|--------|-------------|
| country | varchar | false | Country code |
| country_name | varchar | false | Country name |
| _iceberg_sequence_number | bigint | true | Iceberg snapshot sequence |
| _iceberg_file_path | varchar | true | Data file path |
| _iceberg_file_pos | bigint | true | Position in file |
| _row_id | serial | true | RisingWave internal row ID |

**Note**: There is **NO `_op` column** because this is not a CDC source.

## Solution

### Changed Models to Use VIEW Instead of MATERIALIZED_VIEW

When a model reads from the Iceberg source or joins with data from the Iceberg source, use `materialized='view'` to ensure fresh data on each query.

#### [`rw_countries.sql`](dbt/models/rw_countries.sql)

```sql
{{ config(
    materialized='view',
    persist_docs={"relation": true, "columns": true},
    meta={
        "dagster": {
            "deps": [{"asset_key": ["csv", "iceberg_countries"]}]
        }
    }
) }}

-- View (rw_countries) that shows the current snapshot from Iceberg
-- Reads from native RisingWave SOURCE src_iceberg_countries
-- Using VIEW materialization to ensure fresh data on each query

SELECT
    country::varchar as country,
    country_name::varchar as country_name
FROM {{ ref('src_iceberg_countries') }}
```

#### [`funnel_summary_with_country.sql`](dbt/models/funnel_summary_with_country.sql)

```sql
{{ config(materialized='view') }}

-- Join funnel_summary with Iceberg countries data within RisingWave
-- This avoids cross-catalog join issues in Trino
-- Using VIEW materialization to ensure fresh data on each query

SELECT
    f.window_start,
    f.country,
    c.country_name,
    f.viewers,
    f.carters,
    f.purchasers,
    f.view_to_cart_rate,
    f.cart_to_buy_rate
FROM {{ ref('funnel_summary') }} f
LEFT JOIN {{ ref('rw_countries') }} c
    ON f.country = c.country
```

### When Source Becomes Stale

If you modify data in Iceberg (delete/update rows) and the RisingWave source still shows old data, **recreate the source**:

```sql
DROP SOURCE IF EXISTS src_iceberg_countries CASCADE;

CREATE SOURCE src_iceberg_countries
WITH (
    connector = 'iceberg',
    catalog.type = 'rest',
    catalog.uri = 'http://lakekeeper:8181/catalog',
    warehouse.path = 'risingwave-warehouse',
    database.name = 'analytics',
    table.name = 'iceberg_countries',
    s3.endpoint = 'http://minio-0:9301',
    s3.region = 'us-east-1',
    s3.access.key = 'hummockadmin',
    s3.secret.key = 'hummockadmin'
);
```

Then run dbt:

```bash
dbt run --models rw_countries funnel_summary_with_country
```

## How It Works

1. **VIEW vs MATERIALIZED VIEW**:
   - **VIEW**: Always reads fresh data from sources when queried (no caching)
   - **MATERIALIZED VIEW**: Caches results at creation time (can become stale)

2. **RisingWave Iceberg Source**:
   - Reads a snapshot from Iceberg when created
   - Caches the data (doesn't auto-refresh)
   - When recreated, it reads the latest Iceberg snapshot

3. **The Combination**:
   - Use VIEWs for models that read from Iceberg sources
   - When Iceberg data changes, recreate the source
   - VIEWs automatically see the new source data

## Alternative Solutions

For automatic sync without manual source recreation:

1. **Dagster Asset Sync**: Use `orchestration/assets/risingwave_countries_table.py` which syncs from Iceberg via Trino
2. **Scheduled dbt Runs**: Run `dbt run` periodically to recreate the source
3. **Query Iceberg Directly**: Use Trino for queries that need the absolute latest data

## References

- [RisingWave Iceberg Source](https://docs.risingwave.com/integrations/sources/apache-iceberg)
- [RisingWave Views](https://docs.risingwave.com/sql/commands/sql-create-view)
- [RisingWave Materialized Views](https://docs.risingwave.com/sql/commands/sql-create-materialized-view)
