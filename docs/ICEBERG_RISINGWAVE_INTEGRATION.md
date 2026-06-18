# Iceberg ↔ RisingWave Integration (via Trino) ✅ VERIFIED

This document describes the data flow between Iceberg and RisingWave using Trino as
the write interface and RisingWave's native Iceberg SOURCE for reads.

**Status**: ✅ **TESTED AND WORKING** — immediate refresh verified

## Architecture

```
┌─────────────────┐     ┌─────────────────┐     ┌──────────────────────┐
│   Trino         │────▶│   Iceberg       │◀────│  RisingWave          │
│   (Write)       │     │   (Lakekeeper)  │     │  CREATE SOURCE       │
└─────────────────┘     └─────────────────┘     │  src_iceberg_        │
        │                        │               │  countries           │
        │                        │               └──────────────────────┘
        ▼                        │                          │
┌─────────────────┐              │               inline JOIN at query time
│  Dagster Asset  │              │               (DataFusion reads current
│  iceberg_countries             │                snapshot immediately)
└─────────────────┘              │
                                 └── Trino cross-catalog JOIN
                                     datalake.public.iceberg_countries
```

## Components

### 1. Trino (Write Interface)
- **Location**: `localhost:9080`
- **Catalog**: `datalake` (Lakekeeper REST catalog)
- **Purpose**: Full SQL CRUD operations on Iceberg tables (UPDATE, DELETE, INSERT)

### 2. Iceberg Table
- **Catalog**: Lakekeeper REST Catalog (`http://lakekeeper:8181/catalog`)
- **Warehouse**: `risingwave-warehouse` (the warehouse name, not the S3 path)
- **Table**: `public.iceberg_countries`

### 3. RisingWave SOURCE
- **Name**: `src_iceberg_countries`
- **Type**: `CREATE SOURCE` with `connector = 'iceberg'`
- **Usage**: Inline `LEFT JOIN` in batch queries — DataFusion reads the current Iceberg
  snapshot at query time with no polling delay.
- **Note**: Do NOT wrap in `CREATE VIEW` (broken in RisingWave 3.0 — see
  [ICEBERG_CHANGELOG_HANDLING.md](ICEBERG_CHANGELOG_HANDLING.md)).

## Data Flow

### Write (via Trino)
```bash
docker compose exec trino trino --catalog datalake --schema public \
  --execute "UPDATE iceberg_countries SET country_name = 'Hellas' WHERE country = 'GR'"
# Result: UPDATE: 1 row
```

### Read in RisingWave application backend (immediate)
```sql
SELECT f.window_start, f.country, c.country_name::varchar, f.viewers
FROM funnel_summary f
LEFT JOIN src_iceberg_countries c ON f.country = c.country
```
Country name changes are visible on the next query — no lag.

### Read in Trino notebook (cross-catalog join)
```sql
SELECT f.window_start, f.country, c.country_name, f.viewers
FROM risingwave.public.funnel_summary f
LEFT JOIN datalake.public.iceberg_countries c ON f.country = c.country
ORDER BY f.window_start DESC LIMIT 50
```
Trino cannot query RisingWave SOURCEs directly, so the join goes to Lakekeeper instead.

## dbt Models

| Model | Type | Purpose |
|-------|------|---------|
| `iceberg_countries` (Iceberg) | Trino/Iceberg Table | Source of truth |
| `src_iceberg_countries` | RisingWave SOURCE | Inline JOIN partner for live backend reads |

## dbt Commands

```bash
# Recreate the source (after a reset)
dbt run --select src_iceberg_countries --project-dir dbt --profiles-dir dbt

# Drop and recreate everything
dbt run-operation drop_stale_iceberg_sources --project-dir dbt --profiles-dir dbt
dbt run --select src_iceberg_countries --project-dir dbt --profiles-dir dbt
```

## Important Configuration Note

The `warehouse.path` must be the **warehouse name**, not the S3 path:

```sql
-- ✅ CORRECT
warehouse.path = 'risingwave-warehouse'

-- ❌ WRONG (causes "warehouse does not exist" error)
warehouse.path = 's3://risingwave-warehouse/risingwave-warehouse'
```

## Files

- **Trino Loader**: [`scripts/load_countries_to_iceberg_trino.py`](../scripts/load_countries_to_iceberg_trino.py)
- **dbt SOURCE model**: [`dbt/models/src_iceberg_countries.sql`](../dbt/models/src_iceberg_countries.sql)
- **Detailed RW 3.0 regression notes**: [ICEBERG_CHANGELOG_HANDLING.md](ICEBERG_CHANGELOG_HANDLING.md)
