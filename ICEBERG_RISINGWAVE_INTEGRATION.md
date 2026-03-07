# Iceberg ↔ RisingWave Integration (via Trino) ✅ VERIFIED

This document describes the data flow between Iceberg and RisingWave using Trino as the write interface and RisingWave's native Iceberg source for reads.

**Status**: ✅ **TESTED AND WORKING** - Automatic refresh verified

## Architecture

```
┌─────────────────┐     ┌─────────────────┐     ┌─────────────────┐
│   Trino         │────▶│   Iceberg       │◀────│  RisingWave     │
│   (Write)       │     │   (Lakekeeper)  │     │  SOURCE         │
└─────────────────┘     └─────────────────┘     └─────────────────┘
        │                                               │
        │                                               │ (changelog)
        ▼                                               ▼
┌─────────────────┐                           ┌─────────────────┐
│  Dagster Asset  │                           │  Materialized   │
│  iceberg_countries                           │  View           │
└─────────────────┘                           │  vw_iceberg_    │
                                              │  countries      │
                                              └─────────────────┘
```

## Components

### 1. Trino (Write Interface)
- **Location**: `localhost:8080`
- **Purpose**: Full SQL CRUD operations on Iceberg tables
- **Works with**: UPDATE, DELETE, INSERT (unlike DuckDB)

### 2. Iceberg Table
- **Catalog**: Lakekeeper REST Catalog (`http://lakekeeper:8181/catalog`)
- **Warehouse**: `risingwave-warehouse` (not the S3 path!)
- **Table**: `iceberg_countries` (in `analytics` schema/database)

### 3. RisingWave Source
- **Name**: `src_iceberg_countries` (native RisingWave SOURCE, created outside dbt)
- **Type**: `CREATE SOURCE` with `connector = 'iceberg'`
- **Behavior**: ✅ **Automatic refresh when Iceberg data changes**
- **Note**: Returns **changelog data** (all changes), causing duplicates

### 4. Materialized View (Deduplication Layer)
- **Model**: `rw_countries` ([`dbt/models/rw_countries.sql`](dbt/models/rw_countries.sql))
- **Purpose**: Materialized view that filters changelog to only latest row per country using `ROW_NUMBER()`
- **Result**: Current snapshot only, no duplicates, auto-refreshes

## Verified Data Flow

1. **Write Path** (via Trino):
   ```bash
   # Update data in Iceberg
   docker compose exec trino trino --catalog iceberg --schema analytics \
     --execute "UPDATE iceberg_countries SET country_name = 'Hellas' WHERE country = 'GR'"
   # Result: UPDATE: 1 row
   ```

2. **Read Path** (via Materialized View) - **Immediate automatic refresh**:
   ```bash
   # Query the Materialized View (deduplicated, auto-refreshes)
   psql -h localhost -p 4566 -d dev -U root -c "SELECT * FROM rw_countries WHERE country = 'GR'"
   # Result: GR | Hellas (immediately reflects the Trino update!)
   ```

## Usage

### Load Initial Data
```bash
# Via standalone script (uses Trino Python client)
uv run python scripts/load_countries_to_iceberg_trino.py

# Or via Dagster
# Materialize the 'iceberg_countries' asset in Dagster UI
```

### Query in RisingWave
```bash
psql -h localhost -p 4566 -d dev -U root -c "SELECT * FROM src_iceberg_countries"
```

### Update via Trino
```bash
docker compose exec trino trino --catalog iceberg --schema analytics \
  --execute "UPDATE iceberg_countries SET country_name = 'Hellas' WHERE country = 'GR'"
```

### Verify Automatic Refresh in RisingWave
```bash
psql -h localhost -p 4566 -d dev -U root -c "SELECT * FROM src_iceberg_countries WHERE country = 'GR'"
# Should show 'Hellas' immediately after the Trino update
```

## Key Advantages

1. **✅ Full SQL Support**: Trino supports UPDATE/DELETE (DuckDB doesn't)
2. **✅ Automatic Sync**: RisingWave source immediately reflects Iceberg changes
3. **✅ No Batch Jobs**: No need for scheduled sync jobs or sensors
4. **✅ Fast Queries**: Materialized view provides local RisingWave performance
5. **✅ Native Integration**: Uses RisingWave's built-in Iceberg connector

## Models

| Model | Type | Purpose | Dagster Asset |
|-------|------|---------|---------------|
| `iceberg_countries` (Iceberg) | Trino/Iceberg Table | Source of truth in Iceberg | `csv/iceberg_countries` |
| `src_iceberg_countries` | RisingWave SOURCE | Native source (changelog data) | `public/src_iceberg_countries` |
| `rw_countries` | RisingWave MV | Deduplicates changelog to current snapshot | `public/rw_countries` |
| `funnel_summary_with_country` | RisingWave MV | Pre-joined funnel + countries | `public/funnel_summary_with_country` |

## dbt Commands

```bash
cd dbt

# Create/update the materialized view (deduplicates source data)
dbt run --select rw_countries
```

## Important Configuration Note

The `warehouse.path` must be the **warehouse name** (`risingwave-warehouse`), not the S3 path:

```sql
-- ✅ CORRECT
warehouse.path = 'risingwave-warehouse'

-- ❌ WRONG (causes "warehouse does not exist" error)
warehouse.path = 's3://risingwave-warehouse/risingwave-warehouse'
```

## Files

- **Trino Loader**: [`scripts/load_countries_to_iceberg_trino.py`](scripts/load_countries_to_iceberg_trino.py)
- **Materialized View (Deduplication)**: [`dbt/models/vw_iceberg_countries.sql`](dbt/models/vw_iceberg_countries.sql)
