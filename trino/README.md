# Trino - SQL Query Engine

Trino provides full SQL support for multiple data sources:
- **Iceberg** - Complete CRUD operations (INSERT, UPDATE, DELETE)
- **RisingWave** - Query via PostgreSQL protocol
- **Others** - Can be extended with additional connectors

## Start Trino

```bash
docker compose up -d trino
```

## Catalogs

Trino is configured with two catalogs:

| Catalog | Connector | Description |
|---------|-----------|-------------|
| `datalake` | Iceberg REST | Query Iceberg tables via Lakekeeper catalog |
| `risingwave` | PostgreSQL | Query RisingWave via its PostgreSQL-compatible interface |

---

## Iceberg Catalog

### Load Countries Data

#### Via Standalone Script
```bash
uv run python scripts/load_countries_to_iceberg_trino.py
```

#### Via Dagster Asset
The `iceberg_countries` asset in the `datalake` group uses Trino to create and populate the table.

### Connect to Iceberg

#### Using Trino CLI (inside container)
```bash
docker compose exec trino trino --catalog datalake --schema public
```

#### Using psql-style client (trino-cli)
```bash
docker run -it --rm --network risingwave-test_iceberg_net trinodb/trino:453 trino --server http://trino:8080 --catalog datalake --schema public
```

#### Using Local trino-cli (from devbox)
```bash
# Interactive shell (no catalog specified - can query any catalog)
trino --server http://localhost:9080

# Interactive shell with specific catalog
trino --server http://localhost:9080 --catalog datalake --schema public

# One-liner query
trino --server http://localhost:9080 --catalog datalake --schema public --execute "SELECT * FROM iceberg_countries"
```

#### Using DBeaver/DataGrip
- **JDBC URL**: `jdbc:trino://localhost:9080/datalake/public`
- **User**: `trino` (any username works, no auth required)
- **Password**: (leave empty)

#### Using Python (trino client)
```python
from trino.dbapi import connect

conn = connect(
    host="localhost",
    port=9080,
    user="trino",
)

cur = conn.cursor()
# Use fully qualified names to query any catalog
cur.execute("SELECT * FROM datalake.public.iceberg_countries")
rows = cur.fetchall()
```

### Iceberg Query Examples

```sql
-- List tables
SHOW TABLES FROM datalake.public;

-- Query data
SELECT * FROM datalake.public.iceberg_countries;

-- Update data (works in Trino, not DuckDB!)
UPDATE datalake.public.iceberg_countries SET country_name = 'Greece Updated' WHERE country = 'GR';

-- Delete data
DELETE FROM datalake.public.iceberg_countries WHERE country = 'CY';

-- Insert data
INSERT INTO datalake.public.iceberg_countries VALUES ('JP', 'Japan');

-- Create new table
CREATE TABLE datalake.public.new_table (
    id INTEGER,
    name VARCHAR
) WITH (
    format = 'PARQUET',
    partitioning = ARRAY['id']
);
```

### Iceberg Catalog Configuration

See [`trino/catalog/datalake.properties`](catalog/datalake.properties) for full configuration.

---

## RisingWave Catalog

Trino can query RisingWave using the PostgreSQL connector, since RisingWave provides a PostgreSQL-compatible interface.

### Connect to RisingWave

#### Using Trino CLI (inside container)
```bash
docker compose exec trino trino --catalog risingwave --schema public
```

#### Using Local trino-cli (from devbox)
```bash
# Interactive shell (no catalog specified - can query any catalog)
trino --server http://localhost:9080

# Interactive shell with specific catalog
trino --server http://localhost:9080 --catalog risingwave --schema public

# One-liner query
trino --server http://localhost:9080 --catalog risingwave --schema public --execute "SELECT * FROM rw_countries"
```

#### Using DBeaver/DataGrip
- **JDBC URL**: `jdbc:trino://localhost:9080/risingwave/public`
- **User**: `trino` (any username works, no auth required)
- **Password**: (leave empty)

### RisingWave Query Examples

```sql
-- List tables in RisingWave
SHOW TABLES FROM risingwave.public;

-- Query materialized views
SELECT * FROM risingwave.public.funnel_summary_with_country;

-- Query funnel data
SELECT * FROM risingwave.public.funnel_summary LIMIT 10;

-- Check table schemas
DESCRIBE risingwave.public.rw_countries;

-- Aggregate queries
SELECT country_name, COUNT(*) as count
FROM risingwave.public.rw_countries
GROUP BY country_name;
```

### RisingWave Catalog Configuration

See [`trino/catalog/risingwave.properties`](catalog/risingwave.properties) for full configuration.

---

## Cross-Catalog Queries (Federated Queries)

One of Trino's most powerful features is the ability to query across different data sources in a single query!

### Example: Join Iceberg and RisingWave Data

**Note**: When joining RisingWave materialized views with Iceberg tables, query the Iceberg table directly from its catalog rather than through RisingWave's native Iceberg SOURCE (which may have type compatibility issues in cross-catalog joins).

```sql
-- ✅ WORKING: Join RisingWave MV with Iceberg table directly
SELECT 
    f.window_start,
    f.country,
    c.country_name,
    f.viewers,
    f.carters,
    f.purchasers
FROM risingwave.public.funnel_summary f
JOIN datalake.public.iceberg_countries c
    ON f.country = c.country;

-- ❌ NOT WORKING: Joining two RisingWave views where one reads from Iceberg SOURCE
-- This fails with "Unable to find name datatype in the system catalogs"
-- SELECT * FROM funnel_summary f JOIN vw_iceberg_countries c ON f.country = c.country;
```

**Solution for complex joins**: Create a pre-joined materialized view in RisingWave:
```sql
-- In RisingWave (via dbt model funnel_summary_with_country)
SELECT f.*, c.country_name 
FROM funnel_summary f 
LEFT JOIN rw_countries c ON f.country = c.country;

-- Then query simply in Trino:
SELECT * FROM risingwave.public.funnel_summary_with_country;
```

### Example: Compare Data Between Sources

```sql
-- Find records that exist in Iceberg but not in RisingWave (rw_countries is a view)
SELECT country, country_name FROM datalake.public.iceberg_countries
EXCEPT
SELECT country, country_name FROM risingwave.public.rw_countries;
```

### Example: Real-time Update from Iceberg to RisingWave

This demonstrates how changes in Iceberg are immediately reflected in RisingWave views:

```sql
-- Step 1: Check current country name for GR in RisingWave
SELECT country, country_name FROM risingwave.public.rw_countries WHERE country = 'GR';
-- Result: GR | Greece

-- Step 2: Update the country name in Iceberg
UPDATE datalake.public.iceberg_countries SET country_name = 'Hellas' WHERE country = 'GR';

-- Step 3: Verify the change in Iceberg
SELECT country, country_name FROM datalake.public.iceberg_countries WHERE country = 'GR';
-- Result: GR | Hellas

-- Step 4: Immediately check RisingWave - the change is reflected in real-time!
SELECT country, country_name FROM risingwave.public.rw_countries WHERE country = 'GR';
-- Result: GR | Hellas

-- Step 5: See the updated country name in the funnel summary
SELECT window_start, country, country_name, viewers, carters, purchasers
FROM risingwave.public.funnel_summary_with_country
WHERE country = 'GR';
-- Result shows 'Hellas' as the country_name
```

**How it works:**
1. `rw_countries` is a view that reads from `src_iceberg_countries` (RisingWave's Iceberg SOURCE)
2. When you update data in Iceberg via Trino, the change is written to the Iceberg table
3. RisingWave's Iceberg SOURCE (`src_iceberg_countries`) polls the Iceberg table and detects the change
4. The `rw_countries` view automatically reflects the updated data
5. `funnel_summary_with_country` joins `funnel_summary` with `rw_countries`, so it immediately shows the new country name

---

## Trino vs DuckDB for Iceberg

| Feature | Trino | DuckDB |
|---------|-------|--------|
| SELECT | ✅ | ✅ |
| INSERT | ✅ | ✅ (with limitations) |
| UPDATE | ✅ | ❌ Not implemented |
| DELETE | ✅ | ❌ Not implemented |
| CREATE TABLE | ✅ | ✅ |
| DROP TABLE | ✅ | ✅ |
| Cross-catalog joins | ✅ | ❌ |
| Federated queries | ✅ | ❌ |

**Recommendation**: Use Trino for full SQL CRUD operations on Iceberg tables and cross-catalog queries. Use DuckDB for local analytics and read-only queries.

---

## Available Tables

### Iceberg Catalog (`datalake.public`)
| Table | Description |
|-------|-------------|
| `iceberg_countries` | Country data written via Trino |

### RisingWave Catalog (`risingwave.public`)

**Note**: RisingWave SOURCE objects don't appear in the PostgreSQL table list (they are special objects), but materialized views and regular tables do.

| Table | Description |
|-------|-------------|
| `rw_countries` | Countries from Iceberg (view) |
| `funnel_summary` | Funnel summary table |
| `funnel_summary_with_country` | Pre-joined funnel + countries (recommended for queries) |
| `funnel` | Funnel aggregation materialized view |
| `src_iceberg_countries` | Native RisingWave SOURCE (Iceberg connector) - **Not queryable via Trino** |
| `src_cart` | Cart events source (Kafka) - **Not queryable via Trino** |
| `src_page` | Page view events source (Kafka) - **Not queryable via Trino** |
| `src_purchase` | Purchase events source (Kafka) - **Not queryable via Trino** |

**Note**: To query RisingWave sources directly, use `psql`:
```bash
psql -h localhost -p 4566 -d dev -U root -c "SELECT * FROM src_iceberg_countries"
```

---

## Recommended Query Patterns

### Query Funnel Data with Country Names
```sql
-- Use the pre-joined materialized view (recommended)
SELECT * FROM risingwave.public.funnel_summary_with_country;

-- Or join with Iceberg table directly
SELECT 
    f.window_start,
    f.country,
    c.country_name,
    f.viewers,
    f.carters,
    f.purchasers
FROM risingwave.public.funnel_summary f
JOIN iceberg.analytics.iceberg_countries c
    ON f.country = c.country;
```

### Query Deduplicated Countries
```sql
-- Use rw_countries (view over Iceberg source)
SELECT * FROM risingwave.public.rw_countries;

-- Or query Iceberg directly
SELECT * FROM datalake.public.iceberg_countries;
```
