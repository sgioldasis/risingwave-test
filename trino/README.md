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
| `iceberg` | Iceberg REST | Query Iceberg tables via Lakekeeper catalog |
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
docker compose exec trino trino --catalog iceberg --schema analytics
```

#### Using psql-style client (trino-cli)
```bash
docker run -it --rm --network risingwave-test_iceberg_net trinodb/trino:453 trino --server http://trino:8080 --catalog iceberg --schema analytics
```

#### Using Local trino-cli (from devbox)
```bash
# Interactive shell
trino --server http://localhost:8080 --catalog iceberg --schema analytics

# One-liner query
trino --server http://localhost:8080 --catalog iceberg --schema analytics --execute "SELECT * FROM iceberg_countries"
```

#### Using DBeaver/DataGrip
- **JDBC URL**: `jdbc:trino://localhost:8080/iceberg/analytics`
- **User**: `trino` (any username works, no auth required)
- **Password**: (leave empty)

#### Using Python (trino client)
```python
from trino.dbapi import connect

conn = connect(
    host="localhost",
    port=8080,
    user="trino",
    catalog="iceberg",
    schema="analytics",
)

cur = conn.cursor()
cur.execute("SELECT * FROM iceberg_countries")
rows = cur.fetchall()
```

### Iceberg Query Examples

```sql
-- List tables
SHOW TABLES;

-- Query data
SELECT * FROM iceberg_countries;

-- Update data (works in Trino, not DuckDB!)
UPDATE iceberg_countries SET country_name = 'Greece Updated' WHERE country = 'GR';

-- Delete data
DELETE FROM iceberg_countries WHERE country = 'CY';

-- Insert data
INSERT INTO iceberg_countries VALUES ('JP', 'Japan');

-- Create new table
CREATE TABLE new_table (
    id INTEGER,
    name VARCHAR
) WITH (
    format = 'PARQUET',
    partitioning = ARRAY['id']
);
```

### Iceberg Catalog Configuration

See [`trino/catalog/iceberg.properties`](catalog/iceberg.properties) for full configuration.

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
# Interactive shell
trino --server http://localhost:8080 --catalog risingwave --schema public

# One-liner query
trino --server http://localhost:8080 --catalog risingwave --schema public --execute "SELECT * FROM src_iceberg_countries"
```

#### Using DBeaver/DataGrip
- **JDBC URL**: `jdbc:trino://localhost:8080/risingwave/public`
- **User**: `trino` (any username works, no auth required)
- **Password**: (leave empty)

### RisingWave Query Examples

```sql
-- List tables in RisingWave
SHOW TABLES;

-- Query materialized views
SELECT * FROM mv_countries_from_iceberg;

-- Query streaming sources
SELECT * FROM src_cart LIMIT 10;

-- Check table schemas
DESCRIBE mv_countries_from_iceberg;

-- Aggregate queries
SELECT country_name, COUNT(*) as count
FROM mv_countries_from_iceberg
GROUP BY country_name;
```

### RisingWave Catalog Configuration

See [`trino/catalog/risingwave.properties`](catalog/risingwave.properties) for full configuration.

---

## Cross-Catalog Queries (Federated Queries)

One of Trino's most powerful features is the ability to query across different data sources in a single query!

### Example: Join Iceberg and RisingWave Data

```sql
-- Query Iceberg table from RisingWave
SELECT 
    i.country,
    i.country_name as iceberg_name,
    r.country_name as risingwave_name
FROM iceberg.analytics.iceberg_countries i
JOIN risingwave.public.src_iceberg_countries r 
    ON i.country = r.country;
```

### Example: Compare Data Between Sources

```sql
-- Find records that exist in Iceberg but not in RisingWave
SELECT * FROM iceberg.analytics.iceberg_countries
EXCEPT
SELECT * FROM risingwave.public.src_iceberg_countries;
```

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

### Iceberg Catalog (`iceberg.analytics`)
| Table | Description |
|-------|-------------|
| `iceberg_countries` | Country data written via Trino |

### RisingWave Catalog (`risingwave.public`)

**Note**: RisingWave SOURCE objects don't appear in the PostgreSQL table list (they are special objects), but materialized views and regular tables do.

| Table | Description |
|-------|-------------|
| `mv_countries_from_iceberg` | Materialized view of Iceberg data (queryable via Trino) |
| `countries_iceberg_snapshot` | Table with Iceberg data |
| `funnel` | Funnel aggregation materialized view |
| `funnel_summary` | Funnel summary table |

**Note**: To query RisingWave sources directly, use `psql`:
```bash
psql -h localhost -p 4566 -d dev -U root -c "SELECT * FROM src_iceberg_countries"
```
