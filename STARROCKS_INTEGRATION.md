# StarRocks Integration Guide

This document describes the StarRocks integration with RisingWave and Iceberg datalake, enabling federated analytics across real-time streaming data and historical lakehouse data.

## Architecture Overview

```
┌─────────────────────────────────────────────────────────────────────────────┐
│                              StarRocks                                       │
│  ┌─────────────────────────────────────────────────────────────────────┐    │
│  │                    Federated Query Engine                            │    │
│  │                                                                      │    │
│  │   ┌──────────────────┐      ┌──────────────────┐                    │    │
│  │   │  Iceberg Catalog │      │ RisingWave Catalog│                    │    │
│  │   │  (REST via       │      │ (JDBC/PostgreSQL) │                    │    │
│  │   │   Lakekeeper)    │      │                   │                    │    │
│  │   └────────┬─────────┘      └────────┬──────────┘                    │    │
│  │            │                        │                                │    │
│  │            ▼                        ▼                                │    │
│  │   ┌──────────────────┐      ┌──────────────────┐                    │    │
│  │   │  Iceberg Tables  │      │ RisingWave Views │                    │    │
│  │   │  (Historical)    │      │ (Real-time)      │                    │    │
│  │   └────────┬─────────┘      └────────┬──────────┘                    │    │
│  └────────────┼─────────────────────────┼───────────────────────────────┘    │
│               │                         │                                    │
└───────────────┼─────────────────────────┼────────────────────────────────────┘
                │                         │
                ▼                         ▼
┌──────────────────────────────────┐  ┌──────────────────────────────────┐
│         Iceberg Datalake          │  │         RisingWave               │
│  ┌──────────────────────────┐   │  │  ┌──────────────────────────┐   │
│  │  Lakekeeper Catalog      │   │  │  │  Materialized Views      │   │
│  │  (REST API)              │   │  │  │  Streaming Data          │   │
│  └───────────┬──────────────┘   │  │  └───────────┬──────────────┘   │
│              │                  │  │              │                  │
│              ▼                  │  │              │                  │
│  ┌──────────────────────────┐   │  │  ┌──────────────────────────┐   │
│  │  MinIO S3 Storage        │   │  │  │  Kafka (Redpanda)        │   │
│  │  (Parquet Files)         │   │  │  │  (Event Streaming)       │   │
│  └──────────────────────────┘   │  │  └──────────────────────────┘   │
└──────────────────────────────────┘  └──────────────────────────────────┘
```

## Components

### 1. StarRocks Services

| Service | Container Name | Port | Purpose |
|---------|---------------|------|---------|
| Frontend (FE) | `starrocks-fe` | 9030 (MySQL), 8030 (HTTP) | Query planner & metadata |
| Compute Node (CN) | `starrocks-cn` | 8040 (HTTP) | Query execution |

### 2. External Catalogs

| Catalog | Type | Connection | Data Source |
|---------|------|------------|-------------|
| `iceberg_catalog` | Iceberg REST | `http://lakekeeper:8181/catalog` | Iceberg datalake |
| `risingwave_catalog` | JDBC (PostgreSQL) | `jdbc:postgresql://frontend-node-0:4566/dev` | RisingWave real-time |

## Quick Start

### 1. Start All Services

```bash
# Start the entire stack including StarRocks
docker compose up -d

# Wait for StarRocks to be ready (may take 1-2 minutes)
docker compose logs -f starrocks-init
```

### 2. Verify StarRocks is Running

```bash
# Connect using the helper script
./bin/5_starrocks_query.sh

# Or connect directly via MySQL client
mysql -h localhost -P9030 -uroot

# Or via Docker
mysql -h127.0.0.1 -P9030 -uroot -e "SELECT 1"
```

### 3. Verify Catalog Configuration

```bash
# Check configured catalogs
docker run --rm --network risingwave-test_iceberg_net mysql:8 \
  mysql -hstarrocks-fe -P9030 -uroot -e "SHOW CATALOGS"

# Expected output:
# Catalog            Type       Comment
# default_catalog    Internal   An internal catalog contains this cluster's self-managed tables.
# iceberg_catalog    Iceberg    NULL
```

**Note**: The RisingWave JDBC catalog requires manual installation of the PostgreSQL JDBC driver. See the Troubleshooting section below.

## Usage Examples

### Query Iceberg Tables from StarRocks

```bash
# Query Iceberg tables via Docker (recommended)
docker run --rm --network risingwave-test_iceberg_net mysql:8 \
  mysql -hstarrocks-fe -P9030 -uroot -e "SELECT * FROM iceberg_catalog.public.iceberg_countries LIMIT 10"
```

### Query RisingWave from StarRocks (via Trino)

Since the RisingWave JDBC catalog requires manual driver installation, use Trino for RisingWave queries:

```bash
# Query RisingWave via Trino
docker compose exec trino trino --catalog risingwave --schema public \
  --execute "SELECT * FROM funnel_summary LIMIT 10"
```

### Federated Queries (Iceberg + RisingWave)

```sql
-- Create a view in StarRocks that combines both data sources
CREATE VIEW starrocks_analytics.combined_funnel AS
SELECT 
    f.user_id,
    f.event_type,
    f.timestamp,
    c.country_name,
    f.revenue
FROM risingwave_catalog.public.funnel_summary f
LEFT JOIN iceberg_catalog.public.iceberg_countries c
ON f.country = c.country;

-- Query the federated view
SELECT * FROM starrocks_analytics.combined_funnel
WHERE country_name IS NOT NULL
ORDER BY timestamp DESC
LIMIT 100;
```

### Create External Tables in StarRocks

```sql
-- Create an external table pointing to Iceberg
CREATE EXTERNAL TABLE starrocks_analytics.ext_iceberg_countries
(
    country VARCHAR(10),
    country_name VARCHAR(255)
)
ENGINE = ICEBERG
PROPERTIES (
    "resource" = "iceberg_catalog",
    "database" = "public",
    "table" = "iceberg_countries"
);

-- Query the external table
SELECT * FROM starrocks_analytics.ext_iceberg_countries;
```

### Load Data from Iceberg into StarRocks (for faster queries)

```sql
-- Create a local StarRocks table with the same schema
CREATE TABLE starrocks_analytics.iceberg_countries_local
(
    country VARCHAR(10),
    country_name VARCHAR(255)
)
DUPLICATE KEY(country)
DISTRIBUTED BY HASH(country) BUCKETS 10;

-- Load data from Iceberg into StarRocks
INSERT INTO starrocks_analytics.iceberg_countries_local
SELECT * FROM iceberg_catalog.public.iceberg_countries;

-- Now query the local copy (much faster)
SELECT * FROM starrocks_analytics.iceberg_countries_local;
```

## Connection Methods

### Method 1: Docker MySQL Client (Recommended)

Since StarRocks runs in Docker, the easiest way to connect is using the MySQL Docker client:

```bash
# Run MySQL client from Docker
docker run --rm --network risingwave-test_iceberg_net mysql:8 \
  mysql -hstarrocks-fe -P9030 -uroot -e "SHOW CATALOGS"

# Interactive shell
docker run -it --rm --network risingwave-test_iceberg_net mysql:8 \
  mysql -hstarrocks-fe -P9030 -uroot
```

### Method 2: Local MySQL Client or DBeaver

**For DBeaver/DataGrip:**
- **Host:** `localhost`
- **Port:** `9030`
- **Database:** (leave empty or use `default_catalog`)
- **Username:** `root`
- **Password:** (leave empty)
- **URL:** `jdbc:mysql://localhost:9030`

> **Note:** If you get "Backend node not found" error in DBeaver, run this command to register the compute node:
> ```bash
> docker run --rm --network risingwave-test_iceberg_net mysql:8 \
>   mysql -hstarrocks-fe -P9030 -uroot \
>   -e "ALTER SYSTEM ADD COMPUTE NODE 'starrocks-cn:9050';"
> ```

**Note on External Catalogs:**
External catalogs like `iceberg_catalog` may not appear in DBeaver's database navigator tree. This is normal - they can still be queried using fully qualified names:
```sql
-- Query Iceberg tables directly
SELECT * FROM iceberg_catalog.public.iceberg_countries LIMIT 10;

-- Or switch to the catalog first
USE iceberg_catalog;
SELECT * FROM public.iceberg_countries LIMIT 10;
```

**Command line:**
```bash
# macOS: brew install mysql
# Ubuntu: sudo apt-get install mysql-client

# Connect to StarRocks
mysql -h localhost -P9030 -uroot -e "SHOW CATALOGS"
```

### Method 3: Helper Script

```bash
# Use the interactive helper script
./bin/5_starrocks_query.sh
```

### Method 4: Trino (via StarRocks Catalog)

```bash
# Query StarRocks through Trino
docker compose exec trino trino --catalog starrocks --schema default \
  --execute "SHOW SCHEMAS"
```

## Available Ports

| Service | Port | Protocol | Access |
|---------|------|----------|--------|
| StarRocks FE Query | 9030 | MySQL | Direct queries |
| StarRocks FE HTTP | 8030 | HTTP | Web UI & API |
| StarRocks CN HTTP | 8040 | HTTP | Compute node status |

## Web Interface

Access the StarRocks web UI at: http://localhost:8030

**Default Credentials:**
- **Username:** `root`
- **Password:** (leave blank/empty - no password required)

> **Note:** The web UI provides cluster monitoring, query profiling, and administration features. For SQL queries, use the MySQL client on port 9030.

## Troubleshooting

### StarRocks FE fails to start

```bash
# Check logs
docker compose logs starrocks-fe

# Restart the service
docker compose restart starrocks-fe
```

### Catalog not found

```bash
# Re-run initialization
docker compose restart starrocks-init

# Check initialization logs
docker compose logs starrocks-init
```

### Iceberg catalog connection issues

```bash
# Verify Lakekeeper is running
curl http://localhost:8181/catalog/v1/config

# Check Iceberg catalog configuration
mysql -h127.0.0.1 -P9030 -uroot -e "SHOW CREATE CATALOG iceberg_catalog"
```

### RisingWave catalog connection issues

```bash
# Verify RisingWave is accessible
psql -h localhost -p 4566 -d dev -U root -c "SELECT 1"

# Check JDBC catalog configuration
mysql -h127.0.0.1 -P9030 -uroot -e "SHOW CREATE CATALOG risingwave_catalog"
```

## Performance Tips

1. **Materialize frequently accessed Iceberg data**: Use `INSERT INTO` to load Iceberg data into local StarRocks tables for faster queries.

2. **Use appropriate bucketing**: When creating local tables, use `DISTRIBUTED BY HASH(column)` for better parallelism.

3. **Limit federated query complexity**: Complex JOINs across catalogs can be slow. Consider materializing intermediate results.

4. **Use columnar storage**: StarRocks is optimized for columnar queries - avoid `SELECT *` when possible.

## Integration with Existing Tools

### dbt Integration

You can configure dbt to connect to StarRocks using the MySQL adapter:

```yaml
# profiles.yml
starrocks:
  target: dev
  outputs:
    dev:
      type: mysql
      host: localhost
      port: 9030
      user: root
      schema: starrocks_analytics
```

### Dagster Integration

Query StarRocks from Dagster assets:

```python
import mysql.connector
from dagster import asset

@asset
def starrocks_analytics():
    conn = mysql.connector.connect(
        host="starrocks-fe",
        port=9030,
        user="root"
    )
    cursor = conn.cursor()
    cursor.execute("SELECT * FROM iceberg_catalog.public.iceberg_countries")
    results = cursor.fetchall()
    conn.close()
    return results
```

## Summary

StarRocks provides a powerful federated query layer that enables:
- **Real-time analytics** via RisingWave catalog
- **Historical analytics** via Iceberg catalog
- **Unified queries** combining both data sources
- **High-performance local caching** via StarRocks native tables

This integration allows you to build comprehensive analytics pipelines that leverage both streaming and batch data without moving data between systems.
