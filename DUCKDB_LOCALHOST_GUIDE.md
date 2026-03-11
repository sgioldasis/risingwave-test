# Connect to Iceberg from Local DuckDB

## Prerequisites

Install DuckDB on your local machine:
```bash
# macOS
brew install duckdb

# Linux
wget https://github.com/duckdb/duckdb/releases/download/v1.1.3/duckdb_cli-linux-amd64.zip
unzip duckdb_cli-linux-amd64.zip
sudo mv duckdb /usr/local/bin/

# Or use Python
pip install duckdb
```

## Connection Instructions

### Step 1: Start DuckDB
```bash
duckdb
```

### Step 2: Install and Load Extensions
```sql
INSTALL iceberg;
INSTALL httpfs;
LOAD iceberg;
LOAD httpfs;
```

### Step 3: Configure S3 (MinIO)
```sql
SET s3_region = 'us-east-1';
SET s3_endpoint = 'localhost:9301';  -- Use localhost, not minio-0
SET s3_url_style = 'path';
SET s3_access_key_id = 'hummockadmin';
SET s3_secret_access_key = 'hummockadmin';
SET s3_use_ssl = false;
```

### Step 4: Attach Iceberg Catalog (Lakekeeper)
```sql
ATTACH 'risingwave-warehouse' AS lk (
    TYPE ICEBERG,
    ENDPOINT 'http://localhost:8181/catalog',  -- Use localhost, not lakekeeper
    AUTHORIZATION_TYPE 'none'
);
```

### Step 5: Query the Table
```sql
-- List all tables in the public namespace
SHOW TABLES FROM lk.public;

-- Query the countries table
SELECT * FROM lk.public.iceberg_countries ORDER BY country;

-- Or use fully qualified name
SELECT country, country_name FROM lk.public.iceberg_countries;
```

## Complete Example (One Session)

```bash
$ duckdb
```

```sql
-- Install extensions
INSTALL iceberg;
INSTALL httpfs;
LOAD iceberg;
LOAD httpfs;

-- Configure S3 connection to MinIO
SET s3_region = 'us-east-1';
SET s3_endpoint = 'localhost:9301';
SET s3_url_style = 'path';
SET s3_access_key_id = 'hummockadmin';
SET s3_secret_access_key = 'hummockadmin';
SET s3_use_ssl = false;

-- Attach the Iceberg catalog
ATTACH 'risingwave-warehouse' AS lk (
    TYPE ICEBERG,
    ENDPOINT 'http://localhost:8181/catalog',
    AUTHORIZATION_TYPE 'none'
);

-- Query!
SELECT * FROM lk.public.iceberg_countries ORDER BY country;
```

## Expected Output

```
┌─────────┬──────────────┐
│ country │ country_name │
│ varchar │   varchar    │
├─────────┼──────────────┤
│ AT      │ Austria      │
│ BE      │ Belgium      │
│ CY      │ Cyprus       │
│ DE      │ Germany      │
│ ES      │ Spain        │
│ FR      │ France       │
│ GR      │ Greece       │
│ IT      │ Italy        │
│ NL      │ Netherlands  │
│ PT      │ Portugal     │
├─────────┴──────────────┤
│ 10 rows       2 columns│
└────────────────────────┘
```

## One-Liner Query

```bash
duckdb -c "
INSTALL iceberg;
INSTALL httpfs;
LOAD iceberg;
LOAD httpfs;
SET s3_region = 'us-east-1';
SET s3_endpoint = 'localhost:9301';
SET s3_url_style = 'path';
SET s3_access_key_id = 'hummockadmin';
SET s3_secret_access_key = 'hummockadmin';
SET s3_use_ssl = false;
ATTACH 'risingwave-warehouse' AS lk (TYPE ICEBERG, ENDPOINT 'http://localhost:8181/catalog', AUTHORIZATION_TYPE 'none');
SELECT * FROM lk.analytics.iceberg_countries ORDER BY country;
"
```

## Port Mappings

| Service | Internal DNS | Localhost Port | Usage |
|---------|-------------|----------------|-------|
| Lakekeeper | `lakekeeper:8181` | `localhost:8181` | Iceberg REST Catalog |
| MinIO | `minio-0:9301` | `localhost:9301` | S3-compatible storage |

## Troubleshooting

**Error: "Connection refused"**
- Make sure Docker containers are running: `docker compose ps`
- Check if ports are exposed: `docker compose port lakekeeper 8181`

**Error: "HTTP 403" or "Access Denied"**
- Verify S3 credentials: `hummockadmin` / `hummockadmin`
- Check endpoint uses `localhost:9301` not `minio-0:9301`

**Error: "Catalog not found"**
- Make sure the warehouse exists in Lakekeeper
- Verify the warehouse name: `risingwave-warehouse`
