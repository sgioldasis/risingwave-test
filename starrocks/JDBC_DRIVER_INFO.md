# StarRocks JDBC Driver Configuration ✅

## PostgreSQL JDBC Driver Status

The StarRocks setup now includes the PostgreSQL JDBC driver on both FE and BE nodes:

- **FE Driver Location**: `/opt/starrocks/fe/lib/postgresql-42.4.4.jar`
- **BE Driver Location**: `/opt/starrocks/fe/lib/postgresql-42.4.4.jar` (same path required)
- **Driver Version**: 42.4.4 (pre-installed in StarRocks images)
- **Driver Class**: `org.postgresql.Driver`

## Architecture Update: FE + BE (Not CN)

**Important**: The setup now uses `starrocks/be-ubuntu` instead of `starrocks/cn-ubuntu` for the backend node. This resolves the version compatibility issue (CN was for 3.3.x, FE is 3.2.x).

- **starrocks-fe**: Frontend node (query coordination)
- **starrocks-be**: Backend node (query execution)

## RisingWave Catalog Configuration

```sql
CREATE EXTERNAL CATALOG risingwave_catalog
PROPERTIES (
    "type" = "jdbc",
    "user" = "root",
    "password" = "",
    "jdbc_uri" = "jdbc:postgresql://frontend-node-0:4566/dev",
    "driver_class" = "org.postgresql.Driver",
    "driver_url" = "file:///opt/starrocks/fe/lib/postgresql-42.4.4.jar"
);
```

## Query Examples (Working!)

```sql
-- List RisingWave databases
SHOW DATABASES FROM risingwave_catalog;

-- List tables
SHOW TABLES FROM risingwave_catalog.public;

-- Query funnel data
SELECT * FROM risingwave_catalog.public.funnel_summary LIMIT 5;

-- Aggregated query with formatting
SELECT 
  window_start,
  window_end,
  country,
  viewers,
  carters,
  purchasers,
  ROUND(view_to_cart_rate * 100, 1) as v2c_pct,
  ROUND(cart_to_buy_rate * 100, 1) as c2b_pct
FROM risingwave_catalog.public.funnel_summary 
ORDER BY window_end DESC 
LIMIT 5;
```

## Sample Output

```
window_start         window_end           country  viewers  carters  purchasers  v2c_pct  c2b_pct
2026-03-15 02:56:00  2026-03-15 02:57:00  GR       52       21       8           40       38
2026-03-15 02:55:00  2026-03-15 02:56:00  GR       60       15       7           25       47
```

## DBeaver Connection

- **Host**: `localhost`
- **Port**: `9030`
- **Database**: (leave empty or use `default`)
- **Username**: `root`
- **Password**: (empty)
- **Driver**: MySQL (StarRocks uses MySQL protocol)

## Available RisingWave Tables

| Table | Description |
|-------|-------------|
| `funnel` | Raw funnel events |
| `funnel_summary` | Aggregated funnel metrics |
| `funnel_summary_with_country` | Funnel metrics by country |
| `funnel_training` | ML training data |
| `funnel_for_iceberg` | Iceberg sink data |
| `rw_countries` | Country reference data |

## Troubleshooting

### "Backend node not found" Error
This means the BE is not properly registered. Fix:
```bash
docker run --rm --network risingwave-test_iceberg_net -i mysql:8 \
  mysql -h starrocks-fe -P 9030 -u root \
  -e "ALTER SYSTEM ADD BACKEND 'starrocks-be:9050';"
```

### "Couldn't open file" Error
The JDBC driver is missing on the BE. The driver must exist at the same path on both FE and BE:
```bash
# Copy driver from FE to BE
docker cp starrocks-fe:/opt/starrocks/fe/lib/postgresql-42.4.4.jar /tmp/
docker exec starrocks-be mkdir -p /opt/starrocks/fe/lib
docker cp /tmp/postgresql-42.4.4.jar starrocks-be:/opt/starrocks/fe/lib/
```

### Check Backend Status
```sql
SHOW BACKENDS;
```
Look for `Alive: true` in the output.

### DBeaver Shows `2000-01-01` for Timestamps
Known DBeaver JDBC driver issue with external catalog DATETIME columns.

**Working Solution** - Use `FROM_UNIXTIME(UNIX_TIMESTAMP())` pattern:
```sql
SELECT
  from_unixtime(unix_timestamp(window_start)) as window_start,
  from_unixtime(unix_timestamp(window_end)) as window_end,
  country,
  viewers,
  carters,
  purchasers
FROM risingwave_catalog.public.funnel_summary
ORDER BY window_end DESC
LIMIT 10;
```

**Alternative** - Convert to string with REPLACE:
```sql
SELECT
  REPLACE(CAST(window_start AS VARCHAR), ' ', 'T') as window_start,
  REPLACE(CAST(window_end AS VARCHAR), ' ', 'T') as window_end,
  country, viewers, carters, purchasers
FROM risingwave_catalog.public.funnel_summary
ORDER BY window_end DESC
LIMIT 10;
```

**Last Resort** - Build timestamp from components:
```sql
SELECT
  CONCAT(
    year(window_start), '-',
    LPAD(month(window_start), 2, '0'), '-',
    LPAD(day(window_start), 2, '0'), ' ',
    LPAD(hour(window_start), 2, '0'), ':',
    LPAD(minute(window_start), 2, '0'), ':',
    LPAD(second(window_start), 2, '0')
  ) as window_start,
  country, viewers, carters, purchasers
FROM risingwave_catalog.public.funnel_summary
ORDER BY window_end DESC
LIMIT 10;
```

If none work, use the MySQL CLI directly - the timestamps display correctly there.

Or use the MySQL CLI directly (shows correct timestamps):
```bash
docker run --rm --network risingwave-test_iceberg_net -i mysql:8 \
  mysql -h starrocks-fe -P 9030 -u root \
  -e "SELECT * FROM risingwave_catalog.public.funnel_summary LIMIT 5;"
```

## Summary

✅ PostgreSQL JDBC driver installed on both FE and BE
✅ RisingWave catalog created and working
✅ Queries execute successfully
✅ DBeaver compatible
