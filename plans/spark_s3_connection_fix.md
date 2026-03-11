# Spark S3 Connection Reset Fix - AGGRESSIVE SETTINGS

## Problem

Persistent `java.net.SocketException: Connection reset` errors when Spark reads Iceberg tables from MinIO/S3:
- Occurs during `S3InputStream.openStream()` operations
- Affects multiple Parquet files, especially equality delete files (`-eq-del-` suffix)
- AWS SDK retries exhaust after 3 attempts
- Multiple stage failures (Stage 7.0, 8.0, etc.)

## Root Cause

1. **MinIO Connection Limits**: MinIO may have connection limits that are exceeded
2. **Connection Pool Exhaustion**: Default connection pool (50) too small for parallel reads
3. **Socket Timeouts**: Default timeouts too short for local network
4. **High Parallelism**: Too many concurrent connections to MinIO
5. **AWS SDK HTTP Client**: Apache HTTP client has its own connection management

## Solution Applied

### Aggressive Spark Session Configuration

```python
# Connection timeouts - INCREASED to 60 seconds
.config("spark.sql.catalog.lakekeeper.s3.connection-timeout-ms", "60000")
.config("spark.sql.catalog.lakekeeper.s3.socket-timeout-ms", "60000")

# Connection pooling - INCREASED to 200
.config("spark.sql.catalog.lakekeeper.s3.connection-maximum-connections", "200")

# Retry configuration - AGGRESSIVE (20 retries)
.config("spark.sql.catalog.lakekeeper.s3.max-retries", "20")
.config("spark.sql.catalog.lakekeeper.s3.retry-mode", "adaptive")

# AWS SDK v2 specific settings
.config("spark.sql.catalog.lakekeeper.s3.apahce-http-client.connection-timeout", "60000")
.config("spark.sql.catalog.lakekeeper.s3.apache-http-client.socket-timeout", "60000")

# S3A fallback settings
.config("spark.hadoop.fs.s3a.connection.timeout", "60000")
.config("spark.hadoop.fs.s3a.socket.timeout", "60000")
.config("spark.hadoop.fs.s3a.connection.maximum", "200")
.config("spark.hadoop.fs.s3a.attempts.maximum", "20")

# REDUCE parallelism to ease connection pressure
.config("spark.sql.shuffle.partitions", "4")
.config("spark.default.parallelism", "4")

# Enable adaptive query execution
.config("spark.sql.adaptive.enabled", "true")
.config("spark.sql.adaptive.coalescePartitions.enabled", "true")
```

### Clear and Retry Helper Function

```python
def clear_and_retry_query(query_fn, max_retries=3, retry_delay=2):
    """
    Clear Spark caches and retry a query function with exponential backoff.
    """
    for attempt in range(max_retries):
        try:
            spark.catalog.clearCache()
            spark.sql("CLEAR CACHE")
            result = query_fn()
            return result
        except Exception as e:
            if "Connection reset" in str(e) or "SdkClientException" in str(e):
                if attempt < max_retries - 1:
                    wait_time = retry_delay * (2 ** attempt)
                    time.sleep(wait_time)
                else:
                    raise
            else:
                raise
```

## Troubleshooting Steps

If errors persist after these aggressive settings:

1. **Check MinIO Server Status**:
   ```bash
   docker-compose ps minio
   docker-compose logs minio --tail=100
   ```

2. **Reduce Data Volume**: Query smaller time ranges or add LIMIT clauses

3. **Check MinIO Connection Limits**:
   ```bash
   # MinIO may have default connection limits
   # Consider restarting MinIO with higher limits
   docker-compose restart minio
   ```

4. **Disable Delete Files Reading** (if equality deletes are the issue):
   ```python
   # This is an Iceberg-specific setting
   .config("spark.sql.catalog.lakekeeper.read.delete-files", "false")
   ```

5. **Use DuckDB Instead** (workaround):
   ```bash
   ./bin/5_duckdb_iceberg.sh
   ```

## Monitoring

Watch for these patterns in logs:
- ✅ `Cache cleared (attempt X/Y)` - Retries are working
- ❌ `Max retries exceeded` - Still failing after all retries
- ❌ Stage failures with `Connection reset` - Persistent S3 issue

## Related Files

- `scripts/user_activity_flow.py` - Main notebook with fixes
- `scripts/query_raw_iceberg.py` - DuckDB alternative
