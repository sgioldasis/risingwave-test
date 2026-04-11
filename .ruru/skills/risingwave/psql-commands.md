# RisingWave PSQL Commands

Common psql commands for interacting with RisingWave.

## Connection

```bash
# Connect to RisingWave
psql -h localhost -p 4566 -d dev -U root

# Execute a single command
psql -h localhost -p 4566 -d dev -U root -c "SELECT * FROM funnel LIMIT 5;"

# Execute SQL file
psql -h localhost -p 4566 -d dev -U root -f sql/all_models.sql
```

## Schema Inspection

```sql
-- List all tables
SHOW TABLES;

-- List all sources
SHOW SOURCES;

-- List all materialized views
SHOW MATERIALIZED VIEWS;

-- List all sinks
SHOW SINKS;

-- List all connections
SHOW CONNECTIONS;

-- Describe a table/view structure
DESCRIBE <table_name>;

-- Show create statement
SHOW CREATE TABLE <table_name>;
SHOW CREATE MATERIALIZED VIEW <view_name>;
SHOW CREATE SOURCE <source_name>;
SHOW CREATE SINK <sink_name>;
```

## System Configuration

```sql
-- Show current configuration
SHOW ALL;

-- Set system parameter
ALTER SYSTEM SET barrier_interval_ms = '250';

-- Show specific parameter
SHOW barrier_interval_ms;
```

## Data Exploration

```sql
-- Quick data preview
SELECT * FROM <table> LIMIT 10;

-- Count rows
SELECT COUNT(*) FROM <table>;

-- Check latest data
SELECT * FROM <table> ORDER BY <timestamp_column> DESC LIMIT 5;

-- Check for nulls
SELECT 
    COUNT(*) as total,
    COUNT(<column>) as non_null
FROM <table>;
```

## Process Management

```sql
-- Show running jobs
SHOW JOBS;

-- Cancel a job
CANCEL JOB <job_id>;

-- Show processes
SHOW PROCESSLIST;
```

## Troubleshooting

```sql
-- Check source is receiving data
SELECT COUNT(*) FROM <source_name>;

-- Verify materialized view is updating
SELECT MAX(<timestamp_column>) FROM <view_name>;

-- Check sink status
SHOW CREATE SINK <sink_name>;

-- Check for errors
SELECT * FROM rw_catalog.rw_errors LIMIT 10;
```

## Related Skills

- `risingwave/kafka-source` - Create sources
- `risingwave/materialized-view` - Create views
- `risingwave/kafka-sink` - Create sinks
