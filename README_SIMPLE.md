# Complete Guide: Redpanda -> RisingWave -> Iceberg Streaming Pipeline

This guide provides a complete, step-by-step process to build a real-time data pipeline that ingests events from a Redpanda topic, processes them in RisingWave, and writes the results to an Apache Iceberg table managed by Lakekeeper.

## Prerequisites
- [Docker](https://docs.docker.com/get-docker/) and [Docker Compose](https://docs.docker.com/compose/install/): Docker Compose is included with Docker Desktop for Windows and macOS. Ensure Docker Desktop is running if you're using it.
- [Devbox](https://www.jetify.com/devbox): Install via curl -fsSL https://get.jetify.com/devbox

## How to run the demo
Before you begin, ensure your docker-compose.yml stack is up and running with all services (RisingWave, Lakekeeper, MinIO, Redpanda) healthy.

```bash
docker compose up -d
```

The **Compose** file starts:

* **Lakekeeper** at [`127.0.0.1:8181`](http://127.0.0.1:8181) and provisions the Lakekeeper warehouse
* **RisingWave** at [`127.0.0.1:5691`](http://127.0.0.1:5691) (4566 for psql)
* **MinIO (S3-compatible)** at [`127.0.0.1:9301`](http://127.0.0.1:9301) with Username/Password hummockadmin
* **Redpanda** at [`127.0.0.1:9090`](http://127.0.0.1:9090)


## Step 1: Prepare the Environment
These commands must be run in your host terminal, not in the psql client.

### 1.1. Create the Redpanda Topic
First, create the topic in Redpanda that will act as the source for your data stream.

```bash
docker exec redpanda rpk topic create user_events
```

You should get the following output
```
TOPIC        STATUS
user_events  OK
```

### 1.2. Create the analytics Namespace in Lakekeeper
RisingWave needs a pre-existing namespace in the Iceberg catalog to create the sink. This is a two-step process with Lakekeeper.

```bash
# Get your unique Warehouse ID. This command queries the Lakekeeper management API. You may need to install jq for easy JSON parsing (sudo apt-get install jq or brew install jq).
# This command stores your Warehouse ID in a shell variable
WAREHOUSE_ID=$(curl -s http://localhost:8181/management/v1/warehouse | jq -r '.warehouses[0]."warehouse-id"')

# Use the Warehouse ID to create the analytics namespace.
curl -v -X POST \
  -H "Content-Type: application/json" \
  -d '{"namespace": ["analytics"]}' \
  http://localhost:8181/catalog/v1/"${WAREHOUSE_ID}"/namespaces

```
You should see a 200 OK or 201 Created response, confirming the namespace was created.

## Step 2: Configure the RisingWave Pipeline
Connect to your RisingWave instance with psql:

```bash
psql -h localhost -p 4566 -d dev -U root
```

Run the following SQL script inside your psql session. This script sets up the connection, source, table, and sink.

```sql
-- ==========================================================
-- == COMPLETE SCRIPT FOR REDPANDA -> RISINGWAVE -> ICEBERG ==
-- ==========================================================

-- Reduce barrier interval
ALTER SYSTEM SET barrier_interval_ms = '250';

-- 1. Create a connection object to Lakekeeper
-- This centralizes all catalog and storage configuration.
CREATE CONNECTION IF NOT EXISTS lakekeeper_catalog_conn
WITH (
    type = 'iceberg',
    catalog.type = 'rest',
    catalog.uri = 'http://lakekeeper:8181/catalog/',
    warehouse.path = 'risingwave-warehouse',
    s3.access.key = 'hummockadmin',
    s3.secret.key = 'hummockadmin',
    s3.path.style.access = 'true',
    s3.endpoint = 'http://minio-0:9301',
    s3.region = 'us-east-1'
);

-- 2. Set the connection as the default for Iceberg operations
-- This tells RisingWave to use the above connection for all subsequent
-- Iceberg table and sink commands in this session.
SET iceberg_engine_connection = 'public.lakekeeper_catalog_conn';

-- 3. Create a source to read from the 'user_events' topic in Redpanda
-- This defines the schema of the incoming JSON data.
CREATE SOURCE IF NOT EXISTS user_events_source (
    user_id INT,
    event_type VARCHAR,
    event_timestamp TIMESTAMP
) WITH (
    connector = 'kafka',
    topic = 'user_events',
    properties.bootstrap.server = 'redpanda:9092',
    scan.startup.mode = 'earliest'
) FORMAT PLAIN ENCODE JSON; -- Note: FORMAT clause is outside the WITH block for CREATE SOURCE

-- 4. Create the target Iceberg table
-- This creates the final table in the 'analytics' namespace of your Lakekeeper catalog.
CREATE TABLE IF NOT EXISTS user_events (
    user_id INT,
    event_type VARCHAR,
    event_timestamp TIMESTAMP
) ENGINE = iceberg;

-- 5. Create a sink to write from the source INTO the target table
-- This links the streaming source to the Iceberg table.
-- CREATE SINK IF NOT EXISTS user_events_sink
-- INTO user_events
-- FROM user_events_source;

-- If you want to drop a previously created sink
-- DROP SINK IF EXISTS user_events_sink;

-- For the demo, we set it up to commit on every checkpoint
CREATE SINK user_events_sink
INTO user_events
FROM user_events_source
WITH (
    commit_checkpoint_interval = 1 -- Commit on every checkpoint
);

-------------------------------------------------------------------------------------

-- ==========================================================
-- == SCRIPT TO CREATE A STREAMING AGGREGATION TABLE ==
-- ==========================================================

-- 1. Create a materialized view to calculate counts in real-time
CREATE MATERIALIZED VIEW IF NOT EXISTS user_event_counts_mv AS
SELECT
    user_id,
    COUNT(*) AS event_count
FROM
    user_events
GROUP BY
    user_id;

-- 2. Create the target Iceberg table to store the aggregated results
CREATE TABLE IF NOT EXISTS user_event_counts (
    user_id INT PRIMARY KEY,
    event_count BIGINT
) ENGINE = iceberg;

-- 3. Create an upsert sink to link the view to the table (Corrected Syntax)
CREATE SINK IF NOT EXISTS user_event_counts_sink
INTO user_event_counts
FROM user_event_counts_mv
WITH (
    type = 'upsert',
    primary_key = 'user_id'
);

-- ==========================================================
-- == SETUP COMPLETE! Now proceed to Step 3 for verification ==
-- ==========================================================

```


## Step 3: Verify the Pipeline
Now that everything is configured, let's test the end-to-end flow.

### 3.1. Send Test Data to Redpanda
You can use the following Python script to produce events:

```bash
# python scripts/produce_events.py -n 10
python scripts/produce_events.py --count 10
# or explicitly
python scripts/produce_events.py --count 10 --schema_version 1
```

> **_NOTE:_**
You can also produce events manually. In your host terminal, run the following command. It will wait for you to type a message.

```bash
docker exec -i redpanda rpk topic produce user_events
```

Type the following JSON message, press Enter, and then press Ctrl+D to send it.

```json
{"user_id": 101, "event_type": "savas", "event_timestamp": "2023-10-27T10:00:00.000"}
{"user_id": 102, "event_type": "savas2", "event_timestamp": "2023-10-27T10:00:01.000", "device_type": "browser"}
```

### 3.2. Query the Final Iceberg Table
Finally, in your psql session, run this query to see the data that has flowed through the pipeline.

```sql
SELECT * FROM user_events ORDER BY user_id;
SELECT * FROM user_event_counts ORDER BY user_id;
```

You should see the event you just produced, confirming that your real-time streaming pipeline from Redpanda to Iceberg is working correctly.

> **_NOTE:_**
It might take some time before the table is updated. You can try to flush the data:

```sql
FLUSH;
```


Since your syslog data is stored in an Iceberg table managed by Lakekeeper and MinIO, you can query it using DuckDB in several ways:

## Using DuckDB with REST Catalog

In order to use DuckDB with REST Catalog from your local machine, you first need to add minio-0 to your /etc/hosts file:

```bash
# Add minio-0 to your /etc/hosts file:
echo "127.0.0.1 minio-0" | sudo tee -a /etc/hosts
```

Now start duckdb
```bash
duckdb
```

Then connect to your Iceberg data:

```sql

-- Install extensions
INSTALL aws;
INSTALL httpfs;
INSTALL avro;
INSTALL iceberg;

-- **** LOAD them (this is mandatory!) ****
LOAD aws;
LOAD httpfs;
LOAD avro;
LOAD iceberg;

-- Configure S3
SET s3_endpoint = 'http://localhost:9301';
SET s3_access_key_id = 'hummockadmin';
SET s3_secret_access_key = 'hummockadmin';
SET s3_url_style = 'path';
SET s3_use_ssl = false;

-- Attach catalog (remove trailing space!)
ATTACH 'risingwave-warehouse' AS lakekeeper_catalog (
      TYPE ICEBERG,
      ENDPOINT 'http://127.0.0.1:8181/catalog',
      AUTHORIZATION_TYPE 'none'
);

-- Now query
SELECT * FROM lakekeeper_catalog.public.user_events ORDER BY user_id;

SELECT * FROM lakekeeper_catalog.public.user_event_counts order by user_id;

```

## Schema Evolution

## Step 1: Configure the RisingWave Pipeline
Run the following SQL script inside your psql session. This script adjusts the previously created objects in order to accomodate a new column `device_type`.

```sql
-- ==========================================================
-- == SCHEMA EVOLUTION ==
-- ==========================================================

-- Reduce barrier interval
ALTER SYSTEM SET barrier_interval_ms = '250';

-- 1. Set the connection as the default for Iceberg operations
-- This tells RisingWave to use the above connection for all subsequent
-- Iceberg table and sink commands in this session.
SET iceberg_engine_connection = 'public.lakekeeper_catalog_conn';

-- 2. Add the new column to the source
-- This command adds a nullable VARCHAR column to the source. Now, RisingWave will start reading the device_type field from incoming messages.
ALTER SOURCE user_events_source ADD COLUMN device_type VARCHAR;

-- 3. Drop all existing objects to ensure a clean slate for the new schema
DROP SINK IF EXISTS user_events_sink;
DROP SINK IF EXISTS user_event_counts_sink;
DROP TABLE IF EXISTS user_events CASCADE;
DROP TABLE IF EXISTS user_event_counts;

-- 4. Recreate the raw events table with the new column
CREATE TABLE user_events (
    user_id INT,
    event_type VARCHAR,
    event_timestamp TIMESTAMP,
    device_type VARCHAR
) ENGINE = iceberg;

-- 5. Recreate the materialized view with the ORIGINAL logic (simple count by user_id)
-- This view ignores the new device_type column and provides a total count per user.
CREATE MATERIALIZED VIEW user_event_counts_mv AS
SELECT
    user_id,
    COUNT(*) AS event_count
FROM
    user_events
GROUP BY
    user_id;

-- 6. Recreate the target aggregation table with the ORIGINAL schema
CREATE TABLE user_event_counts (
    user_id INT PRIMARY KEY,  -- Simple primary key on user_id
    event_count BIGINT
) ENGINE = iceberg;

-- 7. Recreate both sinks
CREATE SINK user_events_sink
INTO user_events
FROM user_events_source
WITH (
    commit_checkpoint_interval = 1
);

CREATE SINK user_event_counts_sink
INTO user_event_counts
FROM user_event_counts_mv
WITH (
    type = 'upsert',
    primary_key = 'user_id'  -- Upsert based on user_id only
);

-- ==========================================================
-- == SETUP COMPLETE! ==
-- ==========================================================
```

## Step 2: Send Test Data to Redpanda
You can use the following Python script to produce events with the new schema:

```bash
# python scripts/produce_events_v2.py -n 10
python scripts/produce_events.py --count 5 --schema_version 2
```

## Step 3: Verify 
Now you can query the table as described previously (either from psql or duckdb) to verify that the new column is present in the new records (it should be NULL in the old records)

## Cleanup
After you finish, bring the system down by issuing the following command

```bash
docker compose down --volumes --remove-orphans
```
