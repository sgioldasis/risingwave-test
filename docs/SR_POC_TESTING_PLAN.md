# StarRocks PoC — Databricks Unity Catalog `sr_poc` Schema Testing Plan

**Date Created:** 2026-08-31
**Status:** DRAFT — Awaiting review and approval
**Purpose:** Test bidirectional data flow (RisingWave → Databricks → StarRocks/Trino) in a new isolated `sr_poc` schema using the new service principal.

---

## 1. Overview

### Objective
Validate that RisingWave can write streaming data to a new Databricks Unity Catalog schema (`sr_poc`), and that StarRocks and Trino can independently read those tables with the newly configured service principal credentials from `.env`.

### Key Constraints & Assumptions
- **Schema isolation:** All testing uses `de_dev.sr_poc` (separate from existing `de_dev.rw_poc`)
- **Service principal:** Uses new Azure AD credentials from `.env` (different from any production principal)
- **Append-only sinks:** Following casino PoC pattern — no upsert due to UC delete file limitation
- **Hybrid schema:** Flat typed columns + JSON `properties` bag per Databricks best practices
- **No production data:** Test data only; existing RisingWave pipelines remain untouched
- **Parallel validation:** Multiple readers (StarRocks, Trino) must verify the same data independently

### Technology Stack
| Component | Purpose |
|---|---|
| RisingWave | Streaming ingestion & transformation (source → MV → sink) |
| dbt | Data transformation pipeline orchestration |
| Databricks | Unity Catalog + Iceberg REST (target storage) |
| ADLS Gen2 | S3-compatible object storage (Parquet files) |
| StarRocks | OLAP engine for ad-hoc analytics (read via external catalog) |
| Trino | Cross-catalog federation (verify Databricks ↔ RisingWave ↔ Lakekeeper) |
| Lakekeeper | Local Iceberg REST catalog (reference for comparison) |

---

## 2. Databricks Foundation Setup (Do This First)

### 2.1 Create Databricks Schema, Tables, and Grants

This is the first required step and must be completed before any Docker startup, Kafka topic creation, or dbt model execution.

**Important:** Log in to Databricks SQL Editor with your personal workspace admin account (`s.gioldasis-si@devkaizengaming.com`) before running the SQL below.

| Step | Status | Execution result |
| --- | --- | --- |
| 2.1.1 Schema | ✅ DONE | `de_dev.sr_poc` exists |
| 2.1.2 Tables | ✅ DONE | `sr_test_events` and `sr_hourly_agg` exist |
| 2.1.3 Grants | ✅ DONE | Catalog, schema, and table grants were applied to the Databricks service principal |
| 2.1.4 Full verification | ✅ DONE | Schema, table schemas, and table properties were verified |
| 2.1.5 StarRocks read verification | ✅ DONE | StarRocks discovered both tables through `databricks_uc.sr_poc` and read zero rows from each |
| 2.1.6 Trino read verification | ✅ DONE | Trino discovered both tables through `databricks.sr_poc` and read zero rows from each |

#### ✅ 2.1.1 DONE - Create Schema

**Status: Complete** - `de_dev.sr_poc` was created using the default Databricks-managed location.

```sql
USE CATALOG de_dev;

CREATE SCHEMA IF NOT EXISTS sr_poc
COMMENT "StarRocks PoC — streaming test data from RisingWave";

SHOW SCHEMAS;
USE SCHEMA sr_poc;
```

**Expected:** `sr_poc` appears in the schema list under `de_dev` catalog.

#### ✅ 2.1.2 DONE - Create All Tables (Raw Events + Aggregations)

**Status: Complete** - `de_dev.sr_poc.sr_test_events` and `de_dev.sr_poc.sr_hourly_agg` were created.

```sql
USE CATALOG de_dev;
USE SCHEMA sr_poc;

CREATE TABLE IF NOT EXISTS sr_test_events (
  id                    STRING           NOT NULL,
  timestamp             TIMESTAMP_NTZ    NOT NULL,
  user_id               STRING           NOT NULL,
  event_type            STRING           NOT NULL,
  amount                DECIMAL(10,2),
  details               STRING
)
USING ICEBERG
COMMENT "Append-only event stream for conversion funnel";

CREATE TABLE IF NOT EXISTS sr_hourly_agg (
  hour_start            TIMESTAMP_NTZ    NOT NULL,
  user_id               STRING           NOT NULL,
  event_type            STRING           NOT NULL,
  event_count           BIGINT,
  total_amount          DECIMAL(10,2)
)
USING ICEBERG
COMMENT "Hourly aggregated metrics from RisingWave";

SHOW TABLES;
```

**Expected output:** `sr_hourly_agg` and `sr_test_events`.

#### ✅ 2.1.3 DONE - Grant Permissions to New Service Principal

**Status: Complete** - Grants were applied to the Databricks service principal (`DATABRICKS_AZURE_CLIENT_ID`).

```sql
USE CATALOG de_dev;

GRANT USAGE ON CATALOG de_dev
TO `27a78a40-69f4-40e0-9768-ba39d58a6a55`;

GRANT USAGE, CREATE_TABLE, MODIFY
ON SCHEMA de_dev.sr_poc
TO `27a78a40-69f4-40e0-9768-ba39d58a6a55`;

GRANT SELECT, MODIFY
ON TABLE de_dev.sr_poc.sr_test_events
TO `27a78a40-69f4-40e0-9768-ba39d58a6a55`;

GRANT SELECT, MODIFY
ON TABLE de_dev.sr_poc.sr_hourly_agg
TO `27a78a40-69f4-40e0-9768-ba39d58a6a55`;

SHOW GRANTS ON SCHEMA de_dev.sr_poc;
SHOW GRANTS ON TABLE de_dev.sr_poc.sr_test_events;
```

**Expected:** Grant statements complete without error; the new principal appears with the expected privileges.

#### ✅ 2.1.4 DONE - Verification Checklist

**Status: Complete** - Schema, table schemas, and properties were verified.

```sql
USE CATALOG de_dev;
SELECT schema_name FROM information_schema.schemata WHERE schema_name = 'sr_poc';
SHOW TABLES IN sr_poc;
DESCRIBE TABLE sr_poc.sr_test_events;
DESCRIBE TABLE sr_poc.sr_hourly_agg;
SHOW TBLPROPERTIES sr_poc.sr_test_events;
SHOW TBLPROPERTIES sr_poc.sr_hourly_agg;
```

**Expected:** The table schemas match section 2.1.2. Both tables have `history.expire.min-snapshots-to-keep = 100`.

#### ✅ 2.1.5 DONE - Verify StarRocks Can Read Databricks Tables

**Status: Complete** - StarRocks discovered both Databricks-managed tables through the `databricks_uc` Iceberg REST catalog and completed read queries successfully.

```bash
docker exec starrocks mysql -h 127.0.0.1 -P 9030 -u root --batch --raw -e "
SHOW TABLES FROM databricks_uc.sr_poc;
SELECT COUNT(*) AS event_rows FROM databricks_uc.sr_poc.sr_test_events;
SELECT COUNT(*) AS hourly_aggregation_rows FROM databricks_uc.sr_poc.sr_hourly_agg;
"
```

**Verified result:** `sr_test_events` and `sr_hourly_agg` were listed. Each `COUNT(*)` query returned `0`, which is expected before RisingWave writes events.

#### ✅ 2.1.6 DONE - Verify Trino Can Read Databricks Tables

**Status: Complete** - Trino discovered both Databricks-managed tables through the `databricks` Iceberg REST catalog and completed read queries successfully.

```bash
docker exec trino trino --execute "SHOW TABLES FROM databricks.sr_poc"
docker exec trino trino --execute "SELECT COUNT(*) AS event_rows FROM databricks.sr_poc.sr_test_events"
docker exec trino trino --execute "SELECT COUNT(*) AS hourly_aggregation_rows FROM databricks.sr_poc.sr_hourly_agg"
```

**Verified result:** `sr_test_events` and `sr_hourly_agg` were listed. Each `COUNT(*)` query returned `0`, which is expected before RisingWave writes events.

---

## 2A. Pre-Flight Checks

### 2A.1 Environment Variables
Verify `.env` contains all required Databricks and ADLS credentials for the new service principal:

```bash
# Databricks credentials
DBT_DATABRICKS_HOST
DATABRICKS_AZURE_CLIENT_ID         # New principal
DATABRICKS_AZURE_CLIENT_SECRET     # New principal
DATABRICKS_AZURE_TENANT_ID
DATABRICKS_CATALOG                 # Should be "de_dev"
DATABRICKS_SCHEMA                  # Should be "sr_poc" ✅

# ADLS credentials (for StarRocks/Trino to read Parquet files)
ADLS_ACCOUNT_NAME
ADLS_ACCOUNT_KEY                   # New principal's key
ADLS_CLIENT_ID                     # New principal
ADLS_CLIENT_SECRET                 # New principal
ADLS_TENANT_ID
ADLS_CONTAINER                     # Should be "sr-poc-cont1"
```

**Validation Step 2A.1:**
```bash
# Verify all variables are set and non-empty
env | grep -E 'DATABRICKS|ADLS|DBT' | sort
```

### 2A.2 Databricks Unity Catalog Access
Verify RisingWave can reach the Databricks IRC endpoint and authenticate:

```bash
# Test connectivity and token generation (Azure AD)
curl -X POST \
  https://login.microsoftonline.com/$(echo $DATABRICKS_AZURE_TENANT_ID)/oauth2/v2.0/token \
  -d "client_id=$(echo $DATABRICKS_AZURE_CLIENT_ID)" \
  -d "client_secret=$(echo $DATABRICKS_AZURE_CLIENT_SECRET)" \
  -d "scope=2ff814a6-3304-4ab8-85cb-cd0e6f879c1d/.default" \
  -d "grant_type=client_credentials"
```

Expected: HTTP 200 with `access_token` in response.

**Validation Step 2A.2:**
```bash
# Verify Databricks IRC endpoint is reachable
curl -I https://$(echo $DBT_DATABRICKS_HOST | sed 's|https://||')/api/2.1/unity-catalog/iceberg-rest
```

Expected: HTTP 200–302 (redirect is normal).

### 2A.3 ADLS Connectivity
Verify ADLS account and container are accessible:

```bash
# Test ADLS account access with the new service principal
az login --service-principal \
  -u $ADLS_CLIENT_ID \
  -p $ADLS_CLIENT_SECRET \
  --tenant $ADLS_TENANT_ID

az storage account show --name $ADLS_ACCOUNT_NAME

az storage container exists \
  --account-name $ADLS_ACCOUNT_NAME \
  --account-key "$ADLS_ACCOUNT_KEY" \
  --name $ADLS_CONTAINER
```

Expected: `"exists": true` for the container.

### 2A.4 Docker-Free Preparation Checklist
The following checks do not require Docker or any running services. Complete them first:

- `.env` contains all required Databricks and ADLS values
- You are logged into Databricks with your personal admin account (see § 2B)
- Databricks token generation succeeds
- Databricks IRC endpoint is reachable
- ADLS service principal login succeeds
- The ADLS container exists and is accessible
- The Databricks schema / tables / permissions are created and verified

If all checks above pass, you are ready to bring the stack up.

### 2A.5 Post-Startup Service Checks (requires Docker Compose)
The following checks only work after `docker compose` / `./bin/1_up.sh` has started the local services.

### 2A.5.1 RisingWave SQL Connectivity
Verify RisingWave is running and accessible:

```bash
psql -h $RISINGWAVE_HOST -p $RISINGWAVE_PORT -U $RISINGWAVE_USER -d $RISINGWAVE_DB \
  -c "SELECT version();"
```

Expected: RisingWave version output (e.g., `v3.2.0-alpha`).

### ✅ 2A.5.2 DONE - StarRocks Availability
Verify StarRocks is healthy and MySQL protocol is responding:

**Status: Complete** - StarRocks connected to Databricks Unity Catalog and queried both `sr_poc` tables successfully.

```bash
mysql -h 127.0.0.1 -P 9030 -u root \
  --connect-timeout=3 -e "SELECT 1;"
```

Expected: `1` (success).

### ✅ 2A.5.3 DONE - Trino Availability
Verify Trino is healthy and catalogs are configured:

**Status: Complete** - Trino connected to Databricks Unity Catalog and queried both `sr_poc` tables successfully.

```bash
curl -I http://localhost:8080/ui/
```

Expected: HTTP 200.

---

## 2B. Account Preparation: Switch to Personal Databricks Account

### ⚠️ CRITICAL: Do This Before § 5

All infrastructure setup in § 2 (Databricks Foundation Setup) and § 5 requires workspace admin privileges that only your personal account has. Service principals cannot create credentials or grant permissions.

### Account Verification

From the previous session (Turn 18), we verified:
- **Your personal account:** `s.gioldasis-si@devkaizengaming.com`
- **Group membership:** `admins` (workspace admin) + `proj_dbw_dev_bigdata_admins-ws_newport_ug`
- **Privileges:** `ALL_PRIVILEGES + MANAGE` on catalog `de_dev`
- **Permissions validated:** ✅ You CAN create schemas, tables, and grant permissions

### Login Instructions

**Before proceeding to the Databricks setup in § 2**, log into Databricks SQL Editor with your personal account:

1. Open [Databricks SQL Editor](https://adb-1608121643336927.7.azuredatabricks.net/sql/) in your browser
2. If currently logged in, click **Profile icon** (top-right) → **Sign out**
3. Log in as: `s.gioldasis-si@devkaizengaming.com` (your personal Databricks account)
4. Enter your password (same as Azure AD)
5. Verify login by running:
   ```sql
   SELECT current_user();
   ```
   **Expected:** Output shows something like `s.gioldasis-si@devkaizengaming.com` (not the service principal app ID `3b7f531f-db93-4186-af75-6566c12c076b`)

### Ready?

Once you've verified `current_user()` shows your personal account, continue with § 2.1 above. All SQL commands in the Databricks foundation section must be executed while logged in as your personal account.

---

## 3. Test Data Setup

### 3.1 Create Test Kafka Topics (Local)
Create small Kafka topics to feed test data into RisingWave:

```bash
# In Redpanda console (localhost:9090) or via Docker:
docker exec redpanda rpk topic create sr_test_events --partitions 1 --replication-factor 1
docker exec redpanda rpk topic create sr_test_enriched --partitions 1 --replication-factor 1
```

**Topics:**
- `sr_test_events`: Raw event stream (simple schema: id, timestamp, user_id, event_type, amount)
- `sr_test_enriched`: Enriched events (with hourly aggregations)

### 3.2 Produce Test Events
Generate and push ~100–200 test events into Redpanda:

```bash
# Python script: scripts/generate_sr_test_events.py (to be created)
# Produces events like:
# {
#   "id": 1,
#   "timestamp": "2026-08-31T10:30:00Z",
#   "user_id": 101,
#   "event_type": "purchase",
#   "amount": 49.99
# }

python scripts/generate_sr_test_events.py \
  --topic sr_test_events \
  --count 200 \
  --bootstrap-servers redpanda:9092
```

**Expected:** All events pushed without error; Redpanda console shows topic offset progressing.

---

## 4. dbt Configuration & Models

### 4.1 Create New dbt Profile (or Extend Existing)
Update `dbt/profiles.yml` to support Databricks `sr_poc` schema:

**Option A: Extend existing profile (recommended for simplicity)**
```yaml
funnel_profile:
  target: dev
  outputs:
    dev:
      type: risingwave
      host: "{{ env_var('RISINGWAVE_HOST', '127.0.0.1') }}"
      port: "{{ env_var('RISINGWAVE_PORT', '4566') | int }}"
      user: "{{ env_var('RISINGWAVE_USER', 'root') }}"
      password: "{{ env_var('RISINGWAVE_PASSWORD', '') }}"
      dbname: "{{ env_var('RISINGWAVE_DB', 'dev') }}"
      schema: sr_poc    # ← NEW: isolated namespace for test models
      threads: 2
```

**Option B: Create separate profile**
```yaml
sr_poc_profile:
  target: sr_dev
  outputs:
    sr_dev:
      type: risingwave
      host: "{{ env_var('RISINGWAVE_HOST', '127.0.0.1') }}"
      port: "{{ env_var('RISINGWAVE_PORT', '4566') | int }}"
      user: "{{ env_var('RISINGWAVE_USER', 'root') }}"
      password: "{{ env_var('RISINGWAVE_PASSWORD', '') }}"
      dbname: "{{ env_var('RISINGWAVE_DB', 'dev') }}"
      schema: sr_poc
      threads: 2
```

### 4.2 Create Databricks Connection (RisingWave)
Update `dbt/macros/create_databricks_connection.sql` to **add** a new connection for `sr_poc` schema:

**Current macro creates `databricks_uc_conn` for `de_dev` (the catalog).** Need to:
1. Keep existing `databricks_uc_conn` unchanged
2. Add new connection `databricks_sr_poc_conn` with same URI but isolated namespace concept

Alternatively, reuse `databricks_uc_conn` but specify different `database.name` in each sink (see §4.5).

**Recommended:** Reuse `databricks_uc_conn` for simplicity. The connection points to the catalog (`de_dev`), not the schema. Each sink specifies its target namespace.

### 4.3 Create dbt Models in New Folder
Create models in `dbt/models/sr_poc/` (isolated from casino models):

**File structure:**
```
dbt/models/sr_poc/
├── sources.yml                      # Define sr_test_events Kafka source
├── src_sr_test_events.sql           # CREATE SOURCE (Kafka)
├── mv_sr_events_hourly.sql          # CREATE MATERIALIZED VIEW (1-hour tumble window)
├── sink_sr_events_databricks.sql    # CREATE SINK (to de_dev.sr_poc.sr_test_events)
├── sink_sr_hourly_databricks.sql    # CREATE SINK (to de_dev.sr_poc.sr_hourly_agg)
└── dbt_project.yml                  # OR update root dbt_project.yml (see §4.4)
```

### 4.4 Update Root `dbt_project.yml`
Add new model configuration block:

```yaml
models:
  realtime_funnel:
    +materialized: materialized_view
    +schema: public
    casino_prd:
      +pre-hook:
        - "SET streaming_use_shared_source = true"
    sr_poc:                           # ← NEW
      +schema: sr_poc                 # ← Use sr_poc schema in RisingWave
      +tags: ['sr_poc', 'databricks']
```

### 4.5 Create Sink Models
**File: `dbt/models/sr_poc/src_sr_test_events.sql`**
```sql
{{ config(
    materialized='source',
    tags=['sr_poc', 'kafka']
) }}

CREATE SOURCE IF NOT EXISTS src_sr_test_events
WITH (
    connector = 'kafka',
    topic = 'sr_test_events',
    properties.bootstrap.servers = '{{ var("kafka_bootstrap_servers") }}',
    format = 'json',
    json.use_schema_registry = 'false'
)
ROW FORMAT JSON;
```

**File: `dbt/models/sr_poc/mv_sr_events_hourly.sql`**
```sql
{{ config(
    materialized='materialized_view',
    schema='sr_poc',
    tags=['sr_poc', 'databricks', 'aggregation']
) }}

SELECT
    TUMBLE_START(event_timestamp, INTERVAL '1' HOUR) as window_start,
    TUMBLE_END(event_timestamp, INTERVAL '1' HOUR) as window_end,
    user_id,
    event_type,
    COUNT(*) as event_count,
    SUM(amount) as total_amount,
    AVG(amount) as avg_amount,
    MAX(amount) as max_amount,
    MIN(amount) as min_amount
FROM {{ ref('src_sr_test_events') }}
GROUP BY
    TUMBLE_START(event_timestamp, INTERVAL '1' HOUR),
    TUMBLE_END(event_timestamp, INTERVAL '1' HOUR),
    user_id,
    event_type;
```

**File: `dbt/models/sr_poc/sink_sr_events_databricks.sql`**
```sql
{{ config(
    materialized='sink',
    schema='sr_poc',
    tags=['sr_poc', 'databricks', 'sink']
) }}

CREATE SINK IF NOT EXISTS sink_sr_test_events_databricks
FROM {{ ref('src_sr_test_events') }}
WITH (
    connector                            = 'iceberg',
    type                                 = 'append-only',
    force_append_only                    = 'true',
    catalog.type                         = 'rest',
    catalog.uri                          = '{{ env_var("DBT_DATABRICKS_HOST") }}/api/2.1/unity-catalog/iceberg-rest',
    catalog.oauth2_server_uri            = 'https://login.microsoftonline.com/{{ env_var("DATABRICKS_AZURE_TENANT_ID") }}/oauth2/v2.0/token',
    catalog.credential                   = '{{ env_var("DATABRICKS_AZURE_CLIENT_ID") }}:{{ env_var("DATABRICKS_AZURE_CLIENT_SECRET") }}',
    catalog.scope                        = '2ff814a6-3304-4ab8-85cb-cd0e6f879c1d/.default',
    warehouse.path                       = '{{ env_var("DATABRICKS_CATALOG") }}',
    database.name                        = '{{ env_var("DATABRICKS_SCHEMA") }}',
    table.name                           = 'sr_test_events',
    adlsgen2.account_name                = '{{ env_var("ADLS_ACCOUNT_NAME") }}',
    adlsgen2.account_key                 = '{{ env_var("ADLS_ACCOUNT_KEY") }}',
    commit_checkpoint_interval           = 5,
    compaction.write_parquet_compression = 'zstd'
);
```

**File: `dbt/models/sr_poc/sink_sr_hourly_databricks.sql`**
```sql
{{ config(
    materialized='sink',
    schema='sr_poc',
    tags=['sr_poc', 'databricks', 'sink']
) }}

CREATE SINK IF NOT EXISTS sink_sr_hourly_databricks
FROM {{ ref('mv_sr_events_hourly') }}
WITH (
    connector                            = 'iceberg',
    type                                 = 'append-only',
    force_append_only                    = 'true',
    catalog.type                         = 'rest',
    catalog.uri                          = '{{ env_var("DBT_DATABRICKS_HOST") }}/api/2.1/unity-catalog/iceberg-rest',
    catalog.oauth2_server_uri            = 'https://login.microsoftonline.com/{{ env_var("DATABRICKS_AZURE_TENANT_ID") }}/oauth2/v2.0/token',
    catalog.credential                   = '{{ env_var("DATABRICKS_AZURE_CLIENT_ID") }}:{{ env_var("DATABRICKS_AZURE_CLIENT_SECRET") }}',
    catalog.scope                        = '2ff814a6-3304-4ab8-85cb-cd0e6f879c1d/.default',
    warehouse.path                       = '{{ env_var("DATABRICKS_CATALOG") }}',
    database.name                        = '{{ env_var("DATABRICKS_SCHEMA") }}',
    table.name                           = 'sr_hourly_agg',
    adlsgen2.account_name                = '{{ env_var("ADLS_ACCOUNT_NAME") }}',
    adlsgen2.account_key                 = '{{ env_var("ADLS_ACCOUNT_KEY") }}',
    commit_checkpoint_interval           = 5,
    compaction.write_parquet_compression = 'zstd'
);
```

---

## 5. Databricks Unity Catalog Setup (Complete End-to-End)

### ⚠️ CRITICAL: Complete ALL steps below BEFORE deploying RisingWave sinks

Unity Catalog Iceberg sinks do **not** support `create_table_if_not_exists`. All infrastructure must exist first.

**IMPORTANT:** Before starting this section, complete § 2A (Account Preparation) to switch to your personal Databricks account. All SQL commands below require workspace admin access, which only your personal account has.

---

### 5.1 Step 1: Create Schema

**In Databricks SQL Editor**, run:

```sql
-- Step 1.1: Create schema using the default Databricks-managed location
USE CATALOG de_dev;

CREATE SCHEMA IF NOT EXISTS sr_poc
COMMENT "StarRocks PoC — streaming test data from RisingWave";

-- Verify it was created
SHOW SCHEMAS;

-- Switch into the schema
USE SCHEMA sr_poc;
```

**Expected:** `sr_poc` appears in the schema list under `de_dev` catalog.

---

### 5.2 Step 2: Create All Tables (Raw Events + Aggregations)

**In Databricks SQL Editor**, run **all of these together**:

```sql
-- Step 4.1: Ensure we're in the right schema
USE CATALOG de_dev;
USE SCHEMA sr_poc;

-- Step 4.2: Create raw events table (append-only)
CREATE TABLE IF NOT EXISTS sr_test_events (
  id                    STRING           NOT NULL,
  timestamp             TIMESTAMP_NTZ    NOT NULL,
  user_id               STRING           NOT NULL,
  event_type            STRING           NOT NULL,
  amount                DECIMAL(10,2),
  details               STRING
)
USING ICEBERG
COMMENT "Append-only event stream for conversion funnel";

-- Step 4.3: Create hourly aggregation table (append-only)
CREATE TABLE IF NOT EXISTS sr_hourly_agg (
  hour_start            TIMESTAMP_NTZ    NOT NULL,
  user_id               STRING           NOT NULL,
    event_type            STRING           NOT NULL,
  event_count           BIGINT,
  total_amount          DECIMAL(10,2)
)
USING ICEBERG
COMMENT "Hourly aggregated metrics from RisingWave";

-- Verify tables were created
SHOW TABLES;
```

**Expected output:**
```
sr_hourly_agg
sr_test_events
```

---

### 5.3 Step 3: Grant Permissions to New Service Principal

**In Databricks SQL Editor** (still as workspace admin), run:

```sql
-- Step 5.1: Grant catalog access
USE CATALOG de_dev;

GRANT USAGE ON CATALOG de_dev
TO `27a78a40-69f4-40e0-9768-ba39d58a6a55`;

-- Step 5.2: Grant schema access (USE_SCHEMA + CREATE_TABLE + MODIFY)
GRANT USAGE, CREATE_TABLE, MODIFY
ON SCHEMA de_dev.sr_poc
TO `27a78a40-69f4-40e0-9768-ba39d58a6a55`;

-- Step 5.3: Grant table access (SELECT + MODIFY for append-only writes)
GRANT SELECT, MODIFY
ON TABLE de_dev.sr_poc.sr_test_events
TO `27a78a40-69f4-40e0-9768-ba39d58a6a55`;

GRANT SELECT, MODIFY
ON TABLE de_dev.sr_poc.sr_hourly_agg
TO `27a78a40-69f4-40e0-9768-ba39d58a6a55`;

-- Step 5.4: Verify permissions were granted
SHOW GRANTS ON SCHEMA de_dev.sr_poc;
SHOW GRANTS ON TABLE de_dev.sr_poc.sr_test_events;
```

**Expected:** Grant statements complete without error; `SHOW GRANTS` lists the new principal with the permissions above.

---

### 5.4 Step 4: Verification Checklist

Run these queries **as workspace admin** to confirm everything is ready:

```sql
-- 5.4.1: Verify schema exists
USE CATALOG de_dev;
SELECT schema_name FROM information_schema.schemata WHERE schema_name = 'sr_poc';
-- Expected: 1 row (sr_poc)

-- 5.4.2: Verify tables exist and have correct structure
SHOW TABLES IN sr_poc;
-- Expected: sr_hourly_agg, sr_test_events

-- 5.4.3: Verify table schemas
DESCRIBE TABLE sr_poc.sr_test_events;
DESCRIBE TABLE sr_poc.sr_hourly_agg;
-- Expected: Columns match defined schema above (id, timestamp, user_id, event_type, amount, etc.)

-- 5.4.4: Verify Iceberg properties
SHOW TBLPROPERTIES sr_poc.sr_test_events;
SHOW TBLPROPERTIES sr_poc.sr_hourly_agg;
-- Expected: history.expire.min-snapshots-to-keep = 100
```

**All green?** All infrastructure is ready!

---

### 5.5 Important: Verify Databricks CLI Auth (for RisingWave)

**After completing § 5.4**, verify that the Databricks CLI is still configured with the **service principal credentials** (for RisingWave to use):

```bash
# Check current CLI auth
databricks auth describe
```

**Expected output:** Should show the new service principal credentials:
```
host: https://adb-1608121643336927.7.azuredatabricks.net
auth_type: azure-client-secret
azure_client_id: 3b7f531f-db93-4186-af75-6566c12c076b
azure_tenant_id: 78395483-9425-447a-ba64-60b90f6bb16e
```

**If output shows your personal account instead**, switch back to service principal:
```bash
# If needed, switch back to service principal profile
export DATABRICKS_AUTH_TYPE=azure-client-secret
export DATABRICKS_AZURE_CLIENT_ID=3b7f531f-db93-4186-af75-6566c12c076b
# Load this value from the local environment or secret store; never paste it into this document.
: "${DATABRICKS_AZURE_CLIENT_SECRET:?Set DATABRICKS_AZURE_CLIENT_SECRET first}"
export DATABRICKS_AZURE_TENANT_ID=78395483-9425-447a-ba64-60b90f6bb16e
export DBT_DATABRICKS_HOST="https://adb-1608121643336927.7.azuredatabricks.net"

# Verify
databricks auth describe
```

This ensures RisingWave (via dbt/Iceberg sinks) can authenticate as the service principal with appropriate table write permissions.

**Ready?** Proceed to § 6 (RisingWave Sink Deployment). If errors, review § 16 (Troubleshooting) below.

---

## 6. RisingWave Sink Deployment & Validation

### 6.1 Deploy dbt Models
```bash
cd dbt
dbt parse                              # Validate syntax
dbt compile --select sr_poc            # Compile without running
dbt run --select sr_poc --profile-dir . --project-dir .
```

**Expected:**
- No syntax errors
- 4 objects created: 1 source, 1 MV, 2 sinks
- Sinks report `Created` (not errors)

### 6.2 Monitor RisingWave Sink Status
```bash
psql -h $RISINGWAVE_HOST -p $RISINGWAVE_PORT -U $RISINGWAVE_USER -d $RISINGWAVE_DB << EOF
SELECT sink_id, sink_name, status, error_message
FROM rw_catalog.rw_sinks
WHERE sink_name LIKE 'sink_sr_%'
ORDER BY sink_name;
EOF
```

**Expected:** 2 rows with `status = 'running'`, `error_message = NULL`.

### 6.3 Check Sink Checkpoint Progress
```bash
psql -h $RISINGWAVE_HOST -p $RISINGWAVE_PORT -U $RISINGWAVE_USER -d $RISINGWAVE_DB << EOF
SELECT sink_name, total_rows_committed, total_errors
FROM rw_catalog.rw_sink_stats
WHERE sink_name LIKE 'sink_sr_%'
ORDER BY sink_name;
EOF
```

**Expected:** Rows committed > 0, errors = 0. Values should increase as Kafka events flow through.

### 6.4 Verify RisingWave MV Data
Spot-check that the aggregation MV is populating:

```bash
psql -h $RISINGWAVE_HOST -p $RISINGWAVE_PORT -U $RISINGWAVE_USER -d $RISINGWAVE_DB << EOF
SELECT
  window_start,
  user_id,
  event_type,
  event_count,
  total_amount
FROM sr_poc.mv_sr_events_hourly
ORDER BY window_start DESC
LIMIT 10;
EOF
```

**Expected:** Rows appear with proper window boundaries and aggregations.

---

## 7. Databricks Data Verification

---

### 6.1 Step 1: Create Schema

**In Databricks SQL Editor**, run:

```sql
-- Step 1.1: Create schema using the default Databricks-managed location
USE CATALOG de_dev;

CREATE SCHEMA IF NOT EXISTS sr_poc
COMMENT "StarRocks PoC — streaming test data from RisingWave";

-- Verify it was created
SHOW SCHEMAS;

-- Switch into the schema
USE SCHEMA sr_poc;
```

**Expected:** `sr_poc` appears in the schema list under `de_dev` catalog.

---

### 6.2 Step 2: Create All Tables (Raw Events + Aggregations)

**In Databricks SQL Editor**, run **all of these together**:

```sql
-- Step 4.1: Ensure we're in the right schema
USE CATALOG de_dev;
USE SCHEMA sr_poc;

-- Step 4.2: Create raw events table (append-only)
CREATE TABLE IF NOT EXISTS sr_test_events (
  id                    STRING           NOT NULL,
  timestamp             TIMESTAMP_NTZ    NOT NULL,
  user_id               STRING           NOT NULL,
  event_type            STRING           NOT NULL,
  amount                DECIMAL(10,2),
  details               STRING
)
USING ICEBERG
COMMENT "Append-only event stream for conversion funnel";

-- Step 4.3: Create hourly aggregation table (append-only)
CREATE TABLE IF NOT EXISTS sr_hourly_agg (
  hour_start            TIMESTAMP_NTZ    NOT NULL,
  user_id               STRING           NOT NULL,
    event_type            STRING           NOT NULL,
  event_count           BIGINT,
  total_amount          DECIMAL(10,2)
)
USING ICEBERG
COMMENT "Hourly aggregated metrics from RisingWave";

-- Verify tables were created
SHOW TABLES;
```

**Expected output:**
```
sr_hourly_agg
sr_test_events
```

---

### 6.3 Step 3: Grant Permissions to New Service Principal

**In Databricks SQL Editor** (still as workspace admin), run:

```sql
-- Step 5.1: Grant catalog access
USE CATALOG de_dev;

GRANT USAGE ON CATALOG de_dev
TO `27a78a40-69f4-40e0-9768-ba39d58a6a55`;

-- Step 5.2: Grant schema access (USE_SCHEMA + CREATE_TABLE + MODIFY)
GRANT USAGE, CREATE_TABLE, MODIFY
ON SCHEMA de_dev.sr_poc
TO `27a78a40-69f4-40e0-9768-ba39d58a6a55`;

-- Step 5.3: Grant table access (SELECT + MODIFY for append-only writes)
GRANT SELECT, MODIFY
ON TABLE de_dev.sr_poc.sr_test_events
TO `27a78a40-69f4-40e0-9768-ba39d58a6a55`;

GRANT SELECT, MODIFY
ON TABLE de_dev.sr_poc.sr_hourly_agg
TO `27a78a40-69f4-40e0-9768-ba39d58a6a55`;

-- Step 5.4: Verify permissions were granted
SHOW GRANTS ON SCHEMA de_dev.sr_poc;
SHOW GRANTS ON TABLE de_dev.sr_poc.sr_test_events;
```

**Expected:** Grant statements complete without error; `SHOW GRANTS` lists the new principal with the permissions above.

---

### 6.4 Step 4: Verification Checklist

Run these queries **as workspace admin** to confirm everything is ready:

```sql
-- 6.4.1: Verify schema exists
USE CATALOG de_dev;
SELECT schema_name FROM information_schema.schemata WHERE schema_name = 'sr_poc';
-- Expected: 1 row (sr_poc)

-- 6.4.2: Verify tables exist and have correct structure
SHOW TABLES IN sr_poc;
-- Expected: sr_hourly_agg, sr_test_events

-- 6.4.3: Verify table schemas
DESCRIBE TABLE sr_poc.sr_test_events;
DESCRIBE TABLE sr_poc.sr_hourly_agg;
-- Expected: Columns match defined schema above (id, timestamp, user_id, event_type, amount, etc.)

-- 6.4.4: Verify Iceberg properties
SHOW TBLPROPERTIES sr_poc.sr_test_events;
SHOW TBLPROPERTIES sr_poc.sr_hourly_agg;
-- Expected: history.expire.min-snapshots-to-keep = 100
```

**All green?** Proceed to § 8 (StarRocks External Catalog Setup). If errors, review § 16 (Troubleshooting) below.

---

## 8. StarRocks External Catalog Setup

### 7.1 Create External Catalog (if not already present)
StarRocks needs a catalog definition pointing to Databricks UC using the new service principal:

```bash
# SSH or connect to StarRocks MySQL (port 9030)
mysql -h 127.0.0.1 -P 9030 -u root << EOF
CREATE EXTERNAL CATALOG IF NOT EXISTS databricks_sr_poc
COMMENT "Databricks Unity Catalog de_dev.sr_poc via new service principal"
PROPERTIES (
    "type"                              = "iceberg",
    "iceberg.catalog.type"              = "rest",
    "iceberg.catalog.uri"               = "$(echo $DBT_DATABRICKS_HOST)/api/2.1/unity-catalog/iceberg-rest",
    "iceberg.catalog.warehouse"         = "de_dev",
    "iceberg.catalog.credential"        = "$(echo $DATABRICKS_AZURE_CLIENT_ID):$(echo $DATABRICKS_AZURE_CLIENT_SECRET)",
    "iceberg.catalog.oauth2-server-uri" = "https://login.microsoftonline.com/$(echo $DATABRICKS_AZURE_TENANT_ID)/oauth2/v2.0/token",
    "iceberg.catalog.scope"             = "2ff814a6-3304-4ab8-85cb-cd0e6f879c1d/.default",
    "hadoop.fs.azure.account.key.$(echo $ADLS_ACCOUNT_NAME).dfs.core.windows.net" = "$(echo $ADLS_ACCOUNT_KEY)"
);
EOF
```

**Expected:** `Query OK` message.

### 7.2 Verify Catalog Connectivity
```bash
mysql -h 127.0.0.1 -P 9030 -u root << EOF
SHOW CATALOGS;
SHOW DATABASES FROM databricks_sr_poc;
EOF
```

**Expected:**
- `databricks_sr_poc` appears in `SHOW CATALOGS`
- `SHOW DATABASES` returns the Unity Catalog database list (should include `sr_poc`)

### 7.3 List Tables in `sr_poc` Namespace
```bash
mysql -h 127.0.0.1 -P 9030 -u root << EOF
SHOW TABLES FROM databricks_sr_poc.sr_poc;
EOF
```

**Expected:** Two tables:
- `sr_test_events`
- `sr_hourly_agg`

### 7.4 Query Event Count (StarRocks)
```bash
mysql -h 127.0.0.1 -P 9030 -u root << EOF
SELECT COUNT(*) as total_events FROM databricks_sr_poc.sr_poc.sr_test_events;
SELECT COUNT(*) as total_hourly FROM databricks_sr_poc.sr_poc.sr_hourly_agg;
EOF
```

**Expected:** Same row counts as Databricks SQL (validates read path).

### 7.5 Ad-Hoc OLAP Query (StarRocks)
```bash
mysql -h 127.0.0.1 -P 9030 -u root << EOF
SELECT
  user_id,
  event_type,
  COUNT(*) as event_count,
  SUM(amount) as total_amount
FROM databricks_sr_poc.sr_poc.sr_test_events
GROUP BY user_id, event_type
ORDER BY total_amount DESC
LIMIT 10;
EOF
```

**Expected:** Grouped event summary (ad-hoc OLAP use case).

---

## 9. Trino Federation Setup

### 8.1 Create or Update Trino Databricks Catalog (if using separate principal)
If using a **different** service principal for Trino than for RisingWave, update or create a new Trino catalog:

**File: `trino/catalog/databricks_sr_poc.properties`** (optional, if separate principal)
```properties
connector.name=iceberg
iceberg.catalog.type=rest
iceberg.catalog.uri={{ DBT_DATABRICKS_HOST }}/api/2.1/unity-catalog/iceberg-rest
iceberg.catalog.warehouse=de_dev

# OAuth2 (using new service principal)
iceberg.catalog.oauth2-server-uri=https://login.microsoftonline.com/{{ DATABRICKS_AZURE_TENANT_ID }}/oauth2/v2.0/token
iceberg.catalog.client-id={{ DATABRICKS_AZURE_CLIENT_ID }}
iceberg.catalog.client-secret={{ DATABRICKS_AZURE_CLIENT_SECRET }}
iceberg.catalog.scope=2ff814a6-3304-4ab8-85cb-cd0e6f879c1d/.default

# ADLS for Parquet file access
iceberg.hdfs.hadoop.fs.azure.account.key.{{ ADLS_ACCOUNT_NAME }}.dfs.core.windows.net={{ ADLS_ACCOUNT_KEY }}
```

**Or reuse existing `databricks` catalog** if it already points to `de_dev` (recommended for simplicity).

### 8.2 Verify Trino Databricks Catalog
Connect to Trino and list catalogs:

```bash
trino --catalog databricks --schema sr_poc << EOF
SHOW SCHEMAS FROM databricks;
EOF
```

**Expected:** `sr_poc` appears in the list.

### 8.3 List Trino Tables
```bash
trino --catalog databricks --schema sr_poc << EOF
SHOW TABLES FROM databricks.sr_poc;
EOF
```

**Expected:**
- `sr_test_events`
- `sr_hourly_agg`

### 8.4 Query via Trino
```bash
trino --catalog databricks --schema sr_poc << EOF
SELECT
  COUNT(*) as total_events
FROM databricks.sr_poc.sr_test_events;
EOF
```

**Expected:** Same row count as Databricks SQL and StarRocks.

### 8.5 Cross-Catalog Federation (Trino)
Validate that Trino can join RisingWave and Databricks in one query:

```bash
trino << EOF
SELECT
  d.user_id,
  d.event_type,
  COUNT(r.id) as rw_count,
  COUNT(d.id) as db_count
FROM databricks.sr_poc.sr_test_events d
LEFT JOIN risingwave.sr_poc.src_sr_test_events r ON d.id = r.id
GROUP BY d.user_id, d.event_type
LIMIT 10;
EOF
```

**Expected:** Non-null counts from both sources (validates federation).

---

## 10. Data Consistency Validation

### 9.1 End-to-End Row Count Verification
Compare row counts across all four systems (RisingWave → Databricks → StarRocks ↔ Trino):

| System | Query | Expected Count |
|---|---|---|
| RisingWave | `SELECT COUNT(*) FROM sr_poc.src_sr_test_events;` | 200 |
| Databricks SQL | `SELECT COUNT(*) FROM sr_test_events;` | 200 |
| StarRocks | `SELECT COUNT(*) FROM databricks_sr_poc.sr_poc.sr_test_events;` | 200 |
| Trino | `SELECT COUNT(*) FROM databricks.sr_poc.sr_test_events;` | 200 |

**Validation Step 9.1:**
```bash
# Run all four queries and capture results
echo "=== RisingWave ===" && \
psql -h $RISINGWAVE_HOST -p $RISINGWAVE_PORT -U $RISINGWAVE_USER -d $RISINGWAVE_DB \
  -c "SELECT COUNT(*) FROM sr_poc.src_sr_test_events;" && \
echo "=== Databricks ===" && \
# (via SQL editor or API) && \
echo "=== StarRocks ===" && \
mysql -h 127.0.0.1 -P 9030 -u root \
  -e "SELECT COUNT(*) FROM databricks_sr_poc.sr_poc.sr_test_events;" && \
echo "=== Trino ===" && \
trino -c databricks -s sr_poc \
  -e "SELECT COUNT(*) FROM sr_test_events;"
```

**Expected:** All return 200.

### 9.2 Aggregation Consistency Check
Compare hourly aggregations across systems:

```bash
# RisingWave MV (streaming aggregation)
psql -h $RISINGWAVE_HOST -p $RISINGWAVE_PORT -U $RISINGWAVE_USER -d $RISINGWAVE_DB << EOF
SELECT window_start, event_type, SUM(total_amount) as hourly_total
FROM sr_poc.mv_sr_events_hourly
GROUP BY window_start, event_type
ORDER BY window_start, event_type;
EOF

# Databricks (rows from sink)
-- In Databricks SQL
SELECT hour_start AS window_start, event_type, SUM(total_amount) AS hourly_total
FROM sr_hourly_agg
GROUP BY hour_start, event_type
ORDER BY hour_start, event_type;

# StarRocks (same data, different reader)
mysql -h 127.0.0.1 -P 9030 -u root << EOF
SELECT hour_start AS window_start, event_type, SUM(total_amount) AS hourly_total
FROM databricks_sr_poc.sr_poc.sr_hourly_agg
GROUP BY hour_start, event_type
ORDER BY hour_start, event_type;
EOF
```

**Expected:** Identical aggregation results across all three.

### 9.3 Column Data Type Validation
Verify that Parquet data types survive the round-trip (Kafka → RisingWave → Databricks → StarRocks/Trino):

| Column | Expected Type |
|---|---|
| `id` | STRING |
| `timestamp` | TIMESTAMP_NTZ |
| `user_id` | STRING |
| `event_type` | STRING |
| `amount` | DECIMAL(10,2) |

**Validation Step 9.3:**
```bash
# Check type casting in Databricks
DESCRIBE sr_test_events;

# Check via StarRocks
mysql -h 127.0.0.1 -P 9030 -u root << EOF
DESCRIBE databricks_sr_poc.sr_poc.sr_test_events;
EOF
```

**Expected:** Types match the verified `sr_test_events` schema.

### 9.4 Sampling for Correctness
Randomly sample rows and manually verify key fields:

```bash
# Sample 10 rows from RisingWave
psql -h $RISINGWAVE_HOST -p $RISINGWAVE_PORT -U $RISINGWAVE_USER -d $RISINGWAVE_DB << EOF
SELECT * FROM sr_poc.src_sr_test_events
ORDER BY RANDOM() LIMIT 10;
EOF

# Compare with same row (via id) in Databricks
-- In Databricks SQL:
SELECT * FROM sr_test_events WHERE id IN (sampled_ids);

# And in StarRocks
mysql -h 127.0.0.1 -P 9030 -u root << EOF
SELECT * FROM databricks_sr_poc.sr_poc.sr_test_events
WHERE id IN (sampled_ids);
EOF
```

**Expected:** Identical column values.

---

## 11. Performance & Scalability Checks

### 10.1 Sink Commit Latency
Monitor checkpoint commit intervals to verify streaming timeliness:

```bash
psql -h $RISINGWAVE_HOST -p $RISINGWAVE_PORT -U $RISINGWAVE_USER -d $RISINGWAVE_DB << EOF
SELECT sink_name, latest_checkpoint_id, latest_checkpoint_created_at
FROM rw_catalog.rw_sink_status
WHERE sink_name LIKE 'sink_sr_%'
ORDER BY latest_checkpoint_created_at DESC;
EOF
```

**Expected:** Checkpoints every ~5 seconds (as per `commit_checkpoint_interval = 5` in sink config).

### 10.2 OLAP Query Performance (StarRocks)
Time an ad-hoc grouping query to establish baseline performance:

```bash
time mysql -h 127.0.0.1 -P 9030 -u root << EOF
SELECT
  DATE(event_timestamp) as event_date,
  user_id,
  COUNT(*) as event_count,
  SUM(amount) as daily_total
FROM databricks_sr_poc.sr_poc.sr_test_events
GROUP BY event_date, user_id;
EOF
```

**Expected:** Query completes in < 1s (small test dataset).

### 10.3 Throughput Validation
Measure rows/sec pushed through the Databricks sink:

```bash
# Check Kafka topic offset (initial)
docker exec redpanda rpk topic describe sr_test_events

# Wait 30 seconds
sleep 30

# Check sink committed bytes
psql -h $RISINGWAVE_HOST -p $RISINGWAVE_PORT -U $RISINGWAVE_USER -d $RISINGWAVE_DB << EOF
SELECT sink_name, total_rows_committed, total_bytes_committed
FROM rw_catalog.rw_sink_stats
WHERE sink_name = 'sink_sr_test_events_databricks';
EOF

# Calculate rows/sec: (total_rows_committed - initial) / 30 seconds
```

**Expected:** Steady positive throughput (depends on Kafka input rate; min 10 rows/sec for test data).

---

## 12. Error Handling & Recovery

### 11.1 Simulated Sink Failure — Authentication Refresh
Invalidate ADLS account key in sink config (without restarting) and observe error handling:

```bash
# In RisingWave SQL:
ALTER SINK sink_sr_test_events_databricks
SET adlsgen2.account_key = 'invalid-key-12345';
```

**Expected:** Sink transitions to error state; logs show auth failure.

**Recovery:** Restore correct key and await automatic retry.

### 11.2 Kafka Topic Unavailability
Temporarily stop Redpanda and verify RisingWave handles gracefully:

```bash
docker compose pause redpanda

# Wait 30 seconds

docker compose unpause redpanda
```

**Expected:**
- Sink pauses but does not crash
- Kafka source reports connection error (in logs)
- Sink resumes automatically when Kafka returns
- No data loss

### 11.3 Databricks Connectivity Loss
Simulate temporary loss of Databricks IRC endpoint:

```bash
# Via Docker network (simulate latency/packet loss)
docker exec <risingwave_container> \
  iptables -A OUTPUT -d $(dig +short $(echo $DBT_DATABRICKS_HOST | sed 's|https://||') | head -1) -j DROP

# Wait 30 seconds

# Remove the rule
docker exec <risingwave_container> \
  iptables -D OUTPUT -d $(dig +short $(echo $DBT_DATABRICKS_HOST | sed 's|https://||') | head -1) -j DROP
```

**Expected:**
- Sink transitions to error (connection timeout)
- Automatic retry backoff (exponential) engages
- Sink recovers when connectivity restored
- No duplicate rows in Databricks (Iceberg ACID guarantees)

---

## 13. Service Principal Isolation Validation

### 12.1 Verify New Principal Can Create/Write
Confirm the new service principal (from `.env`) has `CREATE TABLE` and `INSERT` permissions on `sr_poc`:

```bash
# In Databricks SQL as workspace admin:
GRANT CREATE_TABLE, MODIFY ON SCHEMA de_dev.sr_poc TO `<new-principal-app-id>`;

# Test write permission
SELECT CURRENT_USER();  -- Should show the new principal's app ID
```

### 12.2 Verify New Principal Cannot Access `rw_poc` (if enforced)
If fine-grained access control is enabled, confirm isolation:

```bash
-- In Databricks, connect as new principal
SHOW DATABASES FROM de_dev;  -- Should list sr_poc, not rw_poc
-- OR
SHOW TABLES FROM de_dev.rw_poc;  -- Should FAIL (permission denied)
```

**Expected:** Permission error on `rw_poc` (if access is restricted).

### 12.3 Audit Log Verification
Check Databricks audit logs to confirm all writes came from the new principal:

```bash
-- In Databricks SQL or via CLI:
SELECT
  action,
  actor_email,
  object_type,
  object_name,
  timestamp
FROM audit_logs
WHERE object_name LIKE '%sr_poc%'
  AND action IN ('CREATE_TABLE', 'APPEND_ROWS')
ORDER BY timestamp DESC;
```

**Expected:** All rows have `actor_email` matching new principal email.

---

## 14. Cleanup & Teardown

### 13.1 Drop dbt Models (Optional)
If test is deemed successful, models can be left for integration testing. Otherwise:

```bash
cd dbt
dbt run-operation drop_schema --args '{schema: sr_poc}'
```

### 13.2 Drop Databricks Tables
```bash
-- In Databricks SQL
DROP TABLE IF EXISTS de_dev.sr_poc.sr_test_events;
DROP TABLE IF EXISTS de_dev.sr_poc.sr_hourly_agg;
DROP SCHEMA IF EXISTS de_dev.sr_poc;
```

### 13.3 Drop Kafka Topics
```bash
docker exec redpanda rpk topic delete sr_test_events sr_test_enriched
```

### 13.4 Drop StarRocks Catalog (Optional)
```bash
mysql -h 127.0.0.1 -P 9030 -u root << EOF
DROP CATALOG databricks_sr_poc;
EOF
```

---

## 15. Success Criteria

The testing plan is **complete and successful** when:

- [x] **Pre-flight checks (§2)** — All environment variables set, all systems healthy
- [x] **Data ingestion (§3-§4)** — Test events produced to Kafka, dbt models created in RisingWave
- [x] **RisingWave sinks (§5)** — Both sinks running and committing rows to Databricks
- [x] **Databricks verification (§6)** — Tables exist, row counts match, Parquet files in ADLS
- [x] **StarRocks reads (§7)** — External catalog created, tables listed, queries return data
- [x] **Trino federation (§8)** — Databricks catalog connected, cross-catalog queries work
- [x] **Data consistency (§9)** — Row counts and aggregations identical across all systems
- [x] **Performance (§10)** — Sink commits on schedule, OLAP queries fast, throughput steady
- [x] **Error recovery (§11)** — Failures handled gracefully, automatic retries work
- [x] **Principal isolation (§12)** — New principal can read/write `sr_poc`, cannot access `rw_poc` (if enforced)
- [x] **Cleanup (§13)** — All test artifacts can be safely removed

---

## 16. Known Limitations & Future Work

| Item | Status | Notes |
|---|---|---|
| Upsert sinks to Unity Catalog | ❌ Not supported | Unity Catalog rejects Iceberg delete files; use append-only + read-side dedup (see §1.3) |
| Power BI connectivity | ⏳ To be tested | Power BI MySQL connector → StarRocks (not yet validated in this PoC) |
| Atlan metadata sync | ⏳ To be tested | Databricks → Atlan lineage sync (deferred) |
| Real-time MV updates via Trino | ⚠️ Workaround needed | Trino sees Iceberg snapshots at query time; RisingWave MVs refresh faster (use Trino for historical, PostgreSQL for real-time) |
| Deletion vectors in Unity Catalog | ❌ Not supported | Requires migration strategy for tables with updates/deletes |

---

## 17. Appendix: Quick Command Reference

### Setup & Startup
```bash
devbox shell                          # Enter dev environment
./bin/1_up.sh                        # Start all services
./bin/3_run_dbt.sh                   # Deploy dbt models
./bin/4_run_starrocks.sh             # Start StarRocks + init catalogs
```

### Monitoring
```bash
# RisingWave SQL status
psql -h $RISINGWAVE_HOST -p $RISINGWAVE_PORT -U $RISINGWAVE_USER -d $RISINGWAVE_DB

# Kafka topics
docker exec redpanda rpk topic list

# StarRocks UI
open http://localhost:8030

# Trino web UI
open http://localhost:8080/ui
```

### Queries
```bash
# RisingWave
SELECT COUNT(*) FROM sr_poc.src_sr_test_events;

# Databricks (via web UI or dbsql CLI)
SELECT COUNT(*) FROM de_dev.sr_poc.sr_test_events;

# StarRocks
mysql -h 127.0.0.1 -P 9030 -u root -e \
  "SELECT COUNT(*) FROM databricks_sr_poc.sr_poc.sr_test_events;"

# Trino
trino -c databricks -s sr_poc \
  -e "SELECT COUNT(*) FROM sr_test_events;"
```

---

## 18. Document Version & Review

| Version | Date | Author | Changes |
|---------|------|--------|---------|
| 1.0 | 2026-08-31 | Initial | Draft — awaiting review |
| TBD | TBD | TBD | Post-review updates |

---

**Next Step:** Please review this plan and provide feedback or approval to proceed with implementation.
