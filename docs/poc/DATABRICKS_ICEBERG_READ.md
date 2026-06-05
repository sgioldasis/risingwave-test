# Databricks Unity Catalog — Reading from Iceberg Tables in RisingWave

This document covers the investigation into reading Databricks Unity Catalog Iceberg tables from RisingWave as a `CREATE SOURCE`, enabling joins between live streaming data and historical Databricks tables.

> **Status: BLOCKED — same ADLS vended-credentials issue as writes (2026-06-04).** RisingWave can authenticate to Unity Catalog and load table metadata, but cannot read Parquet files from Databricks' managed ADLS storage (`stkznneucommoncdddevstd`). Unblocked once admin creates non-managed-iceberg catalog backed by `stkznneurwpoccdddevstd`. See §5.

---

## 1. Motivation

Once a RisingWave Iceberg source is created, you can:

- **Join live streaming MVs with historical Databricks tables** in a single SQL query
- **Backfill materialized views** from existing Databricks data
- **Cross-system analytics** without moving data out of Databricks

```
Databricks UC table  ──► CREATE SOURCE in RisingWave  ──► JOIN with streaming MV
                                                            ──► New materialized view
```

---

## 2. Iceberg Source vs SQL/JDBC approach

There are two fundamentally different ways to get Databricks data into RisingWave:

### Approach A — Iceberg Source (this document)

RisingWave reads Parquet files **directly from ADLS/S3** using the Iceberg file format. The Unity Catalog REST API is used only for metadata (table schema, snapshot location). No SQL Warehouse needed.

### Approach B — Pipeline/bridge (no native connector)

RisingWave has **no native connector to Databricks SQL Warehouse**. There is no `CREATE SOURCE` that queries Databricks SQL directly. Alternatives that push data into something RisingWave can consume:

| Option | How it works | Latency | Complexity |
|---|---|---|---|
| **Kafka pipeline** | Databricks streams changes to Redpanda via Delta Live Tables or a Databricks job; RisingWave consumes from Kafka | Near real-time | High — requires Databricks streaming job + network access |
| **Dagster periodic sync** | Dagster asset queries Databricks SQL Warehouse via SDK and writes rows to RisingWave via psycopg2 | Minutes (batch) | Low — pure Python, reuses existing auth |
| **Trino** | Trino can query Databricks SQL Warehouse; but RisingWave can't consume Trino as a source — Trino remains a query tool for humans, not a RisingWave data feed | N/A | Medium — useful for ad-hoc analysis, not for RisingWave pipelines |

### Why Approach A (Iceberg Source) is better for RisingWave integration

There is no "view over a Databricks table" in RisingWave without first pulling the data through a file format or message queue. The Iceberg Source approach is the only path that is:
- Native to RisingWave (`CREATE SOURCE`)
- No SQL Warehouse cost
- Usable inside streaming pipelines and materialized views

### Comparison

| | Iceberg Source (A) | Kafka pipeline (B1) | Dagster sync (B2) |
|---|---|---|---|
| Databricks cost | Storage reads only | Streaming job compute | SQL Warehouse per poll |
| Data freshness | Last Iceberg snapshot | Near real-time | Minutes |
| RisingWave native | Yes | Yes (Kafka source) | No (external push) |
| Usable in MVs | Yes | Yes | No |
| Setup complexity | High (ADLS credentials) | Very high | Low |

**For this PoC, Approach A is preferred** — no SQL Warehouse cost, native RisingWave integration, and usable inside materialized views.

---

## 3. Source SQL pattern

```sql
CREATE SOURCE <source_name>
WITH (
    connector              = 'iceberg',
    catalog.type           = 'rest',
    catalog.uri            = 'https://adb-1608121643336927.7.azuredatabricks.net/api/2.1/unity-catalog/iceberg-rest',
    catalog.oauth2_server_uri = 'https://login.microsoftonline.com/78395483-9425-447a-ba64-60b90f6bb16e/oauth2/v2.0/token',
    catalog.credential     = '3b7f531f-db93-4186-af75-6566c12c076b:<DATABRICKS_AZURE_CLIENT_SECRET>',
    catalog.scope          = '2ff814a6-3304-4ab8-85cb-cd0e6f879c1d/.default',
    warehouse.path         = '<catalog_name>',
    database.name          = '<schema_name>',
    table.name             = '<table_name>',
    -- EITHER vended credentials (for non-managed catalog, when UC vending works):
    vended_credentials     = 'true'
    -- OR explicit ADLS key (for tables stored in stkznneurwpoccdddevstd):
    -- adlsgen2.account_name = 'stkznneurwpoccdddevstd',
    -- adlsgen2.account_key  = '<ADLS_ACCOUNT_KEY>'
);
```

After creation, query it like any table:
```sql
SELECT * FROM <source_name> ORDER BY id;
```

---

## 3. Test steps executed

### Step 1.1 — Create test table in Databricks

**Command:**
```bash
cat > /tmp/create_test.json << 'EOF'
{
  "statement": "CREATE TABLE IF NOT EXISTS de_dev.risingwave_poc.test_rw_read (id INT NOT NULL, label STRING, amount DOUBLE, created_at TIMESTAMP) USING DELTA TBLPROPERTIES ('delta.columnMapping.mode' = 'name', 'delta.enableIcebergCompatV2' = 'true', 'delta.universalFormat.enabledFormats' = 'iceberg', 'delta.enableDeletionVectors' = 'false')",
  "warehouse_id": "4d06eca1e71a9ccc",
  "wait_timeout": "30s"
}
EOF
databricks api post /api/2.0/sql/statements --json @/tmp/create_test.json
```

**Result:** SUCCEEDED

> **Key learning:** Creating a Delta table in `de_dev` (Managed Iceberg catalog) WITHOUT Iceberg compat properties results in a table that is NOT accessible via the Iceberg REST API. You MUST include these four `TBLPROPERTIES` together:
> - `delta.columnMapping.mode = 'name'` (required by IcebergCompatV2)
> - `delta.enableIcebergCompatV2 = 'true'`
> - `delta.universalFormat.enabledFormats = 'iceberg'`
> - `delta.enableDeletionVectors = 'false'` (required — deletion vectors must be off for IcebergCompatV2)
>
> Trying to ALTER an existing table to add these properties fails with cascading validation errors. Always create with these properties from the start.

---

### Step 1.2 — Insert test data

```bash
cat > /tmp/insert_test.json << 'EOF'
{
  "statement": "INSERT INTO de_dev.risingwave_poc.test_rw_read VALUES (1, 'alice', 100.5, current_timestamp()), (2, 'bob', 250.0, current_timestamp()), (3, 'charlie', 75.25, current_timestamp())",
  "warehouse_id": "4d06eca1e71a9ccc",
  "wait_timeout": "30s"
}
EOF
databricks api post /api/2.0/sql/statements --json @/tmp/insert_test.json
```

**Result:** SUCCEEDED — 3 rows inserted.

---

### Step 1.3 — Verify table is visible via Iceberg REST API

The Iceberg REST API uses a prefix derived from the `/config` endpoint. For `de_dev`, the prefix is `catalogs/de_dev`.

```python
# /tmp/check_iceberg_rest.py
import urllib.request, urllib.parse, json, os

tenant  = os.environ["DATABRICKS_AZURE_TENANT_ID"]
client  = os.environ["DATABRICKS_AZURE_CLIENT_ID"]
secret  = os.environ["DATABRICKS_AZURE_CLIENT_SECRET"]
host    = "https://adb-1608121643336927.7.azuredatabricks.net"
base    = f"{host}/api/2.1/unity-catalog/iceberg-rest/v1/catalogs/de_dev"

# Get token first
payload = urllib.parse.urlencode({
    "grant_type": "client_credentials", "client_id": client,
    "client_secret": secret, "scope": "2ff814a6-3304-4ab8-85cb-cd0e6f879c1d/.default",
}).encode()
with urllib.request.urlopen(urllib.request.Request(
    f"https://login.microsoftonline.com/{tenant}/oauth2/v2.0/token", data=payload, method="POST"
)) as r:
    token = json.loads(r.read())["access_token"]

headers = {"Authorization": f"Bearer {token}"}

# List tables in namespace
req = urllib.request.Request(f"{base}/namespaces/risingwave_poc/tables", headers=headers)
with urllib.request.urlopen(req) as r:
    print(json.dumps(json.loads(r.read()), indent=2))
```

```bash
python3 /tmp/check_iceberg_rest.py
```

**Result:**
```json
{
  "identifiers": [
    {
      "namespace": ["risingwave_poc"],
      "name": "test_rw_read"
    }
  ]
}
```

> **Key learning — Iceberg REST API URL structure for Databricks:**
> - First call `/config?warehouse=<catalog>` to get the prefix
> - All subsequent calls use `/v1/{prefix}/namespaces/...`
> - For `de_dev`: prefix = `catalogs/de_dev`
> - Full namespace list: `GET /api/2.1/unity-catalog/iceberg-rest/v1/catalogs/de_dev/namespaces`
> - Full table list: `GET /api/2.1/unity-catalog/iceberg-rest/v1/catalogs/de_dev/namespaces/risingwave_poc/tables`

---

### Step 1.4 — Grant SP SELECT on the table

```bash
cat > /tmp/grant_test.json << 'EOF'
{
  "statement": "GRANT SELECT ON TABLE de_dev.risingwave_poc.test_rw_read TO `3b7f531f-db93-4186-af75-6566c12c076b`",
  "warehouse_id": "4d06eca1e71a9ccc",
  "wait_timeout": "30s"
}
EOF
databricks api post /api/2.0/sql/statements --json @/tmp/grant_test.json
```

**Result:** SUCCEEDED

---

### Step 1.5 — Find storage location

```bash
cat > /tmp/describe_test.json << 'EOF'
{
  "statement": "DESCRIBE EXTENDED de_dev.risingwave_poc.test_rw_read",
  "warehouse_id": "4d06eca1e71a9ccc",
  "wait_timeout": "30s"
}
EOF
databricks api post /api/2.0/sql/statements --json @/tmp/describe_test.json \
  | python3 -c "import sys,json; [print(r) for r in json.load(sys.stdin)['result']['data_array'] if any(k in str(r) for k in ['Location','Provider','Type','Is_managed'])]"
```

**Result:**
```
['Type', 'MANAGED', '']
['Location', 'abfss://cross-operator@stkznneucommoncdddevstd.dfs.core.windows.net/catalogs/de_dev/__unitystorage/...', '']
['Provider', 'delta', '']
['Is_managed_location', 'true', '']
```

**Key finding:** The table lives in `stkznneucommoncdddevstd` — Databricks' **internal managed ADLS account**. We don't have the key for this account.

---

### Step 2 — Attempt 1: vended_credentials (Phase 2)

**Source creation:**
```python
# Executed via psycopg2 — create_rw_source.py
sql = """
CREATE SOURCE src_databricks_test_read
WITH (
    connector              = 'iceberg',
    catalog.type           = 'rest',
    catalog.uri            = 'https://adb-1608121643336927.7.azuredatabricks.net/api/2.1/unity-catalog/iceberg-rest',
    catalog.oauth2_server_uri = 'https://login.microsoftonline.com/78395483-9425-447a-ba64-60b90f6bb16e/oauth2/v2.0/token',
    catalog.credential     = '3b7f531f-...:...',
    catalog.scope          = '2ff814a6-3304-4ab8-85cb-cd0e6f879c1d/.default',
    warehouse.path         = 'de_dev',
    database.name          = 'risingwave_poc',
    table.name             = 'test_rw_read',
    vended_credentials     = 'true'
)
"""
```

**Source creation:** ✅ SUCCEEDED — RisingWave accepted the configuration and authenticated to Unity Catalog.

**SELECT test:**
```sql
SELECT * FROM src_databricks_test_read ORDER BY id;
```

**Result:** ❌ FAILED
```
connector error
IcebergV2 error
Unexpected => Failed to load manifest list in cache, source
Unexpected => Failure in doing io operation, source
Unexpected (persistent) at read, context: { timeout: 10 } => io timeout reached
```

**Root cause:** `vended_credentials = true` does not provide working Azure credentials for the OpenDAL ADLS read path. Same blocker as writes — RisingWave 2.8.4 cannot consume Unity Catalog's vended Azure credentials for either reads or writes.

---

## 4. Summary of what works vs what doesn't

| Step | Status | Detail |
|---|---|---|
| Azure AD token auth to Unity Catalog REST API | ✅ Works | `catalog.oauth2_server_uri` + `catalog.credential` + `catalog.scope` |
| Listing namespaces + tables via Iceberg REST API | ✅ Works | Confirmed `test_rw_read` visible |
| `CREATE SOURCE` in RisingWave (metadata) | ✅ Works | Source created without error |
| Reading Parquet files from managed ADLS storage | ❌ Blocked | `vended_credentials` doesn't provide working Azure ADLS credentials |
| Explicit `adlsgen2.account_key` for managed storage | ⛔ Not possible | Would need key for `stkznneucommoncdddevstd` (Databricks' internal account) |

---

## 5. Next steps — what unblocks this

The same admin action that unblocks **writes** also unblocks **reads**: creating a **non-Managed-Iceberg Unity Catalog catalog** backed by `stkznneurwpoccdddevstd` (the PoC storage account we have credentials for).

Once that catalog exists:

1. Create tables in it (external Iceberg tables, not managed Delta)
2. Both RisingWave SINKS (writes) and SOURCES (reads) use `adlsgen2.account_key` for `stkznneurwpoccdddevstd`
3. No vended credentials needed

**Full read source after catalog is available:**
```sql
CREATE SOURCE src_databricks_casino_real_bet
WITH (
    connector              = 'iceberg',
    catalog.type           = 'rest',
    catalog.uri            = 'https://adb-1608121643336927.7.azuredatabricks.net/api/2.1/unity-catalog/iceberg-rest',
    catalog.oauth2_server_uri = 'https://login.microsoftonline.com/78395483-9425-447a-ba64-60b90f6bb16e/oauth2/v2.0/token',
    catalog.credential     = '3b7f531f-db93-4186-af75-6566c12c076b:<DATABRICKS_AZURE_CLIENT_SECRET>',
    catalog.scope          = '2ff814a6-3304-4ab8-85cb-cd0e6f879c1d/.default',
    warehouse.path         = '<new_catalog_name>',
    database.name          = 'risingwave_poc',
    table.name             = 'rw_casino_real_bet',
    adlsgen2.account_name  = 'stkznneurwpoccdddevstd',
    adlsgen2.account_key   = '<ADLS_ACCOUNT_KEY>'
);
```

**Grant required for reads:**
```sql
GRANT SELECT ON TABLE <catalog>.risingwave_poc.rw_casino_real_bet
TO `3b7f531f-db93-4186-af75-6566c12c076b`;
```

---

## 6. Reproduce from scratch checklist

When the new catalog is available, run these steps:

- [ ] Admin creates non-managed-iceberg catalog backed by `stkznneurwpoccdddevstd`
- [ ] Admin creates schema `risingwave_poc` in new catalog
- [ ] Admin opens firewall on `stkznneurwpoccdddevstd` (Databricks VNet + developer IP)
- [ ] Admin grants SP `USE_CATALOG`, `USE_SCHEMA`, `SELECT` on the new catalog/schema
- [ ] Tables created at `abfss://cont1@stkznneurwpoccdddevstd.dfs.core.windows.net/iceberg/...`
- [ ] Run `CREATE SOURCE` SQL above (with new catalog name)
- [ ] Run `SELECT * FROM src_databricks_casino_real_bet LIMIT 10;` to verify

---

## 7. What if the Databricks table is in Delta format (not Iceberg)?

RisingWave's Iceberg connector **cannot read native Delta format** — it only understands Iceberg file layout (`metadata/*.json`, manifest files). Delta uses a completely different transaction log (`_delta_log/`).

### Databricks UniForm (Universal Format)

Databricks' **UniForm** feature writes Iceberg metadata *alongside* the Delta log simultaneously, making a Delta table readable by both Delta and Iceberg readers. This is enabled via `delta.universalFormat.enabledFormats = 'iceberg'`.

**All four properties must be set together** (as done in step 3.1):

```sql
TBLPROPERTIES (
    'delta.columnMapping.mode'             = 'name',
    'delta.enableIcebergCompatV2'          = 'true',
    'delta.universalFormat.enabledFormats' = 'iceberg',
    'delta.enableDeletionVectors'          = 'false'
)
```

### Options for pure Delta tables

| Option | When to use | Notes |
|---|---|---|
| **Create with UniForm from the start** | New tables you control | Include all four `TBLPROPERTIES` at `CREATE TABLE` time — cleanest approach |
| **ALTER existing table** | Existing tables | Cascades into validation failures (column mapping mode, deletion vectors). Generally not viable without data migration. |
| **Recreate the table** | Existing tables where you control the schema | Drop + `CREATE TABLE ... TBLPROPERTIES (...)` + re-load data |
| **Kafka/Dagster pipeline** | Tables you don't control and can't alter | Push data out of Databricks into Kafka or batch-sync via Dagster |

### Why ALTER fails on existing Delta tables

As discovered in step 3.1, trying to add Iceberg compat to an existing Delta table fails with cascading errors:
1. `IcebergCompatV2 requires delta.columnMapping.mode = 'name'` — but changing column mapping on an existing table requires a full rewrite
2. `IcebergCompatV2 requires Deletion Vectors to be disabled` — if DVs were ever used, you'd need `REORG PURGE` first
3. `REORG PURGE` itself may fail if the table has other constraints

**The clean path for existing tables:** drop, recreate with the four properties, reload data.

### Enabling UniForm on many large existing tables — gotchas

#### 1. Deletion Vectors (most common blocker)

Modern Databricks tables have deletion vectors enabled by default for MERGE/UPDATE/DELETE operations. UniForm requires them disabled. You must run:

```sql
REORG TABLE <table> APPLY (PURGE);
```

This **rewrites all data files** — expensive in time and Databricks compute, and blocks writes while running. For large tables, plan for hours of downtime.

#### 2. Column mapping mode

Tables on `NoMapping` (default for older tables) must migrate to `name` mode. This is a metadata change but can break downstream readers (Spark jobs, BI tools) that depend on column IDs rather than names. Requires coordination across all consumers.

#### 3. Iceberg metadata generation for history

On first enable, Databricks generates Iceberg metadata for the entire existing Delta log. Tables with thousands of commits and TBs of data can take hours for this initial sync, with significant I/O.

#### 4. Per-write overhead after enabling

Every future write also updates Iceberg metadata. High-frequency append tables (streaming ingestion) will see added latency per transaction.

#### 5. Incompatible Delta features — cannot enable UniForm if table uses any of these

- Liquid Clustering
- V-Order optimization
- Row tracking
- Type widening
- Certain generated column patterns

Tables using any of these features must remove/disable them before UniForm can be enabled.

#### 6. Protocol upgrade

UniForm requires minimum reader version 2, writer version 7. Older Spark jobs or connectors that don't support these versions will fail to read the table after the upgrade.

#### Practical approach for bulk migration

For many tables, use a tiered strategy:

1. **Identify which tables actually need RisingWave reads** — probably a subset, not everything
2. **Audit each candidate** — `DESCRIBE EXTENDED <table>` → look for deletion vectors, liquid clustering, V-Order
3. **Schedule REORG PURGE** for tables with deletion vectors during a low-traffic window
4. **Create new tables with UniForm from the start** for any new pipelines (avoid the migration entirely)
5. **Use Kafka or Dagster sync** for tables you don't control, can't alter, or that use incompatible features

---

### Liquid Clustered tables updated once per day — recommended pattern

Liquid Clustering is **incompatible with UniForm** — you cannot enable Iceberg compat on a Liquid Clustered table. However, for tables with a daily batch cadence there is no need to switch or lose the clustering. Use an **Iceberg mirror table** instead:

**Keep the original** (Liquid Clustered, fast Databricks reads) + **add a daily-refreshed Iceberg copy** (readable by RisingWave):

```sql
-- Step 1: Create the Iceberg mirror once (no liquid clustering)
CREATE TABLE de_dev.risingwave_poc.my_table_iceberg
TBLPROPERTIES (
    'delta.columnMapping.mode'             = 'name',
    'delta.enableIcebergCompatV2'          = 'true',
    'delta.universalFormat.enabledFormats' = 'iceberg',
    'delta.enableDeletionVectors'          = 'false'
)
AS SELECT * FROM de_dev.original_schema.my_table;

-- Step 2: Add to the existing daily batch job
TRUNCATE TABLE de_dev.risingwave_poc.my_table_iceberg;
INSERT INTO de_dev.risingwave_poc.my_table_iceberg
SELECT * FROM de_dev.original_schema.my_table;
```

**Why this fits well:**
- Original Liquid Clustered table is untouched — Databricks query performance unaffected
- Iceberg mirror refreshes once per day, matching the source update cadence
- 24-hour lag is acceptable since the source data only changes once per day
- RisingWave reads the mirror via `CREATE SOURCE` with `adlsgen2.account_key`
- Storage cost is one full table copy — no ongoing write amplification

**Gotcha — data gap during refresh:** `TRUNCATE + INSERT` leaves the mirror empty between the two operations. If RisingWave must always have data, use one of these alternatives:

```sql
-- Option A: MERGE (upsert by primary key — no gap, but slower)
MERGE INTO de_dev.risingwave_poc.my_table_iceberg AS t
USING de_dev.original_schema.my_table AS s
ON t.id = s.id
WHEN MATCHED THEN UPDATE SET *
WHEN NOT MATCHED THEN INSERT *;

-- Option B: Atomic swap (write to a staging table, rename — no gap)
CREATE OR REPLACE TABLE de_dev.risingwave_poc.my_table_iceberg_new
TBLPROPERTIES (...)
AS SELECT * FROM de_dev.original_schema.my_table;
-- then drop old and rename new (requires DROP + CREATE or clone)
```

---

## 8. Production viability assessment — Delta tables + RisingWave

The PoC is being done to assess production use. Maintaining two copies of large Delta tables (one for Databricks, one Iceberg mirror) is not acceptable at scale. The viable production options without permanent duplication are:

---

### Option 1 — UniForm + drop Liquid Clustering

Keep Delta as the format, disable Liquid Clustering to enable UniForm. Single table, single copy, readable by both Databricks and RisingWave.

- ✅ No duplication, no format migration
- ⚠️ Databricks query performance degrades without Liquid Clustering — classic partitioning + ZORDER can partially compensate but needs benchmarking
- ⚠️ Existing tables need REORG PURGE + recreation (expensive at billions of rows)

---

### Option 2 — Iceberg as primary format (new pipelines)

For pipelines not yet built, design with Iceberg from the start. Databricks 13.3+ reads and writes Iceberg natively. Single table, both systems read it.

- ✅ No duplication, vendor independence long-term
- ⚠️ No Liquid Clustering equivalent in Iceberg
- ⚠️ Full rewrite for existing tables — very painful at billions of rows
- ✅ Clean architecture for net-new work

---

### Option 3 — Kafka CDC from Databricks (no format change)

Keep Delta + Liquid Clustering exactly as-is. Enable Databricks [Change Data Feed](https://docs.databricks.com/en/delta/delta-change-data-feed.html) on each table and publish changes to Kafka. RisingWave consumes from Kafka.

```
Delta table (Databricks) → Change Data Feed → Kafka topic → RisingWave streaming table
```

- ✅ Zero format migration, no duplication, Liquid Clustering untouched
- ✅ Architecturally the most natural fit — RisingWave is a streaming engine, consuming Kafka is what it does best
- ⚠️ Requires engineering to publish CDF output to Kafka as part of the daily batch
- ⚠️ **Bootstrap problem — see below**

#### Historical data / bootstrap problem

Kafka CDC only captures changes **going forward** from when it is enabled. The billions of existing rows don't appear in the topic. RisingWave starts empty and only sees new changes.

Three strategies to handle this:

**A — Full initial dump then CDC**

Read the entire Delta table → publish all rows to Kafka → RisingWave ingests → switch to CDC for incremental updates.
Conceptually simple but expensive at billions of rows: high Databricks compute, high Kafka throughput, slow RisingWave ingestion time.

**B — Snapshot + CDC from that version (recommended)**

Delta Change Data Feed supports `startingVersion` (an integer) or `startingTimestamp`. Use this to guarantee no overlap and no gap:

1. Record the current Delta version: `DESCRIBE HISTORY <table> LIMIT 1` → note `version`
2. Do a **one-time full snapshot** of the table into RisingWave (via Iceberg mirror — the temporary dual-copy is acceptable as a one-time bootstrap, not a permanent fixture)
3. Start Kafka CDC from exactly that version: `startingVersion = <recorded_version>`
4. Once RisingWave has caught up, drop the Iceberg mirror

This is the standard production pattern for CDC bootstrapping. The Iceberg mirror becomes a migration tool used once, not a permanent copy.

**C — Accept partial history**

Enable CDC from today. RisingWave sees data from now forward. Historical analysis stays in Databricks (queried via Trino or Dagster). Clean separation: RisingWave for real-time, Databricks for history.

Whether C is acceptable depends on the use case:
- ✅ Acceptable if RisingWave pipelines only need recent data (rolling windows, last N days)
- ❌ Not acceptable if full history is needed in materialized views from day one

---

### Option 4 — Wait for RisingWave Delta support

File a feature request for a native Delta Lake source connector. Delta is widely used — a reasonable roadmap item. No migration, no duplication, but timeline is uncertain (not available in 2.8.4).

---

### Recommendation for billions-of-rows tables

| Option | Format change | Duplication | LC retained | Bootstrap needed |
|---|---|---|---|---|
| UniForm + drop LC | Metadata only | None | ❌ | No |
| Iceberg primary | Full rewrite | None | ❌ | No |
| Kafka CDC + snapshot | None | Temporary (1×) | ✅ | Yes — Option B |
| Delta support (future) | None | None | ✅ | No |

**Option 3 with Strategy B** is the most production-grade path for large existing tables: preserves Liquid Clustering, avoids permanent duplication, and handles historical data cleanly. The Iceberg mirror is used once for bootstrapping then discarded.

For **net-new pipelines**, design with Iceberg from the start (Option 2) and avoid the problem entirely.

---

## 9. Iceberg REST API reference (for Databricks UC)

Useful for debugging — get a token first:

```bash
TOKEN=$(python3 -c "
import urllib.request, urllib.parse, json, os
payload = urllib.parse.urlencode({
    'grant_type': 'client_credentials',
    'client_id': os.environ['DATABRICKS_AZURE_CLIENT_ID'],
    'client_secret': os.environ['DATABRICKS_AZURE_CLIENT_SECRET'],
    'scope': '2ff814a6-3304-4ab8-85cb-cd0e6f879c1d/.default',
}).encode()
with urllib.request.urlopen(urllib.request.Request(
    f'https://login.microsoftonline.com/{os.environ[\"DATABRICKS_AZURE_TENANT_ID\"]}/oauth2/v2.0/token',
    data=payload, method='POST'
)) as r:
    print(json.loads(r.read())['access_token'])
")

BASE="https://adb-1608121643336927.7.azuredatabricks.net/api/2.1/unity-catalog/iceberg-rest/v1"

# Get prefix for a catalog
curl -s -H "Authorization: Bearer $TOKEN" "$BASE/config?warehouse=de_dev" | python3 -m json.tool

# List namespaces (using prefix from config)
curl -s -H "Authorization: Bearer $TOKEN" "$BASE/catalogs/de_dev/namespaces" | python3 -m json.tool

# List tables in a namespace
curl -s -H "Authorization: Bearer $TOKEN" "$BASE/catalogs/de_dev/namespaces/risingwave_poc/tables" | python3 -m json.tool
```
