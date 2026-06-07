# StarRocks — OLAP Serving Layer

StarRocks is a vectorised columnar OLAP engine with a MySQL-compatible protocol. In this stack it serves as an ad-hoc query layer over Iceberg data — both remote (Databricks Unity Catalog via ADLS) and local (Lakekeeper/MinIO). Power BI connects via the MySQL connector.

> **Status: WORKING ✅ (2026-06-07).** Both `databricks_uc` and `lakekeeper_local` external catalogs operational. Benchmarks vs RisingWave completed on local Lakekeeper data.

---

## 1. Architecture

```
Kafka → RisingWave streaming → MVs (Hummock) ──────────────────────► Power BI (PostgreSQL connector)
                                   │                                  (pre-aggregated KPIs, <10ms)
                                   ▼
                           Lakekeeper/MinIO (Iceberg)
                                   │
                          StarRocks lakekeeper_local catalog
                                   │
                                   ► Power BI (MySQL connector)
                                     (ad-hoc analytics, ~150–400ms)

Databricks Unity Catalog (ADLS) ◄──── RisingWave sinks ────────────────────────────
        │
StarRocks databricks_uc catalog
        │
        ► ad-hoc queries, historical data
```

Key point: **RisingWave does the transformation work once** (raw Kafka events → structured MVs), writes to Lakekeeper, and StarRocks reads those results. No duplicated Kafka consumption or streaming computation. StarRocks adds query-time flexibility without redoing the streaming work.

---

## 2. Docker Compose setup

### Service definition

```yaml
starrocks:
  image: starrocks/allin1-ubuntu:4.0.10
  container_name: starrocks
  entrypoint: ["/usr/bin/tini-static", "--", "/starrocks-entrypoint.sh"]
  ports:
    - "9030:9030"   # MySQL protocol — Power BI / mysql clients
    - "8030:8030"   # HTTP API / StarRocks UI
  volumes:
    - "./starrocks/docker-entrypoint.sh:/starrocks-entrypoint.sh:ro"
  environment:
    - ADLS_ACCOUNT_KEY
    - HTTP_PROXY=
    - HTTPS_PROXY=
    - ALL_PROXY=
    - http_proxy=
    - https_proxy=
    - all_proxy=
    - NO_PROXY=*
    - no_proxy=*
  healthcheck:
    test: ["CMD-SHELL", "mysql -h 127.0.0.1 -P 9030 -u root --connect-timeout=3 -e 'SELECT 1' 2>/dev/null || exit 1"]
    interval: 10s
    timeout: 10s
    retries: 15
    start_period: 60s
  restart: always
  deploy:
    resources:
      limits:
        memory: 2G
      reservations:
        memory: 1G
  networks:
    iceberg_net:

starrocks-init:
  image: alpine:3.23.4
  restart: "no"
  depends_on:
    starrocks:
      condition: service_healthy
  networks:
    iceberg_net:
  environment:
    - DATABRICKS_AZURE_CLIENT_ID=3b7f531f-db93-4186-af75-6566c12c076b
    - DATABRICKS_AZURE_CLIENT_SECRET
    - ADLS_ACCOUNT_KEY
  volumes:
    - "./starrocks/init_catalog.sh:/init_catalog.sh:ro"
  command: sh /init_catalog.sh
```

**No named volume**: StarRocks stores data at `/data/deploy/starrocks/` inside the container. Data is ephemeral across container recreations — this is acceptable for a serving layer where all data lives in Iceberg/MinIO.

### Custom entrypoint — `starrocks/docker-entrypoint.sh`

StarRocks does not forward catalog `PROPERTIES` to Hadoop Configuration. Azure ADLS credentials must be injected via `core-site.xml` for **both FE (metadata) and BE (Parquet scans)** before StarRocks starts:

```bash
#!/bin/bash
set -e

AZURE_XML="
<configuration>
  <property>
    <name>fs.s3.impl</name>
    <value>org.apache.hadoop.fs.s3a.S3AFileSystem</value>
  </property>
  <property>
    <name>fs.azure.account.key.stkznneurwpoccdddevstd.dfs.core.windows.net</name>
    <value>${ADLS_ACCOUNT_KEY}</value>
  </property>
</configuration>"

echo "$AZURE_XML" > /data/deploy/starrocks/fe/conf/core-site.xml
echo "$AZURE_XML" > /data/deploy/starrocks/be/conf/core-site.xml

exec /data/deploy/entrypoint.sh
```

`chmod +x starrocks/docker-entrypoint.sh` is required — Docker will exit 126 otherwise.

---

## 3. Catalogs

### `databricks_uc` — Databricks Unity Catalog (ADLS)

```sql
CREATE EXTERNAL CATALOG databricks_uc
PROPERTIES (
    "type"                              = "iceberg",
    "iceberg.catalog.type"              = "rest",
    "iceberg.catalog.uri"               = "https://adb-1608121643336927.7.azuredatabricks.net/api/2.1/unity-catalog/iceberg-rest",
    "iceberg.catalog.warehouse"         = "de_dev",
    "iceberg.catalog.credential"        = "<CLIENT_ID>:<CLIENT_SECRET>",
    "iceberg.catalog.oauth2-server-uri" = "https://login.microsoftonline.com/<TENANT_ID>/oauth2/v2.0/token",
    "iceberg.catalog.scope"             = "2ff814a6-3304-4ab8-85cb-cd0e6f879c1d/.default"
);
```

**Note on property names**: StarRocks uses the Iceberg Java library names — `credential` (not `oauth2.credential`), `oauth2-server-uri` (not `oauth2.server-uri`). Wrong names produce a 401 "Credential was not sent" error.

**Note on ADLS key**: `hadoop.fs.azure.account.key.*` in `CREATE CATALOG PROPERTIES` is stored but NOT forwarded to Hadoop Configuration. The key must be in `core-site.xml` (see §2 entrypoint). Column scans (BE) fail silently with wrong key even when COUNT(*) works (FE reads only metadata).

**Credential vending**: Unity Catalog's IRC vends SAS tokens, but StarRocks' Azure filesystem implementation falls back to account key. The `core-site.xml` injection is required and not avoidable without a StarRocks patch.

### `lakekeeper_local` — Local Lakekeeper/MinIO

```sql
CREATE EXTERNAL CATALOG lakekeeper_local
PROPERTIES (
    "type"                              = "iceberg",
    "iceberg.catalog.type"              = "rest",
    "iceberg.catalog.uri"               = "http://lakekeeper:8181/catalog/",
    "iceberg.catalog.warehouse"         = "risingwave-warehouse",
    "aws.s3.endpoint"                   = "http://minio-0:9301",
    "aws.s3.access_key"                 = "hummockadmin",
    "aws.s3.secret_key"                 = "hummockadmin",
    "aws.s3.enable_path_style_access"   = "true",
    "aws.s3.region"                     = "us-east-1"
);
```

---

## 4. Query syntax

| Feature | StarRocks | RisingWave |
|---|---|---|
| JSON extraction | `get_json_string(col, 'key')` | `(col::jsonb)->>'key'` |
| JSON cast to int | `CAST(get_json_string(col,'key') AS INT)` | `(col::jsonb->>'key')::int` |
| JSON cast to bool | `get_json_string(col,'is_live') = 'true'` | `(col::jsonb->>'is_live')::boolean` |
| Table reference | `lakekeeper_local.public.rw_casino_transactions` | `src_lakekeeper_casino_transactions` (source) |

### Example — casino transactions by game type

**StarRocks**
```sql
SELECT
    CAST(get_json_string(properties, 'game_type') AS INT) AS game_type,
    get_json_string(properties, 'is_live') = 'true'       AS is_live,
    COUNT(*)                                               AS txns,
    SUM(amount_abs)                                        AS total_amount
FROM lakekeeper_local.public.rw_casino_transactions
GROUP BY 1, 2
ORDER BY txns DESC;
```

**RisingWave (Iceberg source)**
```sql
SELECT
    (properties::jsonb->>'game_type')::int   AS game_type,
    (properties::jsonb->>'is_live')::boolean AS is_live,
    COUNT(*)                                  AS txns,
    SUM(amount_abs::numeric)                  AS total_amount
FROM src_lakekeeper_casino_transactions
GROUP BY 1, 2
ORDER BY txns DESC;
```

---

## 5. Benchmark — RisingWave vs StarRocks

Two benchmarks were run, covering different table types.

---

### 5a. Append-only raw event tables (Iceberg batch scan on both sides)

**Setup**: both engines read the same Parquet files from MinIO via the Lakekeeper REST catalog using `risingwave_source` (RisingWave) and `lakekeeper_local` catalog (StarRocks). Row counts verified identical. Query: JSON extraction + GROUP BY + SUM.

#### casino_transactions (8,385 rows, 424 uncompacted files)

| Run | RisingWave Iceberg source (ms) | StarRocks (ms) |
|-----|:-:|:-:|
| 1 | 409 | 361 |
| 2 | 281 | 228 |
| 3 | 355 | 190 |
| 4 | 312 | 181 |
| 5 | 3,014 ⚠ | **166** |
| **Warmed p50** | ~310–410 | ~166–181 |

#### sportsbook_bets (6,982 rows, 432 uncompacted files)

| Run | RisingWave Iceberg source (ms) | StarRocks (ms) |
|-----|:-:|:-:|
| 1 | 724 | 457 |
| 2 | 349 | 152 |
| 3 | 590 | 146 |
| 4 | 2,236 ⚠ | 125 |
| 5 | 1,095 | **152** |
| **Warmed p50** | ~350–700 | ~125–152 |

StarRocks is ~2–4× faster for ad-hoc JSON extraction queries. However, this is not how RisingWave is meant to be used — reading Iceberg sources bypasses Hummock entirely. See §5b for the correct comparison.

---

### 5b. Pre-aggregated upsert tables — RisingWave Hummock vs StarRocks Lakekeeper

**Setup**: RisingWave queries its own materialized views served directly from Hummock (in-memory, no I/O). StarRocks reads the same data from Lakekeeper Iceberg (the upsert sink output, compacted to 3–9 files). This is the production-relevant comparison.

#### casino_real_bet (13,950 rows, 3 compacted files)

| Run | RisingWave — Hummock (ms) | StarRocks — Lakekeeper (ms) |
|-----|:-:|:-:|
| 1 | 909 | 2,743 |
| 2 | 169 | 331 |
| 3 | 108 | 271 |
| 4 | **68** | 227 |
| 5 | 91 | **190** |
| **Warmed p50** | **~90** | **~230** |

#### turnover_percentage (8,604 rows, 9 compacted files)

| Run | RisingWave — Hummock (ms) | StarRocks — Lakekeeper (ms) |
|-----|:-:|:-:|
| 1 | 55 | 190 |
| 2 | 48 | 174 |
| 3 | 49 | 183 |
| 4 | 46 | 181 |
| 5 | **45** | **191** |
| **Warmed p50** | **~47** | **~183** |

### Interpretation

- **RisingWave is 2.5–4× faster** for its own pre-computed MVs — 45–90ms vs 183–230ms. This is expected: Hummock is in-memory state, StarRocks is reading Parquet from MinIO.
- **StarRocks latency is stable once warm** (~183–230ms) with no checkpoint spikes. The first-query penalty is the Iceberg metadata load + cold MinIO page cache.
- **The comparison is structurally unfair** for pre-computed tables: RisingWave has no I/O at query time; StarRocks always does. StarRocks earns its place for ad-hoc queries on tables that don't have pre-computed MVs in RisingWave.
- **RisingWave checkpoint spikes** (occasional 2–3s outliers in §5a) come from background checkpoint activity, not query execution. These don't affect Hummock MV queries (§5b).
- **Memory constraint**: StarRocks at 2G OOMs on concurrent ADLS + MinIO queries. Raised to 3G — see §10.

---

## 6. Production serving options

### Option A — RisingWave MVs (live dashboards)

Point Power BI at RisingWave's PostgreSQL port (`4566`), querying pre-computed MVs in Hummock.

- **Latency**: <10ms (pre-aggregated, no I/O at query time)
- **Flexibility**: limited to pre-defined MV shapes — ad-hoc drill-downs require new MVs
- **No Iceberg involved**
- **Best for**: live KPI tiles, real-time aggregations, fixed-shape dashboards

### Option B — StarRocks reading Lakekeeper Iceberg (ad-hoc analytics)

StarRocks reads Parquet files from MinIO via Lakekeeper. Every query hits MinIO (with OS cache on subsequent runs).

- **Latency**: ~150–400ms warmed
- **Flexibility**: full ad-hoc SQL, no pre-definition needed
- **No duplicated computation**: RisingWave transforms raw Kafka once, writes to Iceberg; StarRocks reads the output
- **~5s data lag** from RisingWave's `commit_checkpoint_interval = 5`
- **Best for**: ad-hoc analytics, Power BI drill-downs, one-off reports

### Option C — StarRocks native tables (highest performance)

Load data directly into StarRocks' columnar format via Kafka Routine Load. Queries run entirely in-process — no object storage roundtrip.

- **Latency**: sub-100ms, consistent
- **Flexibility**: full ad-hoc SQL
- **Trade-off**: StarRocks consumes Kafka independently of RisingWave — duplicated Kafka consumption and raw data storage. StarRocks aggregates at query time, not pre-computed.
- **Best for**: interactive dashboards requiring consistent sub-100ms regardless of query shape

**Option B is preferred in this stack** because RisingWave already owns ingestion and transformation. StarRocks as an external catalog reader avoids duplicating that work and keeps the architecture clean.

### Recommended split

| Use case | Engine | Why |
|---|---|---|
| Live KPI dashboard tiles | RisingWave (MVs) | <10ms, streaming, pre-computed |
| Ad-hoc analytics / drill-down | StarRocks (Lakekeeper catalog) | ~150–400ms, flexible SQL, no duplication |
| Historical archive queries | StarRocks (Databricks UC catalog) | Direct ADLS scan, no SQL Warehouse cost |
| Power BI — real-time pages | RisingWave via PostgreSQL connector | |
| Power BI — analytical pages | StarRocks via MySQL connector | |

---

## 7. Ad-hoc queries on append-only tables — what the sinks enable

The `rw_casino_transactions` and `rw_sportsbook_bets` Lakekeeper tables expose raw event rows that the pre-aggregated upsert tables (`rw_managed_casino_real_bet`, `rw_managed_turnover_percentage`) cannot answer. The primary use case is **cross-table row-level joins and event-level drill-downs**.

### Example — customers who were active on casino AND sportsbook in the same window

This query finds customers who had real-money casino transactions **and** placed a sportsbook bet within a 10-minute window of each other. It requires individual row timestamps from both tables — impossible from the aggregated MVs which only carry rolling totals.

> **Synthetic data note:** the `in_play` flag in `properties` is not correlated with casino activity in the test data generator, so filtering on `get_json_string(b.properties, 'in_play') = 'true'` returns no rows in the PoC. The filter is omitted below; add it back for production data where the flag is meaningful.

```sql
SELECT
    c.customer_id,
    COUNT(DISTINCT c.transaction_created_at)   AS casino_txns,
    ROUND(SUM(c.amount_abs), 2)                AS casino_stake,
    COUNT(DISTINCT b.bet_id)                   AS concurrent_bets,
    ROUND(SUM(b.stake_euro), 2)                AS bet_stake_eur,
    MIN(c.transaction_created_at)              AS window_start
FROM lakekeeper_local.public.rw_casino_transactions c
JOIN lakekeeper_local.public.rw_sportsbook_bets b
    ON  c.customer_id = b.customer_id
    AND b.placed_at BETWEEN c.transaction_created_at - INTERVAL 10 MINUTE
                        AND c.transaction_created_at + INTERVAL 10 MINUTE
WHERE c.message_type_id = 1
GROUP BY c.customer_id
HAVING casino_txns >= 2
ORDER BY casino_stake DESC
LIMIT 10;
```

**Why this can't be done from the aggregated tables:**
- `rw_managed_casino_real_bet` has one row per `(customer_id, currency_id, event_ts)` with a rolling amount — no individual transaction timestamps to join on
- `rw_managed_turnover_percentage` has one row per customer with lifetime totals — no per-bet timestamps or in-play flags

#### Trino variant

Catalog is `datalake` (see `trino/catalog/datalake.properties`). Two syntax differences from StarRocks:
- `INTERVAL '10' MINUTE` — Trino requires quotes around the numeric literal
- `HAVING` must repeat the full expression — Trino does not allow referencing SELECT aliases in HAVING

```sql
SELECT
    c.customer_id,
    COUNT(DISTINCT c.transaction_created_at)   AS casino_txns,
    ROUND(SUM(c.amount_abs), 2)                AS casino_stake,
    COUNT(DISTINCT b.bet_id)                   AS concurrent_bets,
    ROUND(SUM(b.stake_euro), 2)                AS bet_stake_eur,
    MIN(c.transaction_created_at)              AS window_start
FROM datalake.public.rw_casino_transactions c
JOIN datalake.public.rw_sportsbook_bets b
    ON  c.customer_id = b.customer_id
    AND b.placed_at BETWEEN c.transaction_created_at - INTERVAL '10' MINUTE
                        AND c.transaction_created_at + INTERVAL '10' MINUTE
WHERE c.message_type_id = 1
GROUP BY c.customer_id
HAVING COUNT(DISTINCT c.transaction_created_at) >= 2
ORDER BY casino_stake DESC
LIMIT 10;
```

#### Trino — Databricks UC tables

RisingWave also sinks these tables to Databricks UC (`sink_casino_transactions_databricks`, `sink_sportsbook_bets_databricks`). In Trino the Databricks catalog is named `databricks` (see `trino/catalog/databricks.properties`; `de_dev` is the warehouse parameter, not the catalog name). Same interval and HAVING syntax as the Lakekeeper variant above.

```sql
SELECT
    c.customer_id,
    COUNT(DISTINCT c.transaction_created_at)   AS casino_txns,
    ROUND(SUM(c.amount_abs), 2)                AS casino_stake,
    COUNT(DISTINCT b.bet_id)                   AS concurrent_bets,
    ROUND(SUM(b.stake_euro), 2)                AS bet_stake_eur,
    MIN(c.transaction_created_at)              AS window_start
FROM databricks.rw_poc.rw_casino_transactions c
JOIN databricks.rw_poc.rw_sportsbook_bets b
    ON  c.customer_id = b.customer_id
    AND b.placed_at BETWEEN c.transaction_created_at - INTERVAL '10' MINUTE
                        AND c.transaction_created_at + INTERVAL '10' MINUTE
WHERE c.message_type_id = 1
GROUP BY c.customer_id
HAVING COUNT(DISTINCT c.transaction_created_at) >= 2
ORDER BY casino_stake DESC
LIMIT 10;
```

#### Databricks (Spark SQL) variant

Run in a Databricks SQL warehouse or notebook. `de_dev` is the Unity Catalog catalog name; `rw_poc` is the schema. One syntax difference from StarRocks:
- `INTERVAL '10' MINUTE` — same ANSI quoting as Trino (StarRocks omits the quotes)

```sql
SELECT
    c.customer_id,
    COUNT(DISTINCT c.transaction_created_at)   AS casino_txns,
    ROUND(SUM(c.amount_abs), 2)                AS casino_stake,
    COUNT(DISTINCT b.bet_id)                   AS concurrent_bets,
    ROUND(SUM(b.stake_euro), 2)                AS bet_stake_eur,
    MIN(c.transaction_created_at)              AS window_start
FROM de_dev.rw_poc.rw_casino_transactions c
JOIN de_dev.rw_poc.rw_sportsbook_bets b
    ON  c.customer_id = b.customer_id
    AND b.placed_at BETWEEN c.transaction_created_at - INTERVAL '10' MINUTE
                        AND c.transaction_created_at + INTERVAL '10' MINUTE
WHERE c.message_type_id = 1
GROUP BY c.customer_id
HAVING casino_txns >= 2
ORDER BY casino_stake DESC
LIMIT 10;
```

**Maintenance note:** these are append-only sinks with no RisingWave compaction. Run Trino `optimize` + `expire_snapshots` before heavy queries to avoid scanning hundreds of small Parquet files. See §11.

---

## 8. RisingWave Iceberg sources for Lakekeeper (benchmark only)

Created as dbt models (tag: `lakekeeper`, materialization: `risingwave_source`) so RisingWave can query the same Iceberg tables as StarRocks.

**`src_lakekeeper_casino_transactions`** and **`src_lakekeeper_sportsbook_bets`** — created by running:

```bash
dbt run --select "tag:lakekeeper,config.materialized:risingwave_source"
```

These are batch sources (snapshot scan on each query), not streaming. They are used for benchmarking and cross-engine validation — not for real-time MVs.

---

## 9. Known limitations

| Limitation | Detail |
|---|---|
| **ADLS credential vending doesn't work** | StarRocks' Azure ABFS implementation ignores IRC-vended SAS tokens. Account key via `core-site.xml` is required. Same limitation applies to Trino. |
| **Hadoop properties not forwarded** | `hadoop.*` keys in `CREATE CATALOG PROPERTIES` are stored but not applied to Hadoop Configuration. Must use `core-site.xml`. |
| **Ephemeral catalog state** | `starrocks-init` re-creates catalogs on each stack start (uses `DROP ... IF EXISTS`). Idempotent but adds ~30s startup time. |
| **No partition pruning on Lakekeeper tables** | RisingWave sinks write unpartitioned tables; StarRocks scans all files regardless of `WHERE` filters. |
| **Iceberg metadata cache cold start** | With `iceberg_metadata_memory_cache_capacity > 0`, StarRocks enumerates all historical Iceberg snapshots on first query after restart. At 100+ snapshots this causes a 3-minute cold start. Left at 0 (disabled) until snapshot expiration is working. See §10. |

---

## 10. Caching configuration

Implemented in `starrocks/docker-entrypoint.sh` (appended to FE/BE conf before startup) and `docker-compose.yml`.

### What was changed

| Setting | Before | After | Where |
|---|---|---|---|
| Container memory | 2G limit / 1G reservation | **3G / 2G** | `docker-compose.yml` |
| BE data cache (`datacache_mem_size`) | auto (~332MB) | **640MB explicit** | `be.conf` |
| Background metadata refresh interval | 600,000ms (10 min) | **60,000ms (1 min)** | `fe.conf` |
| Iceberg metadata memory cache | 0 (disabled) | **0 (kept disabled)** | `fe.conf` |

### Why the Iceberg metadata cache is disabled

`iceberg_metadata_memory_cache_capacity = 268435456` was tested. With ~100 accumulated Iceberg snapshots (RisingWave commits every 5 checkpoints ≈ every 20s with the current barrier config), the FE enumerates all snapshot manifests on the first post-restart query, causing a **3-minute cold start**.

Once warm, query latency is comparable to the disabled state (~170–260ms) because the BE data cache (Parquet blocks in MinIO page cache) dominates. The Lakekeeper REST metadata roundtrip saved by the FE cache is only ~50–100ms.

**Re-enable when**: snapshot expiration is working (keeping snapshot count to ~5–10). With 10 snapshots the cold start drops to ~1s, which is acceptable.

### BE data cache status

The BE data cache is operational and confirmed via `information_schema.be_datacache_metrics`:

```sql
SELECT BE_ID, MEM_QUOTA_BYTES, MEM_USED_BYTES, DISK_QUOTA_BYTES, STATUS
FROM information_schema.be_datacache_metrics;
```

After warmup, `MEM_USED_BYTES` grows to fill the quota — subsequent queries on the same tables skip MinIO reads for cached Parquet blocks.

---

## 11. Lakekeeper table maintenance

### RisingWave compaction — upsert sinks only

RisingWave's built-in compaction (`enable_compaction = 'true'`, `compaction.trigger_snapshot_count = '5'`) merges small Parquet files automatically **for upsert-type sinks only**. The casino/sportsbook tables in this stack use two sink types:

| Table | Sink type | RW compaction | Result |
|---|---|---|---|
| `rw_managed_casino_real_bet` | upsert | ✅ working | 3 files despite 90+ snapshots |
| `rw_managed_turnover_percentage` | upsert | ✅ working | 9 files despite 88+ snapshots |
| `rw_casino_transactions` | append-only | ❌ silently ignored | files accumulate unbounded |
| `rw_sportsbook_bets` | append-only | ❌ silently ignored | files accumulate unbounded |

`enable_compaction` was tested on the append-only sinks (RisingWave 2.8.4) — the option is accepted in `CREATE SINK` DDL but stripped from the live sink definition and has no effect.

### RisingWave snapshot expiration — broken on 2.8.4 with REST catalog

`enable_snapshot_expiration = 'true'` is set on the upsert sinks but confirmed **still broken on RisingWave 2.8.4** with the Lakekeeper REST catalog. All snapshots accumulate regardless. Verified by checking `$snapshots` via Trino: oldest and newest spans the full session without any gaps indicating pruning.

This is consistent with the known issue on 2.7.4 and 2.8.0 (see project memory).

### Trino compaction for append-only tables

Run periodically (or on-demand before heavy StarRocks reads) to merge small files and prune snapshot history:

```sql
-- Compact Parquet files (428 files → 1 for casino_transactions in testing)
ALTER TABLE rw_casino_transactions EXECUTE optimize;
ALTER TABLE rw_sportsbook_bets EXECUTE optimize;

-- Expire old snapshots (requires session override — default min retention is 7 days)
SET SESSION datalake.expire_snapshots_min_retention = '1m';
ALTER TABLE rw_casino_transactions EXECUTE expire_snapshots(retention_threshold => '5m');
ALTER TABLE rw_sportsbook_bets EXECUTE expire_snapshots(retention_threshold => '5m');
```

Run via Trino CLI (`docker exec -i trino trino --server http://localhost:8080 --catalog datalake --schema public`) or as a Dagster op using the existing Trino connection pattern.

**Effect on StarRocks**: after `optimize`, query latency on the append-only tables drops from ~350–700ms to ~170–260ms (fewer MinIO roundtrips). After `expire_snapshots`, the Iceberg metadata cache cold start (if enabled) drops from 3 minutes to ~1 second.
