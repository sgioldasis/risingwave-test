# Lakekeeper — Production Hosting & Databricks Integration

Lakekeeper is the open-source Iceberg REST Catalog (IRC) used locally in the PoC stack. This document covers what a production deployment looks like and how Lakekeeper and Databricks Unity Catalog coexist in the same architecture.

---

## 1. Why host your own Lakekeeper

Unity Catalog is the right catalog for tables that Databricks SQL users and BI tools need to query directly. Lakekeeper fills a different role — it is the catalog for tables that RisingWave owns and Trino reads, where Databricks SQL Warehouse access is not required.

Reasons to run your own Lakekeeper in production:

| Reason | Detail |
|---|---|
| **RisingWave-native** | RisingWave treats Lakekeeper as a first-class Iceberg catalog — upsert sinks with compaction, snapshot expiration, and `EXTERNAL USE` grants all work without workarounds |
| **No Databricks cost** | Tables in Lakekeeper do not consume Databricks DBUs or Unity Catalog managed-table overhead |
| **Vended credentials** | Lakekeeper can issue short-lived Azure SAS tokens to clients so no long-lived storage keys need to be embedded in sink config |
| **Vendor independence** | Tables stored in your own ADLS account under your own catalog, not inside Databricks-managed storage |
| **Fine-grained access control** | OIDC/OAuth2 + per-warehouse/per-namespace grants; same Azure AD SP model as the rest of the stack |

---

## 2. Production deployment

### Infrastructure

| Component | Recommended option |
|---|---|
| **Lakekeeper server** | Kubernetes — official Helm chart (`lakekeeper/lakekeeper`) |
| **Metadata database** | Azure Database for PostgreSQL (Flexible Server, zone-redundant) |
| **Object storage** | Azure ADLS Gen2 — same storage account as the rest of the lakehouse |
| **TLS** | Terminate at the ingress (NGINX / Azure Application Gateway) |
| **Auth** | Azure AD OIDC — Lakekeeper acts as an OIDC resource server; clients authenticate with Azure AD tokens |

### Helm deployment sketch

```bash
helm repo add lakekeeper https://lakekeeper.github.io/lakekeeper-charts
helm repo update

helm install lakekeeper lakekeeper/lakekeeper \
  --namespace lakekeeper \
  --set database.uri="postgresql://lakekeeper:<pass>@lakekeeper-pg.postgres.database.azure.com:5432/lakekeeper?sslmode=require" \
  --set auth.oidc.enabled=true \
  --set auth.oidc.issuer="https://login.microsoftonline.com/<tenant>/v2.0" \
  --set auth.oidc.audience="<lakekeeper-app-registration-client-id>"
```

### Warehouse configuration (ADLS Gen2)

After deployment, create a warehouse pointing at your ADLS account:

```bash
curl -X POST https://lakekeeper.internal/catalog/management/v1/warehouse \
  -H "Authorization: Bearer $TOKEN" \
  -H "Content-Type: application/json" \
  -d '{
    "warehouse-name": "production",
    "storage-profile": {
      "type": "adls",
      "account-name": "yourstorageaccount",
      "filesystem": "iceberg",
      "authority-host": "https://login.microsoftonline.com"
    },
    "storage-credential": {
      "type": "azure-client-credentials",
      "client-id": "<sp-client-id>",
      "client-secret": "<sp-client-secret>",
      "tenant-id": "<tenant-id>"
    }
  }'
```

### Vended credentials

With a properly configured warehouse, Lakekeeper vends short-lived Azure SAS tokens to clients on each table open. RisingWave receives a token scoped to that table's prefix — no long-lived `adlsgen2.account_key` in sink config:

```sql
-- RisingWave sink in production (no account_key)
CREATE SINK sink_casino_real_bet_lakekeeper
FROM mv_casino_real_bet
WITH (
    connector                   = 'iceberg',
    catalog.type                = 'rest',
    catalog.uri                 = 'https://lakekeeper.internal/catalog',
    catalog.credential          = '<sp-client-id>:<sp-client-secret>',
    catalog.oauth2_server_uri   = 'https://login.microsoftonline.com/<tenant>/oauth2/v2.0/token',
    catalog.scope               = '<lakekeeper-app-id>/.default',
    warehouse.path              = 'production',
    database.name               = 'risingwave',
    table.name                  = 'casino_real_bet'
);
```

---

## 3. What each layer can see

| Query layer | Lakekeeper tables | Unity Catalog tables |
|---|---|---|
| **RisingWave** | ✅ sink + source | ✅ sink (append-only) |
| **Trino** | ✅ `datalake` catalog | ✅ `databricks` catalog |
| **Databricks Spark** (notebooks, Jobs) | ✅ via Spark catalog config | ✅ native |
| **Databricks SQL Warehouse** | ❌ not supported | ✅ native |
| **Grafana (via Trino)** | ✅ | ✅ |
| **PyIceberg / pandas** | ✅ | ✅ (with pyiceberg REST support) |

The key gap: **Databricks SQL Warehouse only understands Unity Catalog.** SQL Warehouses cannot be pointed at an external IRC endpoint. This means Databricks SQL editor, Databricks-connected BI tools (Power BI via Databricks connector, Tableau Server), and any JDBC/ODBC client connecting to a SQL Warehouse cannot see Lakekeeper tables.

---

## 4. Databricks Spark integration

Databricks notebooks and Jobs can read Lakekeeper tables by configuring an additional Spark catalog. This works on any Databricks Runtime 11.3+:

```python
# In a notebook or cluster init script
spark.conf.set("spark.sql.catalog.lakekeeper",
               "org.apache.iceberg.spark.SparkCatalog")
spark.conf.set("spark.sql.catalog.lakekeeper.catalog-impl",
               "org.apache.iceberg.rest.RESTCatalog")
spark.conf.set("spark.sql.catalog.lakekeeper.uri",
               "https://lakekeeper.internal/catalog")
spark.conf.set("spark.sql.catalog.lakekeeper.credential",
               "<sp-client-id>:<sp-client-secret>")
spark.conf.set("spark.sql.catalog.lakekeeper.oauth2-server-uri",
               "https://login.microsoftonline.com/<tenant>/oauth2/v2.0/token")
spark.conf.set("spark.sql.catalog.lakekeeper.scope",
               "<lakekeeper-app-id>/.default")
spark.conf.set("spark.sql.catalog.lakekeeper.warehouse",
               "production")

# Query
df = spark.sql("""
    SELECT customer_id, rolling_1d_real_bet_amount
    FROM lakekeeper.risingwave.casino_real_bet
    WHERE rolling_1d_real_bet_amount > 1000
""")
```

For cluster-level configuration (applies to all notebooks on the cluster), add the same `spark.conf.set` calls to the cluster's **Spark config** in the Databricks UI.

---

## 5. Bridging the SQL Warehouse gap

If a Lakekeeper table needs to be queryable from Databricks SQL Warehouse (e.g., for a BI dashboard or a SQL-based data product), three options exist:

### Option A — Mirror into Unity Catalog (daily batch)

A Databricks Job reads from Lakekeeper (via Spark) and writes to a Unity Catalog managed table. Simple but adds latency and duplicates storage.

```python
# Databricks Job — runs daily
df = spark.table("lakekeeper.risingwave.casino_real_bet")
df.write.format("delta").mode("overwrite") \
    .saveAsTable("de_dev.rw_poc.casino_real_bet_daily")
```

### Option B — Trino as the unified query layer

Trino already has both catalogs wired up (`datalake` + `databricks`). Route SQL Warehouse users to Trino instead — same SQL dialect, no data duplication. Grafana uses this today.

This is the lowest-friction option for analytical queries and monitoring.

### Option C — Unity Catalog Iceberg Federation (future)

Databricks has announced plans to allow Unity Catalog to federate external Iceberg REST catalogs — Unity Catalog would "see" Lakekeeper tables as if they were registered locally. Not GA as of mid-2025.

---

## 6. Recommended production architecture for this stack

```
Kafka (production topics)
    │
    ▼
RisingWave streaming
    ├──► Lakekeeper IRC ──► ADLS Gen2 (iceberg/)
    │         │                    │
    │         │              Trino datalake catalog
    │         │                    │
    │         └── rolling windows, funnel state,
    │               intermediate MVs (operational tables)
    │
    └──► Databricks Unity Catalog IRC ──► ADLS Gen2 (rw_poc/)
               │                               │
               │                    Trino databricks catalog
               │                    Databricks SQL Warehouse
               │                    Databricks Spark
               │
               └── flat event facts (casino_transactions,
                     sportsbook_bets), aggregated summaries
                     for SQL/BI consumers
```

### Decision rule — which catalog for a new table?

| Table type | Catalog |
|---|---|
| Rolling window / session state — only RisingWave and Trino read it | Lakekeeper |
| Intermediate MV — joins, enrichments, not exposed to BI | Lakekeeper |
| Flat event fact — Databricks SQL users need it | Unity Catalog |
| Aggregated summary — Power BI, Tableau, Databricks SQL | Unity Catalog |
| Reference / lookup — read by multiple engines | Either; Lakekeeper preferred if RisingWave owns the write |

### Why Trino is the unifying query layer

Trino speaks to both catalogs with full SQL and is already in the stack. Grafana, engineering analytics, and cross-catalog joins (`datalake.risingwave.casino_real_bet JOIN databricks.rw_poc.rw_casino_transactions`) all go through Trino without duplicating data or maintaining mirrors. Databricks SQL Warehouse remains available for Databricks-native users who only need Unity Catalog tables.

---

## 7. Power BI integration

The production use case driving this stack is Power BI dashboards that currently read historical tables from Databricks and need real-time capabilities added via RisingWave.

### Current path (historical tables)

```
Power BI → Databricks SQL Warehouse → Unity Catalog Delta tables
```

Every dashboard refresh or DirectQuery hit consumes DBUs. This is the standard Databricks-connected Power BI setup and remains correct for historical, well-optimised Delta tables.

### Real-time path — Option A: RisingWave direct (live KPI tiles)

```
Power BI → RisingWave (PostgreSQL connector, port 4566) → Hummock MVs
```

Pre-computed materialized views served directly from RisingWave's in-memory state store. No I/O at query time — queries return in <10ms. Best for fixed-shape KPI tiles that don't require ad-hoc drill-downs. Power BI connects via any PostgreSQL-compatible ODBC/JDBC driver.

### Real-time path — Option B: StarRocks (ad-hoc analytics)

```
Power BI → StarRocks (MySQL connector, port 9030) → Lakekeeper Iceberg → MinIO
```

StarRocks reads the Iceberg tables written by RisingWave sinks. Full ad-hoc SQL flexibility, ~150–400ms warmed latency. No duplicated computation — RisingWave transforms raw Kafka events once; StarRocks reads the result. Power BI connects via the MySQL connector.

Benchmarked (2026-06-07) against RisingWave reading the same Lakekeeper Iceberg data — StarRocks is ~2× faster on casino_transactions and ~3–4× faster on sportsbook_bets for JSON-extraction aggregation queries. See [STARROCKS_SERVING_LAYER.md](STARROCKS_SERVING_LAYER.md) for full benchmark results and setup details.

### Real-time path — Option C: Trino (cross-catalog joins)

```
Power BI → Trino (Presto connector) → Unity Catalog Iceberg → ADLS Gen2
                                    → Lakekeeper Iceberg    → MinIO
```

Trino reads Parquet directly from object storage — no SQL Warehouse DBUs consumed. The only option that can join across Unity Catalog tables and Lakekeeper tables in a single query. Power BI has a built-in **Presto connector** (protocol-compatible with Trino, default port 8080).

Data freshness is limited only by how quickly RisingWave writes new checkpoints — typically seconds.

### Trade-offs

| Concern | RisingWave (A) | StarRocks (B) | Trino (C) |
|---|---|---|---|
| Query latency | <10ms | ~150–400ms | ~500ms–5s |
| Ad-hoc flexibility | Low (pre-defined MVs) | High | High |
| Cross-catalog joins | No | No | Yes |
| PBI connector | PostgreSQL (ODBC) | MySQL | Presto |
| Governance | None beyond RW | None | None |
| Infrastructure to own | RisingWave (already in stack) | StarRocks | Trino (already in stack) |

### Recommendation — three-tier setup

| Table type / use case | Query layer for Power BI |
|---|---|
| Live KPI tiles (pre-defined aggregations) | **RisingWave** (Option A) |
| Ad-hoc analytics / drill-downs on real-time data | **StarRocks** (Option B) |
| Historical Delta tables (existing) | **Databricks SQL Warehouse** |
| Cross-catalog joins (real-time + historical) | **Trino** (Option C) |

Power BI supports multiple data source connections in a single report. Dashboard pages can combine all four sources independently.

### Compaction requirement for Trino performance

RisingWave appends small Iceberg files on every checkpoint. A Power BI query running `SUM(stake_euro)` over a table with thousands of tiny files will be slow. Mitigation options (in order of preference):

1. **Predictive Optimization** (Unity Catalog tables) — already enabled on `de_dev.rw_poc`; handles background compaction automatically
2. **Dagster `databricks_optimize` step** — on-demand compaction before scheduled report refreshes
3. **Trino `ALTER TABLE ... EXECUTE optimize`** — for Lakekeeper-managed tables (Trino Iceberg connector supports this natively)

---

## 8. Production checklist

- [ ] Lakekeeper deployed on Kubernetes with Helm, PostgreSQL backend
- [ ] ADLS Gen2 warehouse registered in Lakekeeper with SP credentials
- [ ] Vended credentials enabled — no long-lived `adlsgen2.account_key` in RisingWave sink config
- [ ] Azure AD OIDC configured as auth provider
- [ ] Trino `datalake` catalog pointing at production Lakekeeper URI (update `trino/catalog/datalake.properties`)
- [ ] Databricks cluster init script configured for `lakekeeper` Spark catalog (for Jobs/notebooks that need cross-catalog reads)
- [ ] Predictive Optimization enabled on `de_dev.rw_poc` Unity Catalog schema (already done in PoC — `ALTER SCHEMA ... ENABLE PREDICTIVE OPTIMIZATION`)
- [ ] Dagster `databricks_optimize` step wired into production orchestration for on-demand compaction before heavy reads
- [ ] Power BI Presto connector configured to point at Trino coordinator (port 8080) for cross-catalog joins
- [ ] Power BI PostgreSQL connector configured to point at RisingWave (port 4566) for live KPI tiles
- [ ] Power BI MySQL connector configured to point at StarRocks (port 9030) for ad-hoc analytics
- [ ] StarRocks memory sized at ≥4G in production; metadata cache TTL tuned for warm query latency
- [ ] StarRocks `core-site.xml` injection verified for both FE and BE (required for ADLS scans — see [STARROCKS_SERVING_LAYER.md](STARROCKS_SERVING_LAYER.md) §2)
- [ ] Power BI reports split: SQL Warehouse for historical Delta tables, RisingWave for live KPIs, StarRocks for ad-hoc analytics, Trino for cross-catalog joins
