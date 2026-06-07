# Databricks Iceberg Read + DataFusion Demo

RisingWave reads the Databricks Unity Catalog Iceberg tables written by the casino sinks back into itself as `CREATE SOURCE` + `CREATE VIEW` objects, then executes OLAP-style queries through the embedded DataFusion engine. Same psql connection, same SQL dialect — no SQL Warehouse cost, no Spark.

> **Status: WORKING ✅ (2026-06-06).** Both tables readable via `adlsgen2.account_key`. Sources created in `de_dev.rw_poc` which is backed by `stkznneurwpoccdddevstd` (same account used for writes).

---

## Architecture

```
Kafka → RisingWave streaming → sink_casino_transactions_databricks → de_dev.rw_poc.rw_casino_transactions (ADLS)
                             → sink_sportsbook_bets_databricks    → de_dev.rw_poc.rw_sportsbook_bets    (ADLS)
                                                                              ↓
                                                    Iceberg REST API (Unity Catalog) + adlsgen2 key
                                                                              ↓
                                             src_databricks_* (CREATE SOURCE — dbt, live snapshot)
                                                                              ↓
                                              v_databricks_* (CREATE VIEW — dbt, stable query surface)
                                                                              ↓
                                                      OPTIMIZE (Dagster, compact small files)
                                                                              ↓
                                                      DataFusion vectorized execution (Arrow)
                                                                              ↓
                                                        SELECT queries via psql:4566
```

**Why reads work without new Databricks setup:** Both tables are `USING ICEBERG` (pure Iceberg, not Delta+UniForm) and stored in `stkznneurwpoccdddevstd` via the schema's `MANAGED LOCATION`. RisingWave authenticates to the Unity Catalog IRC with the same OAuth2 credentials used for writes, then reads Parquet files directly from ADLS using `adlsgen2.account_key`. No vended credentials needed.

**Sources are live:** each query reads the current Iceberg snapshot — no drop+recreate needed to get fresh data.

---

## What Databricks admin steps are needed

**None.** The prerequisites are already in place from the sink setup:

- ✅ Tables created with `USING ICEBERG` in `de_dev.rw_poc`
- ✅ Schema backed by `stkznneurwpoccdddevstd` (MANAGED LOCATION)
- ✅ SP has `SELECT` on the schema
- ✅ SP has `EXTERNAL USE SCHEMA` on the schema

---

## CREATE SOURCE SQL

Sources and views are created automatically as dbt models (`src_databricks_*.sql`, `v_databricks_*.sql`) when `casino_prd_full_job` runs. For manual creation:

```sql
CREATE SOURCE IF NOT EXISTS src_databricks_casino_transactions
WITH (
    connector              = 'iceberg',
    catalog.type           = 'rest',
    catalog.uri            = '<DBT_DATABRICKS_HOST>/api/2.1/unity-catalog/iceberg-rest',
    catalog.oauth2_server_uri = 'https://login.microsoftonline.com/<TENANT_ID>/oauth2/v2.0/token',
    catalog.credential     = '<CLIENT_ID>:<CLIENT_SECRET>',
    catalog.scope          = '2ff814a6-3304-4ab8-85cb-cd0e6f879c1d/.default',
    warehouse.path         = 'de_dev',
    database.name          = 'rw_poc',
    table.name             = 'rw_casino_transactions',
    adlsgen2.account_name  = 'stkznneurwpoccdddevstd',
    adlsgen2.account_key   = '<ADLS_ACCOUNT_KEY>'
);

CREATE SOURCE IF NOT EXISTS src_databricks_sportsbook_bets
WITH (
    connector              = 'iceberg',
    catalog.type           = 'rest',
    catalog.uri            = '<DBT_DATABRICKS_HOST>/api/2.1/unity-catalog/iceberg-rest',
    catalog.oauth2_server_uri = 'https://login.microsoftonline.com/<TENANT_ID>/oauth2/v2.0/token',
    catalog.credential     = '<CLIENT_ID>:<CLIENT_SECRET>',
    catalog.scope          = '2ff814a6-3304-4ab8-85cb-cd0e6f879c1d/.default',
    warehouse.path         = 'de_dev',
    database.name          = 'rw_poc',
    table.name             = 'rw_sportsbook_bets',
    adlsgen2.account_name  = 'stkznneurwpoccdddevstd',
    adlsgen2.account_key   = '<ADLS_ACCOUNT_KEY>'
);

CREATE VIEW IF NOT EXISTS v_databricks_casino_transactions AS
    SELECT * FROM src_databricks_casino_transactions;

CREATE VIEW IF NOT EXISTS v_databricks_sportsbook_bets AS
    SELECT * FROM src_databricks_sportsbook_bets;
```

---

## Demo queries

All queries run against the `v_databricks_*` views (RisingWave `psql:4566`).

### Q1 — Data volume & freshness

**RisingWave (psql:4566)**

```sql
SELECT
    'casino_transactions'       AS table_name,
    COUNT(*)                    AS row_count,
    MIN(transaction_created_at) AS earliest,
    MAX(transaction_created_at) AS latest
FROM v_databricks_casino_transactions
UNION ALL
SELECT
    'sportsbook_bets',
    COUNT(*),
    MIN(placed_at),
    MAX(placed_at)
FROM v_databricks_sportsbook_bets;
```

**Trino (`localhost:9080`)**

```sql
SELECT
    'casino_transactions'       AS table_name,
    COUNT(*)                    AS row_count,
    MIN(transaction_created_at) AS earliest,
    MAX(transaction_created_at) AS latest
FROM databricks.rw_poc.rw_casino_transactions
UNION ALL
SELECT
    'sportsbook_bets',
    COUNT(*),
    MIN(placed_at),
    MAX(placed_at)
FROM databricks.rw_poc.rw_sportsbook_bets;
```

---

### Q2 — Top 10 customers by real-bet amount (UC1 equivalent)

**RisingWave (psql:4566)**

```sql
SELECT
    customer_id,
    currency_id,
    SUM(amount_abs)::numeric AS total_real_bet
FROM v_databricks_casino_transactions
WHERE message_type_id = 1
  AND account_id = 1
  AND amount_abs IS NOT NULL
GROUP BY customer_id, currency_id
ORDER BY total_real_bet DESC
LIMIT 10;
```

---

### Q3 — Turnover ratio per customer — casino vs sportsbook (UC2 equivalent)

**RisingWave (psql:4566)**

```sql
WITH casino AS (
    SELECT customer_id, SUM(amount_abs) AS casino_turnover
    FROM v_databricks_casino_transactions
    WHERE message_type_id = 2 AND account_id IN (1, 4) AND amount_abs IS NOT NULL
    GROUP BY customer_id
),
sportsbook AS (
    SELECT customer_id, SUM(stake_euro) AS sportsbook_turnover
    FROM v_databricks_sportsbook_bets
    GROUP BY customer_id
),
combined AS (
    SELECT
        COALESCE(c.customer_id, s.customer_id)  AS customer_id,
        COALESCE(c.casino_turnover, 0)           AS casino_turnover,
        COALESCE(s.sportsbook_turnover, 0)       AS sportsbook_turnover
    FROM casino c FULL OUTER JOIN sportsbook s ON c.customer_id = s.customer_id
)
SELECT
    customer_id,
    ROUND(casino_turnover::numeric, 2)                               AS casino_turnover,
    ROUND(sportsbook_turnover::numeric, 2)                           AS sportsbook_turnover,
    ROUND((casino_turnover + sportsbook_turnover)::numeric, 2)       AS total_turnover,
    ROUND(CASE
        WHEN casino_turnover + sportsbook_turnover = 0 THEN 0
        ELSE casino_turnover / (casino_turnover + sportsbook_turnover)
    END::numeric, 4)                                                 AS casino_ratio
FROM combined
ORDER BY total_turnover DESC
LIMIT 20;
```

---

### Q4 — Sportsbook bet segmentation by type

**RisingWave (psql:4566)**

```sql
SELECT
    bet_type_id,
    COUNT(*)                            AS bet_count,
    ROUND(SUM(stake_euro)::numeric, 2)  AS total_stake_euro,
    ROUND(AVG(stake_euro)::numeric, 2)  AS avg_stake_euro
FROM v_databricks_sportsbook_bets
GROUP BY bet_type_id
ORDER BY total_stake_euro DESC;
```

---

### Q5 — Properties bag exploration

**RisingWave (psql:4566)** — cast to `jsonb`, use `->>` operator

```sql
SELECT
    customer_id,
    ROUND(amount_abs::numeric, 2)               AS amount,
    transaction_created_at,
    (properties::jsonb)->>'game_id'             AS game_id,
    (properties::jsonb)->>'game_type'           AS game_type,
    (properties::jsonb)->>'is_live'             AS is_live,
    (properties::jsonb)->>'transaction_type_id' AS transaction_type_id,
    (properties::jsonb)->>'casino_provider_id'  AS casino_provider_id
FROM v_databricks_casino_transactions
WHERE amount_abs IS NOT NULL AND amount_abs > 0
ORDER BY transaction_created_at DESC
LIMIT 10;
```

> JSON extraction syntax by engine: RisingWave `(properties::jsonb)->>'key'` · Databricks `properties:key` · Trino `json_extract_scalar(properties, '$.key')`. All available keys: see §3a of `DATABRICKS_ICEBERG_SINK.md`.

---

## Running the demo

### Option 1 — Dagster UI

1. **Run `casino_prd_full_job`** — sets up the full streaming pipeline and creates the Iceberg sources + views as dbt models (`src_databricks_*`, `v_databricks_*`, group: `databricks`). Only needed once per stack start.

2. **Run `databricks_datafusion_job`** — runs two steps in order:
   - `databricks_optimize` — compacts small Parquet files via Databricks SQL API
   - `databricks_datafusion_demo` — runs all 5 queries against the views and logs results as markdown tables in the compute log

`databricks_datafusion_job` can be re-run any time without re-running `casino_prd_full_job`.

### Option 2 — psql (manual)

```bash
psql -h localhost -p 4566 -U root -d dev
```

Then paste any of the queries above. Sources and views persist across sessions once created by `casino_prd_full_job`.

---

## How query execution works

**No query is sent to Databricks.** The Unity Catalog REST API is used only at source creation time to locate the Iceberg metadata file. After that, RisingWave reads the Parquet files directly from ADLS and executes everything locally via its embedded DataFusion engine. No SQL Warehouse is involved; Databricks compute is completely out of the picture.

At query time, RisingWave does a lightweight metadata refresh (re-fetches the latest snapshot pointer from the Unity Catalog REST API) to get the current file list, then scans all Parquet files from ADLS in-process.

**Predicate pushdown** happens within DataFusion at the Parquet level — column pruning and row-group filtering against Parquet min/max statistics. However, there is no partition pruning: RisingWave 2.8.4 opens every Parquet file regardless of `WHERE` clause filters on partitioned columns (see Known limitations).

---

## Performance expectations

| Operation | Expected latency | Notes |
|---|---|---|
| `CREATE SOURCE` (dbt, once) | 3–8s | Fetches Iceberg metadata JSON from ADLS via Unity Catalog REST — schema + snapshot pointer only, no Parquet data read |
| `CREATE VIEW` | ~0s | Pure catalog registration, no I/O |
| Snapshot refresh (per query) | <500ms | Re-fetches latest snapshot pointer from Unity Catalog REST before each scan |
| `OPTIMIZE` (Dagster step) | 2–30s | Compacts small files written by sinks; fast after first run |
| Aggregation on flat columns | 50–300ms | DataFusion vectorized Arrow scan on DECIMAL/INT columns |
| Cross-table join (Q3) | 200ms–2s | Both tables in memory; depends on distinct customer count |
| `(properties::jsonb)->>'key'` (Q5) | 2–5× typed columns | STRING → JSONB parse per row, not vectorized |
| Data freshness | Current Iceberg snapshot | Source reads the latest committed snapshot on each query — no recreate needed |

**At PoC scale** (tens of thousands of rows): all queries complete in well under 1 second after source creation and OPTIMIZE.

---

## Known limitations

| Limitation | Detail |
|---|---|
| **Sources are live, not streaming** | A `CREATE SOURCE` reads the current Iceberg snapshot on each query — data is always fresh. However, it is a batch scan, not a continuous stream; you cannot base a `CREATE MATERIALIZED VIEW` on it for real-time incremental updates. |
| **Small-file accumulation** | RisingWave commits one Parquet file per checkpoint interval (~10s). `databricks_datafusion_job` runs `OPTIMIZE` before queries to compact files. Increase `commit_checkpoint_interval` in the sink to reduce accumulation rate. |
| **Not suitable for very large tables** | Full Parquet scan from ADLS. For billion-row tables: OOM risk + slow network I/O. See §13 of `DATABRICKS_ICEBERG_SINK.md` for large-table strategies. |
| **No partition pruning** | RisingWave 2.8.4 opens all Parquet files regardless of `WHERE` clause filters — no partition-level skipping. Row-group pruning via Parquet min/max statistics still applies within each file. Partition pruning is a future roadmap item. |
| **`properties` JSON is not vectorized** | JSON extraction via `(properties::jsonb)->>'key'` falls back to row-by-row string parsing. Use flat typed columns for high-frequency analytical aggregations. |
