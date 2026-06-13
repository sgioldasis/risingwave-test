# Databricks Unity Catalog — Reading Iceberg Tables in RisingWave

RisingWave reads Databricks Unity Catalog Iceberg tables back into itself as `CREATE SOURCE` + `CREATE VIEW` objects, then executes OLAP-style queries through the embedded DataFusion engine. Same psql connection, same SQL dialect — no SQL Warehouse cost, no Databricks compute.

> **Status: WORKING ✅ (2026-06-07).** Both tables readable via `adlsgen2.account_key`. Sources created in `de_dev.rw_poc` which is backed by `stkznneurwpoccdddevstd`. See §9 for the investigation history and why vended credentials don't work.

---

## 1. What you can do with Iceberg sources

Once a RisingWave Iceberg source is created:

- **Join live streaming MVs with historical Databricks tables** in a single SQL query
- **Backfill materialized views** from existing Databricks data
- **Cross-system analytics** without moving data out of Databricks
- **Run OLAP queries via DataFusion** — vectorized Arrow execution directly over Parquet files in ADLS

---

## 2. Architecture

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

## 3. How query execution works

**No query is sent to Databricks.** The Unity Catalog REST API is used only at source creation time to locate the Iceberg metadata file. After that, RisingWave reads the Parquet files directly from ADLS and executes everything locally via its embedded DataFusion engine. No SQL Warehouse is involved; Databricks compute is completely out of the picture.

At query time, RisingWave does a lightweight metadata refresh (re-fetches the latest snapshot pointer from the Unity Catalog REST API) to get the current file list, then scans all Parquet files from ADLS in-process.

**Predicate pushdown** happens within DataFusion at the Parquet level — column pruning and row-group filtering against Parquet min/max statistics. There is no partition pruning: RisingWave 2.8.4 opens every Parquet file regardless of `WHERE` clause filters on partitioned columns.

---

## 4. Iceberg Source vs alternatives

### Approach A — Iceberg Source (this document)

RisingWave reads Parquet files **directly from ADLS** using the Iceberg file format. The Unity Catalog REST API is used only for metadata. No SQL Warehouse needed.

### Approach B — Pipeline / bridge (no native connector)

RisingWave has **no native connector to Databricks SQL Warehouse**. Alternatives:

| Option | How it works | Latency | Complexity |
|---|---|---|---|
| **Kafka pipeline** | Databricks streams changes via Delta Live Tables or a job; RisingWave consumes from Kafka | Near real-time | High — requires Databricks streaming job + network access |
| **Dagster periodic sync** | Dagster asset queries Databricks SQL Warehouse via SDK and writes rows to RisingWave via psycopg2 | Minutes (batch) | Low — pure Python, reuses existing auth |
| **Trino** | Trino queries Databricks via IRC; not a RisingWave data feed — query tool for humans only | N/A | Medium |

### Comparison

| | Iceberg Source (A) | Kafka pipeline (B1) | Dagster sync (B2) |
|---|---|---|---|
| Databricks cost | Storage reads only | Streaming job compute | SQL Warehouse per poll |
| Data freshness | Latest Iceberg snapshot | Near real-time | Minutes |
| RisingWave native | Yes | Yes (Kafka source) | No (external push) |
| Usable in MVs | Yes (batch scan only) | Yes | No |
| Setup complexity | Medium (ADLS key needed) | Very high | Low |

---

## 5. CREATE SOURCE SQL

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

**Creation cost:** `CREATE SOURCE` fetches only Iceberg metadata JSON from ADLS (~3–8s, network round-trip to Azure). No Parquet data is read at creation time. `CREATE VIEW` is free — pure catalog registration.

---

## 6. Queries

All three engines can query the same data. Syntax differences:

| Engine | JSON extraction | Table reference |
|---|---|---|
| RisingWave (`psql:4566`) | `(properties::jsonb)->>'key'` | `v_databricks_*` views |
| Databricks SQL | `properties:key` | `de_dev.rw_poc.*` |
| Trino (`localhost:9080`) | `json_extract_scalar(properties, '$.key')` | `databricks.rw_poc.*` |

---

### Health check — data volume & freshness

**RisingWave**
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

**Databricks SQL**
```sql
SELECT COUNT(*), MIN(transaction_created_at), MAX(transaction_created_at)
FROM de_dev.rw_poc.rw_casino_transactions;

SELECT COUNT(*), MIN(placed_at), MAX(placed_at)
FROM de_dev.rw_poc.rw_sportsbook_bets;
```

**Trino**
```sql
SELECT COUNT(*), MIN(transaction_created_at), MAX(transaction_created_at)
FROM databricks.rw_poc.rw_casino_transactions;
```

---

### Accessing `properties` fields

Both tables have a `properties STRING` column with a JSON object containing all remaining proto fields.

**RisingWave**
```sql
SELECT
    customer_id, amount_abs, transaction_created_at,
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

**Databricks SQL**
```sql
SELECT
    customer_id, amount_abs, transaction_created_at,
    properties:game_id             AS game_id,
    properties:game_type           AS game_type,
    properties:is_live             AS is_live,
    properties:transaction_type_id AS transaction_type_id,
    properties:casino_provider_id  AS casino_provider_id
FROM de_dev.rw_poc.rw_casino_transactions
WHERE amount_abs IS NOT NULL AND amount_abs > 0
ORDER BY transaction_created_at DESC
LIMIT 10;
```

**Trino**
```sql
SELECT
    customer_id, amount_abs, transaction_created_at,
    json_extract_scalar(properties, '$.game_id')             AS game_id,
    json_extract_scalar(properties, '$.game_type')           AS game_type,
    json_extract_scalar(properties, '$.is_live')             AS is_live,
    json_extract_scalar(properties, '$.transaction_type_id') AS transaction_type_id,
    json_extract_scalar(properties, '$.casino_provider_id')  AS casino_provider_id
FROM databricks.rw_poc.rw_casino_transactions
WHERE amount_abs IS NOT NULL AND amount_abs > 0
ORDER BY transaction_created_at DESC
LIMIT 10;
```

All `properties` keys for **casino transactions**: `company_id`, `casino_provider_id`, `external_provider_id`, `game_id`, `game_type`, `is_live`, `provider_game_code`, `round_ref`, `round_created_at`, `message_id`, `session_id`, `token_type_id`, `jackpot_win_amount`, `jackpot_contribution_amount`, `is_round_closed`, `transaction_id`, `currency_rate_to_euro`, `source_id`, `bonus_action`, `pandora_journey_id`, `customer_campaign_id`, `campaign_id`, `campaign_type_id`, `transaction_type_id`.

All `properties` keys for **sportsbook bets**: `operator_id`, `bet_type`, `in_play`, `total_odds`, `potential_returns_euro`, `lines`, `bonus_type`, `is_placement`, `last_updated`.

---

### UC1 — Top customers by real-bet amount (batch)

**RisingWave**
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

**UC1 — 5-minute rolling real-bet amount (windowed)**

**Databricks SQL**
```sql
SELECT
    customer_id, currency_id, transaction_created_at,
    SUM(amount_abs) OVER (
        PARTITION BY customer_id, currency_id
        ORDER BY transaction_created_at
        RANGE BETWEEN INTERVAL 300 SECONDS PRECEDING AND CURRENT ROW
    ) AS rolling_5m_real_bet_amount
FROM de_dev.rw_poc.rw_casino_transactions
WHERE message_type_id = 1 AND account_id = 1 AND amount_abs IS NOT NULL;
```

**Trino** — `INTERVAL '300' SECOND` (quoted value, singular unit)
```sql
SELECT
    customer_id, currency_id, transaction_created_at,
    SUM(amount_abs) OVER (
        PARTITION BY customer_id, currency_id
        ORDER BY transaction_created_at
        RANGE BETWEEN INTERVAL '300' SECOND PRECEDING AND CURRENT ROW
    ) AS rolling_5m_real_bet_amount
FROM databricks.rw_poc.rw_casino_transactions
WHERE message_type_id = 1 AND account_id = 1 AND amount_abs IS NOT NULL;
```

**RisingWave** — `INTERVAL '300 seconds'` (PostgreSQL style)
```sql
SELECT
    customer_id, currency_id, transaction_created_at,
    SUM(amount_abs) OVER (
        PARTITION BY customer_id, currency_id
        ORDER BY transaction_created_at
        RANGE BETWEEN INTERVAL '300 seconds' PRECEDING AND CURRENT ROW
    ) AS rolling_5m_real_bet_amount
FROM v_databricks_casino_transactions
WHERE message_type_id = 1 AND account_id = 1 AND amount_abs IS NOT NULL;
```

> The window is parameterised — change the interval without touching the RisingWave pipeline.

---

### UC2 — Turnover ratio per customer (casino vs sportsbook)

**RisingWave**
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

**Databricks SQL**
```sql
WITH casino AS (
    SELECT customer_id, SUM(amount_abs) AS casino_turnover
    FROM de_dev.rw_poc.rw_casino_transactions
    WHERE message_type_id = 2 AND account_id IN (1, 4) AND amount_abs IS NOT NULL
    GROUP BY customer_id
),
sportsbook AS (
    SELECT customer_id, SUM(stake_euro) AS sportsbook_turnover
    FROM de_dev.rw_poc.rw_sportsbook_bets
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
    ROUND(casino_turnover, 2)                                    AS casino_turnover,
    ROUND(sportsbook_turnover, 2)                                AS sportsbook_turnover,
    ROUND(casino_turnover + sportsbook_turnover, 2)              AS total_turnover,
    ROUND(CASE
        WHEN casino_turnover + sportsbook_turnover = 0 THEN 0
        ELSE casino_turnover / (casino_turnover + sportsbook_turnover)
    END, 4)                                                      AS casino_ratio
FROM combined
ORDER BY total_turnover DESC;
```

**Trino**
```sql
WITH casino AS (
    SELECT customer_id, SUM(amount_abs) AS casino_turnover
    FROM databricks.rw_poc.rw_casino_transactions
    WHERE message_type_id = 2 AND account_id IN (1, 4) AND amount_abs IS NOT NULL
    GROUP BY customer_id
),
sportsbook AS (
    SELECT customer_id, SUM(stake_euro) AS sportsbook_turnover
    FROM databricks.rw_poc.rw_sportsbook_bets
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
    ROUND(casino_turnover, 2)    AS casino_turnover,
    ROUND(sportsbook_turnover, 2) AS sportsbook_turnover,
    ROUND(casino_turnover + sportsbook_turnover, 2) AS total_turnover,
    ROUND(CASE
        WHEN casino_turnover + sportsbook_turnover = 0 THEN 0
        ELSE casino_turnover / (casino_turnover + sportsbook_turnover)
    END, 4) AS casino_ratio
FROM combined
ORDER BY total_turnover DESC;
```

---

### Latest rolling turnover per customer (upsert semantics without delete files)

This is the read-time replacement for an upsert sink. Unity Catalog does not
support Iceberg delete files, so an upserting `mv_casino_turnover_latest` sink
stalls silently (see `DATABRICKS_ICEBERG_SINK.md` §16). Instead, RisingWave
lands the per-event rolling-turnover snapshots **append-only** into
`de_dev.rw_poc.rw_casino_turnover_90d` (sink `sink_casino_turnover_90d_databricks`),
and the "latest row per customer" collapse happens here via `QUALIFY` — no
delete files, no window recompute on the read side.

The view `de_dev.rw_poc.v_casino_turnover_latest` is created automatically by
the Dagster asset `databricks_turnover_latest_view` (part of `casino_prd_full_job`,
recreated each run). It reproduces the RisingWave MV `mv_casino_turnover_latest`
exactly:

**Databricks SQL** (the view definition)
```sql
CREATE OR REPLACE VIEW de_dev.rw_poc.v_casino_turnover_latest AS
SELECT
    customer_id,
    rolling_7d_turnover AS casino_turnover,
    event_ts
FROM de_dev.rw_poc.rw_casino_turnover_90d
QUALIFY ROW_NUMBER() OVER (
    PARTITION BY customer_id
    ORDER BY event_ts DESC
) = 1;
```

Query it like any table — one current row per customer:
```sql
SELECT customer_id, ROUND(casino_turnover, 2) AS casino_turnover, event_ts
FROM de_dev.rw_poc.v_casino_turnover_latest
ORDER BY casino_turnover DESC
LIMIT 20;
```

**Trino** — two caveats vs Databricks SQL (both verified against this stack):
1. Trino **cannot query the view** `v_casino_turnover_latest`: its Iceberg federation
   connector can't load Unity Catalog views (`Failed to load table ... in rw_poc
   namespace`). Query the base table instead.
2. Trino has **no `QUALIFY`** — use a `ROW_NUMBER()` subquery with `WHERE rn = 1`.

```sql
-- Dedup the base table in Trino (equivalent to the view)
SELECT customer_id, rolling_7d_turnover AS casino_turnover, event_ts
FROM (
    SELECT *, ROW_NUMBER() OVER (PARTITION BY customer_id ORDER BY event_ts DESC) AS rn
    FROM databricks.rw_poc.rw_casino_turnover_90d
) t
WHERE rn = 1
ORDER BY casino_turnover DESC
LIMIT 20;
```

Notes:
- **Why append-only is correct here:** each `rolling_7d_turnover` value is fixed
  once emitted — a later transaction at `t'` never falls inside an earlier row's
  `[t-window, t]` frame — so `force_append_only` on the sink drops nothing
  meaningful. The append-only table is the immutable event log; the view is the
  current-state projection.
- **Storage grows unbounded.** The append-only table keeps every snapshot, so
  the view scans the full history each query. Compact it with `OPTIMIZE`
  (see `databricks_optimize`) and prune old rows on the Databricks side if the
  retention isn't needed — the view only ever surfaces the latest per customer.
- **Tie behavior** matches the MV: rows sharing a customer's max `event_ts` are
  broken arbitrarily by `ROW_NUMBER`.

---

### Sportsbook bet segmentation by type

**RisingWave**
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

## 7. Running the DataFusion demo

### Option 1 — Dagster UI

1. **Run `casino_prd_full_job`** — sets up the full streaming pipeline and creates the Iceberg sources + views as dbt models (`src_databricks_*`, `v_databricks_*`). Only needed once per stack start.

2. **Run `databricks_datafusion_job`** — runs two steps in order:
   - `databricks_optimize` — compacts small Parquet files via Databricks SQL API
   - `databricks_datafusion_demo` — runs all queries against the views and logs results as markdown tables in the compute log

`databricks_datafusion_job` can be re-run any time without re-running `casino_prd_full_job`.

### Option 2 — psql (manual)

```bash
psql -h localhost -p 4566 -U root -d dev
```

Sources and views persist across sessions once created by `casino_prd_full_job`.

---

## 8. Performance expectations

| Operation | Expected latency | Notes |
|---|---|---|
| `CREATE SOURCE` (dbt, once) | 3–8s | Fetches Iceberg metadata JSON from ADLS via Unity Catalog REST — schema + snapshot pointer only, no Parquet data read |
| `CREATE VIEW` | ~0s | Pure catalog registration, no I/O |
| Snapshot refresh (per query) | <500ms | Re-fetches latest snapshot pointer from Unity Catalog REST before each scan |
| `OPTIMIZE` (Dagster step) | 2–30s | Compacts small files written by sinks; fast after first run |
| Aggregation on flat columns | 50–300ms | DataFusion vectorized Arrow scan on DECIMAL/INT columns |
| Cross-table join (UC2) | 200ms–2s | Both tables in memory; depends on distinct customer count |
| `(properties::jsonb)->>'key'` | 2–5× typed columns | STRING → JSONB parse per row, not vectorized |
| Data freshness | Current Iceberg snapshot | Source reads the latest committed snapshot on each query |

**At PoC scale** (tens of thousands of rows): all queries complete in well under 1 second after OPTIMIZE.

---

## 9. Known limitations

| Limitation | Detail |
|---|---|
| **Sources are live, not streaming** | A `CREATE SOURCE` reads the current Iceberg snapshot on each query — data is always fresh. However, it is a batch scan, not a continuous stream; you cannot base a `CREATE MATERIALIZED VIEW` on it for real-time incremental updates. |
| **Small-file accumulation** | RisingWave commits one Parquet file per checkpoint (~20s). Mitigated by Predictive Optimization (enabled on `de_dev.rw_poc`) and the `databricks_optimize` Dagster step. See §17 of `DATABRICKS_ICEBERG_SINK.md`. |
| **Not suitable for very large tables** | Full Parquet scan from ADLS. For billion-row tables: OOM risk + slow network I/O. See §11 (production viability) below. |
| **No partition pruning** | RisingWave 2.8.4 opens all Parquet files regardless of `WHERE` clause filters — no partition-level skipping. Row-group pruning via Parquet min/max statistics still applies within each file. Future roadmap item. |
| **`properties` JSON is not vectorized** | JSON extraction via `(properties::jsonb)->>'key'` falls back to row-by-row string parsing. Use flat typed columns for high-frequency analytical aggregations. |

---

## 10. Reading existing Delta tables (UniForm)

RisingWave's Iceberg connector **cannot read native Delta format** — it only understands Iceberg metadata (`metadata/*.json`, manifest files). Delta uses a different transaction log (`_delta_log/`).

### Databricks UniForm

UniForm writes Iceberg metadata *alongside* the Delta log simultaneously, making a Delta table readable by both Delta and Iceberg readers.

**All four properties must be set at `CREATE TABLE` time** — cannot `ALTER` an existing table:

```sql
CREATE TABLE de_dev.rw_poc.my_table ( ... )
USING DELTA
TBLPROPERTIES (
    'delta.columnMapping.mode'             = 'name',
    'delta.enableIcebergCompatV2'          = 'true',
    'delta.universalFormat.enabledFormats' = 'iceberg',
    'delta.enableDeletionVectors'          = 'false'
)
```

Once created, the same `CREATE SOURCE` SQL (§5) works — RisingWave reads the Iceberg metadata layer.

### Why ALTER fails on existing Delta tables

1. `IcebergCompatV2 requires delta.columnMapping.mode = 'name'` — changing column mapping on an existing table requires a full rewrite
2. `IcebergCompatV2 requires Deletion Vectors to be disabled` — if DVs were ever used, you'd need `REORG PURGE` first
3. `REORG PURGE` may fail if the table has other constraints

**The clean path for existing tables:** drop, recreate with the four properties, reload data.

### Gotchas when enabling UniForm on large existing tables

| Blocker | Detail |
|---|---|
| **Deletion Vectors** | Modern Databricks tables have DVs enabled by default. Must run `REORG TABLE <t> APPLY (PURGE)` — rewrites all data files, blocks writes, can take hours |
| **Column mapping mode** | Tables on `NoMapping` must migrate to `name` mode — can break Spark jobs that depend on column IDs |
| **Initial metadata sync** | On first enable, Databricks generates Iceberg metadata for the entire Delta log — can take hours for large tables |
| **Per-write overhead** | Every future write also updates Iceberg metadata — adds latency on high-frequency append tables |
| **Incompatible features** | Cannot enable UniForm if table uses: Liquid Clustering, V-Order, row tracking, type widening |
| **Protocol upgrade** | Requires reader version 2, writer version 7 — older Spark jobs may fail |

---

## 11. Production viability for large existing Delta tables

Maintaining two copies of large tables (one Delta, one Iceberg) is not acceptable at scale. Options without permanent duplication:

### Option 1 — UniForm + drop Liquid Clustering

Keep Delta format, disable Liquid Clustering to enable UniForm. Single table readable by both systems.

- ✅ No duplication, no format migration
- ⚠️ Databricks query performance degrades without LC — classic partitioning + ZORDER can partially compensate
- ⚠️ Existing tables need REORG PURGE + recreation (expensive at billions of rows)

### Option 2 — Iceberg as primary format (new pipelines)

Design net-new pipelines with Iceberg from the start. Databricks 13.3+ reads and writes Iceberg natively.

- ✅ No duplication, vendor independence long-term
- ⚠️ Full rewrite for existing tables — painful at billions of rows
- ✅ Clean architecture for new work

### Option 3 — Kafka CDC from Databricks (no format change)

Keep Delta + Liquid Clustering exactly as-is. Enable [Databricks Change Data Feed](https://docs.databricks.com/en/delta/delta-change-data-feed.html) and publish changes to Kafka. RisingWave consumes from Kafka.

```
Delta table → Change Data Feed → Kafka topic → RisingWave streaming table
```

- ✅ Zero format migration, LC untouched, no duplication
- ✅ Architecturally natural — RisingWave is a streaming engine
- ⚠️ Requires engineering to publish CDF to Kafka
- ⚠️ Bootstrap problem — Kafka only captures changes going forward

**Handling the bootstrap problem (Strategy B — recommended):**

1. Record the current Delta version: `DESCRIBE HISTORY <table> LIMIT 1` → note `version`
2. One-time full snapshot of the table into RisingWave via a temporary Iceberg mirror
3. Start Kafka CDC from exactly that version: `startingVersion = <recorded_version>`
4. Once RisingWave has caught up, drop the Iceberg mirror

The Iceberg mirror is a migration tool used once, not a permanent copy.

**Strategy C — accept partial history:** Enable CDC from today; historical analysis stays in Databricks. Acceptable if RisingWave pipelines only need recent data (rolling windows, last N days).

### Option 4 — Wait for RisingWave Delta connector

File a feature request. No timeline — not in v2.9.0 roadmap.

### Summary

| Option | Format change | Duplication | LC retained | Bootstrap |
|---|---|---|---|---|
| UniForm + drop LC | Metadata only | None | ❌ | No |
| Iceberg primary | Full rewrite | None | ❌ | No |
| Kafka CDC + snapshot | None | Temporary (1×) | ✅ | Yes — Strategy B |
| Delta connector (future) | None | None | ✅ | No |

**Option 3 with Strategy B** is the most production-grade path for large existing tables. For net-new pipelines, design with Iceberg from the start (Option 2).

---

## 12. Liquid Clustered tables — Iceberg mirror pattern

Liquid Clustering is **incompatible with UniForm**. For tables with a daily batch cadence, use an Iceberg mirror instead of losing the clustering:

```sql
-- Step 1: Create the Iceberg mirror once
CREATE TABLE de_dev.rw_poc.my_table_iceberg
TBLPROPERTIES (
    'delta.columnMapping.mode'             = 'name',
    'delta.enableIcebergCompatV2'          = 'true',
    'delta.universalFormat.enabledFormats' = 'iceberg',
    'delta.enableDeletionVectors'          = 'false'
)
AS SELECT * FROM de_dev.original_schema.my_table;

-- Step 2: Refresh daily (add to existing batch job)
TRUNCATE TABLE de_dev.rw_poc.my_table_iceberg;
INSERT INTO de_dev.rw_poc.my_table_iceberg
SELECT * FROM de_dev.original_schema.my_table;
```

**Data gap during refresh:** `TRUNCATE + INSERT` leaves the mirror empty between operations. Alternatives:

```sql
-- Option A: MERGE (no gap, slower)
MERGE INTO de_dev.rw_poc.my_table_iceberg AS t
USING de_dev.original_schema.my_table AS s
ON t.id = s.id
WHEN MATCHED THEN UPDATE SET *
WHEN NOT MATCHED THEN INSERT *;

-- Option B: Atomic swap (write to staging, rename — no gap)
CREATE OR REPLACE TABLE de_dev.rw_poc.my_table_iceberg_new
TBLPROPERTIES (...) AS SELECT * FROM de_dev.original_schema.my_table;
-- then drop old and rename new
```

---

## 13. Investigation history — why vended credentials don't work

### What was tested

**Step 1 — Test table creation:**
Created `de_dev.risingwave_poc.test_rw_read` as a Delta+UniForm table. `CREATE SOURCE` in RisingWave succeeded (auth and metadata load worked). `SELECT` failed:

```
IcebergV2 error: Failed to load manifest list in cache
Unexpected (persistent) at read, context: { timeout: 10 } => io timeout reached
```

**Root cause:** The table lives in `stkznneucommoncdddevstd` — Databricks' internal managed ADLS account. We don't have the key for this account, and `vended_credentials = true` does not provide working Azure credentials through RisingWave's OpenDAL read path (same limitation as writes — see §7 of `DATABRICKS_ICEBERG_SINK.md`).

### What works vs what doesn't

| Step | Status | Detail |
|---|---|---|
| Azure AD token auth to Unity Catalog REST API | ✅ Works | `catalog.oauth2_server_uri` + `catalog.credential` + `catalog.scope` |
| Listing namespaces + tables via Iceberg REST | ✅ Works | Tables visible immediately after creation |
| `CREATE SOURCE` (metadata load) | ✅ Works | Source created without error |
| Reading from Databricks' managed ADLS (`stkznneucommoncdddevstd`) | ❌ Blocked | `vended_credentials` doesn't provide working Azure ADLS credentials |
| Reading from PoC ADLS (`stkznneurwpoccdddevstd`) | ✅ Works | Direct `adlsgen2.account_key` — this is what `de_dev.rw_poc` uses |

### What unblocked reads

The same admin action that unblocked writes: creating `de_dev.rw_poc` as a schema with `MANAGED LOCATION 'abfss://cont1@stkznneurwpoccdddevstd.dfs.core.windows.net/iceberg'`. Tables created in this schema land in our PoC storage account, which we have `adlsgen2.account_key` for. `vended_credentials` is never needed.

---

## 14. Iceberg REST API reference

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

# List namespaces
curl -s -H "Authorization: Bearer $TOKEN" "$BASE/catalogs/de_dev/namespaces" | python3 -m json.tool

# List tables in a namespace
curl -s -H "Authorization: Bearer $TOKEN" "$BASE/catalogs/de_dev/namespaces/rw_poc/tables" | python3 -m json.tool
```
