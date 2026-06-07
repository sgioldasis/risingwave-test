# Trino Federation — Batch/Stream Unification

Trino acts as the cross-catalog query layer in this stack. It federates three independent data sources in a single SQL query:

| Catalog | Connector | What it holds |
|---|---|---|
| `databricks` | Iceberg (UC IRC) | Databricks Unity Catalog — Iceberg tables + UniForm Delta tables |
| `risingwave` | PostgreSQL | RisingWave materialized views — continuously updated by stream processing |
| `datalake` | Iceberg (REST) | Lakekeeper / MinIO — append-only raw event sinks from RisingWave |

This enables **batch/stream unification**: historical or reference data in Databricks joined with live RisingWave MVs in one query, with no data movement.

---

## 1. Architecture

```
Kafka/Redpanda
      │
      ▼
RisingWave (stream processing)
      │                          ┌─────────────────────────────────────┐
      ├─── MVs in Hummock ───────► risingwave catalog (PostgreSQL JDBC) │
      │    (always fresh)        └─────────────────────────────────────┘
      │                                            │
      ├─── Lakekeeper sinks ─────► datalake catalog (Iceberg REST)      │  TRINO
      │    (append-only)                           │                  joins
      │                                            │                  across
Databricks UC                                      │                  all three
      │                          ┌─────────────────────────────────────┐
      └─── Iceberg + UniForm ────► databricks catalog (Iceberg IRC)    │
           tables                └─────────────────────────────────────┘
```

---

## 2. Catalog configuration

Defined in `trino/catalog/`:

| File | Catalog name | Auth |
|---|---|---|
| `databricks.properties` | `databricks` | OAuth2 client credentials + ADLS account key |
| `risingwave.properties` | `risingwave` | PostgreSQL JDBC (`frontend-node-0:4566`) |
| `datalake.properties` | `datalake` | Lakekeeper REST, no auth (MinIO S3) |

---

## 3. UniForm — reading Delta tables as Iceberg

Databricks UniForm (External Iceberg Reads) allows Delta tables to expose Iceberg metadata via Unity Catalog IRC. Trino's `databricks` catalog reads them transparently alongside native Iceberg tables — no extra connectors needed.

**The underlying table is still Delta.** UniForm does not convert or migrate the table. After enabling it, Databricks continues to read and write the table as Delta (the `Provider` field stays `delta`). What UniForm adds is a second set of Iceberg metadata files generated alongside each Delta commit at the same storage location — so external engines that speak the Iceberg REST Catalog protocol (like Trino) can read the same Parquet files via that Iceberg metadata layer. No data is duplicated or moved.

### Creating a UniForm table in Databricks

```sql
CREATE TABLE de_dev.rw_poc.my_reference_table (
    id    BIGINT,
    name  STRING,
    ...
)
USING DELTA
TBLPROPERTIES (
    'delta.columnMapping.mode'             = 'name',
    'delta.enableIcebergCompatV2'          = 'true',
    'delta.universalFormat.enabledFormats' = 'iceberg'
);
```

The three `TBLPROPERTIES` are mandatory:
- `columnMapping.mode = 'name'` — required for Iceberg v2 compatibility (Iceberg uses column names, not IDs)
- `enableIcebergCompatV2 = 'true'` — enables the Iceberg v2 metadata layer
- `universalFormat.enabledFormats = 'iceberg'` — triggers background Iceberg metadata generation on each Delta commit

### Enabling UniForm on an existing Delta table

UniForm is incompatible with deletion vectors. If the table has `delta.enableDeletionVectors=true` (the default for tables created in modern Databricks runtimes), disable it first:

```sql
-- Step 1: disable deletion vectors (and row tracking, which depends on them)
ALTER TABLE de_dev.my_schema.existing_table
SET TBLPROPERTIES (
    'delta.enableDeletionVectors' = 'false',
    'delta.enableRowTracking'     = 'false'
);

-- Step 2: enable UniForm
ALTER TABLE de_dev.my_schema.existing_table
SET TBLPROPERTIES (
    'delta.columnMapping.mode'             = 'name',
    'delta.enableIcebergCompatV2'          = 'true',
    'delta.universalFormat.enabledFormats' = 'iceberg'
);
```

> **Note:** After enabling, existing data is readable immediately. New commits trigger Iceberg metadata regeneration asynchronously — there may be a brief delay before Trino sees the latest rows after a write.

---

## 4. Demo — currency codes reference tables

Two tables demonstrate the before/after of enabling UniForm, both holding the same 20 ISO 4217 currency rows. `currency_id = 7` maps to BRL (Brazilian Real) — the currency used in the PoC synthetic data.

### 4a — `uniform_currency_codes` (created with UniForm from the start)

```sql
CREATE TABLE de_dev.rw_poc.uniform_currency_codes (
    currency_id     INT,
    iso_code        STRING,
    currency_name   STRING,
    symbol          STRING,
    decimal_places  INT,
    region          STRING
)
USING DELTA
TBLPROPERTIES (
    'delta.columnMapping.mode'             = 'name',
    'delta.enableIcebergCompatV2'          = 'true',
    'delta.universalFormat.enabledFormats' = 'iceberg'
);
```

### 4b — `delta_currency_codes` (plain Delta → UniForm migration)

Created first as a plain Delta table (no UniForm), then migrated. This demonstrates the contrast:

| State | `SHOW TABLES FROM databricks.rw_poc` | `SELECT * FROM databricks.rw_poc.delta_currency_codes` |
|---|---|---|
| Before UniForm | ✓ visible (UC lists it) | ✗ fails — no Iceberg metadata |
| After UniForm | ✓ visible | ✓ works |

After migration the table is still `Provider: delta` in Databricks — only Iceberg metadata was added alongside the existing Delta log. Migration applied:

```sql
-- Step 1: modern Databricks tables have deletion vectors on by default — disable first
ALTER TABLE de_dev.rw_poc.delta_currency_codes
SET TBLPROPERTIES (
    'delta.enableDeletionVectors' = 'false',
    'delta.enableRowTracking'     = 'false'
);

-- Step 2: enable UniForm
ALTER TABLE de_dev.rw_poc.delta_currency_codes
SET TBLPROPERTIES (
    'delta.columnMapping.mode'             = 'name',
    'delta.enableIcebergCompatV2'          = 'true',
    'delta.universalFormat.enabledFormats' = 'iceberg'
);
```

---

## 5. Demo queries

### 5a — Delta/UniForm (Databricks) + Iceberg (RisingWave sink)

Enriches streaming casino transaction data with reference currency metadata. Both tables are in the `databricks` catalog.

```sql
SELECT
    c.iso_code,
    c.currency_name,
    c.symbol,
    c.region,
    CAST(COUNT(*) AS BIGINT)                    AS transactions,
    CAST(ROUND(SUM(t.amount_abs), 2) AS DOUBLE) AS total_volume
FROM databricks.rw_poc.uniform_currency_codes c
JOIN databricks.rw_poc.rw_casino_transactions t
    ON c.currency_id = t.currency_id
WHERE t.message_type_id = 1
GROUP BY c.iso_code, c.currency_name, c.symbol, c.region
ORDER BY total_volume DESC;
```

### 5b — Migrated Delta table + Iceberg sink (same pattern, different origin)

Same query as §5a but using `delta_currency_codes` — the table that started as plain Delta and was migrated to UniForm. From Trino's perspective it is identical to `uniform_currency_codes`.

```sql
SELECT
    c.iso_code,
    c.currency_name,
    c.symbol,
    c.region,
    CAST(COUNT(*) AS BIGINT)                    AS transactions,
    CAST(ROUND(SUM(t.amount_abs), 2) AS DOUBLE) AS total_volume
FROM databricks.rw_poc.delta_currency_codes c
JOIN databricks.rw_poc.rw_casino_transactions t
    ON c.currency_id = t.currency_id
WHERE t.message_type_id = 1
GROUP BY c.iso_code, c.currency_name, c.symbol, c.region
ORDER BY total_volume DESC;
```

### 5d — Delta/UniForm (Databricks) + RisingWave MV (real-time)

Joins a Databricks reference table with a live RisingWave MV across two catalogs — the batch/stream unification pattern.

```sql
SELECT
    c.iso_code,
    c.currency_name,
    c.region,
    CAST(r.casino_turnover AS DOUBLE)     AS casino_turnover,
    CAST(r.sportsbook_turnover AS DOUBLE) AS sportsbook_turnover,
    CAST(r.total_turnover AS DOUBLE)      AS total_turnover
FROM databricks.rw_poc.uniform_currency_codes c
JOIN risingwave.public.mv_turnover_percentage r
    ON c.currency_id = r.customer_id   -- placeholder: replace with a real shared key in production
ORDER BY r.total_turnover DESC;
```

> The join key above is illustrative — `mv_turnover_percentage` is keyed by `customer_id`, not `currency_id`. In production, the reference table would share a natural key with the MV (e.g., `player_country`, `market_id`, `product_id`).

---

## 6. Trino catalog visibility

```sql
-- list all catalogs
SHOW CATALOGS;

-- list tables across catalogs
SHOW TABLES FROM databricks.rw_poc;     -- Iceberg + UniForm tables
SHOW TABLES FROM risingwave.public;     -- RisingWave MVs and sources
SHOW TABLES FROM datalake.public;       -- Lakekeeper append-only sinks
```

From Trino, all tables in the `databricks` catalog appear as Iceberg regardless of their underlying format — UniForm Delta tables are indistinguishable from native Iceberg tables at the connector level. To inspect the actual storage format and UniForm status, use the **Databricks SQL editor** (not Trino).

**Storage format — all tables at once:**

```sql
SELECT table_name, data_source_format
FROM de_dev.information_schema.tables
WHERE table_schema = 'rw_poc'
ORDER BY table_name;
```

Expected output for this PoC:

| table_name | data_source_format |
|---|---|
| `delta_currency_codes` | DELTA |
| `rw_casino_transactions` | ICEBERG |
| `rw_sportsbook_bets` | ICEBERG |
| `uniform_currency_codes` | DELTA |

**UniForm status — per table** (Databricks DDL commands cannot be composed across multiple tables in a single query). Look for the `# Delta Uniform Iceberg` section in the output — if it is present with a `Metadata location`, UniForm is active on that table.

```sql
DESCRIBE EXTENDED de_dev.rw_poc.delta_currency_codes;
DESCRIBE EXTENDED de_dev.rw_poc.uniform_currency_codes;
DESCRIBE EXTENDED de_dev.rw_poc.rw_casino_transactions;
DESCRIBE EXTENDED de_dev.rw_poc.rw_sportsbook_bets;
```

Expected: both currency tables show the `# Delta Uniform Iceberg` section; the two transaction tables do not (they are native Iceberg, not Delta + UniForm).

---

## 7. DBeaver JDBC workarounds

Trino through DBeaver requires two fixes:

**1 — Driver property (one-time per connection):**

In DBeaver → right-click connection → Edit Connection → Driver Properties → add:

| Property | Value |
|---|---|
| `assumeLiteralNamesInMetadataCallsForNonConformingClients` | `true` |

**2 — Cast `number` columns to `DOUBLE` in queries:**

RisingWave exposes aggregation columns as PostgreSQL `numeric` / `number`. Trino's PostgreSQL connector maps these to `decimal`, which DBeaver's JDBC type resolver returns as null — causing `"Error executing query: null"`. Wrap any column sourced from `risingwave.*` that holds a numeric aggregation:

```sql
CAST(r.total_turnover AS DOUBLE)
CAST(r.casino_turnover AS DOUBLE)
```

This is only needed in DBeaver. Trino CLI, DataGrip, and most other JDBC clients handle the type correctly.

---

## 8. Known limitations

| Limitation | Detail |
|---|---|
| **UniForm metadata delay** | Iceberg metadata is generated asynchronously after each Delta write. Trino may see stale data for a few seconds after a Databricks commit. |
| **No write-back via Trino** | Trino can read UniForm tables but cannot write to them. Writes go through Databricks SQL or Spark. |
| **Cross-catalog joins are point-in-time** | Trino computes the result at query time — it does not produce a continuously updating stream. RisingWave handles that. |
| **DBeaver `number` type** | See §7 above — requires explicit `CAST(... AS DOUBLE)` for RisingWave numeric columns. |
| **ADLS credential vending** | Credential vending (IRC-vended SAS tokens) does not work on Azure for Trino. Account key via `azure.access-key` in `databricks.properties` is required. |
| **Deletion vectors block UniForm** | Modern Databricks tables have `delta.enableDeletionVectors=true` by default. UniForm (IcebergCompatV2) is incompatible — must disable deletion vectors and row tracking before enabling UniForm on existing tables. |
