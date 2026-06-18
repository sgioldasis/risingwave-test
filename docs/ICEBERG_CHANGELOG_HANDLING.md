# Iceberg → RisingWave Live Lookup Guide

## Problem

When a country name changes in the `iceberg_countries` Iceberg table, downstream RisingWave views
and the application dashboard should reflect the change immediately (or near-immediately).

This became non-trivial in **RisingWave 3.0**, which introduced a breaking regression affecting
the approach that worked in 2.8.5.

---

## RisingWave 3.0 Regression: DataFusion Batch Engine

RisingWave 3.0 (released 2026-06-11) made **DataFusion the default batch query engine**.
This broke `CREATE VIEW` over `CREATE SOURCE (connector='iceberg')`:

```
Expected RisingWave plan in BatchPlanChoice, but got DataFusion plan
```

The batch planner (`BatchPlanChoice`) encounters a DataFusion plan node when validating the
view's query and fails. Direct `SELECT * FROM iceberg_source` still works (DataFusion executes
it end-to-end), but wrapping it in `CREATE VIEW` does not.

---

## Approaches Tried and Why They Failed

### Approach 1: CREATE VIEW over Iceberg SOURCE (worked in 2.8.5, broken in 3.0)

```sql
CREATE SOURCE src_iceberg_countries WITH (connector='iceberg', ...);
CREATE VIEW rw_countries AS SELECT * FROM src_iceberg_countries;
```

**Result in 3.0**: `CREATE VIEW` fails with the DataFusion plan error above.

### Approach 2: MATERIALIZED VIEW over Iceberg SOURCE

```sql
CREATE MATERIALIZED VIEW rw_countries AS SELECT * FROM src_iceberg_countries;
```

MVs use the streaming engine (not the batch planner), so creation succeeds. However, the Iceberg
streaming connector reads new snapshots in **append-only** mode. When a row is updated in Iceberg
(e.g. `Greece → Hellas`), the connector emits a new INSERT for `(GR, Hellas)` but **never
retracts** the old `(GR, Greece)` row. This causes both values to accumulate in the MV,
resulting in duplicate rows per country.

### Approach 3: FULL_RELOAD TABLE with periodic refresh

```sql
CREATE TABLE src_iceberg_countries ( PRIMARY KEY (country) )
WITH (connector='iceberg', refresh_mode='FULL_RELOAD', refresh_interval_sec='1', ...);
```

Resolves the duplicate-row problem — a full table replacement forces correct retraction in
downstream joins. However, the refreshed data only becomes visible after RisingWave's next
**checkpoint cycle**. Even with `refresh_interval_sec='1'`, the effective latency is several
seconds because the checkpoint gates visibility.

Using this table as the join partner in a streaming MV also resulted in 20–30 second update
latency due to streaming retraction overhead across all MV rows when the table changed.

---

## Final Solution

Two separate RisingWave objects backed by the same Iceberg table, serving two different purposes:

### Object 1: `src_iceberg_countries` — CREATE SOURCE

```sql
CREATE SOURCE src_iceberg_countries
WITH (
    connector      = 'iceberg',
    catalog.type   = 'rest',
    catalog.uri    = 'http://lakekeeper:8181/catalog',
    warehouse.path = 'risingwave-warehouse',
    database.name  = 'public',
    table.name     = 'iceberg_countries',
    s3.endpoint    = 'http://minio-0:9301',
    s3.region      = 'us-east-1',
    s3.access.key  = 'hummockadmin',
    s3.secret.key  = 'hummockadmin',
    s3.path.style.access = 'true'
);
```

Used by the application backend via an **inline batch JOIN**. Because this is a plain
`SELECT` (not wrapped in `CREATE VIEW`), DataFusion executes the entire query including the
Iceberg read — it reads the **current Iceberg snapshot at query time**. No polling, no
checkpoint wait. Country name changes are visible on the very next request.

### Object 2: `iceberg_countries_ref` — FULL_RELOAD TABLE

```sql
CREATE TABLE iceberg_countries_ref ( PRIMARY KEY (country) )
WITH (
    connector      = 'iceberg',
    refresh_mode   = 'FULL_RELOAD',
    refresh_interval_sec = '1',
    ...
);
```

Used only by `rw_countries` VIEW for Dagster pipeline display. Has ~1–3 second latency due to
the checkpoint cycle, but avoids duplicate rows and works correctly with `CREATE VIEW`.

### `rw_countries` — VIEW over FULL_RELOAD TABLE

```sql
CREATE VIEW rw_countries AS
SELECT country::varchar, country_name::varchar
FROM iceberg_countries_ref;
```

Works in RW 3.0 because `iceberg_countries_ref` is native RisingWave storage — no DataFusion
in the batch planner's path.

### Application backend — inline JOIN with SOURCE

Instead of querying `funnel_summary_with_country` (a pre-built view), the backend inlines the
join:

```sql
SELECT
    f.window_start,
    f.window_end,
    f.country,
    c.country_name::varchar AS country_name,
    f.viewers,
    f.carters,
    f.purchasers,
    f.view_to_cart_rate,
    f.cart_to_buy_rate
FROM funnel_summary f
LEFT JOIN src_iceberg_countries c ON f.country = c.country
WHERE f.window_start >= :start_time
  AND f.window_end   <= :end_time
ORDER BY f.window_start DESC, f.country
LIMIT 1000
```

`funnel_summary` is a native RisingWave MV. `src_iceberg_countries` is the Iceberg SOURCE.
DataFusion handles both sides of the join as a single batch query. The result reflects the
current Iceberg snapshot every time the endpoint is called.

---

## Architecture Summary

```
iceberg_countries (Lakekeeper/MinIO)
    │
    ├── CREATE SOURCE src_iceberg_countries
    │       └── backend inline JOIN → immediate reads (DataFusion, at query time)
    │
    └── FULL_RELOAD TABLE iceberg_countries_ref  (refresh every 1s)
            └── VIEW rw_countries  (Dagster display, ~1–3s latency)
```

---

## Key Insight: Streaming Path vs Batch Path

| Path | Object | On Iceberg update | Latency |
|------|--------|-------------------|---------|
| **Batch** (direct SELECT) | `CREATE SOURCE` | DataFusion reads current snapshot | Immediate |
| **Streaming** (MV over SOURCE) | `CREATE SOURCE → MV` | Append-only: new row inserted, old row NOT retracted → duplicate rows | — |
| **Polling** (FULL_RELOAD TABLE) | `CREATE TABLE refresh_mode=FULL_RELOAD` | Full replace on each cycle, visible after checkpoint | Several seconds |

For mutable lookup tables (country names, configs), always use the **batch path** — inline
`SELECT` with DataFusion — when immediate propagation is required.

---

## dbt Models

| Model | Materialization | Object type |
|-------|----------------|-------------|
| `src_iceberg_countries` | `risingwave_source` | `CREATE SOURCE` |
| `iceberg_countries_ref` | `risingwave_iceberg_table` | `CREATE TABLE` (FULL_RELOAD) |
| `rw_countries` | `view` | `CREATE VIEW` over `iceberg_countries_ref` |
| `funnel_summary_with_country` | `view` | `CREATE VIEW` over `funnel_summary` + `iceberg_countries_ref` (Dagster display only — backend uses inline JOIN) |

## Resetting Iceberg Objects

```bash
dbt run-operation drop_stale_iceberg_sources --project-dir dbt --profiles-dir dbt
dbt run --select src_iceberg_countries iceberg_countries_ref rw_countries funnel_summary_with_country \
    --project-dir dbt --profiles-dir dbt
```

The `drop_stale_iceberg_sources` macro handles all object types (SOURCE, TABLE, VIEW, MV) in
the correct dependency order.

---

## References

- [RisingWave Iceberg source](https://docs.risingwave.com/integrations/sources/apache-iceberg)
- [RisingWave refreshable tables](https://docs.risingwave.com/sql/commands/sql-create-table#refreshable-tables)
- [RisingWave views](https://docs.risingwave.com/sql/commands/sql-create-view)
