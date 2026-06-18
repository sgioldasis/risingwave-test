# Iceberg → RisingWave Live Lookup Guide

## Problem

When a country name changes in the `iceberg_countries` Iceberg table, the application
dashboard should reflect the change immediately (or near-immediately).

This became non-trivial in **RisingWave 3.0**, which introduced a breaking regression.

---

## RisingWave 3.0 Regression: DataFusion Batch Engine

RisingWave 3.0 made **DataFusion the default batch query engine**. This broke
`CREATE VIEW` over `CREATE SOURCE (connector='iceberg')`:

```
Expected RisingWave plan in BatchPlanChoice, but got DataFusion plan
```

Direct `SELECT * FROM iceberg_source` still works (DataFusion executes it end-to-end),
but wrapping it in `CREATE VIEW` does not.

---

## Approaches Tried and Why They Failed

### Approach 1: CREATE VIEW over Iceberg SOURCE (worked in 2.8.5, broken in 3.0)

```sql
CREATE SOURCE src_iceberg_countries WITH (connector='iceberg', ...);
CREATE VIEW rw_countries AS SELECT * FROM src_iceberg_countries;
```

**Result in 3.0**: `CREATE VIEW` fails with the DataFusion plan error above.

### Approach 2: MATERIALIZED VIEW over Iceberg SOURCE

MVs use the streaming engine, so creation succeeds. However, the Iceberg streaming
connector is **append-only**. When a row is updated in Iceberg (e.g. `Greece → Hellas`),
the connector emits a new INSERT but **never retracts** the old row — causing duplicate
rows per country.

### Approach 3: FULL_RELOAD TABLE + VIEW

```sql
CREATE TABLE iceberg_countries_ref (PRIMARY KEY (country))
WITH (connector='iceberg', refresh_mode='FULL_RELOAD', refresh_interval_sec='1', ...);
CREATE VIEW rw_countries AS SELECT * FROM iceberg_countries_ref;
```

Avoids duplicates. However, the refreshed data only becomes visible after RisingWave's
checkpoint cycle — several seconds of latency even at 1-second refresh. Also tried as the
join partner in downstream VIEWs; discarded after discovering those VIEWs were not
actually needed (see below).

---

## Final Solution: Single CREATE SOURCE, inline JOINs only

`iceberg_countries` is a **mutable lookup table** — it changes rarely and is never needed
as an input to a RisingWave streaming MV or VIEW. All consumers that need `country_name`
do so at query time. The only correct approach is:

1. Create a plain `CREATE SOURCE` in RisingWave.
2. Every query that needs `country_name` inlines the JOIN at query time — DataFusion
   reads the current Iceberg snapshot immediately, with zero checkpoint wait.

### `src_iceberg_countries` — CREATE SOURCE

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

### Application backend (`/api/query/funnel`)

```sql
SELECT
    f.window_start, f.window_end, f.country,
    c.country_name::varchar AS country_name,
    f.viewers, f.carters, f.purchasers,
    f.view_to_cart_rate, f.cart_to_buy_rate
FROM funnel_summary f
LEFT JOIN src_iceberg_countries c ON f.country = c.country
WHERE f.window_start >= :start_time
  AND f.window_end   <= :end_time
ORDER BY f.window_start DESC, f.country
LIMIT 1000
```

DataFusion executes this as a single batch query. Country name changes are visible on
the very next API call.

### Trino notebooks

Trino cannot query RisingWave SOURCEs via its PostgreSQL connector. Use a cross-catalog
join instead — Trino reads `funnel_summary` from the `risingwave` catalog and
`iceberg_countries` directly from the `datalake` (Lakekeeper) catalog:

```sql
SELECT f.window_start, f.country, c.country_name, f.viewers, f.carters, f.purchasers
FROM funnel_summary f
LEFT JOIN datalake.public.iceberg_countries c ON f.country = c.country
ORDER BY f.window_start DESC
LIMIT 50
```

### `funnel_enriched` VIEW (Analytics tab)

`funnel_enriched` aggregates across all countries — `country_name` is not needed. It reads
from `funnel_summary` directly:

```sql
CREATE VIEW funnel_enriched AS
WITH country_aggregated AS (
    SELECT window_start, window_end,
        SUM(viewers) as viewers, SUM(carters) as carters, SUM(purchasers) as purchasers,
        SUM(viewers * view_to_cart_rate) / NULLIF(SUM(viewers), 0) as view_to_cart_rate,
        SUM(carters * cart_to_buy_rate) / NULLIF(SUM(carters), 0) as cart_to_buy_rate
    FROM funnel_summary
    GROUP BY window_start, window_end
)
SELECT ..., conversion_category(...), calculate_funnel_score(...), ...
FROM country_aggregated
```

---

## Architecture Summary

```
iceberg_countries (Lakekeeper/MinIO)
    │
    └── CREATE SOURCE src_iceberg_countries
            ├── backend inline JOIN  → immediate reads (DataFusion, at query time)
            └── Trino cross-catalog JOIN via datalake.public.iceberg_countries
```

---

## Key Insight: Streaming Path vs Batch Path

| Path | Object | On Iceberg update | Latency |
|------|--------|-------------------|---------|
| **Batch** (direct SELECT) | `CREATE SOURCE` | DataFusion reads current snapshot | Immediate |
| **Streaming** (MV over SOURCE) | `CREATE SOURCE → MV` | Append-only: old row never retracted → duplicate rows | — |
| **Polling** (FULL_RELOAD TABLE) | `CREATE TABLE refresh_mode=FULL_RELOAD` | Full replace on each cycle, visible after checkpoint | Several seconds |

For mutable lookup tables (country names, configs), always use the **batch path** —
inline `SELECT` via DataFusion — when immediate propagation is required.

---

## dbt Models

| Model | Materialization | Object type |
|-------|----------------|-------------|
| `src_iceberg_countries` | `risingwave_source` | `CREATE SOURCE` (Iceberg connector) |
| `funnel_enriched` | `view` | `CREATE VIEW` over `funnel_summary` + Python UDFs |

## Resetting Iceberg Objects

```bash
dbt run-operation drop_stale_iceberg_sources --project-dir dbt --profiles-dir dbt
dbt run --select src_iceberg_countries funnel_enriched --project-dir dbt --profiles-dir dbt
```

The `drop_stale_iceberg_sources` macro also drops any legacy objects from previous
architectures (`iceberg_countries_ref`, `rw_countries`, `funnel_summary_with_country`)
in case they exist from an older deployment.

---

## References

- [RisingWave Iceberg source](https://docs.risingwave.com/integrations/sources/apache-iceberg)
- [RisingWave views](https://docs.risingwave.com/sql/commands/sql-create-view)
