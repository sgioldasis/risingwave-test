# DataFusion Batch Analytics Demo — RisingWave Casino PoC

## What this demonstrates

RisingWave 2.8 introduced **Apache DataFusion** as its embedded OLAP query engine for Iceberg tables. This unifies stream processing and batch analytics in a single system:

- **Streaming**: RisingWave continuously processes casino Kafka events → materialized views → Iceberg sinks
- **Batch**: DataFusion-powered `SELECT` queries on those same Iceberg tables — vectorized, Arrow-native columnar execution

No Trino or Spark needed for ad-hoc analytics on the streaming data. Same psql connection, same SQL dialect.

```
Kafka → RisingWave streaming → Iceberg (Lakekeeper/MinIO)
                                      ↓
                              SELECT via DataFusion  ← same RisingWave connection
```

## How it works

DataFusion is transparent — standard `SELECT` on an Iceberg source automatically routes through the DataFusion vectorized engine. No session variables or special syntax required.

RisingWave guarantees SQL semantics and planning; DataFusion provides vectorized execution (Apache Arrow RecordBatches, tight loops, SIMD-friendly column scans).

## New Iceberg sources

Two Iceberg read-sources were created as dbt models:

| Source | Reads from | Iceberg table |
|---|---|---|
| `src_iceberg_casino_real_bet` | Lakekeeper | `rw_managed_casino_real_bet` |
| `src_iceberg_turnover_percentage` | Lakekeeper | `rw_managed_turnover_percentage` |

These are the same tables written by the streaming sinks, now exposed for batch reads.

## Running the demo

### Option 1 — Dagster UI

Trigger the `casino_datafusion_demo` asset from the Dagster UI. It runs four analytical queries and logs results to the compute log.

### Option 2 — psql

```bash
psql -h localhost -p 4566 -U root -d dev -f docs/poc/DATAFUSION_DEMO.sql
```

### Option 3 — Manual queries

```bash
psql -h localhost -p 4566 -U root -d dev
```

Then paste queries from [DATAFUSION_DEMO.sql](DATAFUSION_DEMO.sql).

## Queries — RisingWave (DataFusion) vs Trino

The same analytical questions expressed in both engines. RisingWave uses PostgreSQL dialect on Iceberg sources; Trino uses ANSI SQL with full `catalog.schema.table` paths.

---

### Q1 — Top 10 customers by real bet amount

**RisingWave (DataFusion)**
```sql
SELECT customer_id, currency_id,
       SUM(rolling_1d_real_bet_amount)::numeric AS total_bet
FROM src_iceberg_casino_real_bet
GROUP BY customer_id, currency_id
ORDER BY total_bet DESC
LIMIT 10;
```

**Trino**
```sql
SELECT customer_id, currency_id,
       SUM(rolling_1d_real_bet_amount) AS total_bet
FROM datalake.public.rw_managed_casino_real_bet
GROUP BY customer_id, currency_id
ORDER BY total_bet DESC
LIMIT 10;
```

---

### Q2 — Turnover ratio segmentation

**RisingWave (DataFusion)**
```sql
SELECT
    CASE
        WHEN casino_ratio > 0.7     THEN 'casino-heavy'
        WHEN sportsbook_ratio > 0.7 THEN 'sports-heavy'
        ELSE 'balanced'
    END AS segment,
    COUNT(*) AS customers,
    ROUND(AVG(total_turnover)::numeric, 2) AS avg_turnover
FROM src_iceberg_turnover_percentage
GROUP BY segment
ORDER BY customers DESC;
```

**Trino**
```sql
SELECT
    CASE
        WHEN casino_ratio > 0.7     THEN 'casino-heavy'
        WHEN sportsbook_ratio > 0.7 THEN 'sports-heavy'
        ELSE 'balanced'
    END AS segment,
    COUNT(*) AS customers,
    ROUND(AVG(total_turnover), 2) AS avg_turnover
FROM datalake.public.rw_managed_turnover_percentage
GROUP BY CASE
             WHEN casino_ratio > 0.7     THEN 'casino-heavy'
             WHEN sportsbook_ratio > 0.7 THEN 'sports-heavy'
             ELSE 'balanced'
         END
ORDER BY customers DESC;
```

---

### Q3 — Cross-table join: top 20 customers — bets + turnover ratio

**RisingWave (DataFusion)**
```sql
SELECT b.customer_id,
       SUM(b.rolling_1d_real_bet_amount)::numeric AS real_bet,
       ROUND(t.casino_ratio::numeric, 3) AS casino_ratio
FROM src_iceberg_casino_real_bet b
JOIN src_iceberg_turnover_percentage t ON b.customer_id = t.customer_id
GROUP BY b.customer_id, t.casino_ratio
ORDER BY real_bet DESC
LIMIT 20;
```

**Trino**
```sql
SELECT b.customer_id,
       SUM(b.rolling_1d_real_bet_amount) AS real_bet,
       ROUND(CAST(t.casino_ratio AS DOUBLE), 3) AS casino_ratio
FROM datalake.public.rw_managed_casino_real_bet b
JOIN datalake.public.rw_managed_turnover_percentage t ON b.customer_id = t.customer_id
GROUP BY b.customer_id, t.casino_ratio
ORDER BY real_bet DESC
LIMIT 20;
```

---

### Q4 — Data volume and freshness

**RisingWave (DataFusion)**
```sql
SELECT COUNT(*) AS rows,
       MAX(event_ts) AS latest_event
FROM src_iceberg_casino_real_bet;
```

**Trino**
```sql
SELECT COUNT(*) AS rows,
       MAX(event_ts) AS latest_event
FROM datalake.public.rw_managed_casino_real_bet;
```

---

### Key syntax differences

| Concern | RisingWave | Trino |
|---|---|---|
| Cast to decimal | `::numeric` | `CAST(x AS DOUBLE)` or omit |
| Table reference | `src_iceberg_casino_real_bet` | `datalake.public.rw_managed_casino_real_bet` |
| ROUND | `ROUND(x::numeric, n)` | `ROUND(CAST(x AS DOUBLE), n)` |
| `GROUP BY` alias | Supported (`GROUP BY segment`) | Not supported — must repeat the expression |
| Connection | psql port 4566 | Trino port 9080 / Redpanda Console |

## Known limitations (RisingWave 2.8)

- OOM risk on join-heavy queries over large datasets — spill-to-disk not mature yet
- Some expressions fall back to RisingWave's framework (Arrow↔DataChunk conversion overhead)
- Iceberg metadata tables (`$snapshots`, `$files`) still require Trino
