---
title: StarRocks Query Rewrite Demo
description: Building and validating a transparent-query-rewrite demo to strengthen the StarRocks evaluation
---

<!-- markdownlint-disable-file -->

## Goal

Demonstrate StarRocks' transparent materialized-view query rewrite: an
analyst (or BI tool) queries a *raw* base table with no knowledge that a
materialized view exists, and StarRocks silently redirects execution to the
pre-aggregated MV for a large speedup — no query changes required.

This is a separate proof point from the hot/cold unified serving layer
([SR_POC_UNIFIED_PLAN.md](SR_POC_UNIFIED_PLAN.md)); see that doc's
brainstorm list for how this fits into the broader StarRocks evaluation.

## Why the existing MV can't be the demo vehicle

Checked before building anything new, rather than assumed:

```sql
SELECT TABLE_NAME, QUERY_REWRITE_STATUS, QUERY_REWRITE_STATUS_REASON
FROM information_schema.materialized_views WHERE TABLE_NAME='mv_unified_funnel_summary';
```

```text
QUERY_REWRITE_STATUS:        INVALID
QUERY_REWRITE_STATUS_REASON: UNSUPPORTED_DEFINITION
```

`mv_unified_funnel_summary` is disqualified from rewrite entirely: it's a
`UNION ALL` across two external catalogs (JDBC RisingWave + external Iceberg
Databricks), each branch filtered by a volatile,
`CURRENT_TIMESTAMP()`-relative predicate. StarRocks' optimizer can't
statically prove a rewrite is valid when the MV's own filter shifts every
time it's evaluated. A new, purpose-built MV is needed: a stable SPJG
(Select-Project-Join-Group-by) rollup with no volatile predicates.

## Model shape

`dbt_starrocks/models/mv_funnel_daily_country_rollup.sql`:

```sql
{{
  config(
    materialized='materialized_view',
    distributed_by=['day'],
    properties={'query_rewrite_consistency': 'loose'}
  )
}}

SELECT
  date_trunc('day', window_start) AS day,
  country,
  SUM(viewers) AS viewers,
  SUM(carters) AS carters,
  SUM(purchasers) AS purchasers
FROM {{ source('databricks_uc', 'funnel_summary_historical') }}
GROUP BY day, country
```

Design choices:

- **Targets the Databricks Iceberg cold table specifically**, not the JDBC
  RisingWave source or the existing union view — Iceberg external catalogs
  have materially better rewrite support in StarRocks than JDBC federation.
- **No `refresh_method`** — defaults to `MANUAL` (confirmed from the
  `dbt-starrocks` adapter's macro source). Deliberate: trigger a refresh once
  before a demo so results stay static and reproducible for the whole
  session, rather than changing under a scheduled refresh mid-explanation.
- **`query_rewrite_consistency: loose`** — matches the existing MV. Since
  the base table is an external Iceberg catalog, StarRocks can't reliably
  track fine-grained freshness against it (same limitation hit earlier with
  `mv_unified_funnel_summary`'s partitioning), so `loose` avoids rewrite
  being refused on a freshness technicality.
- **No `WHERE` clause** — maximizes rewrite flexibility. A query with
  `WHERE day >= X AND country = 'GR'` can still match, because the optimizer
  can push that filter onto the MV's own output columns.

**Open question going in:** whether `date_trunc('day', ...)` as a `GROUP BY`
expression is itself rewrite-friendly, or needs to appear identically in the
querying SQL to match. Verified below rather than assumed.

## Deployment log

Deployed via `dbt_starrocks_build_job` (Dagster) on 2026-09-06. One snag hit
along the way: Dagster's cached `dbt_starrocks/target/manifest.json` didn't
know about the new model file until `dbt parse --profiles-dir .` was run
manually inside the `dbt_starrocks/` directory first — the job initially
failed with `KeyError: 'model.starrocks_unified_funnel.mv_funnel_daily_country_rollup'`
because the manifest was stale. After reparsing, the job found 5 models
(was 4) and completed with no errors.

Immediately checked eligibility rather than assuming the design worked:

```sql
SELECT TABLE_NAME, IS_ACTIVE, QUERY_REWRITE_STATUS, QUERY_REWRITE_STATUS_REASON, TABLE_ROWS
FROM information_schema.materialized_views WHERE TABLE_NAME='mv_funnel_daily_country_rollup';
```

```text
TABLE_NAME:                  mv_funnel_daily_country_rollup
IS_ACTIVE:                   true
QUERY_REWRITE_STATUS:        VALID
QUERY_REWRITE_STATUS_REASON: OK
TABLE_ROWS:                  0
```

**Confirmed: `QUERY_REWRITE_STATUS: VALID`.** The `date_trunc('day', ...)`
`GROUP BY` expression did not disqualify the MV — the design holds.
`TABLE_ROWS: 0` is expected: `refresh_method` defaults to `MANUAL` as
designed, and it hasn't been refreshed yet.

## Proof: the rewrite actually fires

Triggered a manual refresh:

```sql
REFRESH MATERIALIZED VIEW sr_local_db_sr_local_db.mv_funnel_daily_country_rollup WITH SYNC MODE;
```

Confirmed data landed (queried the MV directly):

```text
day                  country  viewers  carters  purchasers
2026-09-05 00:00:00  GR       57807    17440    5605
2026-09-06 00:00:00  GR       161148   48620    15760
```

Then ran `EXPLAIN` on a query against the **raw base table** — no mention of
the MV anywhere in the SQL:

```sql
EXPLAIN SELECT date_trunc('day', window_start) AS day, country, SUM(viewers) AS viewers
FROM databricks_uc.sr_poc_external.funnel_summary_historical
GROUP BY day, country;
```

```text
PLAN FRAGMENT 1
  0:OlapScanNode
     TABLE: mv_funnel_daily_country_rollup
     PREAGGREGATION: ON
     partitions=1/1
     rollup: mv_funnel_daily_country_rollup
     cardinality=2
     MaterializedView: true
```

**This is the whole pitch, proven.** The query text only ever referenced
`databricks_uc.sr_poc_external.funnel_summary_historical` — an external
Iceberg table. The execution plan shows an `OlapScanNode` (StarRocks'
*internal* storage engine scan, not an Iceberg/external scan) hitting
`mv_funnel_daily_country_rollup` directly, with `MaterializedView: true` and
`cardinality=2` (matching the MV's actual row count). The query never
touched Iceberg at all.

## Contrast: same query, rewrite forced off

```sql
SET enable_materialized_view_rewrite = false;
EXPLAIN SELECT date_trunc('day', window_start) AS day, country, SUM(viewers) AS viewers
FROM databricks_uc.sr_poc_external.funnel_summary_historical
GROUP BY day, country;
```

```text
PLAN FRAGMENT 2
  2:AGGREGATE (update serialize)
  |  output: sum(4: viewers)
  |  group by: 11: date_trunc, 3: country
  0:IcebergScanNode
     TABLE: sr_poc_external.funnel_summary_historical
     TABLE VERSION: Snapshot@(2218860942153489654)
     cardinality=4280
```

Same SQL text, same result set — but now the plan shows an
`IcebergScanNode` reading all `4280` raw rows plus two extra distributed
aggregation stages doing the `GROUP BY`/`SUM` live, work the MV had already
done once at refresh time.

## Latency: measured, not asserted

Ran the identical query 3x each way (wall-clock, via `mysql` client):

| | Run 1 | Run 2 | Run 3 |
|---|---|---|---|
| **With rewrite** | 0.15s | 0.06s | 0.05s |
| **Without rewrite** (forced off) | 5.50s | 1.78s | 1.80s |

**~20-35x faster with rewrite**, even at this trivial scale (4,280 raw
rows). Most of the "without rewrite" cost here is Iceberg metadata/file-list
overhead against external storage (REST catalog + ADLS), which the rewrite
path skips entirely by never touching Iceberg. This gap should only widen as
the historical table grows — worth re-running this same comparison later
once real data volume accumulates, to get a scaling curve rather than a
single snapshot.

## How to run this live for an audience

**Setup, before anyone's watching:**

1. Deploy the model if not already live: Dagster UI → Jobs →
   `dbt_starrocks_build_job` → Launchpad → Launch Run. (If the model was
   just added/changed, `dbt parse` needs to run first so Dagster's manifest
   picks it up — see "Deployment log" above for the exact snag hit.)
2. Refresh it once so results are static for the whole session:
   ```sql
   REFRESH MATERIALIZED VIEW sr_local_db_sr_local_db.mv_funnel_daily_country_rollup WITH SYNC MODE;
   ```
3. Confirm eligibility one more time:
   ```sql
   SELECT QUERY_REWRITE_STATUS, QUERY_REWRITE_STATUS_REASON
   FROM information_schema.materialized_views WHERE TABLE_NAME='mv_funnel_daily_country_rollup';
   -- expect: VALID / OK
   ```

**Live, in front of the audience:**

1. Show the raw query — emphasize it only mentions the raw historical
   table, nothing about any MV:
   ```sql
   SELECT date_trunc('day', window_start) AS day, country, SUM(viewers) AS viewers
   FROM databricks_uc.sr_poc_external.funnel_summary_historical
   GROUP BY day, country
   ORDER BY day;
   ```
2. Run `EXPLAIN` on it. Point at `OlapScanNode` / `TABLE:
   mv_funnel_daily_country_rollup` / `MaterializedView: true` — that's the
   "aha": the query never asked for this table.
3. `SET enable_materialized_view_rewrite = false;` in the session, run the
   *exact same query* again, `EXPLAIN` it again. Point at `IcebergScanNode`
   and the much higher `cardinality`. Same SQL, same answer, completely
   different plan.
4. Time both (even a simple client-side stopwatch is convincing — see the
   measured numbers above: ~0.05-0.15s vs ~1.8-5.5s).
5. `SET enable_materialized_view_rewrite = true;` again to leave the session
   in its default state before moving on.
6. Close the narrative: an analyst using a BI tool that only knows about the
   raw table gets this speedup automatically — nobody had to teach the tool
   about `mv_funnel_daily_country_rollup`.

## Caveats to state honestly if asked

- This works because the base table is Iceberg (external catalog with
  reasonable rewrite support). The equivalent pattern **did not work** for
  `mv_unified_funnel_summary`, which unions a JDBC source with
  `CURRENT_TIMESTAMP()`-relative filters — StarRocks flatly refuses rewrite
  for that shape (`UNSUPPORTED_DEFINITION`). Rewrite is not a blanket
  capability; it requires a specific, stable query shape.
- `refresh_method` is `MANUAL` here by design for demo stability, not
  because that's the recommended production setting — a real deployment
  would want a scheduled refresh (same tradeoff already documented for
  `mv_unified_funnel_summary` in
  [SR_POC_UNIFIED_PLAN.md](SR_POC_UNIFIED_PLAN.md)).
- The timing numbers above are from a single local run at tiny data volume
  (2 days, 4,280 raw rows) — a real evaluation should re-run this at
  production-representative volume before citing the multiplier as a hard
  number.
