---
title: StarRocks Serving Features Demo
description: Generated (computed) columns, ASOF JOIN, workload groups, Stream Load and lake data cache — the serving features the other dashboards don't cover
---

<!-- markdownlint-disable-file -->

**The Superset surface is the "StarRocks Serving Features" dashboard
(`starrocks-serving-features`, dashboard 4).** Every chart on it connects
as user `superset_demo`, so the whole dashboard physically runs inside
the `rg_superset_demo` workload group. This doc is the build/verification
log; everything below was verified live on StarRocks 4.1.7 on
2026-11-16.

## 1. Generated (computed) columns — one definition, zero drift

**The wart being fixed:** the repo carried the identical
`round(carters / nullif(viewers, 0), 2)` expression in
`funnel_summary_direct` (a VIEW), in dashboard SQL, and in demo queries —
three places to drift.

**The fix:** `sr_local_db.funnel_window_stats` is a PRIMARY
KEY table whose rates are **generated (computed) columns**, defined once
at CREATE TABLE:

```sql
CREATE TABLE funnel_window_stats (
    window_start DATETIME NOT NULL,
    viewers BIGINT NOT NULL,
    carters BIGINT NOT NULL,
    purchasers BIGINT NOT NULL,
    view_to_cart_rate DOUBLE AS round(carters / nullif(viewers, 0), 2),
    cart_to_buy_rate  DOUBLE AS round(purchasers / nullif(carters, 0), 2)
) PRIMARY KEY (window_start) DISTRIBUTED BY HASH(window_start);
```

- **Gotcha, confirmed live:** the computed column needs an EXPLICIT type
  (`view_to_cart_rate DOUBLE AS ...`) — an untyped `c AS a + b` fails
  with "Column's type is null and it's not agg state column".
- **Refresh:** the 1-minute scheduled task `funnel_window_stats_refresh`
  (SUBMIT TASK, created by the `starrocks_feature_demo_setup` asset)
  upserts every window from `funnel_summary_direct`; the PK table makes
  re-inserting idempotent, and the INSERT lists only base columns (rates
  are never written, always derived).
- **Verified:** table populates within a minute of the task's first tick,
  rates match the view's own arithmetic exactly.

## 2. ASOF JOIN — nearest-timestamp lookup

Each page view is matched to the most recent risk-score publication for
its country at or before the event:

```sql
SELECT ... FROM (
    SELECT p.event_time, u.country
    FROM page_views_direct p JOIN dim_users u ON p.user_id = u.user_id
) e
ASOF JOIN country_scores c
    ON e.country = c.country AND e.event_time >= c.ts
```

`country_scores` is a per-country hourly feed (PRIMARY KEY (country, ts),
deterministic wave, seeded by the asset). The join is semantically the
same "country becomes data" story as the rewrite demo: the raw events
carry no country; `dim_users` supplies it and the feed publishes per
country.

**Syntax gotchas, confirmed live on 4.1.7** (each one cost a probe):

- The temporal condition only accepts BIGINT, DATE or DATETIME — a string
  timestamp fails with "ASOF JOIN temporal condition supports only...".
- The ON clause needs a **real-column equi predicate** alongside the
  temporal `>=`. Equi on a constant/aliased column through a subquery
  (`w.fx_key = f.fx_key` with `fx_key` computed in the subquery), or any
  subquery-derived right side, is rejected as an "Unsupported 'ASOF INNER
  JOIN' clause ... correlated condition". Both ASOF sides must be real
  tables (or CTEs of real columns).
- A literal predicate on the right side (`r.pair = 'EUR'`) is fine ON TOP
  of a real equi predicate.
- ASOF composes cleanly after a regular join (page_views ⋈ dim_users
  feeds it here), and Superset's sqlglot transpile preserves the ASOF
  clause (verified before building the dataset).

## 3. UNIQUE KEY table model — the fourth model

The Wallet dashboard shows DUPLICATE, AGGREGATE and PRIMARY KEY tables;
`sr_local_db.wallet_type_totals_unique` completes the set with
**UNIQUE KEY** — same per-key upsert semantics as PRIMARY KEY, but
whole-row REPLACE: an INSERT re-writing a key replaces the entire row (no
partial-column update).

```sql
CREATE TABLE wallet_type_totals_unique (
    type VARCHAR(32) NOT NULL,
    total DECIMAL(38,9),
    updated_at DATETIME
) UNIQUE KEY (type) DISTRIBUTED BY HASH(type);
```

- **Refresh:** the 1-minute task `wallet_type_totals_unique_refresh` (created
  by the `starrocks_feature_demo_setup` asset) INSERT-SELECTs the per-type
  `SUM(amount)` from the wallet table every minute; the UNIQUE KEY upsert
  makes re-inserting idempotent — one row per type, always.
- **The contrast to chart:** the AGGREGATE KEY twin (`wallet_type_totals_agg`,
  Wallet dashboard) maintains SUM incrementally **at ingest**; UNIQUE does a
  full-row upsert **at refresh**. Same answer, different mechanics.
- Chart 27 on the Serving Features dashboard ("Total Amount per Type —
  UNIQUE KEY").

## 4. GROUPING SETS — one query, every granularity

```sql
SELECT COALESCE(u.country, '(all countries)') AS country,
       COALESCE(CAST(DATE(p.event_time) AS VARCHAR), '(all days)') AS day,
       COUNT(*) AS page_views,
       GROUPING(u.country) AS g_country,
       GROUPING(DATE(p.event_time)) AS g_day
FROM page_views_direct p JOIN dim_users u ON p.user_id = u.user_id
GROUP BY GROUPING SETS ((u.country), (DATE(p.event_time)),
                        (u.country, DATE(p.event_time)), ())
```

- One scan computes (country), (day), (day × country) and the grand total —
  vs one query per level. `GROUPING()` returns 1 for a dimension aggregated
  away in that row's set; the `COALESCE` labels mark subtotal rows.
- **Verified before building** (the ASOF lesson): sqlglot (Superset's MySQL-
  dialect transpiler) preserves both `GROUP BY GROUPING SETS (...)` and
  `GROUPING(...)` through the virtual-dataset wrapper.
- Chart 26 on the Serving Features dashboard, rows ordered subtotal-first.

## 5. Delete-via-load — `__op` tombstones + `DELETE WHERE`

`bin/3_starrocks_delete_via_load_demo.sh` — how corrections and tombstones
flow into a PRIMARY KEY table from a load pipeline:

- **`__op` values are the reverse of what you might guess** (verified live
  2026-11-16): `__op=0` upserts (whole-row replace), `__op=1` deletes the
  key. My first probe with `__op=0` as "delete" silently upserted a row
  instead. The `__op` field rides in each JSON row and must be listed in
  the `columns` header (`columns: tx_id, note, amount, __op`).
- **`DELETE ... WHERE`** works directly on PK tables — the SQL-side
  equivalent for interactive corrections.
- Together these are the two paths a real CDC/correction pipeline uses;
  the demo walks all the way from plain upsert to tombstone to SQL DELETE.

## 6. Workload groups — dashboard queries isolated from refresh traffic

**The observed problem:** MV refreshes jammed dashboard queries during
the rewrite-demo slowness investigation (20-46s rebuilds landing
mid-query; `docs/SR_POC_QUERY_REWRITE_DEMO.md`). The stack already ran
`default_mv_wg` (cpu_weight 1) for refresh tasks and `default_wg` (100)
for everything else — BI traffic had no slice of its own.

**What was built:**

```sql
CREATE RESOURCE GROUP rg_superset_demo
TO (USER='superset_demo')
WITH ('cpu_weight_percent'='80', 'mem_limit'='80%');
```

`superset_demo` is a SELECT-only user (empty password, tables-only grant
— see the note below) that owns every dataset on the Serving Features
dashboard, so those queries physically run in the group no matter what
the refresh tasks are doing. The classifier is visible on the group row
of `SHOW RESOURCE GROUPS ALL`.

**Honest scope note (re-verified 2026-11-17 on shared-data 4.1.7 — the
earlier "shared-nothing" attribution was wrong; this cluster has been
shared-data on MinIO since 2026-09-08, see
`docs/SR_POC_STARROCKS_SHARED_DATA_MINIO.md`):** query queueing per
resource group is not available on this build, and the reasons are
different from what was first written:

- The 3.x-era properties (`max_concurrency`, `max_queue_size`,
  `queue_timeout`, `max_core_slots`) are rejected as "Unknown property" —
  they no longer exist; the current property is `concurrency_limit`
  (accepted and visible in `SHOW RESOURCE GROUPS ALL`).
- `concurrency_limit` does NOT queue: since v4.1 the FE runs **query
  queue v2** by default (`enable_query_queue_v2`, default true), and
  group-level queuing via `concurrency_limit` is a **v1-only** feature
  (per the StarRocks query-queues doc). Verified live: three concurrent
  `SELECT SLEEP(5)` calls against a group with `concurrency_limit=2`
  all ran in ~5s regardless of
  `SET GLOBAL enable_group_level_query_queue` or
  `enable_query_queue_select`.
- What `concurrency_limit` DOES do here (verified live 2026-11-17): with
  `enable_group_level_query_queue=false` (the default), the
  third-plus concurrent query **fails** with
  `ERROR: Exceed concurrency limit: 2` — admission control, not
  queueing. And disabling v2 does NOT bring queueing back: with
  `enable_query_queue_v2=false` + `enable_group_level_query_queue=true`
  + `enable_query_queue_select=true`, `SHOW USAGE RESOURCE GROUPS`
  showed 3 concurrent queries running in a `concurrency_limit=2` group —
  no queue, no error, the limit unenforced everywhere. StarRocks PR
  #78425 (unmerged at the time of writing) confirms this from the
  maintainer side: v2 has no resource-group logic, and turning
  `enable_group_level_query_queue` on makes the BE skip its check while
  the FE never queues — the group runs unbounded. v2-off is strictly
  worse; it was reverted the same day.

What ships in this demo: weight isolation + classifier pinning, with
fail-fast admission control armed on `rg_superset_demo`
(`concurrency_limit=10`, re-asserted by the demo setup asset on every
run — far above a dashboard render's ~5 concurrent charts, so demos
never trip it; an 11th concurrent query fails fast instead of piling
up).

**Privilege gotcha:** db-level grants do not cover views on 4.1.7 —
`GRANT SELECT ON db.*` passes base tables only, and an explicit
`GRANT ... ON db.view` fails with "cannot find table". All
`superset_demo` datasets therefore query base tables directly.

## 7. Stream Load — the third ingestion path

`bin/3_starrocks_stream_load_demo.sh` POSTs a JSON batch straight to the
FE's `_stream_load` HTTP endpoint:

- **Gotcha, cost the first working version:** the FE requires the
  `Expect: 100-continue` handshake, and that handshake **hangs through
  Docker's host port-forward** (host → localhost:8030 GETs fine, but a
  POST with `-T` times out at 30s; without the Expect header the FE
  rejects with "There is no 100-continue header"). Running curl inside
  the docker network (`docker exec -i superset ... http://starrocks:8030`)
  succeeds instantly.
- The batch label rides in the payload (every row carries it) so the PK
  table's `label` column needs no expression quoting in the `columns`
  header; `loaded_at=now()` is computed in-flight.
- **Verified:** 4/4 rows loaded, `loaded_at` populated server-side, and
  the idempotency re-run returns `Label Already Exists`.

## 8. Lake scan data cache — cold vs warm

`bin/5_starrocks_lake_cache_benchmark.sh` drops the UC metadata cache
(`REFRESH EXTERNAL TABLE`), then runs the rewrite-OFF Iceberg query twice
via `EXPLAIN ANALYZE`. Reference run (2026-11-16): **732ms cold → 186ms
warm** on the same connection; the rewrite-ON MV path stays ~0.02s warm.
The multi-second colds from the rewrite demo appear when the UC metadata
fetch itself is slow — rerun the benchmark ~5 minutes after lake activity
to catch the full cold cost.

## Out of scope, and why

- **Vector search (ANN)** — needs an embedding producer this stack doesn't
  have (the ML leg is River online learning over scalars, not embeddings);
  a synthetic-vector demo would prove syntax, not value.
- **Native CDC ingestion** — the reverse-ETL demo hand-builds what StarRocks's
  CDC connectors automate, but the official path is new infrastructure
  (Flink/CDC connector + its runtime) duplicating a demo that already works.
  The delete-via-load demo above covers the tombstone half of the CDC story
  without new infra.
- ~~Tiered storage~~ — **not out of scope after all**: the cluster has run
  in shared-data mode on MinIO since 2026-09-08 (see
  `docs/SR_POC_STARROCKS_SHARED_DATA_MINIO.md`), which IS the tiered-storage
  architecture — every table's data files live in the MinIO `starrocks`
  bucket (cold tier) and the CN's local block cache serves hot reads (hot
  tier). There's no per-table hot/cold toggle in shared-data mode; the
  tiering is the cache. Demoed by `bin/5_starrocks_tiered_storage_demo.sh`
  (storage volume + physical MinIO files + `be_datacache_metrics` + warm
  EXPLAIN ANALYZE). The original out-of-scope note was written from a wrong
  "shared-nothing" premise.

## Bloom filter index — probed, negative result (2026-11-16)

`bin/5_starrocks_bloom_filter_benchmark.sh` builds twin DUPLICATE KEY tables
(identical data, one with `bloom_filter_columns = 'user_id'`) and times
point lookups on both. **Result: no meaningful warm-scan difference** —
~13ms with or without the filter at 10M narrow rows (no difference at 5M
either; the script's `SERIES` knob rebuilds bigger). The columnar scan is
already fast enough that per-page bloom skipping buys nothing on this data
shape (narrow rows, low distinct-values-per-page). StarRocks's bloom filter
pays off with wider rows, larger segments and much higher cardinality; the
PoC's real point-lookup case (`wallet_transactions`) is covered by the
PRIMARY KEY index instead — so no bloom filter is applied to any live
table. Kept as a re-runnable probe, honestly labeled.

## Assets and files

- `orchestration/assets/starrocks_feature_demo_setup.py` — all data pieces
  (funnel_window_stats, country_scores, wallet_type_totals_unique) + the
  user/group, idempotent (safe on every demo reset)
- `bin/3_starrocks_stream_load_demo.sh`, `bin/3_starrocks_delete_via_load_demo.sh`,
  `bin/5_starrocks_lake_cache_benchmark.sh`, `bin/5_starrocks_bloom_filter_benchmark.sh`
- Superset bundle: `databases/StarRocks_demo_user.yaml`,
  `datasets/StarRocks_demo_user/*.yaml` (16/17),
  `charts/*_24.yaml`, `*_25.yaml`,
  `dashboards/StarRocks_Serving_Features_4.yaml`
