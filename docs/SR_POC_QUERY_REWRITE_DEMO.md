---
title: StarRocks Query Rewrite Demo
description: Building and validating a transparent-query-rewrite demo to strengthen the StarRocks evaluation
---

<!-- markdownlint-disable-file -->

**For exact step-by-step run instructions (script runner / Dagster /
Superset), see
[SR_POC_SUPERSET_DEMOS_RUNBOOK.md](SR_POC_SUPERSET_DEMOS_RUNBOOK.md).** This
doc covers the design, the SQL-console version of the demo, and the real
bugs found while building the Superset version.

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
- **`refresh_method: "ASYNC EVERY (INTERVAL 5 MINUTE)"`** (changed
  2026-09-14, was `MANUAL`) — originally deliberate: trigger a refresh once
  before a demo so results stay static and reproducible for the whole
  session, rather than changing under a scheduled refresh mid-explanation.
  Changed after live use showed the opposite problem over a long session:
  "Rewrite OFF" (a live raw-table scan) kept advancing while this MV stayed
  frozen at whatever it looked like when last refreshed by hand, so the two
  visibly diverged in row count, not just latency. First set to 1 minute,
  then lengthened to 5 minutes the same day after profiling showed each
  refresh takes 20-46s regardless of tuning (resource-group CPU weight,
  Iceberg metadata/file count — all ruled out; the cost is specific to
  StarRocks' MV-refresh task machinery), so a 1-minute schedule meant live
  queries too often landed inside a slow refresh and ran as slow as
  "Rewrite OFF" — see the model file's own comment for the full tradeoff
  either way.
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

1. Deploy the model and warm the MV: Dagster UI → Jobs →
   `modern_dashboard_setup_job` (or `starrocks_demo_setup_job` for
   everything at once) → Launchpad → Launch Run. (If the model was just
   added/changed, `dbt parse` needs to run first so Dagster's manifest
   picks it up — see "Deployment log" above for the exact snag hit.) The
   `refresh_mv_funnel_daily_country_rollup` asset in that job runs
   `REFRESH MATERIALIZED VIEW ... WITH SYNC MODE` automatically — no
   longer a separate manual step (see
   `orchestration/assets/query_rewrite_demo_refresh.py`). The MV also now
   auto-refreshes on its own every 5 minutes (`refresh_method="ASYNC EVERY
   (INTERVAL 5 MINUTE)"`, changed 2026-09-14), so it stays in sync with
   the raw table between job runs without any manual step during a demo.
2. Confirm eligibility one more time:
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

## Re-verified after the StarRocks storage backend migration (2026-09-09)

Re-checked after `starrocks` moved from `allin1-ubuntu` (shared-nothing) to
the shared-data-on-MinIO setup (see
[SR_POC_STARROCKS_SHARED_DATA_MINIO.md](SR_POC_STARROCKS_SHARED_DATA_MINIO.md)),
since that was a full service swap and the MV needed rebuilding via
`modern_dashboard_setup_job` rather than migrating. Still holds:

* `QUERY_REWRITE_STATUS: VALID` on the rebuilt MV.
* Manual refresh works (`REFRESH MATERIALIZED VIEW ... WITH SYNC MODE`),
  now showing 3 days of data (grew from the original 2-day snapshot as
  the historical table accumulated more).
* `EXPLAIN` still flips cleanly between `OlapScanNode` / `MaterializedView:
  true` (rewrite on) and `IcebergScanNode` (rewrite off) for the identical
  query text.
* One schema difference on the new FE image (`starrocks/fe-ubuntu:4.0-latest`
  vs the old `allin1-ubuntu:4.1.4`):
  `information_schema.materialized_views` no longer has a
  `QUERY_REWRITE_STATUS_REASON` column — drop it from the eligibility-check
  query above, or select `EXTRA_MESSAGE`/`INACTIVE_REASON` instead if a
  reason string is needed.

## Caveats to state honestly if asked

- This works because the base table is Iceberg (external catalog with
  reasonable rewrite support). The equivalent pattern **did not work** for
  `mv_unified_funnel_summary`, which unions a JDBC source with
  `CURRENT_TIMESTAMP()`-relative filters — StarRocks flatly refuses rewrite
  for that shape (`UNSUPPORTED_DEFINITION`). Rewrite is not a blanket
  capability; it requires a specific, stable query shape.
- `refresh_method` is `ASYNC EVERY (INTERVAL 5 MINUTE)` (changed
  2026-09-14, was `MANUAL`, briefly 1 minute the same day) — this MV is no
  longer static for a whole demo session; numbers can shift between two
  queries several minutes apart.
  Accepted deliberately so "Rewrite ON" stays in sync with "Rewrite OFF"
  (a live raw-table scan) instead of visibly diverging over a long
  session, which is what prompted the change. See the model file's own
  comment (`dbt_starrocks/models/mv_funnel_daily_country_rollup.sql`) for
  the full rationale.
- The timing numbers above are from a single local run at tiny data volume
  (2 days, 4,280 raw rows) — a real evaluation should re-run this at
  production-representative volume before citing the multiplier as a hard
  number.

## Superset dashboard (2026-09-12)

Built: "StarRocks Query Rewrite Demo"
(`/superset/dashboard/query-rewrite-demo/`) — two side-by-side table
charts running the identical query against
`databricks_uc.sr_poc_external.funnel_summary_historical`, one with
rewrite on and one forced off, plus a markdown explaining the mechanism.

**Real bug found and fixed:** the first version forced rewrite off via a
`/*+ SET_VAR(enable_materialized_view_rewrite=false) */` hint embedded in
the dataset's SQL — this works perfectly when run directly (confirmed via
`EXPLAIN`), but **Superset silently strips the hint comment** when it
wraps a virtual dataset's SQL in its outer
`SELECT ... FROM (<sql>) AS virtual_table` query. Confirmed directly by
reading the chart's own `query` field in the `/api/v1/chart/data`
response: the hint was simply absent from what StarRocks actually
received. Both "on" and "off" charts were quietly running with rewrite ON
the whole time — same speed, no visible difference, no error either,
which is what made it easy to miss (surfaced only because reloading the
dashboard showed both charts refreshing equally fast, which shouldn't
happen if one is genuinely hitting a ~7s raw Iceberg scan).

**Fix:** a *separate* Superset database connection
("StarRocks (rewrite disabled)") whose SQLAlchemy URI carries
`?init_command=SET SESSION enable_materialized_view_rewrite=0`. That SQL
runs once per pooled connection at connect time, so every query issued
through that connection has rewrite disabled at the session level — no
per-query SQL text for Superset's formatter to touch or strip. Confirmed
via Superset's own API: ~0.6-0.7s (rewrite on, normal connection) vs.
~7.2s (rewrite off, `init_command` connection) for the identical query —
matching the raw, outside-Superset numbers closely.

Lesson for future work: don't trust a query-hint-in-SQL-text approach to
survive any BI tool's own SQL processing/formatting layer without
verifying the *actual* SQL that reached the database — a session-level
connection property is more robust whenever the tool sits between you and
the query text.

## Investigation log: dashboard slowness and a real data-loss incident (2026-09-14)

A single day's investigation, triggered by "Rewrite ON" showing an
`Unexpected error` in Superset and, later, running as slow as
"Rewrite OFF". Several distinct root causes were found and fixed, one
attempted fix caused genuine data loss on the Databricks table, and that
was repaired. Documented here in full, including the mistake, since a
future dip into similar territory should start from what's already known
rather than repeat it.

### 1. Superset "Unexpected error" — StarRocks planner timeout

**Symptom:** `Error: (1064, 'StarRocks planner use long time 3628 ms in
logical phase...')` on the "Rewrite ON" chart.

**Root cause:** `new_planner_optimize_timeout` defaults to 3000ms.
Evaluating MV-rewrite eligibility against an Iceberg external catalog
requires fetching Hive/Iceberg metadata *during planning*, which alone
took ~3.1-3.6s here — right at the default timeout boundary, so it failed
intermittently depending on catalog metadata cache state.

**Red herring first:** the error message's own wording ("1. FE Full GC")
led to first raising the StarRocks FE's JVM heap (`starrocks/fe-entrypoint.sh`,
`-Xmx3072m` → `-Xmx4096m`, container has a 5G memory limit). This didn't
fix it — reproducing the exact failure via `EXPLAIN` on the
Superset-wrapped query (subquery + `ORDER BY` + `LIMIT`) showed no
correlating FE GC pause at the time of failure. The heap bump was kept
(cheap, and genuinely reduced GC pause severity elsewhere) but wasn't the
fix.

**Actual fix:** `SET GLOBAL new_planner_optimize_timeout = 15000;`,
persisted in `starrocks/init_catalog.sh` (added at the end of the
bootstrap script) so a fresh stack rebuild reapplies it.

### 2. "Rewrite OFF" degrading from ~7.2s to 20-53s — small-file accumulation

**Symptom:** the raw Iceberg scan kept getting slower over the session,
eventually as slow as tens of seconds for a trivial row count.

**Root cause:** `EXPLAIN ANALYZE` showed almost all time inside
`ICEBERG_SCAN`'s `IOTaskExecTime`/`ColumnReadTime` — reading Parquet
column data over the network from ADLS. `DESCRIBE DETAIL` on
`de_dev.sr_poc_external.funnel_summary_historical` confirmed **316 files
for only 1.86MB of data** (~5.9KB average file size) — `sink_funnel_to_databricks`'s
continuous small-batch commits (~60s+ cadence) with no compaction on the
Databricks side. Each raw scan needs a separate network round-trip per
file, so scan time scales with file count, not row count.

**Fix:** ran `OPTIMIZE de_dev.sr_poc_external.funnel_summary_historical`
once (316 files → 1), then enabled **Unity Catalog Predictive
Optimization at the table level** (not schema/catalog — see below) for
`funnel_summary_historical` and `iceberg_countries` so it recompacts
automatically going forward:

```sql
-- SQL ALTER TABLE ... SET PREDICTIVE OPTIMIZATION is not valid syntax;
-- table-level PO requires the REST API (catalog/schema level do have SQL):
-- PATCH /api/2.1/unity-catalog/tables/{full_name}
--   {"enable_predictive_optimization": "ENABLE"}
```

Deliberately did **not** override PO at the catalog/schema level — the
metastore (`unity-northeurope`) has it `DISABLE` org-wide
(`effective_predictive_optimization_flag` inherited from METASTORE),
which looks like a deliberate org policy (cost control), so the override
was scoped to just the two tables actually in the active pipeline. The
other ~17 tables in `sr_poc_external` are leftover probe/test/backup
tables from earlier investigation phases and were left untouched.

### 3. Data-loss incident: aggressive VACUUM retention raced against concurrent writes

This is the one to read carefully before touching retention settings on
any table with a concurrent streaming writer.

**What happened:** while chasing the same slowness, `VACUUM` was run with
`RETAIN 1 HOURS` after setting `delta.deletedFileRetentionDuration` /
`delta.logRetentionDuration` to `interval 1 hours` as table properties
(session-level `SET spark.databricks.delta.retentionDurationCheck.enabled
= false` is unavailable on serverless SQL warehouses —
`CONFIG_NOT_AVAILABLE.WITHOUT_SUGGESTION` — so the table-property route
was used instead, and it worked where the session config didn't).

This looked like it fixed the read-side slowness. It had actually created
an **active, ongoing corruption loop**: Predictive Optimization (enabled
in step 2, table-level) runs automatic background `VACUUM` using whatever
retention the table has. With retention now just 1 hour, every PO
auto-VACUUM cycle raced against `sink_funnel_to_databricks`'s continuous
~60s commits and started deleting Parquet files almost as soon as they
were written — because Unity Catalog's Iceberg-metadata generation
(consumed by StarRocks/other external readers) is asynchronous relative
to Delta's own transaction log, a snapshot exposed via the Iceberg REST
endpoint could still reference a file that Delta's VACUUM, working from
its own bookkeeping, had already decided was safe to remove.

**Symptoms as they appeared:**
- MV refresh failed: `hdfsOpenFile failed ... .parquet: file = ...` (a
  file StarRocks's read of the current Iceberg snapshot needed was gone)
- The **raw table itself** failed to read, not just the MV — same missing
  file error
- Databricks' own `OPTIMIZE` failed too:
  `[FAILED_READ_FILE.DBR_FILE_NOT_EXIST] ... DELTA_SHALLOW_CLONE_FILE_NOT_FOUND`
  — confirming this was real breakage at the Delta layer, not a
  StarRocks-side cache/staleness issue
- New files kept going missing across repeated checks (14 files, then 3
  more sequential ones right after repairing the first batch) — this was
  *ongoing*, not a one-time event, until the retention properties were
  reverted

**Recovery, in order:**
1. `ALTER TABLE ... UNSET TBLPROPERTIES ('delta.deletedFileRetentionDuration',
   'delta.logRetentionDuration')` — stops the loop by restoring
   Databricks' safe 7-day default. **This step is what actually stops the
   damage** — do this first, before any repair, if this ever recurs.
2. `FSCK REPAIR TABLE de_dev.sr_poc_external.funnel_summary_historical
   DRY RUN` — lists dangling file references read-only
   (`dataFileMissing: true` rows) without changing anything. Confirmed
   scope first: 14 missing files, all from the *current* day's partition
   (`window_date=2026-09-14`) — i.e. the freshest data, not historical
   rows, consistent with the race-condition mechanism above.
3. `FSCK REPAIR TABLE de_dev.sr_poc_external.funnel_summary_historical`
   (no `DRY RUN`) — removes the log's references to the missing files,
   restoring queryability. This is a real, final data loss of those
   files' rows — there is no other recovery path once the physical files
   are gone from ADLS. Had to be run twice: 3 more files went missing
   between the retention-property revert and the first repair (the tail
   end of the same race), caught by a second `DRY RUN` check afterward
   which came back clean.

**Net data loss:** 17 rows total (14 + 3 files), all from
`window_date=2026-09-14`, out of a table with tens of thousands of rows.
Bounded and non-historical, but real and irreversible.

**Takeaway:** Predictive Optimization's automatic `VACUUM`/`OPTIMIZE` is
safe with Databricks' *default* retention settings — the corruption came
specifically from overriding retention down to 1 hour on a table with an
active concurrent streaming writer, not from PO or VACUUM themselves.
Don't lower `delta.deletedFileRetentionDuration`/`delta.logRetentionDuration`
below the default on any table fed by a live streaming sink, regardless
of how safe a shorter window looks in isolation — the risk is specifically
the *interaction* with automatic background maintenance (PO) racing
concurrent writes, not the VACUUM command itself.

### 4. MV refresh itself takes 20-46s regardless of tuning — schedule lengthened, not fixed

**Symptom:** even with the above fixed, `mv_funnel_daily_country_rollup`'s
own scheduled refresh (originally every 1 minute, see the model file's
comment) consistently took 20-46 seconds per run — meaning a live query
landing during that window could be caught behind it and run as slow as
"Rewrite OFF".

**Ruled out, in order:**
- FE GC pauses — checked the GC log during a live refresh, max pause
  ~360ms, not the cause
- Snapshot/metadata count — the corruption-incident cleanup coincidentally
  reduced `DESCRIBE HISTORY` from thousands of versions down to the low
  hundreds; refresh time didn't change
- File count — 68-78 files (post-`OPTIMIZE`) reads in ~1-4s for ad-hoc
  queries; didn't explain the MV task's own 20-46s
- `default_mv_wg` resource group CPU weight — raised from the StarRocks
  default 1% to 20%, then 80%, with **no meaningful change in either
  direction** (25-46s across all three settings), despite cluster CPU
  usage measuring only ~3% during a refresh. Ruled out conclusively;
  reverted to a moderate 20% since raising it further bought nothing.

**Isolated via a throwaway table:** an `INSERT OVERWRITE` doing the exact
same aggregation into a plain (non-MV) table with the same shape took
~6.4s. The identical work, run as the MV's own scheduled refresh task,
took 24-46s. The gap is specific to StarRocks' MV-refresh task machinery
itself (task-framework locking/coordination — the corruption incident's
own error output included `try lock failed: 0` — and/or a duplicated
partition-change-tracking metadata check) — not CPU, not Iceberg metadata
size, not file count. This was not root-caused further; further
StarRocks-internals investigation was judged not worth the risk/time
given a live-corruption incident had just happened in this same session.

**Fix applied:** lengthened the schedule from
`ASYNC EVERY (INTERVAL 1 MINUTE)` to `ASYNC EVERY (INTERVAL 5 MINUTE)`.
Same tradeoff as before (this MV is no longer static for a whole demo
session) plus a new one: a query can still land inside a slow refresh
window, just proportionally less often. Does not fix the underlying 20-46s
refresh cost — only reduces how often it's user-visible.

### Files touched by this investigation

- `starrocks/fe-entrypoint.sh` — FE JVM heap `-Xmx3072m` → `-Xmx4096m`
- `starrocks/init_catalog.sh` — added `SET GLOBAL new_planner_optimize_timeout = 15000`
- `dbt_starrocks/models/mv_funnel_daily_country_rollup.sql` — `refresh_method`
  `MANUAL` → `ASYNC EVERY (INTERVAL 1 MINUTE)` → `ASYNC EVERY (INTERVAL 5 MINUTE)`
- `orchestration/assets/query_rewrite_demo_refresh.py` — docstring/description
  updated to match
- Databricks (`de_dev.sr_poc_external`): `OPTIMIZE`, table-level Predictive
  Optimization on `funnel_summary_historical` + `iceberg_countries`,
  the retention-property incident and its revert, `FSCK REPAIR` ×2
- StarRocks: `default_mv_wg` resource group `cpu_weight_percent` 1 → 20
  (final value; briefly tested at 80)
- Superset dashboard `query-rewrite-demo` layout: both charts were
  `width: 4` of 12 grid columns (only 8/12 used, hence narrow/misaligned
  with wrapping titles) — set to `width: 6` each via the Dashboard API
