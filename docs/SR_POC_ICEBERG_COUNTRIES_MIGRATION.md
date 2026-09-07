---
title: Country Reference Table — Databricks Migration & Multi-Engine Write Support
description: Moving iceberg_countries to Databricks Unity Catalog, and what it took to get StarRocks + Trino + Databricks all reading/writing it correctly
---

<!-- markdownlint-disable-file -->

## Why this table moved

`iceberg_countries` (country code → name reference data used for dashboard
joins) originally lived in the local `lakekeeper_local` Iceberg catalog
(MinIO-backed). That meant it was wiped by every full stack teardown
(`bin/6_down.sh` → `docker compose down --volumes`) and had to be recreated —
a genuine ~3 minute one-time cost per teardown (see the cold-start
investigation in [SR_POC_UNIFIED_PLAN.md](SR_POC_UNIFIED_PLAN.md)), directly
visible if that recreation ever happened live in front of a demo audience.

Moved to Databricks Unity Catalog (`de_dev.sr_poc_external.iceberg_countries`)
on 2026-09-07 — same pattern as the existing `funnel_summary_historical`
table, which is external/persistent and survives every local teardown. This
eliminated the cold-start cost permanently, not just avoided it.

## Final format: Delta + UniForm (not Managed Iceberg)

**This table's actual, current format is Delta with UniForm enabled** — the
sections below on Managed Iceberg (v2/v3, StarRocks write limits, the
`OPTIMIZE`/`INSERT OVERWRITE` workarounds) describe the path taken to reach
this conclusion and are kept as a record of what was tried and why it wasn't
the final answer, not the current state. If you just need "how do I edit
this table right now," skip to
[How to actually edit this table](#how-to-actually-edit-this-table-current-state)
at the bottom.

**Why the switch**: Managed Iceberg tables require external engines (Trino,
even Databricks' own native writer once v3 is involved) to write deletion
vectors for any `UPDATE`/`DELETE`, which StarRocks cannot read at all —
breaking the dashboard immediately after any edit until a manual `OPTIMIZE`
compaction runs. Delta + UniForm instead keeps the table natively Delta
(full native `UPDATE`/`MERGE`/`DELETE`, no special handling, no deletion
vector exposure to external readers) while auto-generating Iceberg metadata
for external engines to read. The trade-off — external engines lose write
access entirely — costs nothing here, since the actual workflow is "edit via
the Databricks UI, read via StarRocks/the dashboard," never the other way
around.

```sql
CREATE TABLE de_dev.sr_poc_external.iceberg_countries (
  country STRING,
  country_name STRING
)
TBLPROPERTIES (
  'delta.columnMapping.mode' = 'name',
  'delta.enableDeletionVectors' = 'false',
  'delta.enableIcebergCompatV2' = 'true',
  'delta.universalFormat.enabledFormats' = 'iceberg'
);
```

Confirmed directly, in order:
1. StarRocks reads it fine via the existing `databricks_uc` catalog (same
   FQN, no code/catalog changes needed).
2. A plain native `UPDATE ... SET ... WHERE ...` (no `OPTIMIZE`, no
   `INSERT OVERWRITE`) worked with zero errors — the actual goal.
3. `DELETE` likewise worked cleanly.
4. **UniForm generates Iceberg metadata asynchronously** — a documented
   behavior, not a bug. Immediately after an edit, StarRocks briefly returned
   stale data; waiting ~8 seconds resolved it. This is a real, unavoidable
   delay (Databricks' own metadata generation), separate from the caching
   issue fixed below.
5. As expected, StarRocks can no longer *write* to this table at all:
   `INSERT` now fails with `Table ... is not a Managed Iceberg table` —
   confirming the read/write boundary is exactly as documented.

### Eliminating the manual `REFRESH EXTERNAL TABLE` step entirely

The scenario driving this: a modern-dashboard user should see an edit made
by someone else through Databricks on their *next* query, with nobody
running any StarRocks-side command in between. A manual
`REFRESH EXTERNAL TABLE` doesn't satisfy that — it requires a human (or the
app) to explicitly know to run it.

**Fix**: set `iceberg_meta_cache_ttl_sec = 0` on the `databricks_uc` catalog.
This tells StarRocks to fetch fresh Iceberg metadata on every query instead
of caching it — confirmed directly: after this property was set, a
Databricks `UPDATE` became visible on a **plain `SELECT`** (exactly what
`modern-dashboard/backend/api.py` issues — no app code changes needed) with
no `REFRESH EXTERNAL TABLE` call at all, just the same ~8-second UniForm
async-generation wait as before.

```sql
ALTER CATALOG databricks_uc SET ("iceberg_meta_cache_ttl_sec" = "0");
```

Applied live via the command above, and (initially, incompletely) made
"permanent" by adding it to
[dbt_starrocks/dbt_project.yml](../dbt_starrocks/dbt_project.yml)'s
`databricks_uc` `CREATE EXTERNAL CATALOG` statement.

**That was insufficient, and it bit us for real** — 2026-09-07, after several
StarRocks container recreations (during the JVM heap investigation below),
the property silently disappeared and the dashboard showed a stale country
name for several seconds after a live Databricks edit. Root cause:
[starrocks/init_catalog.sh](../starrocks/init_catalog.sh) (run by the
`starrocks-init` container, which runs on *every* stack start, before
Dagster/dbt ever runs) does `DROP CATALOG IF EXISTS databricks_uc;` followed
by an unconditional `CREATE EXTERNAL CATALOG databricks_uc` — no
`IF NOT EXISTS`. It unconditionally drops and recreates the catalog from
*its own* definition every time, which didn't have the property, completely
overwriting whatever the dbt hook had set on a previous run. The dbt hook's
`CREATE EXTERNAL CATALOG IF NOT EXISTS` never gets a chance to matter for
this catalog, because the shell script already recreated it moments earlier
on the same stack start.

**Real fix**: added `"iceberg_meta_cache_ttl_sec" = "0"` to the actual
`CREATE EXTERNAL CATALOG` statement inside
[starrocks/init_catalog.sh](../starrocks/init_catalog.sh) itself — the
authoritative source, not the dbt hook (left in place too, for consistency,
but it's not what actually runs first). Verified end-to-end after the fix:
a Databricks `UPDATE` with zero manual StarRocks-side commands became
visible on a plain `SELECT` after the usual ~8s UniForm async-generation
wait — no stale-for-longer behavior.

**Lesson**: this project has *two* places that create the same StarRocks
catalogs (the shell script for initial/every bring-up, the dbt on-run-start
hook for dbt-driven rebuilds) and they can silently drift out of sync. If
either mechanism's catalog definition changes again, check both files.

**Performance check, since this applies catalog-wide** (to
`funnel_summary_historical` too, not just `iceberg_countries`): confirmed no
regression. Live dashboard queries hit `dashboard_funnel_serving`
(live RisingWave + the already-refreshed `mv_unified_funnel_summary`), never
the raw historical table directly — so the only things paying the
no-cache cost are the tiny country table (negligible) and
`mv_unified_funnel_summary`'s own 5-minute background refresh cycle (a
periodic cost, not user-facing). Measured `dashboard_funnel_serving` query
latency before/after: ~0.6-0.8s, unchanged.

### Query latency investigation (2026-09-07): what's fixed, what isn't

A user report of multi-second dashboard queries ("Degraded" banner, 13.88s)
led to a longer investigation. Three genuinely separate issues were found
and are tracked separately here since they have different fixes and
different status:

**1. FE JVM heap ceiling too small for the MV refresh's memory spike —
fixed.** The `mv_unified_funnel_summary` background refresh (every ~5min,
pulls from `databricks_uc.sr_poc_external.funnel_summary_historical` over
the network) allocates up to ~210MB of FE heap per run
(`QueryFEAllocatedMemory=220553112` observed in `fe.audit.log`). Against
the `-Xmx2048m` ceiling set during an earlier GC investigation (see that
section's history in this file/git log), that single query was >10% of the
heap, and interactive queries landing during/after a refresh saw 600-900ms
instead of settling lower. Bumped to `-Xmx3072m` in
[starrocks/docker-entrypoint.sh](../starrocks/docker-entrypoint.sh). Also
fixed a latent bug in that same file: the `sed` that overrides `-Xmx` only
matched the image's literal default (`8192m`), so on every restart *after*
the first it silently no-op'd (the value already on disk was whatever we'd
set previously, not `8192m`) — changed to a regex so it's idempotent
regardless of the current value.

**2. Query planner hitting a hard timeout on JDBC-catalog queries —
fixed.** Queries touching the `risingwave` JDBC catalog (e.g.
`dashboard_funnel_serving`, which joins `public.funnel_summary`)
intermittently failed with:

```
StarRocksPlannerException: StarRocks planner use long time 4015 ms in
logical phase, This probably because 1. FE Full GC ... 2. Hive external
table fetch metadata took a long time, 3. The SQL is very complex.
```

`new_planner_optimize_timeout` defaults to 3000ms; the query then
auto-retries and succeeds, so the visible symptom was a consistent ~4.2s
tax on affected queries, not an actual failure. Root cause: StarRocks
auto-triggers ANALYZE jobs on connector (external/JDBC catalog) tables when
they're queried; those jobs spawn `stats-cache-refresher` background
threads that fail with `IllegalStateException` in
`StatisticsUtils.getTableByUUID` (see #3 below) — with enough of these
queued concurrently, the resulting lock contention pushed planning past the
3s ceiling. **Fix**: set
`connector_table_query_trigger_analyze_max_running_task_num = 0` (added to
[starrocks/docker-entrypoint.sh](../starrocks/docker-entrypoint.sh)) to stop
new analyze jobs from being queued at query time. Confirmed via repeated
`EXPLAIN ANALYZE` runs post-fix: the 4s timeout failures stopped
recurring.

**3. `stats-cache-refresher` `IllegalStateException` on every query
touching the JDBC catalog — NOT fixed, no known fix exists.** Even with #2
applied, every query against `dashboard_funnel_serving` still throws this
in `fe.log` (confirmed via burst pattern: 3-4 occurrences per query,
in-sync with query timing, not a periodic background job):

```
java.util.concurrent.CompletionException: java.lang.IllegalStateException
	at ...ConnectorColumnStatsCacheLoader.lambda$asyncLoadAll$1(...)
Caused by: java.lang.IllegalStateException
	at com.google.common.base.Preconditions.checkState(...)
	at com.starrocks.connector.statistics.StatisticsUtils.getTableByUUID(...)
```

The query still succeeds — the planner catches this and falls back to
default cost estimates — but `EXPLAIN ANALYZE` shows the real cost:
`TotalTime` consistently 500-900ms while `ExecutionTime` is only 15-70ms.
The missing ~450-650ms is spent in the planner's logical-optimization phase
on every single query, not execution. Tried and confirmed **not** the fix:
`enable_statistic_collect = false`, `statistic_use_meta_statistics = false`
(neither changed the exception rate or the timing).

Web research (2026-09-07) turned up a strong likely explanation but no
fix: [StarRocks issue #40293](https://github.com/StarRocks/starrocks/issues/40293)
documents that **JDBC catalogs have no metadata caching layer** and issue
repeated, synchronous, uncached round-trips during query planning (26
identical `INFORMATION_SCHEMA.PARTITIONS` queries observed in a single
`EXPLAIN` in that report). The issue is closed as stale with no fix or
workaround shipped. This is architecturally exactly our situation — the
`risingwave` catalog is JDBC (Postgres wire protocol), and every query
against a view that touches it pays this cost. Did not find a GitHub issue
matching the exact `getTableByUUID`/`ConnectorColumnStatsCacheLoader`
stack trace itself (closest hits, e.g.
[#64424](https://github.com/StarRocks/starrocks/issues/64424) and
[#65692](https://github.com/StarRocks/starrocks/issues/65692), are
different root causes — Iceberg partition evolution and a semantic-analysis
edge case, respectively — not JDBC catalogs).

**Current baseline, accepted as-is for now**: ~500-900ms per
`dashboard_funnel_serving` query, consistent with (not a regression from)
the ~0.6-0.8s already measured and accepted in the section above. If this
needs to come down further, the real fix is architectural, not
config: stop having hot dashboard queries touch the `risingwave` JDBC
catalog live, e.g. by having `dashboard_funnel_serving` read only from
already-materialized StarRocks-local data. That's a bigger change and
hasn't been requested or scoped.

**Follow-up, same day: the real dashboard query was worse than the
synthetic benchmark above, and got a real fix.** The actual
`/api/query/funnel` endpoint (`modern-dashboard/backend/api.py`) doesn't
just query `dashboard_funnel_serving` — it also does
`LEFT JOIN databricks_uc.sr_poc_external.iceberg_countries` to resolve
country names, live, on every request. That's a **second** external
catalog touched in the same query, and it pays the same per-catalog
planning tax as #3 above **again**, on top of the first: confirmed via
`EXPLAIN ANALYZE` on the real query (with the join, a date-range `WHERE`,
and `ORDER BY`) at `TotalTime: 2.286s` / `ExecutionTime: 197ms` — i.e. the
gap roughly doubled versus the single-catalog case.

**Fix applied**: mirrored `iceberg_countries` into a local StarRocks
materialized view,
[dbt_starrocks/models/mv_iceberg_countries_cache.sql](../dbt_starrocks/models/mv_iceberg_countries_cache.sql),
refreshed `ASYNC EVERY (INTERVAL 60 SECOND)` — StarRocks enforces a 60s
floor (`materialized_view_min_refresh_interval`); 15s, the first thing
tried, was rejected. `modern-dashboard/backend/api.py`'s two queries now
join against this local MV instead of the live external catalog, so the
hot query path touches only the `risingwave` JDBC catalog (the accepted
#3 cost above), not `databricks_uc` as well. Confirmed via
`EXPLAIN ANALYZE`: `TotalTime` dropped from 2.286s to 1.025s immediately
after creating the MV, and the live `/api/query/funnel` endpoint measured
585-728ms after a backend restart — back at the single-catalog baseline.

**Trade-off accepted**: a Databricks-side edit to `iceberg_countries`
(e.g. a country rename) now takes up to ~60s to reach the dashboard,
instead of being visible on the very next query. This reverses the
"instant on next query" behavior built earlier in this file for this
*specific* lookup only — the funnel numbers themselves (viewers/carters/
purchasers) are unaffected and still as fresh as before. Chosen because
`iceberg_countries` is tiny and essentially static outside of demos, so a
bounded ~60s lag was judged worth roughly halving every dashboard query's
latency.

**Second follow-up, same day: the remaining ~500-900ms was the `risingwave`
JDBC catalog itself — also fixed, same pattern.** Even after removing
`iceberg_countries` from the live query path, `EXPLAIN ANALYZE` on the real
dashboard query still showed the same shape (`TotalTime` ~860-934ms,
`ExecutionTime` ~75ms) — because `dashboard_funnel_serving`'s "hot" branch
was still doing a live `SELECT` against `risingwave.public.funnel_summary`
through the JDBC catalog on every request, paying the same #3-style
planning tax. Applied the identical fix: mirrored the live 5-minute
RisingWave window into a new local MV,
[dbt_starrocks/models/mv_hot_funnel_cache.sql](../dbt_starrocks/models/mv_hot_funnel_cache.sql)
(`REFRESH ASYNC EVERY (INTERVAL 10 SECOND)`), and repointed
`dashboard_funnel_serving`'s hot branch at it instead of the live JDBC
source. Result: `dashboard_funnel_serving` now touches **zero** external
catalogs at query time — both branches are local StarRocks-native objects.
Measured via the real `/api/query/funnel` endpoint: dropped from
585-728ms to **26-71ms** in a settled state. Same trade-off as the country
name fix: viewer/carter/purchaser counts now lag live RisingWave by up to
~10s instead of being instantaneous.

**Third follow-up, same day: a separate, worse problem surfaced once the
above was in place — `mv_unified_funnel_summary`'s own 5-minute refresh had
degraded into a near-continuous, overlapping background job.** Even with
zero external catalogs in the interactive query path, queries were still
occasionally hitting 700ms-2.9s. Root cause, confirmed from
`fe.audit.log` timestamps: refresh runs that used to take 13-37s had grown
to 54-117s, and successive runs were **starting before the previous one
finished** (one started at 09:39:01 while the prior run, started 09:37:47,
didn't finish until 09:39:44) — a runaway, always-running background job
competing with every foreground query on the same FE, and very plausibly
also feeding the `stats-cache-refresher` noise from finding #3 (it touches
`databricks_uc` every cycle). **Mitigation applied**: 
`ALTER MATERIALIZED VIEW mv_unified_funnel_summary INACTIVE;` — pauses the
refresh entirely; the historical data freezes at its last successful state
but stays fully readable (`SELECT` still works normally). Applied live,
**not yet persisted to any startup script** — this was a demo-day
emergency measure, deliberately manual so it doesn't silently become
permanent. **To resume normal operation after the demo**:
```sql
ALTER MATERIALIZED VIEW sr_local_db_sr_local_db.mv_unified_funnel_summary ACTIVE;
REFRESH MATERIALIZED VIEW sr_local_db_sr_local_db.mv_unified_funnel_summary;
```
The underlying question of *why* refresh duration grew 3-4x was not
investigated (declined in favor of pausing immediately, given demo timing
pressure) — worth revisiting if this recurs.

**Net result after all three follow-ups**: `/api/query/funnel` is ~85-90%
in the 87-350ms range, with an occasional (~10-15%) 700ms-1.2s outlier
tied to the still-firing (now much less frequent) `stats-cache-refresher`
background noise from the two 10s-refresh cache MVs. Not a 100% guarantee
of sub-1s, but a large, real improvement from the original 2.1-13.9s.
Lengthening the cache MVs' refresh interval further would reduce outlier
frequency at the cost of freshness lag — evaluated and deliberately not
applied, to avoid over-tuning immediately before a demo.

**Fourth follow-up, same day: replaced the hot-data poll-based MV with a
push-based Kafka Routine Load, the architecturally "correct" fix.**
`mv_hot_funnel_cache` (the 10s-refresh MV from the second follow-up) worked,
but was still a *pull* mechanism -- polling RisingWave via JDBC every 10s.
RisingWave already sinks `funnel_summary` to a Kafka topic
(`dbt/models/sink_funnel_to_kafka.sql`, topic `funnel`, `force_append_only`)
for the existing SSE dashboard consumer. StarRocks can consume that same
topic directly and continuously via **Routine Load** -- no JDBC catalog,
no polling, push-based.

One important discovery while building this: despite
`sink_funnel_to_kafka.sql`'s comment claiming "EMIT ON WINDOW CLOSE
finalises rows once ... downstream sinks see immutable rows," sampling the
actual topic (`rpk topic consume funnel`) showed the *same*
`(window_start, country)` key emitted multiple times with *increasing*
counts (e.g. `viewers` 1 -> 3 for the same window) as the window filled in
-- the comment is inaccurate for this stream. This means the target table
**must** be a StarRocks Primary Key table (keyed on
`window_start, country`), not a plain append/duplicate table -- a Primary
Key table naturally keeps only the latest revision per key regardless of
how many times a window is re-emitted; an append table would accumulate
every stale revision and require a window function to pick the latest at
query time.

**What was built**: `sr_local_db_sr_local_db.hot_funnel_kafka`, a native
`PRIMARY KEY (window_start, country)` table, populated by
`CREATE ROUTINE LOAD ... hot_funnel_kafka_load` consuming the `funnel`
topic directly (JSON format, field names match column names 1:1, no
`jsonpaths` needed). Both created idempotently in
[starrocks/init_catalog.sh](../starrocks/init_catalog.sh) -- unlike the
catalog `DROP CATALOG IF EXISTS` + unconditional `CREATE` pattern earlier
in that same script, this uses `CREATE TABLE IF NOT EXISTS` and a
`SHOW ROUTINE LOAD` existence check before creating the job, since this
table holds live streaming data that must survive every stack restart, not
just be idempotently recreatable from empty. `dashboard_funnel_serving`'s
hot branch now reads from this table instead of `mv_hot_funnel_cache`
(dropped, along with its dbt model file). Verified via the real
`/api/query/funnel` endpoint: 15 consecutive runs, 28-75ms, zero outliers
-- the tightest, most consistent result of the whole investigation, and
with no periodic background refresh at all for this path (Routine Load is
a continuously-running consumer, not a scheduled job).

**Fifth follow-up, same day: switched `mv_unified_funnel_summary` from
paused to `REFRESH MANUAL` permanently.** The pause above was a stopgap.
The real requirement, clarified after discussion: historical data isn't
static forever (`dbt/models/sink_funnel_to_databricks.sql` continuously
appends newly-finalized RisingWave windows into
`funnel_summary_historical` whenever the producer runs) but it also
doesn't need *scheduled* refreshing -- the dashboard's hot path no longer
depends on this MV at all (`hot_funnel_kafka` via Routine Load covers
that), so this MV only needs to run once per stack startup (already
handled by the existing `starrocks_mv_warm` Dagster asset,
`orchestration/assets/starrocks_mv_warm.py`) plus on demand when
specifically demonstrating the live hot-to-cold aging transition.
`REFRESH MANUAL` gets exactly that: no scheduled background job, ever,
while `starrocks_mv_warm` (or a manual
`REFRESH MATERIALIZED VIEW ... WITH SYNC MODE`) still works identically on
demand.

**Pitfall hit while applying this**: `ALTER MATERIALIZED VIEW ... ACTIVE`
(to reactivate the MV that had been paused via `INACTIVE`) hung
indefinitely -- StarRocks's own connection watchdog logged repeated
`kill timeout query` attempts against it every second for 6+ minutes
without actually killing it, and `KILL QUERY <id>` had no effect either.
The MV's metadata was never actually changed by the stuck command (still
showed `IS_ACTIVE=false` throughout), so no corruption resulted, but the
container needed a full restart to clear the hung connection. **Avoid
`ALTER MATERIALIZED VIEW ... ACTIVE` on this StarRocks version (4.1.4) if
possible** -- a plain `DROP MATERIALIZED VIEW IF EXISTS` +
`CREATE MATERIALIZED VIEW ... REFRESH MANUAL` (same approach used
throughout this doc for the other MVs, via direct `mysql` client rather
than the dbt-starrocks adapter, which separately hits its own
"MySQL Connection not available" error on drop+create) worked cleanly and
is the safer path for any future refresh-scheme change on this MV.

**Sixth follow-up, next day: Kafka Routine Load's hard 5s floor wasn't
actually real-time, and got replaced with a native RisingWave sink.**
After starting the producer to validate the Kafka Routine Load
architecture from the fourth follow-up, direct observation
(`SHOW ROUTINE LOAD`, polling `hot_funnel_kafka` every few seconds) showed
the current window's values only updating roughly every 10-13s, matching
`maxBatchIntervalS` (Routine Load's Kafka-consumption batching interval).
Tried lowering it: **StarRocks enforces a hard minimum of 5 seconds**
(`max_batch_interval should >= 5` -- confirmed via a rejected `ALTER
ROUTINE LOAD ... PROPERTIES ("max_batch_interval" = "1")`); 5s was applied
instead (also requires pausing the job first -- `ALTER` on a running job
fails with "Only supports modification of PAUSED jobs").

5s still wasn't real-time enough. Investigated the actual latency floor:
RisingWave's own checkpoint cadence in this project was
`barrier_interval_ms = 2000` with `checkpoint_frequency = 2` (checkpoint
every 2 barriers) = **~4 second** cadence -- meaning RisingWave itself
was already close to Routine Load's 5s floor, so switching ingestion
mechanisms alone wouldn't help much. The real fix: RisingWave has a native
StarRocks sink connector (`connector = 'starrocks'`) that uses **Stream
Load** under the hood, not Kafka-consumption batching -- no 5s floor at
all. Its own flush cadence is governed by `commit_checkpoint_interval`
(measured in RisingWave checkpoints, default 10), so tightening RisingWave's
checkpoint interval directly translates into dashboard freshness, with no
StarRocks-side floor in the way.

**What was built**:
- `dbt/models/sink_funnel_to_starrocks.sql` -- a new sink, `type = 'upsert'`
  with `primary_key = 'window_start,country'`, targeting the same
  `hot_funnel_kafka` table, `commit_checkpoint_interval = 1` (flush every
  checkpoint). Required explicit casts in the sink's `SELECT` that weren't
  needed for the Kafka JSON sink: `funnel_summary`'s `window_start`/
  `window_end` are `TIMESTAMP WITH TIMEZONE` (StarRocks sink rejects that --
  "doesn't store time values with timezone information"), `viewers`/
  `carters`/`purchasers` are `BIGINT` vs the table's `INT` columns, and
  `view_to_cart_rate`/`cart_to_buy_rate` are `DECIMAL` vs the table's
  `DOUBLE` columns -- StarRocks sink validates types strictly at
  `CREATE SINK` time (each mismatch was a separate, sequential error to
  find and fix).
- `risingwave.toml`: `barrier_interval_ms` 2000 -> 500,
  `checkpoint_frequency` 2 -> 1 (~4s -> ~500ms cadence). This is a
  system-wide setting -- affects checkpoint overhead for every MV/sink in
  the pipeline, not just this one. Applied by restarting
  meta/compute/frontend-node/both compactors (this file is bind-mounted,
  not hot-reloaded), which briefly interrupted the already-running
  producer's stream (it reconnected on its own).
- Stopped (not dropped) the `hot_funnel_kafka_load` Routine Load job
  (`STOP ROUTINE LOAD`) -- running both the old and new ingestion paths
  into the same Primary Key table simultaneously would double-write it
  from two independent sources. `starrocks/init_catalog.sh` no longer
  recreates this job; the table keeps its name (still says "kafka") for
  history, documented in both that script and
  `dbt_starrocks/models/dashboard_funnel_serving.sql`.

**Verified**: polled RisingWave's `funnel_summary` and StarRocks's
`hot_funnel_kafka` at the same instant -- identical values (`56/14/2` at
the moment checked), confirming the two are now essentially in sync, not
just "eventually consistent within N seconds." The apparent ~3-8s gaps
observed between value changes turned out to be **the producer's own
event-generation rate**, not sink or checkpoint latency -- confirmed by
polling `funnel_summary` directly (bypassing StarRocks, the sink, and
Kafka entirely) and seeing the exact same gap pattern. The StarRocks-side
bottleneck this whole investigation was chasing is now fully eliminated;
remaining "freshness" is bounded by upstream data arrival, not by
anything in this pipeline.

**Seventh follow-up, same day: reverted the entire hot-data chain back to
plain JDBC.** The native sink's `commit_checkpoint_interval=1` fix from the
sixth follow-up above created a new problem under sustained live producer
traffic: `hot_funnel_kafka`'s tablet version count climbed continuously
(267 -> 272 -> 286 over a few minutes) faster than background compaction
absorbed it, confirmed via `EXPLAIN ANALYZE` to be adding real read-side
overhead (`OLAP_SCAN` cost on that table rose from a ~1-5ms baseline to
20-30ms+), and dashboard query latency rose into the 200ms-5.6s range --
worse than the plain-JDBC baseline this whole chain was trying to beat.
Raised `commit_checkpoint_interval` to 4 (~2s cadence) as a mitigation, but
before that was even validated under load, the decision was made to step
back entirely rather than keep tuning a fourth variant of the same problem
class (this hot-data path had, by this point, gone through: live JDBC ->
10s-refresh MV -> Kafka Routine Load -> native Stream Load sink, each
fixing the previous approach's failure mode while introducing a new one).

**Reverted, in full**:
- `dbt_starrocks/models/dashboard_funnel_serving.sql` -- hot branch back to
  a live `SELECT` against `{{ source('risingwave', 'funnel_summary') }}`,
  its original, very first form.
- Dropped `sink_funnel_to_starrocks` (RisingWave sink) and its dbt model
  file.
- Dropped `hot_funnel_kafka` (StarRocks table) entirely -- its Routine Load
  job had already been stopped in the sixth follow-up.
- `risingwave.toml`: `barrier_interval_ms`/`checkpoint_frequency` restored
  to the original `2000`/`2`.
- `starrocks/init_catalog.sh`: removed the whole `hot_funnel_kafka`
  table-creation block, replaced with a note pointing here so a future
  session doesn't re-attempt the same chain without reading this history
  first.

**What's kept from this whole investigation**: the JVM heap fix
(`3072m`), the analyze-concurrency fix
(`connector_table_query_trigger_analyze_max_running_task_num=0`), and
`mv_iceberg_countries_cache` (the country-name cache, unrelated to this
hot-data path, refreshing cleanly every 60s) all remain in place and
unaffected by this reversion.

**Verified after reverting**: `dashboard_funnel_serving` returns 240 rows
including the live current-minute window, `degraded: false`, and 40-run
timing settled to the ~530-750ms band (occasional 1.3-2.8s outliers) --
matching the originally-documented finding #3 baseline exactly. This is
the accepted, final state: a known, bounded, well-understood cost, chosen
deliberately over a longer chain of increasingly complex caching layers
that each solved one problem while creating another.

**Eighth follow-up, same day: confirmed the exact cause of the remaining
outliers, and confirmed no config exists to eliminate it.** After
reverting to plain JDBC, the ~530-750ms baseline still had occasional
900ms-2.8s outliers. Correlated them precisely against
`stats-cache-refresher` exception timestamps in `fe.log`:

```
11:15:28.863  query took 1071ms   <-  stats-cache-refresher fired 11:15:28.470-28.587
11:15:42.084  query took  915ms   <-  stats-cache-refresher fired 11:15:41.924-42.211
11:15:42.239  query took 1068ms   <-  (same burst)
11:15:52.390  query took  931ms   <-  stats-cache-refresher fired 11:15:52.101-52.111
```

Every observed outlier lined up with a burst of this exception (recurring
roughly every 10-15s, independent of anything in this project). This is
finding #3 from earlier in this doc, definitively confirmed as the sole
cause of 100% of the outliers sampled -- not a new or different issue.

Searched specifically for a way to disable it (distinct from the
collection-scheduling configs already tried): `SHOW VARIABLES LIKE
'%stat%'` revealed StarRocks has dedicated per-connector-type toggles --
`enable_iceberg_column_statistics`, `enable_delta_lake_column_statistics`,
`enable_hive_column_stats`, `enable_paimon_column_statistics` -- but
**no equivalent for JDBC**. It's simply absent from the variable list.
Combined with everything else already ruled out (`enable_statistic_collect`,
`statistic_use_meta_statistics`,
`connector_table_query_trigger_analyze_max_running_task_num`,
`iceberg_meta_cache_ttl_sec`), there is no known way to disable this
behavior for JDBC-catalog tables in this StarRocks version (4.1.4). This is
now treated as a confirmed, permanent characteristic of the accepted
baseline, not an open question.

**Ninth follow-up: decided against shortening `mv_iceberg_countries_cache`'s
refresh further.** The request was for faster country-rename visibility
for one specific demo moment, not as a general default -- so lowering the
scheduled interval again (trading general-case collision risk for
always-on freshness) was the wrong tool. Instead, the recommended approach
is an on-demand manual trigger at the exact moment of the edit:

```sql
REFRESH MATERIALIZED VIEW sr_local_db_sr_local_db.mv_iceberg_countries_cache WITH SYNC MODE;
```

Run this right after saving a Databricks edit, wait ~8s for
Databricks/UniForm's own metadata-generation delay, and the next dashboard
query shows the new value -- zero collision risk added, since it's a single
deliberate trigger rather than a recurring schedule. The 60s scheduled
refresh stays as the general-case default. Not yet wired into a Dagster
asset (offered, declined for now, same pattern as `starrocks_mv_warm` for
the historical-data MV if wanted later).

**Unrelated false lead, for the record**: during this investigation one
test run stalled for 694 seconds with zero trace in `fe.audit.log`. Cause
was external to StarRocks — the host laptop was switched away from on a
KVM switch and macOS suspended it, freezing the in-flight `docker exec`
client process until it woke back up. Not a StarRocks issue; flagged here
only so it isn't mistaken for one if seen again in a log review.

## Earlier approach considered: Managed Iceberg (superseded)

Originally created via Databricks SQL directly (not via StarRocks
`CREATE TABLE`, to avoid the same external-write constraints already
documented for the RisingWave sink in this doc's Part 1), matching
`funnel_summary_historical`'s exact format:

```sql
CREATE TABLE de_dev.sr_poc_external.iceberg_countries (
  country STRING,
  country_name STRING
) USING ICEBERG;

ALTER TABLE de_dev.sr_poc_external.iceberg_countries DROP FEATURE catalogManaged;
```

`DESCRIBE DETAIL` confirmed this produced identical `format`,
`minReaderVersion`/`minWriterVersion`, compression, and `tableFeatures` to
`funnel_summary_historical` — dropping `catalogManaged` was necessary to
match (a fresh `USING ICEBERG` table has it by default; the existing table
doesn't).

Loaded 20 rows from `data/countries.csv` via a plain `INSERT` (Databricks
SQL). Verified readable through StarRocks' existing `databricks_uc` catalog
immediately — no new catalog needed. This was the format in place for the
StarRocks-write-capability, Trino-credential, and v2-vs-v3 investigations
documented below, before the switch to Delta + UniForm above.

**Code changes:**
- `orchestration/assets/iceberg_countries.py` — back to pure validation (no
  more create/load-via-StarRocks logic), pointed at
  `databricks_uc.sr_poc_external.iceberg_countries`.
- `orchestration/assets/modern_dashboard_preflight.py` — `country_reference`
  check updated to the same location.
- `modern-dashboard/backend/api.py` — both dashboard query `LEFT JOIN`s
  updated to the same location.
- `scripts/load_countries_to_iceberg_trino.py` is now unused (superseded);
  not deleted yet.

## StarRocks write capability: what actually works

Tested directly against the live table rather than assumed:

| Operation | Result |
|---|---|
| `INSERT` | ✅ Works |
| `DELETE` (format-version 2) | ❌ `Iceberg delete files are not supported with format version < 3` |
| `DELETE` (format-version 3) | ❌ `Must use DVs for position deletes in V3` — StarRocks 4.1.4 doesn't write Databricks-compatible deletion-vector deletes |
| `UPDATE` (any version) | ❌ `table does not support update` — confirmed this is universal, not Databricks-specific: reproduced the identical error against a plain native Iceberg table (Lakekeeper-backed), and no StarRocks release notes or docs mention `UPDATE` support for external Iceberg catalogs at any version |

The relevant StarRocks GitHub issue
([#63819, "Support Iceberg v3 Capabilities"](https://github.com/StarRocks/starrocks/issues/63819))
is closed as stale with no confirmed implementation — no known StarRocks
version fixes the v3 deletion-vector write gap yet.

## Trino: works, but needed a credential fix

Trino's Iceberg connector fully supports `UPDATE`/`DELETE`/`MERGE` — confirmed
directly against a native Iceberg table before touching Databricks. Its
`databricks` catalog originally failed with an ADLS signature mismatch
(`Signature did not match`, HTTP 403) on any write attempt.

**Root cause, found by comparing configs rather than assuming:**
StarRocks' `databricks_uc` catalog
([dbt_starrocks/dbt_project.yml](../dbt_starrocks/dbt_project.yml)) has *no*
separate storage credentials at all — it relies entirely on Unity Catalog's
**credential vending** (Databricks automatically hands back short-lived
scoped ADLS credentials alongside REST catalog responses). Trino's
`databricks.properties` explicitly disables this
(`fs.native-azure.enabled` was never it — the real issue: Trino's Azure
filesystem support doesn't consume vended SAS tokens at all, a confirmed,
still-only-partially-resolved upstream limitation —
[trinodb/trino#23238](https://github.com/trinodb/trino/issues/23238), closed
with a workaround, full fix still in progress as
[trinodb/trino#26921](https://github.com/trinodb/trino/pull/26921) at time of
writing). The original setup worked around this with a **separate,
dedicated service principal** (`ADLS_CLIENT_ID`/`ADLS_TENANT_ID`/`ADLS_CLIENT_SECRET`)
for direct OAuth-based ADLS access.

That separate service principal's secret was confirmed **not expired** (its
own OAuth token acquisition against Azure AD succeeded fine) — the 403 was
happening at the storage **authorization** layer, most likely a lost/never-granted
RBAC role (e.g. Storage Blob Data Contributor) on the current storage
account, plausibly tied to the account-migration incident already noted
elsewhere in this doc ("Corrected the Devbox account value and removed stale
hardcoded account values...").

**Fix applied:** swapped Trino to use the *same* `DATABRICKS_AZURE_CLIENT_ID`
/ `DATABRICKS_AZURE_TENANT_ID` / `DATABRICKS_AZURE_CLIENT_SECRET` identity
StarRocks uses (not via vending — Trino still can't consume that — just as a
plain directly-configured OAuth identity, same mechanism, different
credential):

- [trino/catalog/databricks.properties](../trino/catalog/databricks.properties) —
  `azure.oauth.*` now references `DATABRICKS_AZURE_*` env vars instead of
  `ADLS_*`.
- [docker-compose.yml](../docker-compose.yml) — added the previously-missing
  `DATABRICKS_AZURE_TENANT_ID` env var to the `trino` service (it already had
  `DATABRICKS_AZURE_CLIENT_ID`/`SECRET` for the REST catalog OAuth, but not
  the tenant ID needed for direct ADLS OAuth).

Confirmed working after `docker compose up -d trino`: `INSERT`, `UPDATE`, and
`DELETE` all succeeded through Trino against the live Databricks table.

The separate `ADLS_*` service principal and its broken storage RBAC are no
longer load-bearing for anything (Trino no longer uses them) but haven't
been cleaned up or fixed at the Azure/RBAC level — low priority, flagged
here for whoever eventually revisits it.

## The v2 vs v3 tradeoff

**Format-version 3 is required for delete/update support — and this was
proven, not assumed.** Downgrading back to v2 was tested directly:

- Attempted `ALTER TABLE ... SET TBLPROPERTIES ('format-version' = '2')` on
  the v3 table — failed outright
  (`IcebergWriterCompatV1 is incompatible with feature rowTracking`); an
  in-place downgrade isn't possible once v3 features are enabled.
- Recreated the table cleanly at v2 (matching `funnel_summary_historical`
  exactly again) and re-tested Trino `DELETE` — **it failed too**, with the
  identical `format version < 3` error StarRocks originally hit. This
  confirms the rejection happens at **Databricks' Unity Catalog IRC endpoint
  itself** (server-side), not in a StarRocks or Trino client — no external
  engine can write deletes to this table below v3, period.

So the actual choice is:

| | v2 | v3 |
|---|---|---|
| `INSERT` (any engine) | ✅ | ✅ |
| `UPDATE`/`DELETE` (any engine) | ❌ nobody can | ✅ via Trino only |
| StarRocks reads always work | ✅ | ⚠️ only *until* a Trino edit — see below |
| Matches `funnel_summary_historical` format | ✅ | ❌ |

v2 doesn't recover anything by reverting — it just removes the capability
entirely, with no compensating benefit. **Staying at v3.**

## Critical operational rule: `OPTIMIZE` after every Trino edit

Discovered live: a Trino `DELETE` at v3 writes a proper Iceberg deletion
vector (`.puffin` file). **StarRocks cannot read Iceberg V3 deletion
vectors at all** — immediately after the Trino `DELETE`, StarRocks queries
against this table failed outright
(`Iceberg V3 Deletion Vectors are not supported`), even though Databricks SQL
still read the table fine.

**Fix, and now a required step**: run

```sql
OPTIMIZE de_dev.sr_poc_external.iceberg_countries;
```

via Databricks SQL after any Trino `UPDATE`/`DELETE` on this table. This
compacts away the deletion vectors (`OPTIMIZE`'s own output reported
`numDeletionVectorsRemoved`) and restores StarRocks readability. Confirmed
via `REFRESH EXTERNAL TABLE` + `SELECT COUNT(*)` through StarRocks
immediately after.

**Do not treat this as optional cleanup.** Without it, the table looks like
it's suffered a StarRocks outage the next time anyone edits a country name
through Trino, with no obvious cause unless you already know this.

## A cleaner alternative: `INSERT OVERWRITE` instead of `UPDATE`/`DELETE`

Confirmed 2026-09-07: `UPDATE`/`DELETE` write deletion vectors because they
surgically mark specific rows inside *existing* data files. `INSERT
OVERWRITE` instead writes an entirely new data file and drops the old one
from the snapshot — no delete markers involved at all. Tested directly:

```sql
INSERT OVERWRITE de_dev.sr_poc_external.iceberg_countries VALUES
('US','United States'), ('CA','Canada'), /* ...all 20 rows, one changed... */
('GR','Hellas'), ...;
```

Result: `DESCRIBE DETAIL` showed `numFiles: 1` (single clean file), StarRocks
read it correctly immediately (after a `REFRESH EXTERNAL TABLE`, see below)
with **no `OPTIMIZE` step needed** and no deletion-vector error at any point.

This is the better approach for this table specifically, since it's small
and fully rewritten anyway on every edit (20 rows, no real cost to
rewriting all of them). It also works from **any** engine that can write
Iceberg data files as a fresh snapshot — including the Databricks UI's SQL
editor directly, StarRocks, or Trino — not just Trino like `UPDATE`/`DELETE`
required.

**Trade-off**: you must supply the full row set each time (not just the
changed value), which only makes sense for a small, fully-rewritable table
like this one — not a general substitute for `UPDATE`/`DELETE` on larger
tables.

## How to actually edit this table (current state)

The table is Delta + UniForm now (see above) — this is the real,
current procedure:

1. **Edit normally in Databricks** (UI SQL editor, or any Databricks SQL
   client) — plain `UPDATE`, `DELETE`, `INSERT`, or `MERGE`, exactly as
   you'd write it for any ordinary Delta table. No special syntax, no
   `OPTIMIZE`, no `INSERT OVERWRITE` workaround needed.
2. **Wait for it to propagate** — this now happens in two stages, not one:
   (a) UniForm's Iceberg metadata generation is asynchronous (~8s) before
   `databricks_uc` (still set to `iceberg_meta_cache_ttl_sec = 0`) can see
   it at all; (b) the dashboard no longer reads `databricks_uc` live for
   this — it reads `mv_iceberg_countries_cache`, a local StarRocks cache
   that refreshes on a 60s schedule (see the "Query latency investigation"
   section above for why: a live per-query catalog read cost 500ms-1.5s+ of
   planning time on every single dashboard query, twice over once combined
   with the JDBC catalog cost — moving it to a periodically-refreshed local
   cache fixed that at the price of this propagation delay). **Worst case:
   up to ~68s** (8s UniForm + up to 60s cache tick). For a specific demo
   moment where that's too slow, force an immediate refresh instead of
   waiting for the schedule:
   ```sql
   REFRESH MATERIALIZED VIEW sr_local_db_sr_local_db.mv_iceberg_countries_cache WITH SYNC MODE;
   ```
   Run it right after the Databricks edit, wait ~8s for UniForm, then query
   — no need to wait for the 60s tick.
3. **StarRocks/the dashboard can only read this table now, not write to it**
   — `INSERT` via StarRocks now fails (`Table ... is not a Managed Iceberg
   table`), by design. If a use case ever needs StarRocks or Trino to write
   to this table again, converting back to Managed Iceberg (see the
   superseded sections above) would be the path, with all the same
   deletion-vector caveats documented there.

### Superseded guidance (Managed Iceberg era — kept for reference only)

The `INSERT OVERWRITE` approach, the Trino `UPDATE`/`DELETE` +
mandatory-`OPTIMIZE` procedure, and the v2-vs-v3 tradeoff below all applied
to the *Managed Iceberg* version of this table and are no longer the
operating procedure. They're kept in this doc because the underlying
findings (StarRocks' write limitations, the Trino credential fix, why v2
blocks deletes at the Databricks server level) remain accurate and may be
relevant again if this or another table ever needs Managed Iceberg's
full-external-read-write capability instead of UniForm's Databricks-native/
external-read-only split.
