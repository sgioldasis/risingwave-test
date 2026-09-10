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

**Tenth follow-up: partitioned `funnel_summary_historical` by date, with a
full migration of existing data.** Raised in discussion after the live
Databricks-read test above (finding that a full unfiltered scan cost
1.2-3.0s of real `ScanTime`): at real scale (billions of rows), a live
per-query scan only stays viable if the query can be pruned down to a
handful of partitions/files. The table had no partitioning at all.

**Gotcha hit immediately**: `CREATE TABLE ... USING ICEBERG PARTITIONED BY
(days(window_start))` -- the standard Iceberg hidden-partitioning
transform syntax -- was rejected by Databricks: `BAD_REQUEST
[DELTA_OPERATION_NOT_ALLOWED] Operation not allowed: Partitioning by
expressions is not supported for Delta tables`. Managed-Iceberg CREATE
TABLE in Databricks only accepts `PARTITIONED BY` on a **plain column
reference**, not an expression transform -- confirming the
`databricks-iceberg` skill's warning about this ("MUST NOT use
expression-based partition transforms... with PARTITIONED BY on managed
Iceberg tables"). Fix: added a real, visible `window_date DATE` column
(`funnel_for_iceberg.sql`: `window_start::DATE as window_date`) and
partitioned by that column instead of a hidden `day(window_start)`
transform.

**Migration performed** (option (b) -- full rewrite, not "accept a gap
for old data" -- chosen because today's volume, 7290 rows, made this
trivial):
1. `CREATE TABLE funnel_summary_historical_v2 USING ICEBERG PARTITIONED BY
   (window_date) AS SELECT ..., CAST(window_start AS DATE) AS window_date
   FROM funnel_summary_historical` -- via the Databricks CLI directly
   (`databricks experimental aitools tools query ... --profile personal`),
   not through StarRocks, since this is authoritative Databricks-side DDL.
2. Verified row count matched (7290 = 7290) before proceeding.
3. `ALTER TABLE funnel_summary_historical RENAME TO
   funnel_summary_historical_unpartitioned_backup`, then `ALTER TABLE
   funnel_summary_historical_v2 RENAME TO funnel_summary_historical` --
   the external-facing table name never changes, so nothing downstream
   (RisingWave sink, StarRocks catalog references) needed reconfiguring
   for the rename itself. Old table kept as a backup, not dropped.
4. `dbt/models/sink_funnel_to_databricks.sql`: added
   `partition_by = 'window_date'` to the sink's `WITH` clause (RisingWave's
   Iceberg sink does support the raw `day(window_start)` transform syntax
   at the protocol level -- unlike Databricks' own DDL surface -- but
   `window_date` was used instead for consistency with the migrated
   table's actual schema).
5. Full `dbt run` (not just the two affected models) was needed to recreate
   this, because the RisingWave stack had been freshly restarted
   (`docker compose down` + up) earlier and none of its models existed yet
   -- `funnel_summary`, `funnel_for_iceberg`, etc. all had to be rebuilt
   from scratch first. One unrelated pre-existing failure surfaced during
   this rebuild and was left alone: `sink_funnel_to_postgres` (needs a
   Dagster asset, `postgres_funnel_table`, that creates its target table --
   hasn't run since the restart; unrelated to this migration). The
   casino_prd/sportsbook models also failed (storage-account permission
   and Kafka broker connectivity issues) -- pre-existing, unrelated
   separate demo per this repo's CLAUDE.md, not touched.

**Verified**: re-ran the same live-read `EXPLAIN ANALYZE` from the ninth
follow-up, now filtered to a single `window_date`: `ScanTime` dropped from
1.2-3.0s (unfiltered, full table) to **540ms** (single-partition filter).
The remaining ~2.6s gap in `TotalTime` is the same unrelated, already-
documented external-catalog planning tax (findings #3/eighth follow-up),
not something partitioning addresses. Real-world payoff of this change
scales with how many distinct partitions exist -- today's data spans only
1-2 days, so this test undersells it; at real volume (many months/years of
daily partitions), filtering to one day out of thousands would show a far
larger relative improvement than the 2-6x seen here.

**Eleventh follow-up: settled on 20s as the country-cache refresh
interval.** Revisited after a live Greece -> Hellas rename test: confirmed
the source-of-truth check first (querying `de_dev.sr_poc_external.iceberg_countries`
directly via `databricks experimental aitools tools query --profile personal`
showed the edit hadn't actually landed on the first attempt -- a reminder
to check the Databricks-side value directly before assuming a caching
problem). After the edit was redone and confirmed, tried 20s as a middle
ground between the earlier 10s (2.7-5.5s collision spikes) and 60s
(no spikes, but slower rename visibility). Recreated the MV, confirmed
"Hellas" flowed through the full path (StarRocks cache ->
`dashboard_funnel_serving` -> `/api/query/funnel`) correctly, then ran a
30-run timing test spanning ~1-2 refresh cycles: max 1.03s, mostly
440-980ms -- no repeat of the 10s-interval spikes. Kept at 20s. If spikes
ever reappear under heavier load, 60s remains the documented, verified
fallback.

**Twelfth follow-up: implemented a zero-copy architecture, deliberately,
after measuring the real cost.** Raised as "can we plan a zero-copy
approach with the same functionality" -- i.e. remove both remaining local
materialized copies (`mv_iceberg_countries_cache`, `mv_unified_funnel_summary`)
and read Databricks live at query time for everything, trading query speed
for zero staleness by construction (no cache means the "why didn't my edit
show up" question becomes structurally impossible, not just faster to
answer).

**Measured before changing anything** (the plan's explicit first step):
hand-wrote the live three-way query (hot JDBC + cold Iceberg, day-partition
filtered + country-name Iceberg join) and ran `EXPLAIN ANALYZE` + a 3-run
batch. First run 2.838s (cold), settled to a consistent **2.0-2.1s**
across two more runs, with `ExecutionTime` itself tiny (150-460ms) --
almost the entire cost is the same per-external-table planning tax
documented throughout this file, just paid three times per query instead
of zero. Compared to the ~500ms-1s the caching layers delivered, this is a
real, consistent 2-4x regression, not an occasional outlier -- confirmed
with the user before proceeding, since the earlier plan explicitly flagged
this as a decision point, not an assumption.

**Important correctness finding during measurement**: `funnel_summary_historical`
is partitioned on `window_date`, a *plain* column (not a hidden
`day(window_start)` transform -- see the tenth follow-up for why). Filtering
only on `window_start`/`window_end` (what the API layer already does, not
`window_date` directly) does **not** get true partition-level pruning --
but it still gets a real ~5x benefit via per-file Iceberg column statistics
(456ms vs 2.38s scan time, confirmed directly). Exposing `window_date` as
an output column for the stronger benefit was considered and explicitly
skipped, to avoid touching the API response shape for a further, smaller
gain on top of an already-accepted cost.

**What was changed**:
- `dbt_starrocks/models/dashboard_funnel_serving.sql` -- rewritten to
  inline both branches as live queries: the cold branch now reads
  `databricks_uc.sr_poc_external.funnel_summary_historical` directly (with
  the same per-window dedup `GROUP BY` the old MV used), the hot branch is
  unchanged (`risingwave.public.funnel_summary`, already zero-copy since
  the seventh follow-up).
- Dropped `mv_iceberg_countries_cache` and `mv_unified_funnel_summary`,
  removed their dbt model files.
- `modern-dashboard/backend/api.py` -- both country-name joins now target
  `databricks_uc.sr_poc_external.iceberg_countries` directly instead of the
  dropped cache. The "hot catalog unavailable" degraded-fallback queries
  (both `/api/query/funnel` and `/api/query/funnel/aggregate`) previously
  read the now-dropped `mv_unified_funnel_summary` -- rewritten to query
  `funnel_summary_historical` directly with the same dedup logic, since
  there's no cached cold layer left to fall back to.
- Retired the `starrocks_mv_warm` Dagster asset and the `demo_warm_job` job
  that existed solely to wrap it (`orchestration/definitions.py`,
  `orchestration/assets/starrocks_mv_warm.py` deleted) -- there's no
  longer a unified MV to warm at startup or before a demo.
  [SR_POC_LIVE_DEMO_RUNBOOK.md](SR_POC_LIVE_DEMO_RUNBOOK.md) still
  references `demo_warm_job` in several places and needs a follow-up pass
  to remove that guidance -- not done as part of this change, flagged here
  so it isn't mistaken for an oversight if noticed later.
- Ran `dbt parse` on both dbt projects and restarted `dagster-daemon` per
  this project's standing convention after model/config changes.

**Verified end-to-end**: view returns correct data (checked directly via
`SHOW CREATE VIEW` and a live `SELECT`), both API endpoints return correct
data (`degraded: false`), and a 15-run timing batch against the real
running app landed at **1.97-2.56s**, matching the hand-written
measurement closely. This is the accepted, final state for this
architecture -- a deliberate trade of query speed for structural
freshness guarantees, chosen with the real cost known in advance rather
than discovered after the fact.

**Thirteenth follow-up: added a side-by-side demo of both architectures,
instead of choosing one.** Rather than only having the zero-copy
architecture available, restored the pre-zero-copy cached objects
alongside it (not replacing anything) so both can be run and compared
directly in a live demo.

**Restored** (exact content recovered via `git show HEAD:<path>`, not
reconstructed from memory, to guarantee it matched the last known-good
state): `dbt_starrocks/models/mv_iceberg_countries_cache.sql` (20s refresh)
and `mv_unified_funnel_summary.sql` (`REFRESH MANUAL`). **New**:
`dashboard_funnel_serving_cached.sql` -- the pre-zero-copy form of
`dashboard_funnel_serving.sql` (also recovered via `git show`), kept under
a new name so both views can coexist. All three built via
`dbt run` and verified with data (`mv_iceberg_countries_cache`: 20 rows,
`mv_unified_funnel_summary`/`dashboard_funnel_serving_cached`: 286 rows
each, matching).

**Backend**: two new parallel endpoints,
`/api/query/funnel/cached` and `/api/query/funnel/cached/aggregate`
(`modern-dashboard/backend/api.py`), mirroring the existing zero-copy
endpoints exactly (same response shape, same degraded-fallback pattern)
but reading `dashboard_funnel_serving_cached`/`mv_iceberg_countries_cache`
instead.

**Frontend**: an "Architecture" toggle added to the Queries tab
(`modern-dashboard/frontend/src/components/QueriesTab.jsx`) switching
between `live` (zero-copy, default) and `cached`, wired to call the
matching endpoint pair. The existing "Query run in X.XXs" duration display
now also shows which architecture produced that specific timing (captured
at query time, not read from the toggle's current position, so it stays
correct even if the toggle is flipped afterward without re-running).

**Verified the demo actually demonstrates something**: ran both endpoints
5 times each with identical query parameters --
zero-copy: 1.35-1.79s, cached: 0.40-0.67s. A clear, consistent ~3x
difference, visible directly in the UI by flipping the toggle and
re-running the same query.

**Note on staleness**: the cached path's `mv_unified_funnel_summary` needs
an explicit refresh to pick up newly-archived historical data (no Dagster
asset wraps this anymore, since `starrocks_mv_warm` was retired in the
twelfth follow-up) --
`REFRESH MATERIALIZED VIEW sr_local_db_sr_local_db.mv_unified_funnel_summary WITH SYNC MODE;`
if it looks stale during a demo. `mv_iceberg_countries_cache` still
self-refreshes every 20s on its own schedule.

**Fourteenth follow-up: consolidated `modern_dashboard_setup_job`'s asset
groups down to exactly four** (`risingwave`, `starrocks`, `postgres`,
`setup`), from what had grown to ten (`casino_databricks`,
`dashboard_setup`, `datalake`, `default` x3, `postgres`, `risingwave`,
`starrocks`, ...). Note: **Dagster asset groups are global**, not
job-scoped -- there's no way for an asset to show one group in this job
and a different one elsewhere, so this changed how these assets display
everywhere they appear, not just here.

**Mapping applied**:
- `risingwave`: everything already there, plus `sink_funnel_to_databricks`
  (was `casino_databricks` -- it shares the same `databricks` tag as every
  casino_prd sink, which swept it into that bucket even though it's a
  funnel-project RisingWave sink) and the `risingwave.funnel_summary` dbt
  source.
- `starrocks`: unchanged.
- `postgres`: unchanged (`postgres_funnel_table` was already there).
- `setup`: `modern_dashboard_databricks_table`/`modern_dashboard_preflight`
  (were `dashboard_setup`), `iceberg_countries` the Python asset (was
  `datalake`), and the `databricks_uc.*`/`lakekeeper_local.*` dbt sources.

**Three failed attempts before the one that worked**, each instructive
about how dagster-dbt actually resolves group names for dbt *sources*
(external references this project depends on but never builds) as opposed
to models/sinks it does build:
1. Tag-based classification in `CustomDagsterDbtTranslator.get_asset_spec`
   (the existing mechanism used for models) -- confirmed via a direct call
   that it returns the *correct* group when invoked manually, but the
   actual `@dbt_assets`-built job still showed these sources as Dagster's
   bare `default` group. Conclusion: `get_asset_spec` is simply never
   invoked for pure source nodes by the `@dbt_assets` machinery.
2. Overriding `DagsterDbtTranslator.get_group_name` (a separate,
   documented hook whose docstring explicitly mentions sources) -- same
   result: correct output when called directly, no effect on the built
   job. Removed after confirming it was dead code.
3. `AssetsDefinition.map_asset_specs` on `starrocks_unified_dbt_assets` --
   confirmed via `AssetKey(...) in starrocks_unified_dbt_assets.keys` that
   these source keys **aren't part of that AssetsDefinition's own spec
   list at all** -- they're bare external-dependency placeholders Dagster
   auto-synthesizes for any key something depends on but nothing defines,
   with no owning `AssetsDefinition` to call `map_asset_specs` on.
4. **What worked**: passing bare `AssetSpec(key=..., group_name=...)`
   objects directly into `Definitions(assets=[...])` -- confirmed Dagster
   1.13.19 accepts this as a documented way to declare/annotate an
   external asset. Added `external_dbt_source_assets` in
   `orchestration/definitions.py` for all four dbt_starrocks sources
   (including `lakekeeper_local.funnel_summary`, not part of this job but
   fixed for consistency).

**Verified**: direct inspection of `modern_dashboard_setup_job`'s resolved
asset graph shows exactly 4 groups, zero `default` entries. Re-ran the
full job end-to-end after the change: `RUN_SUCCESS`, zero step failures,
both `/api/query/funnel` and `/api/query/funnel/cached` still returning
`200` afterward.

**Unrelated false lead, for the record**: during this investigation one
test run stalled for 694 seconds with zero trace in `fe.audit.log`. Cause
was external to StarRocks — the host laptop was switched away from on a
KVM switch and macOS suspended it, freezing the in-flight `docker exec`
client process until it woke back up. Not a StarRocks issue; flagged here
only so it isn't mistaken for one if seen again in a log review.

## `realtime_funnel_dbt_assets` job runtime: made the sink drop opt-in

**Question raised**: "Why does this job take such a long time to run?"
First hypothesis was wrong — checked `run_results.json` from a real
invocation and confirmed the `dbt build` step itself was already correctly
scoped (exactly the 9 models the job's selected assets needed, ~8.5s total
execution), not rebuilding the whole ~50-model project as first assumed.

**Real cost**: `orchestration/definitions.py`'s `realtime_funnel_dbt_assets`
function runs `dbt run-operation drop_prebuild_sinks` before every `dbt
build`, so that `CREATE SINK IF NOT EXISTS` definitions get refreshed if a
sink's SQL changed. Confirmed via `context.is_subset` /
`context.selected_asset_keys` (temporarily added debug logging, then
removed) that the existing scoping logic already worked correctly — it
correctly narrowed the drop to just the sinks actually selected (e.g.
`['funnel_kafka_sink', 'sink_funnel_to_databricks']`), so scoping was never
the bug.

The actual variable cost is dropping `sink_funnel_to_databricks`
specifically: repeated manual timing showed 35s → 7s → 3s → 5s, and two
job runs with byte-identical scoped code took 9s vs 2m42s. RisingWave's
`meta-node-0` / `compute-node-0` / `frontend-node-0` logs showed **zero**
lines during both a fast and a slow drop window (calibrated against a
known-fast window showing the same zero-log behavior), ruling out a
RisingWave-side stall as the visible cause. Best explanation: tearing down
a stale/cold Iceberg-Databricks connection sometimes needs a slow
TCP-timeout-based detection before the drop can proceed — external network
variability, not a code bug, consistent with the connection behavior seen
elsewhere with Databricks/Iceberg throughout this project.

**Fix (2026-09-08)**: the drop is only actually needed after editing a
sink's SQL/config — rare relative to how often this job runs routinely.
Made it opt-in, mirroring the existing `FORCE_ICEBERG_SOURCE_REFRESH`
pattern in the same function: new `FORCE_SINK_REFRESH` env var, default
`false` (skip the drop, `CREATE SINK IF NOT EXISTS` just leaves the
existing sink alone). Set `FORCE_SINK_REFRESH=true` in the Dagster env
when a sink's definition has changed and needs to be picked up.

## StarRocks storage backend: shared-nothing -> shared-data on MinIO

Prompted by "can we set up StarRocks to use MinIO as storage?" — a natural
follow-up to the earlier Azure ADLS shared-data feasibility test (which
confirmed the mechanism works but got blocked on Azure credentials, see
[SR_POC_STARROCKS_SHARED_DATA_AZURE.md](SR_POC_STARROCKS_SHARED_DATA_AZURE.md)).
MinIO is S3-compatible and already running in this stack with static
access-key/secret auth — exactly what StarRocks's S3 storage-volume
properties expect, unlike ADLS2's OAuth-only gap. Confirmed working
end-to-end on the first attempt: standalone test cluster up, storage
volume created, table write/read round-tripped, independently verified by
inspecting MinIO's own backing filesystem for the actual data file.

That standalone test was then **wired in as the main `starrocks` service**
the same day, replacing the old `starrocks/allin1-ubuntu` (shared-nothing)
image with the split `fe-ubuntu`/`cn-ubuntu` shared-data pair — full
details, exact SQL, and verification results in
[SR_POC_STARROCKS_SHARED_DATA_MINIO.md](SR_POC_STARROCKS_SHARED_DATA_MINIO.md).
Two things worth calling out here since they're specific to this project's
history rather than StarRocks generally:

1. **The old allin1 service's custom `docker-entrypoint.sh` did far more
   than shared-nothing bootstrap** — it also wrote the ADLS
   `core-site.xml` credential needed to read the Databricks Unity Catalog
   table's actual data files (physically stored in ADLS, separate from the
   REST catalog's own OAuth metadata credential), plus every performance
   fix accumulated in this document: the JVM heap ceiling reduction
   (`-Xmx3072m`, see the GC-pause section above), disabling
   connector-table auto-analyze, the 10s MV refresh floor, and the BE data
   cache sizing. Swapping the image without carrying these forward would
   have silently reintroduced all of them. Ported everything into two new
   scripts, [starrocks/fe-entrypoint.sh](../starrocks/fe-entrypoint.sh) and
   [starrocks/cn-entrypoint.sh](../starrocks/cn-entrypoint.sh), adapted to
   the split images' conf paths (`/opt/starrocks/fe/conf/` and
   `/opt/starrocks/cn/conf/` instead of the allin1 image's
   `/data/deploy/starrocks/{fe,be}/conf/`). Caught the missing ADLS
   credential immediately — a `databricks_uc` query failed with "Failed to
   get file system for path: abfss://..." until the port was done.
2. **Existing catalogs/tables don't migrate, they get rebuilt** — a fresh
   FE starts with empty metadata, so this was a genuine service swap, not
   an in-place upgrade. `starrocks-init` re-registers all three external
   catalogs automatically; the `dbt_starrocks`-managed views/MVs
   (`mv_iceberg_countries_cache`, `dashboard_funnel_serving`, etc.) needed
   a full `modern_dashboard_setup_job` re-run, which completed
   `RUN_SUCCESS` with zero errors.

## `sink_funnel_to_databricks` silently stalled: `catalogManaged` blocking external writes

Discovered 2026-09-10 while investigating why a Metabase funnel chart's
totals kept *decreasing* over time under steady producer traffic. Root
cause turned out to be two separate, stacked issues -- one in the
dashboard view's query logic, one in the actual data pipeline -- worth
recording separately since they'd otherwise look like the same bug.

### Issue 1: the hot/cold cutoff itself, fixed twice

`dashboard_funnel_serving.sql`'s original rolling 3-minute cutoff (hot =
last 3 minutes from RisingWave, cold = older from Databricks) caused a
real, observed gap: `sink_funnel_to_databricks` commits in batches (Iceberg
sinks don't flush per-row), so a row less than ~3 minutes old that hadn't
landed in Databricks yet was invisible to the view entirely once it aged
past the hot window -- confirmed live via Metabase (total viewers
decreasing) and via direct comparison of RisingWave's `funnel_summary`
(fresh data through the current minute) against
`databricks_uc...funnel_summary_historical` (stuck at `2026-09-07`, the
sink wasn't committing at all -- see Issue 2 below).

First fix: switched the cutoff to `CURRENT_DATE` (today always from
RisingWave, everything before today from Databricks) -- removed the
observed gap, since today's data no longer depended on the sink's flush
cadence at all.

That fix had its own latent issue, caught before it ever caused a visible
problem: RisingWave's own storage is wiped by `bin/6_down.sh` (`docker
compose down --volumes`), so a mid-day restart loses that day's
pre-restart data from RisingWave -- and a calendar-date cutoff would then
hide it from Databricks too (even if the sink had already replicated it
there) until midnight rolled the cutoff over.

Final fix: replaced the calendar-date cutoff with one that tracks
RisingWave's *actual* retained range instead of a calendar boundary --
cold serves anything strictly older than RisingWave's current
`MIN(window_start)`, hot serves everything RisingWave currently has,
unconditionally:

```sql
-- cold
WHERE window_start < COALESCE(
  (SELECT MIN(window_start) FROM {{ source('risingwave', 'funnel_summary') }}),
  CAST('9999-12-31 00:00:00' AS DATETIME)  -- if RisingWave is empty, cold serves everything
)
-- hot: no date filter at all -- whatever's currently in funnel_summary
```

This self-heals after *any* restart -- partial-day or full -- as long as
the sink has kept up, without needing to know or care what day it is.
Verified: the view spans a continuous range (`2026-09-05` through the
current minute) with zero duplicate `window_start` rows at the boundary.
Note: `TIMESTAMP '...'` literal syntax isn't accepted by StarRocks here --
use `CAST('...' AS DATETIME)` instead.

### Issue 2: the sink had been silently dead since it was created

The gap symptom in Issue 1 was actually masking a much bigger problem: the
sink hadn't committed *anything* since the table was created on
2026-09-07. Confirmed via `DESCRIBE HISTORY` on the Databricks side --
only one version existed (`CREATE TABLE AS SELECT`, 7290 rows, all from a
one-time backfill) -- the sink had, as far as the evidence shows, never
actually worked.

Ruled out, in order, each with concrete evidence (not assumption):

1. Network reachability to both the Iceberg REST endpoint and the
   Microsoft OAuth token server -- both fast, both correct responses.
2. OAuth credential validity -- a live token request returned `HTTP 200`
   with a real access token.
3. A poisoned JVM/connector state in `compute-node-0` -- restarting the
   container and recreating the sink reproduced the exact same silent
   hang (no errors, no commits) on a completely fresh process.
4. Unity Catalog grants -- the service principal had `ALL_PRIVILEGES` at
   the schema level.
5. Azure RBAC -- the same principal had `Storage Blob Data Contributor` on
   the exact storage account.

Every external permission and auth boundary checked out. The actual root
cause: **`funnel_summary_historical` had the `delta.feature.catalogManaged`
protocol feature**, which this project had already discovered once before
on an isolated probe table (see "StarRocks write capability" section
below / docs/SR_POC_TESTING_PLAN.md section 2.1.9) -- it silently blocks
external-engine (RisingWave/StarRocks/Trino) writes via Unity Catalog's
Iceberg REST Catalog commit endpoint, with **zero error surfaced anywhere**,
even though reads, auth, and metadata fetches all work normally. It
apparently got reintroduced when the table was last recreated (via CTAS,
2026-09-07) -- the CTAS's own recorded properties show
`databricks.internal.autoUpgrades.delta.feature.catalogManaged: supported`,
suggesting Databricks adds this automatically on new managed-Iceberg-shaped
tables rather than it being something set explicitly.

Fix (a plain property unset is not enough -- confirmed the same finding
twice now):

```sql
-- Does NOT remove the protocol feature:
ALTER TABLE de_dev.sr_poc_external.funnel_summary_historical
UNSET TBLPROPERTIES ('delta.feature.catalogManaged');

-- This is the actual fix -- a Delta protocol-feature operation:
ALTER TABLE de_dev.sr_poc_external.funnel_summary_historical
DROP FEATURE catalogManaged;
```

Verified: recreated the sink after the fix, and `funnel_summary_historical`
went from 7290 rows (stuck since Sep 7) to actively growing within one
commit cycle, with real new `window_start` values landing continuously.

**Saved to memory** (this Claude session's persistent memory, not this
repo) so a future session checks for `catalogManaged` first, before
re-deriving all five ruled-out causes above from scratch.

### Follow-up: commit cadence aligned to the data's natural window

While fixing the above, also changed `sink_funnel_to_databricks`'s
`commit_checkpoint_interval` from `20` to `15`. At this project's
`barrier_interval_ms=2000` / `checkpoint_frequency=2` (4s per checkpoint),
that's a commit roughly every 60s instead of ~80s -- aligning with
`funnel_summary`'s own 1-minute tumbling window rather than an interval
arbitrary relative to it. Not a correctness fix (the Issue 1 fix above
already made commit cadence a non-issue for user-facing freshness); just a
cleanliness improvement, chosen to stay well clear of this project's
existing documented lesson that committing *too* frequently (per-checkpoint
Stream Load, tried earlier in this doc's history) overloads compaction.

## StarRocks JDBC catalog returning corrupted timestamps (2026-09-10)

### Symptom

While capturing a baseline before testing `bin/6_down.sh`'s restart
behavior (see Issue 1 above), queries against the `risingwave` JDBC
catalog started intermittently returning garbage. `SELECT MIN(window_start)
FROM risingwave.public.funnel_summary` -- and even a plain `ORDER BY
window_start DESC LIMIT 1`, no aggregate, no `WHERE` -- would sometimes
return `2000-01-01 00:00:00` instead of the real value. Row counts through
the same catalog also fluctuated across identical repeated calls (e.g. 431,
148, 148, 149 across 5 calls). Direct comparison against RisingWave itself
(via `psql`) confirmed RisingWave's own data was always correct -- the
corruption was introduced entirely on the StarRocks side.

### Investigation

Ruled out, with evidence, before finding the real cause:

- **Stats-cache shortcut** (this project's known JDBC-catalog stats-cache
  issue, see the GC-pause section earlier in this doc): ruled out via
  `EXPLAIN`, which showed a genuine pushed-down scan
  (`SELECT "window_start" FROM "public"."funnel_summary"`) with TOP-N
  applied on the CN side, not a stats-based shortcut.
- **`ClosedChannelException` noise in FE logs**: red herring -- routine,
  benign closes from short-lived `docker exec ... mysql -e` CLI
  connections, uncorrelated with which specific calls returned good vs.
  bad data.
- **`frontend-node-0`'s steady 5s "early eof" pgwire errors**: red herring
  -- that's the container's own Docker healthcheck (`docker-compose.yml`)
  sending a garbage HTTP GET to the pgwire port every 5s, present since
  container start, unrelated to JDBC catalog traffic.
- **A one-time `HikariPool$PoolInitializationException`** in FE logs from
  ~3 hours before the investigation window (right after an earlier stack
  restart, before RisingWave was reachable): a real event, but a dead end
  -- unrelated to the ongoing symptom.
- **Query cache**: confirmed disabled (`enable_query_cache = false`).
- **Connector I/O concurrency** (`connector_io_tasks_per_scan_operator`,
  `enable_connector_split_io_tasks`, `enable_connector_adaptive_io_tasks`,
  `enable_connector_async_list_partitions`,
  `enable_connector_deploy_scan_ranges_background`): all forced to
  single-threaded/disabled and retested with a fresh CN process. **Zero
  effect** -- the corruption still appeared at exactly the 6th query after
  every CN restart, with or without concurrency. This determinism (always
  call #6, never earlier or later, regardless of concurrency) is what
  ruled out a race condition and pointed at a fixed-size buffer/cache
  instead.

The key clue: `2000-01-01 00:00:00` is not a StarRocks default -- it's
**PostgreSQL's own internal timestamp epoch** (Postgres stores timestamps
as microseconds since 2000-01-01, unlike Unix's 1970-01-01 epoch). That
strongly suggested the CN's native JDBC bridge was, on some calls,
decoding a zeroed/never-written buffer as "0 microseconds since the
Postgres epoch" -- i.e. an uninitialized-memory bug specific to how it
parses RisingWave's Postgres binary-wire-protocol timestamps, not a
general StarRocks/JDBC corruption.

### StarRocks version upgrade: tried, did not fix it

Before finding the binary-protocol angle, the project was running
`starrocks/fe-ubuntu:4.0-latest` / `starrocks/cn-ubuntu:4.0-latest`
(4.0.14) -- itself an unintended regression: the shared-data migration
(see "StarRocks storage backend: shared-nothing -> shared-data on MinIO"
above) copied image tags verbatim from StarRocks's own official
shared-data quickstart `docker-compose.yml`
(`github.com/StarRocks/demo`), which happens to pin `4.0-latest`; the
project had previously run `allin1-ubuntu:4.1.4` before that migration.

A specific fix -- StarRocks PR
[#71016](https://github.com/StarRocks/starrocks/pull/71016), "fix pg date
time bug", which replaced two `Calendar` fields in `JDBCScanner.java` that
were incorrectly reused/mutated across rows under a mistaken
single-threaded assumption -- looked like a strong match. Confirmed via
git ancestry against the real StarRocks repo: absent from 4.0.14, present
starting at 4.1.1. Upgraded to `starrocks/fe-ubuntu:4.1.4` /
`starrocks/cn-ubuntu:4.1.4` (pinned exact version, not `4.1-latest`) and
re-ran the stress test.

**Result: the timestamp corruption still reproduced identically** -- same
sticky flip at call #6, cleared only by a CN restart. The upgrade did fix
a real, separate bug: row counts through the JDBC catalog became
perfectly stable across repeated calls (198 every time, matching
RisingWave), where they'd fluctuated wildly on 4.0.14. But it did not fix
the timestamp corruption -- PR #71016 was not the (or not the only) root
cause. **The version bump to 4.1.4 was kept anyway** (real improvement,
no downside found), but it does not by itself close this issue.

### The actual fix: force text protocol on the JDBC connection

Forcing the PostgreSQL JDBC driver to use text protocol instead of binary,
via a `binaryTransfer=false` parameter on the catalog's `jdbc_uri`, was
tested by dropping and recreating the `risingwave` catalog:

```sql
DROP CATALOG risingwave;
CREATE EXTERNAL CATALOG risingwave
COMMENT 'RisingWave PostgreSQL JDBC federation'
PROPERTIES (
  'schema_resolver' = 'postgresql',
  'driver_class' = 'org.postgresql.Driver',
  'driver_url' = 'https://repo1.maven.org/maven2/org/postgresql/postgresql/42.7.7/postgresql-42.7.7.jar',
  'type' = 'jdbc',
  'user' = 'root',
  'password' = 'root',
  'jdbc_uri' = 'jdbc:postgresql://frontend-node-0:4566/dev?binaryTransfer=false'
);
```

Confirmed clean across **100+ repeated queries** after a fresh CN restart
-- mixing `MIN()`, `ORDER BY ASC LIMIT 1`, `ORDER BY DESC LIMIT 1`, and
plain row counts, the exact query shapes that previously corrupted within
~6 calls. Zero corruption, row counts stable, all values matched
RisingWave's ground truth exactly. This is now baked into
`starrocks/init_catalog.sh`'s `CREATE EXTERNAL CATALOG risingwave`
statement, so a fresh `starrocks-init` run (e.g. after `bin/6_down.sh`)
creates the catalog correctly from the start.

### Cleanup: the earlier `WHERE viewers >= 0` workaround was removed

`dashboard_funnel_serving.sql`'s hot/cold cutoff subquery had picked up a
`WHERE viewers >= 0` predicate mid-investigation, on the mistaken theory
that it forced a real scan past a stats-cache shortcut. `EXPLAIN` already
disproved that theory (see above -- it was always a real scan), and the
predicate did not reliably prevent the corruption anyway. Removed now that
the real, catalog-level fix (`binaryTransfer=false`) is in place; the
cutoff subquery is back to a plain `SELECT MIN(window_start) FROM
{{ source('risingwave', 'funnel_summary') }}`.

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
