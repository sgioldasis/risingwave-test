---
title: StarRocks Upsert / Point-Lookup Demo (Wallet-Inspired)
description: Design and build log for demonstrating StarRocks Primary Key table upsert visibility and point lookups, motivated by the PAM Operational Query Layer proposal
---

<!-- markdownlint-disable-file -->

**For exact step-by-step run instructions (script runner / Dagster /
Superset), see
[SR_POC_SUPERSET_DEMOS_RUNBOOK.md](SR_POC_SUPERSET_DEMOS_RUNBOOK.md).** This
doc covers the design and the real bugs found while building it.

## Status: built and confirmed working (2026-09-12)

Full pipeline built and validated end-to-end, including running through the
actual Dagster webserver (not just `dbt` directly). Core result, confirmed
live: `count(*) == count(DISTINCT transaction_id)` in the StarRocks table --
every reversed transaction produced 2 Kafka events, but the table holds
exactly 1 row per `transaction_id`, showing the *latest* (reversed) state.
Point lookup by `transaction_id`: ~90ms including `docker exec` overhead.

Superset: dashboard "Wallet Upsert Demo" (`/superset/dashboard/wallet-upsert-demo/`),
one table chart over the native `wallet_transactions` StarRocks dataset.

## Motivation

An internal proposal ("PAM High-Performance Operational Query Layer") argues
for moving several read workloads off the Wallet's transactional database
and onto a StarRocks-backed Operational Query Service (OQS), fed from Kafka.
Two of its four target use cases hinge on a capability this project's
existing demo had never exercised:

- **Sub-second upsert visibility** — when a reversal/correction event
  arrives for an existing transaction, a query must see the corrected state
  immediately, not a stale duplicate row.
- **Primary-key point lookup** — retrieving a single row by an exact key
  (`BetId`/`TransactionId`) fast enough to sit inside a live financial flow
  (the proposal's own target: P99 < 100ms), which is an unusual ask of an
  OLAP engine and the main open question the proposal itself flags as R&D.

Our funnel demo (`dashboard_funnel_serving`, `dashboard_funnel_serving_cached`,
the query-rewrite MV) is entirely append-only aggregation over time windows
and says nothing about upserts or point lookups. This is a small, separate,
fully-synthetic demo pipeline built to show exactly that — no real customer,
payment, or transaction data involved anywhere.

## What this validates (and what it doesn't)

This is a capability demo, not a load test. It shows the *mechanism*
(a reversal event overwrites the prior state, and a point lookup returns
only the latest state) works, and gives a rough single-query timing. It does
**not** attempt to reproduce the proposal's actual SLOs (P99 <100ms lookup
under load, 12B/month throughput, 4-year dataset) — those require
production-representative load this local single-node stack can't generate
meaningfully.

## Pipeline (as built)

### 1. Producer — script runner

`bin/3_run_wallet_producer.sh` (registered in `scripts/script_runner.py`'s
`SCRIPTS` list and `BACKGROUND_SERVICES_CONFIG`, same pattern as the
existing funnel producer), backed by `scripts/wallet_producer.py`. A single
run of this one producer emits onto **two separate Kafka topics** (both
added to `redpanda-init`'s topic-creation list in `docker-compose.yml`),
feeding all three write paths covered by this doc:

**`wallet_transactions`** — the original transaction/reversal stream, fed
into both the RisingWave-mediated path and the direct-Kafka path:

| Field | Type | Notes |
|---|---|---|
| `transaction_id` | string (uuid4) | primary key downstream |
| `account_id` | string | synthetic, no real identities |
| `type` | string | `bet` / `win` / `deposit` / `reversal` |
| `amount` | decimal | synthetic |
| `status` | string | `settled` / `reversed` |
| `event_time` | timestamp | |

`--reversal-rate` (default 0.2) of transactions get a reversal emitted
`--reversal-delay` seconds later (default 5s), carrying the *same*
`transaction_id`.

**`wallet_status_updates`** — a separate, independent stream feeding only
the partial-update path (see "Add-on comparison: partial-column update"
below): `--status-update-rate` (default 0.15) of the transactions that
did *not* get a reversal get a `{transaction_id, status}`-only event
emitted `--status-update-delay` seconds later (default 8s), with
`status` set to `flagged`. No `amount`/`type`/`account_id`/`event_time` in
this event at all — the whole point of this stream is that its consumer
never sees them.

### 2. RisingWave ingestion — `dbt/models/src_wallet_transactions.sql`

Plain `CREATE SOURCE` over the Kafka topic. **`amount` is `decimal`, not
`double precision`** — see the type-mismatch section below for why.

### 3. RisingWave → StarRocks sink — `dbt/models/sink_wallet_transactions_to_starrocks.sql`

```sql
CREATE SINK IF NOT EXISTS sink_wallet_transactions_to_starrocks AS
SELECT
    transaction_id, account_id, type, amount, status,
    CAST(event_time AS TIMESTAMP) AS event_time
FROM {{ ref('src_wallet_transactions') }}
WITH (
    connector = 'starrocks',
    type = 'upsert',
    primary_key = 'transaction_id',
    starrocks.host = 'starrocks',
    starrocks.mysqlport = '9030',
    starrocks.httpport = '8030',
    starrocks.user = 'root',
    starrocks.password = '',
    starrocks.database = 'sr_local_db_sr_local_db',
    starrocks.table = 'wallet_transactions'
)
```

Confirmed live: `starrocks.mysqlport` / `starrocks.httpport` (one word, no
underscore) are the correct property names — a search had turned up both
this form and `starrocks.query_port` / `starrocks.http_port` from different
doc sources; this is the one that actually works.

### 4. StarRocks-side native table — `dbt_starrocks/models/wallet_transactions.sql`

```sql
{{ config(
    materialized='table',
    table_type='PRIMARY',
    keys=['transaction_id'],
    distributed_by=['transaction_id']
) }}

SELECT
    CAST(NULL AS VARCHAR(64)) AS transaction_id,
    CAST(NULL AS VARCHAR(64)) AS account_id,
    CAST(NULL AS VARCHAR(16)) AS type,
    CAST(1e0 AS DOUBLE) AS amount,
    CAST(NULL AS VARCHAR(16)) AS status,
    CAST(NULL AS DATETIME) AS event_time
WHERE 1 = 0
```

A genuine dbt-managed StarRocks Primary Key table (confirmed via the
`dbt-starrocks` adapter source, `materializations/adapters/relation_helpers.sql`)
— unlike the external catalogs, which live in `starrocks/init_catalog.sh`
because dbt can't create those, this is a normal dbt model / Dagster asset.

### 5. Orchestration — `orchestration/definitions.py`

A new, narrowly-scoped job:

```python
wallet_pipeline_setup_job = define_asset_job(
    name="wallet_pipeline_setup_job",
    executor_def=in_process_executor,
    selection=AssetSelection.assets(
        AssetKey(["public", "src_wallet_transactions"]),
        AssetKey(["public", "sink_wallet_transactions_to_starrocks"]),
        AssetKey(["sr_local_db", "wallet_transactions"]),
    ),
    ...
)
```

The cross-project dependency (RisingWave sink → StarRocks table must exist
first) is declared via `meta.dagster.deps` on the sink model:
`{'asset_key': ['sr_local_db', 'wallet_transactions']}`. That asset-key
schema segment (`sr_local_db`, not the fully-resolved
`sr_local_db_sr_local_db`) was confirmed empirically by inspecting
`dbt_starrocks/target/manifest.json`'s raw `config.schema` field and
cross-checking against `dagster_dbt.asset_utils.default_asset_key_fn`'s
source — Dagster's default asset-key function uses the *raw* configured
schema, not dbt's fully-resolved database schema name.

**Real issue found and fixed (2026-09-13): `wallet_transactions` incorrectly
required `sink_funnel_to_databricks`.** Launching `wallet_pipeline_setup_job`
from the Dagster Launchpad started warning "these assets may fail because
the upstream asset `public/sink_funnel_to_databricks` has not been
materialized" — a completely unrelated Funnel Dashboard asset with no real
connection to the wallet pipeline. Root cause, found in
`CustomDagsterDbtTranslator` in `orchestration/definitions.py`: a rule
meant to wire the cross-PoC dependency for the StarRocks *unified funnel
view* specifically was gated on `package_name ==
"starrocks_unified_funnel"` — but that's the entire `dbt_starrocks/`
project's `dbt_project.yml` name, so the rule silently applied to *every*
model in the project, including `wallet_transactions` and
`mv_iceberg_countries_cache`, neither of which reads
`databricks_uc.funnel_summary_historical` at all. Fixed by scoping the
rule to the three models that genuinely do:
`dashboard_funnel_serving`, `mv_funnel_daily_country_rollup`, and
`mv_unified_funnel_summary`. Confirmed via Dagster's own asset-graph API
(`assetNodes { dependencyKeys }`) before and after: `wallet_transactions`
lost the bogus dependency, the three genuine Funnel Dashboard models kept
it, and `wallet_pipeline_setup_job` launched clean afterward.

### 6. Superset

Dashboard "Wallet Upsert Demo" (`/superset/dashboard/wallet-upsert-demo/`):

- A table chart over a dataset on the native `wallet_transactions` StarRocks
  table (`raw` query mode, all columns, no filter) — showing the
  point-lookup result set directly.
- Two side-by-side bar charts, "Count per Type" and "Total Amount per
  Type" (`type` = bet/win/deposit/reversal), added two dataset metrics
  (`cnt` = `COUNT(*)`, `sum__amount` = `SUM(amount)`) — the default `count`
  metric name is reserved by Superset and can't be reused for a custom
  metric, hence `cnt`. Both charts sort alphabetically by `type`
  (`timeseries_limit_metric` set to the `type` column, `order_desc: false`
  — same mechanism confirmed working for the funnel dashboard's table
  charts) so the two bars line up consistently; each chart defaults to
  sorting by its own metric otherwise, which looked inconsistent
  side-by-side.
- A markdown block explaining the pipeline and the confirmed upsert
  result.

**Reversal amount, evolved twice on 2026-09-12:**

1. The producer originally set `amount: 0.0` on reversal events, so "Total
   Amount per Type" showed reversals as contributing nothing — technically
   correct (that's what the event said) but not what you'd actually want
   to see. Since a reversal overwrites the original row in this Primary Key
   table, the *only* place the original amount can survive is in the
   reversal event itself.
2. First fix: the reversal event carried the *same* `amount` as the
   transaction it reverses. Better, but summing amounts per type then
   double-counts a reversed transaction's value (settled +X, reversed +X
   again) instead of netting it out.
3. Final fix: the reversal event carries `-amount` (negative of the
   original). `SUM(amount)` now nets out correctly — a settled transaction
   contributes +X, its reversal contributes -X, net zero — matching what
   "reversed" should actually mean financially.

Rows produced before each fix keep whatever sign/value they had at write
time (each `transaction_id` only reverses once, so they don't self-correct)
— this resolves fully on the next full pipeline rebuild, or gets diluted
out as new data accumulates.

## Real issues found and fixed while building this

None of these were guessed in advance — all found by actually running the
pipeline and reading the real error.

### `amount`: StarRocks CTAS always coerces to `decimal(38,9)`

First attempt: `CAST(NULL AS DOUBLE)` in the StarRocks table's placeholder
CTAS. Result: the created column was `decimal(38,9)`, not `DOUBLE`. Tried
`CAST(0.0 AS DOUBLE)`, `CAST(1e0 AS DOUBLE)` (scientific notation, normally
typed as DOUBLE in MySQL-compatible engines), `CAST(RAND() AS DOUBLE)`, and
`CAST(1 AS DOUBLE) / CAST(3 AS DOUBLE)` — **every one of these became
`decimal(38,9)`** in the resulting table. This StarRocks version's CTAS
appears to unconditionally coerce floating-point results to
`decimal(38,9)` for persisted table columns, regardless of the source
expression's declared type.

Rather than keep fighting this, changed the RisingWave-side source column
from `double precision` to `decimal` to match — which is arguably the more
correct design anyway (fixed-point decimal avoids floating-point precision
issues on monetary amounts, which is exactly the kind of thing a real
financial system would want).

Side note: RisingWave itself doesn't support parameterized
`DECIMAL(38,9)`/`NUMERIC(38,9)` at all ("Feature is not yet implemented");
plain unparameterized `decimal` is what works.

### `event_time`: StarRocks sink rejects `TIMESTAMP WITH TIME ZONE`

The RisingWave source declares `event_time timestamptz` (needed for the
`WATERMARK` clause). Sinking that directly into the StarRocks connector
failed: `"TIMESTAMP WITH TIMEZONE is not supported for Starrocks sink as
Starrocks doesn't store time values with timezone information."` Fixed by
switching the sink from `CREATE SINK ... FROM ref(...)` to
`CREATE SINK ... AS SELECT ... FROM ref(...)`, explicitly
`CAST(event_time AS TIMESTAMP)` in the sink's own SELECT — matches
StarRocks's `datetime` column type, which has no timezone concept.

### `dg launch` from the host hits a DNS false-positive, not a real limitation

Running `uv run dg launch --job wallet_pipeline_setup_job` directly from the
host shell failed repeatedly: `connection to server at "frontend-node-0"
(34.143.73.2 ...) failed: timeout expired`. Those IPs are bogus — `host
frontend-node-0` resolves to `frontend-node-0.run.app` (a wildcard-DNS
domain), because the host's DNS resolver appends a search suffix to the
Docker-internal hostname when it can't resolve it directly. Setting
`RISINGWAVE_HOST=localhost` (inline or exported) did **not** fix this even
though a direct `dbt build` with the same override worked fine — the exact
mechanism wasn't tracked down, but it doesn't matter: **`dg launch` from the
host was never the right way to test this.** The actual intended path is
the already-running Dagster webserver/daemon (which run *inside* Docker,
where `frontend-node-0` resolves correctly as a real container hostname).
Triggering the job via the webserver's GraphQL API
(`mutation { launchRun(...) }`) worked on the first try and completed with
`SUCCESS`. Lesson: test through the actual running Dagster instance, not a
fresh host-side CLI invocation, when Docker-internal hostnames are involved.

### Rebuilding the job wipes the StarRocks table (expected, not a bug)

`materialized='table'` in dbt always drops and recreates on every run —
same behavior already documented for `mv_unified_funnel_summary` elsewhere
in this project. Every `wallet_pipeline_setup_job` run resets
`wallet_transactions` to empty; the producer needs to be running (or
restarted) afterward to repopulate it. Not something to "fix" — consistent
with how the rest of this project's StarRocks tables already behave.

### End-to-end lag: ~24-40s by default, ~2-3s tuned (2026-09-12)

Noticed live during a demo: the point-lookup table's newest row consistently
lagged wall-clock time by 10-30+ seconds, even right after a "force
refresh" in Superset (confirmed via a direct `POST
/api/v1/chart/data` with `force: true` — the API itself returned data no
fresher than the lag implied, so this wasn't a caching artifact anywhere
in Superset).

**Root cause:** RisingWave sink decoupling is on by default for all sinks
(confirmed via `SELECT * FROM rw_sink_decouple` — `is_decouple = t`), and a
decoupled sink's default `commit_checkpoint_interval` is 10 — i.e. it only
commits to the downstream system every 10 checkpoints. This project's
`risingwave.toml` tunes `barrier_interval_ms = 2000` /
`checkpoint_frequency = 2`, meaning a checkpoint every ~4s — so the default
commit cadence for this sink was every ~40s, on top of the StarRocks
stream-load round trip itself.

**Fix:** added `commit_checkpoint_interval = 1` to the sink's `WITH`
options in
[dbt/models/sink_wallet_transactions_to_starrocks.sql](../dbt/models/sink_wallet_transactions_to_starrocks.sql),
committing on every checkpoint instead of every tenth one. Since the model
uses `CREATE SINK IF NOT EXISTS`, changing the `WITH` clause alone doesn't
take effect on an existing sink — had to `DROP SINK
sink_wallet_transactions_to_starrocks;` manually before re-running
`wallet_pipeline_setup_job` so it actually got recreated with the new
option. Confirmed via `SHOW CREATE SINK` that the option was applied, then
measured `MAX(event_time)` against wall-clock time twice a few seconds
apart: lag dropped to ~2-3 seconds.

This is a demo-specific tradeoff, not a universal "always do this" — sink
decoupling exists to protect RisingWave from a slow/unavailable downstream
system by buffering; setting `commit_checkpoint_interval = 1` gives up most
of that buffering in exchange for minimum visible latency, which is exactly
the right tradeoff for a "look how real-time this is" demo and the wrong
one for a production pipeline whose downstream system might actually stall.

## Add-on comparison: direct Kafka → StarRocks, no RisingWave (2026-09-12)

The PAM Operational Query Layer proposal that motivated this whole demo
(see "Motivation" above) specifically argues for StarRocks to serve
upsert-heavy financial reads *directly from Kafka*, not via an intermediate
stream processor. Since this pipeline's RisingWave hop does no
transformation at all (straight passthrough from source to sink), it's a
fair, cheap comparison to build the direct path too and show both side by
side on the same live traffic.

**Built:** `orchestration/assets/wallet_direct_kafka_setup.py` — a Dagster
asset that idempotently creates a second StarRocks Primary Key table
(`wallet_transactions_direct_kafka`, same schema) and a StarRocks Routine
Load job (`wallet_direct_kafka_load`) that reads the *same*
`wallet_transactions` Kafka topic straight into it. `event_time` is parsed
to a real `DATETIME` at load time via a computed column in the Routine
Load `COLUMNS` clause (`str_to_date(substr(event_time_raw, 1, 26),
'%Y-%m-%dT%H:%i:%s.%f')` — the producer's ISO 8601 string is always UTC
with a fixed-length `+00:00` suffix, so a plain `SUBSTR` reliably drops it
before parsing) — see "Real issue found: event_time displayed
inconsistently across the two point-lookup tables" below for why this
replaced an earlier VARCHAR-based version. No RisingWave asset anywhere in
this path — only the Kafka topic (already created by
`redpanda-init`) and StarRocks itself.

**Real issue found:** `max_batch_interval` (Routine Load's own micro-batch
property) has a hard floor of 5 — `"3"` is rejected outright with
`max_batch_interval should >= 5`. Used `5`, the minimum.

**Real issue found:** StarRocks Routine Load job names aren't reusable
while any job with that name is still active, but a `STOPPED`/`CANCELLED`
job's name IS reusable — confirmed live: `CREATE ROUTINE LOAD` with a name
matching a currently-`STOPPED` job succeeds immediately, no need to (and no
way to — calling `STOP ROUTINE LOAD` on an already-stopped job errors with
`not found when checking privilege`) clean it up first. The asset's
idempotency check queries `information_schema.routine_load_jobs` for the
most recent row by that name and only re-`CREATE`s if there's no row at all
or the latest one is `STOPPED`/`CANCELLED`.

**Measured:** both tables confirmed upsert-correct
(`COUNT(*) == COUNT(DISTINCT transaction_id)`). Lag against wall-clock time
was initially eyeballed from a couple of single-point checks as "direct
Kafka somewhat faster" — that wasn't rigorous. Re-measured properly
(2026-09-13) with 12 samples taken 3s apart, computing lag = wall-clock
minus each table's own `MAX(event_time)`:

| | avg | min | max |
|---|---|---|---|
| RisingWave-mediated | 2.97s | 1.3s | 5.3s |
| Direct Kafka Routine Load | 3.36s | 0.7s | 5.7s |

**No consistent winner** — both hover in the same few-seconds band and
fluctuate depending on where you happen to sample relative to each path's
own batch cycle (RisingWave commits every checkpoint, ~4s in this
project's tuned config; the Routine Load job's `max_batch_interval` is 5s
but lands bursty rather than on a strict clock). The real difference this
comparison demonstrates is architectural, not a latency win: one fewer
moving part, one fewer thing that can lag or fail, for a pipeline that has
no actual use for RisingWave's stream-processing capabilities — not "the
direct path is faster," which the data doesn't actually support.

**Superset:** added a second point-lookup table chart
("Wallet Transactions — Point Lookup [direct Kafka -> StarRocks, no
RisingWave]", dataset over `wallet_transactions_direct_kafka`) side by side
with the existing one (renamed to "... [via RisingWave sink]" for clarity),
plus a markdown block explaining the comparison. See "Superset" section
above for the general dashboard layout.

**Real issue found:** the new chart rendered as "There is no chart
definition associated with this component" in the dashboard even though the
chart existed and its `position_json` node looked correct (right `chartId`,
right `uuid`, right `parents`). Root cause: creating a chart via a raw
`POST /api/v1/chart/` call without a `dashboards` field in the payload
creates the chart but never inserts the `dashboard_slices` association row
— referencing the chart's id/uuid in the dashboard's `position_json` alone
isn't enough, Superset's dashboard renderer also checks that association
table. Fixed live with `PUT /api/v1/chart/{id}` `{"dashboards": [id]}`.
Worth noting this class of bug is self-healing on the next
`export_assets.sh` + reimport cycle regardless — Superset's own v1 importer
(`commands/dashboard/importers/v1/__init__.py`) explicitly rebuilds
`dashboard_slices` from the chart uuids it finds in each dashboard's
`position_json`, so a stale/missing association in the live instance gets
corrected automatically once the bundle is reimported. Confirmed the fixed
association survived a fresh export.

**Real issue found: `event_time` displayed inconsistently across the two
point-lookup tables (2026-09-13).** Originally `event_time` on
`wallet_transactions_direct_kafka` was kept as `VARCHAR` (the producer's
raw ISO 8601 string, e.g. `2026-09-13T03:42:13.609764+00:00`) rather than
parsed to `DATETIME` — reasoning that ISO 8601's lexicographic order
already matches chronological order, so sorting/`MAX()` work fine without
a `STR_TO_DATE` expression in the Routine Load `COLUMNS` clause. That
reasoning was correct but produced a confusing side-by-side dashboard: the
RisingWave-mediated table's `event_time` (a real `DATETIME`, via
`CAST(event_time AS TIMESTAMP)` in the RisingWave sink) displayed as
`2026-09-13 03:42:18`, while the direct-Kafka table's displayed as the raw
string — spotted live when a user asked "why do timestamps look different
between the two tables?" and then, reasonably, "can't we have a timestamp
in the second case as well?"

Fixed by parsing `event_time` to a real `DATETIME` in the Routine Load
`COLUMNS` clause instead: list the raw JSON field under a temp name
(`event_time_raw`) and compute the real column from it —
`event_time = str_to_date(substr(event_time_raw, 1, 26), '%Y-%m-%dT%H:%i:%s.%f')`.
`SUBSTR(...,1,26)` drops the fixed 6-character `+00:00` suffix (always
present and always this length, since the producer always emits UTC),
leaving a string `str_to_date` can parse with microsecond precision intact
— confirmed live on a throwaway table before touching the real one. Applied
the same fix to `wallet_transactions_log`'s Routine Load (the synchronous-MV
add-on below) for consistency across all three wallet tables.

(A natural follow-up question: is `DATETIME` accurate enough, and does
StarRocks even have a separate `TIMESTAMP` type? Short answer: StarRocks
has **no `TIMESTAMP` type at all** — `DATETIME` is the only temporal
column type, supports microsecond precision since v3.3.5 (this project
runs v4.1.4), and is timezone-naive, same reason the sink rejects
`TIMESTAMP WITH TIME ZONE` above. See the `starrocks` skill's "Temporal
types" section for the full detail — not worth duplicating here.)

Also had to refresh Superset's own cached column metadata for both
affected datasets after this — Superset caches each column's type at
dataset-creation time and doesn't auto-detect an underlying schema change.
`PUT /api/v1/dataset/{id}/refresh` picked up the new `DATETIME` type, but
left `is_dttm: false`; had to `PUT` the dataset's `columns` array directly
(`?override_columns=true`) to set `is_dttm: true` on `event_time` to match
the RisingWave-mediated table's dataset, so both charts format the column
identically. Worth remembering for any future StarRocks column-type
change on a table Superset already has a dataset for.

**A second real issue found while applying this fix:** since the column
type change required dropping and recreating both tables, the
`wallet_transactions_log` asset's rollup-MV idempotency check (`SHOW ALTER
TABLE ROLLUP FROM <schema>`, added when building the synchronous-MV
add-on) turned out to be checking the wrong thing — that command returns
**job history**, which survives a `DROP TABLE` + recreate under the same
name. After recreating the table, the asset saw the old, stale `FINISHED`
record from the dropped table and concluded the rollup already existed,
silently skipping recreation — `EXPLAIN` confirmed queries were scanning
the base table directly, not the rollup, with no error anywhere to signal
it. Fixed by checking `DESC <table> ALL` instead, which reflects the
table's actual current indexes rather than historical job records. Applied
the missing `CREATE MATERIALIZED VIEW` manually to recover the live table,
then verified the corrected check behaves idempotently on a subsequent run
(`EXPLAIN` still showed `rollup: mv_wallet_type_rollup` afterward).

## Add-on comparison: partial-column update, no RisingWave (2026-09-12)

A third write pattern, distinct from both the streaming upsert (full-row
replace on a matching key) and the direct-Kafka comparison above (same
full-row replace, different ingestion path): StarRocks Primary Key tables
support **partial-column update** — writing only some columns of a row,
leaving the rest untouched, with no need to know or resend the columns you
don't own. Motivated by suggestion #4 from a StarRocks-skill-driven review
of this project's `dbt_starrocks/` inventory: "a PK-table-specific feature
... completely unexplored here."

**Scenario modeled:** an independent fraud-review service that only ever
touches the `status` column and knows nothing about `amount`/`type`/
`account_id` — a genuinely different pattern than one writer owning the
whole row, and something a Unique Key table's whole-row-replace model can't
express at all (its only write path is "reload the entire row with the same
key").

**Built:**
- `scripts/wallet_producer.py` now also emits, independently of the
  original transaction/reversal stream, a `{transaction_id, status}`-only
  event to a **separate** Kafka topic (`wallet_status_updates`) for a
  random subset of settled, non-reversed transactions (`--status-update-rate`,
  default 0.15, delayed `--status-update-delay` seconds, default 8.0 —
  gated on NOT already chosen for reversal, so a single row's correction
  path stays demo-legible instead of muddying which mechanism did what).
- `orchestration/assets/wallet_direct_kafka_setup.py` — a second Routine
  Load job (`wallet_status_update_load_job` asset,
  `wallet_status_update_load` job name) on the *same*
  `wallet_transactions_direct_kafka` table, reading the new topic with
  `"partial_update" = "true"` and `COLUMNS(transaction_id, status)`. Applied
  only to the direct-Kafka table, not the RisingWave-mediated one — no
  RisingWave sink option needed or tested for this comparison.

**Verified live before building anything** (per this project's established
practice of testing StarRocks claims rather than trusting docs at face
value): created a throwaway Primary Key table with columns `id, a, b, c`,
a Routine Load job with `partial_update = true` writing only column `a`,
produced one event, confirmed column `a` changed while `b`/`c` were
untouched — then built the real thing.

**Confirmed after building:**
```sql
SELECT transaction_id, account_id, type, amount, status, event_time
FROM sr_local_db_sr_local_db.wallet_transactions_direct_kafka
WHERE transaction_id = '<a flagged id>';
```
showed `status = 'flagged'` with `account_id`/`type`/`amount`/`event_time`
all exactly as originally written — the status-update writer never touched
them, and didn't need to know their values to write the row.

**Superset:** added a table chart ("Wallet Transactions — Flagged via
Partial Update [status-only writer, no RisingWave]") over
`wallet_transactions_direct_kafka`, filtered to `status = 'flagged'`, plus a
markdown block explaining the mechanism — placed in its own row right after
the direct-Kafka comparison row, before the Count/Total-Amount bar charts.
Linked to the dashboard correctly on creation this time (`dashboards: [2]`
in the same `POST /api/v1/chart/` call) — see the "real issue found" note
in the direct-Kafka section above for what happens when that's omitted.

## Add-on comparison: PK-table UPDATE and DELETE via plain SQL, no pipeline at all (2026-09-13)

A fourth capability, and the cheapest of the four add-ons: StarRocks
Primary Key tables support genuine `UPDATE ... SET ... WHERE ...` and
`DELETE FROM ... WHERE ...` as plain SQL DML. Unique Key (and Duplicate/
Aggregate Key) tables can't do this at all — their only write path is
reloading a row with the same key; there's no ad-hoc `UPDATE`/`DELETE`
statement for them. Motivated by suggestion #3 from the same
StarRocks-skill-driven review that produced the other two add-ons.

**Why this is a distinct capability, not a variant of the others:** every
other write path in this doc (streaming upsert, direct-Kafka Routine Load,
partial-update Routine Load) goes through the event pipeline — a new
Kafka message triggers the change. This one bypasses the pipeline
entirely: an operator runs SQL directly against the table, with no event,
no topic, no Routine Load job, no RisingWave sink in the picture at all.

**Not a new table or asset — just a live-demo action**, same pattern as the
"graceful degradation" demo in
[SR_POC_LIVE_DEMO_RUNBOOK.md](SR_POC_LIVE_DEMO_RUNBOOK.md#demo-graceful-degradation-risingwave-outage):
run the SQL in a client in front of the audience, then refresh the Point
Lookup chart to show the change.

```sql
-- pick a transaction_id that's already `settled` and past its ~5s
-- reversal window first (see caveat below)
UPDATE sr_local_db_sr_local_db.wallet_transactions
SET status = 'under_review'
WHERE transaction_id = '<a settled id>';

DELETE FROM sr_local_db_sr_local_db.wallet_transactions
WHERE transaction_id = '<a settled id>';
```

**Confirmed live (2026-09-13):** ran both against the RisingWave-mediated
`wallet_transactions` table (works identically against
`wallet_transactions_direct_kafka` — it's the same Primary Key table type).
`UPDATE` changed only `status`, `account_id`/`type`/`amount`/`event_time`
all exactly as before. `DELETE` removed the row entirely —
`COUNT(*)`/`COUNT(DISTINCT transaction_id)` both dropped by exactly one,
invariant still holds. Both confirmed visible through Superset's own
`/api/v1/chart/data` (`force: true`) immediately after, no cache/refresh
issue anywhere.

**Caveat worth stating during the demo, not discovering live:** pick a
`transaction_id` that's already `settled` and past its ~5s reversal window
(and, if demoing on the direct-Kafka table specifically, also past the
~8s status-update window) before running the `UPDATE`/`DELETE` —
otherwise the producer's own pending reversal or status-update event for
that same row could land moments later and silently overwrite (or,
for the deleted row, re-insert) whatever was just shown, which would look
like a bug rather than the two independent mechanisms operating normally
side by side.

## Add-on comparison: synchronous (rollup) materialized view, no RisingWave (2026-09-13)

A fifth capability, and the only one of the five add-ons that isn't about
a write path — it's about a *read-side* contrast: every MV elsewhere in
this project (`mv_unified_funnel_summary`, `mv_funnel_daily_country_rollup`,
`mv_iceberg_countries_cache`) is **asynchronous** — it needs an explicit
`REFRESH` to pick up new data, which is exactly why staleness/refresh
handling has been a recurring theme in this project. A **synchronous**
MV (a "rollup") is fundamentally different: it's maintained directly by
the storage engine on a single base table, updates in lockstep with every
write, and has **no `REFRESH` statement at all** — it's not possible to
issue one.

**Constraint that rules out attaching this to any existing table in this
project, checked against docs before building:** synchronous MVs only work
on `DUPLICATE KEY` or `AGGREGATE KEY` base tables — not `PRIMARY KEY`
(both `wallet_transactions` and `wallet_transactions_direct_kafka` are
Primary Key), and only on a local StarRocks-native table, no external
catalog and no joins (which rules out ever using this for the Funnel
Dashboard — see the exploratory discussion this add-on came from). So this
needed its own base table.

**Built:**
- `wallet_transactions_log` — a new `DUPLICATE KEY (transaction_id)` table,
  fed by a *third* independent Routine Load job
  (`wallet_log_kafka_load`) reading the same `wallet_transactions` Kafka
  topic. Unlike the two Primary Key tables, Duplicate Key keeps **every**
  event as a separate row — an original transaction and its reversal both
  land as distinct rows here, rather than the reversal overwriting the
  original.
- `mv_wallet_type_rollup` — a synchronous rollup:
  ```sql
  CREATE MATERIALIZED VIEW sr_local_db_sr_local_db.mv_wallet_type_rollup AS
  SELECT type, SUM(amount) AS total_amount, COUNT(transaction_id) AS event_count
  FROM sr_local_db_sr_local_db.wallet_transactions_log
  GROUP BY type;
  ```
  Queried transparently: you query `wallet_transactions_log GROUP BY type`
  directly (never the rollup by name — it's an index, not a separate
  queryable object), and the optimizer silently redirects, same "aha" as
  the async query-rewrite demo but with zero possible staleness.
- `orchestration/assets/wallet_sync_mv_setup.py` — idempotent Dagster
  asset creating all three (table, Routine Load job, rollup MV), wired
  into `wallet_pipeline_setup_job`.

**Two real gotchas found, both verified live on a throwaway table before
touching the real one:**
1. `COUNT(*)` is **rejected** by synchronous MV creation:
   `"The materialized view currently does not support const expr in
   select statement: {}. Please use Asynchronous Materialized View
   instead."` `COUNT(<real column>)` (e.g. `COUNT(transaction_id)`) works
   fine — StarRocks apparently parses the bare `*` as a constant
   expression in this specific code path, unlike ordinary `SELECT`s where
   `COUNT(*)` is completely normal.
2. `CREATE MATERIALIZED VIEW IF NOT EXISTS` does **not** suppress the
   "already exists" error for synchronous MVs the way it does for tables —
   confirmed live: re-running the exact same `IF NOT EXISTS` statement
   against an already-built rollup still errors with `"Materialized
   view[...] already exists in the table ..."`. The Dagster asset checks
   existence manually instead (there's no `information_schema` view for
   sync MVs the way there is for async ones via
   `information_schema.materialized_views`) — via `DESC <table> ALL`,
   which reflects the table's actual current indexes. An earlier version
   checked `SHOW ALTER TABLE ROLLUP FROM <schema>` instead, which returned
   **job history** rather than current state — that history survives a
   `DROP TABLE` + recreate under the same name, so after this table's
   schema was later migrated (see "Real issue found: `event_time`
   displayed inconsistently" above), the asset saw a stale `FINISHED`
   record from the dropped table and silently skipped recreating the
   rollup on the new one, with `EXPLAIN` confirming queries were scanning
   the base table directly and no error anywhere to signal it. Switched to
   `DESC ... ALL` after finding this live.

**Confirmed live (2026-09-13):** inserted/streamed rows into
`wallet_transactions_log`, queried the base table with a matching
`GROUP BY` immediately after each batch — the aggregate was correct every
time, no `REFRESH` call anywhere in the whole process. `EXPLAIN` confirmed
the rewrite: `rollup: mv_wallet_type_rollup` in the plan. With the
producer running, row count and the four-way `type` breakdown both kept
advancing live.

**Superset:** added a table chart ("Wallet Type Breakdown — Synchronous
Rollup MV [zero-lag, no REFRESH ever]") over `wallet_transactions_log`,
`query_mode: aggregate` grouped by `type` with `SUM(amount)`/
`COUNT(transaction_id)` metrics, plus a markdown block explaining the
mechanism — placed in its own row right after the partial-update row,
before the Count/Total-Amount bar charts.

## Everything runs through script runner + Dagster

| Piece | How it runs |
|---|---|
| Producer (transactions, reversals, and status updates) | `bin/3_run_wallet_producer.sh` via script runner |
| RisingWave source | Dagster asset, `dbt/` project (`AssetKey(["public", "src_wallet_transactions"])`) |
| RisingWave → StarRocks sink | Dagster asset, `dbt/` project (`AssetKey(["public", "sink_wallet_transactions_to_starrocks"])`) |
| StarRocks Primary Key table (RisingWave-mediated) | Dagster asset, `dbt_starrocks/` project (`AssetKey(["sr_local_db", "wallet_transactions"])`) |
| StarRocks Primary Key table + Routine Load (direct Kafka, full-row) | Dagster asset, `orchestration/assets/wallet_direct_kafka_setup.py` (`wallet_transactions_direct_kafka`) |
| Second Routine Load job (partial-column status update, same table) | Dagster asset, `orchestration/assets/wallet_direct_kafka_setup.py` (`wallet_status_update_load_job`) |
| Duplicate Key log table + Routine Load + synchronous rollup MV | Dagster asset, `orchestration/assets/wallet_sync_mv_setup.py` (`wallet_transactions_log`) |
| Full pipeline build (all four paths) | `wallet_pipeline_setup_job` (confirmed via the Dagster webserver GraphQL API, run SUCCESS) |
| Visualization | Superset, dashboard "Wallet Upsert Demo" |

No manual `starrocks-init`-style DDL step needed for this one, unlike the
external catalogs — the native Primary Key table is fully dbt-managed.

The `UPDATE`/`DELETE` add-on above is the one exception to "everything runs
through script runner + Dagster" — it's a live SQL action against the
already-built table, no script/asset/job involved by design.

## How to reproduce

```bash
# 1. bring the stack up, then start generating transactions + reversals
./bin/3_run_wallet_producer.sh 5     # 5 TPS, via script runner or directly

# 2. build the pipeline (creates source, sink, StarRocks table)
#    via Dagster UI (http://localhost:3000 -> wallet_pipeline_setup_job -> Launch),
#    NOT via `dg launch` from a host shell (see the DNS caveat above)

# 3. verify upsert worked -- on BOTH tables (RisingWave-mediated and
#    direct-Kafka), same check
mysql -h127.0.0.1 -P9030 -uroot -e "
  SELECT count(*), count(DISTINCT transaction_id)
  FROM sr_local_db_sr_local_db.wallet_transactions"
mysql -h127.0.0.1 -P9030 -uroot -e "
  SELECT count(*), count(DISTINCT transaction_id)
  FROM sr_local_db_sr_local_db.wallet_transactions_direct_kafka"
# count(*) should equal count(DISTINCT transaction_id) on both

# 4. verify partial update worked -- some rows should be 'flagged' with
#    their original amount/type/account_id/event_time untouched
mysql -h127.0.0.1 -P9030 -uroot -e "
  SELECT transaction_id, account_id, type, amount, status, event_time
  FROM sr_local_db_sr_local_db.wallet_transactions_direct_kafka
  WHERE status = 'flagged' LIMIT 5"

# 5. verify the synchronous rollup MV is live and current -- query the
#    BASE table (never the rollup by name), then EXPLAIN the same query
#    to confirm the optimizer actually used the rollup
mysql -h127.0.0.1 -P9030 -uroot -e "
  SELECT type, SUM(amount) AS total_amount, COUNT(transaction_id) AS event_count
  FROM sr_local_db_sr_local_db.wallet_transactions_log GROUP BY type"
mysql -h127.0.0.1 -P9030 -uroot -e "
  EXPLAIN SELECT type, SUM(amount) AS total_amount, COUNT(transaction_id) AS event_count
  FROM sr_local_db_sr_local_db.wallet_transactions_log GROUP BY type" | grep -i rollup
# expect: rollup: mv_wallet_type_rollup
```
