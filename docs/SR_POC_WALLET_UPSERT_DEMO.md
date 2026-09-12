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
(`wallet_transactions_direct_kafka`, same schema, `event_time` kept as
`VARCHAR` rather than parsed to `DATETIME` at load time — ISO 8601's
lexicographic order already matches chronological order, so this avoids
needing a `STR_TO_DATE` expression in the Routine Load `COLUMNS` clause) and
a StarRocks Routine Load job (`wallet_direct_kafka_load`) that reads the
*same* `wallet_transactions` Kafka topic straight into it. No RisingWave
asset anywhere in this path — only the Kafka topic (already created by
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
(`COUNT(*) == COUNT(DISTINCT transaction_id)`). Lag against wall-clock time:
~2-3s for the RisingWave-mediated path (after the `commit_checkpoint_interval`
tuning above), ~1-2s for the direct Routine Load path — the direct path is
somewhat faster here, though both are close once the RisingWave sink was
tuned. The real difference this comparison demonstrates is architectural:
one fewer moving part, one fewer thing that can lag or fail, for a pipeline
that has no actual use for RisingWave's stream-processing capabilities.

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

## Everything runs through script runner + Dagster

| Piece | How it runs |
|---|---|
| Producer (transactions, reversals, and status updates) | `bin/3_run_wallet_producer.sh` via script runner |
| RisingWave source | Dagster asset, `dbt/` project (`AssetKey(["public", "src_wallet_transactions"])`) |
| RisingWave → StarRocks sink | Dagster asset, `dbt/` project (`AssetKey(["public", "sink_wallet_transactions_to_starrocks"])`) |
| StarRocks Primary Key table (RisingWave-mediated) | Dagster asset, `dbt_starrocks/` project (`AssetKey(["sr_local_db", "wallet_transactions"])`) |
| StarRocks Primary Key table + Routine Load (direct Kafka, full-row) | Dagster asset, `orchestration/assets/wallet_direct_kafka_setup.py` (`wallet_transactions_direct_kafka`) |
| Second Routine Load job (partial-column status update, same table) | Dagster asset, `orchestration/assets/wallet_direct_kafka_setup.py` (`wallet_status_update_load_job`) |
| Full pipeline build (all three paths) | `wallet_pipeline_setup_job` (confirmed via the Dagster webserver GraphQL API, run SUCCESS) |
| Visualization | Superset, dashboard "Wallet Upsert Demo" |

No manual `starrocks-init`-style DDL step needed for this one, unlike the
external catalogs — the native Primary Key table is fully dbt-managed.

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
```
