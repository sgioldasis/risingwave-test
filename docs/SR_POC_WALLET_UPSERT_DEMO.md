---
title: StarRocks Upsert / Point-Lookup Demo (Wallet-Inspired)
description: Design and build log for demonstrating StarRocks Primary Key table upsert visibility and point lookups, motivated by the PAM Operational Query Layer proposal
---

<!-- markdownlint-disable-file -->

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
existing funnel producer), backed by `scripts/wallet_producer.py`. Emits
synthetic events onto the Kafka topic `wallet_transactions` (added to
`redpanda-init`'s topic-creation list in `docker-compose.yml`):

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

## Everything runs through script runner + Dagster

| Piece | How it runs |
|---|---|
| Producer (incl. reversal simulation) | `bin/3_run_wallet_producer.sh` via script runner |
| RisingWave source | Dagster asset, `dbt/` project (`AssetKey(["public", "src_wallet_transactions"])`) |
| RisingWave → StarRocks sink | Dagster asset, `dbt/` project (`AssetKey(["public", "sink_wallet_transactions_to_starrocks"])`) |
| StarRocks Primary Key table | Dagster asset, `dbt_starrocks/` project (`AssetKey(["sr_local_db", "wallet_transactions"])`) |
| Full pipeline build | `wallet_pipeline_setup_job` (confirmed via the Dagster webserver GraphQL API, run SUCCESS) |
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

# 3. verify upsert worked
mysql -h127.0.0.1 -P9030 -uroot -e "
  SELECT count(*), count(DISTINCT transaction_id)
  FROM sr_local_db_sr_local_db.wallet_transactions"
# count(*) should equal count(DISTINCT transaction_id)
```
