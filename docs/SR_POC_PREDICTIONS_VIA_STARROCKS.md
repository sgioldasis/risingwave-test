---
title: ML Predictions Served via StarRocks
description: Writing ML predictions into a StarRocks Primary Key table so the dashboard can read them via SQL instead of proxying to the ML service — tried, then fully reverted
---

<!-- markdownlint-disable-file -->

## Status: reverted (2026-09-09)

Built, fully wired, and working end to end the same day — then reverted at
the user's request ("introducing StarRocks to Predictions just makes it
more complicated. Maybe go back to RisingWave direct access?"), after a
string of operational issues made the actual cost of this path clear. See
[Postmortem](#postmortem-why-this-was-reverted) at the bottom for the full
story and what to reconsider before trying this again. The rest of this doc
describes the design and implementation as they existed while this was
live — kept for reference, not as a description of the current system.
`/api/predictions/next` is back to proxying live to `ml/serving`, exactly
as described in "What changed" below, i.e. the state *before* this doc's
own changes.

## What changed

Previously, the dashboard's Predictions tab worked like this:

```
frontend -> dashboard backend (/api/predictions/*) -> HTTP proxy -> ml/serving (port 8001) -> live compute -> response
```

Every request round-tripped synchronously to the ML service. If `ml/serving`
was down or hung, the whole tab broke — which is exactly what happened on
2026-09-09 (see the `RISINGWAVE_HOST`/host-networking fix in
`bin/4_run_ml_serving.sh`).

Added a second path: `ml/serving` now also writes each prediction into a
StarRocks table, and the dashboard backend has a new endpoint that reads
from StarRocks directly instead of calling `ml/serving` over HTTP.

## Why this isn't "StarRocks improving the ML predictions"

Worth being explicit about this, since it came up directly: StarRocks does
not compute anything here and doesn't make the model better, faster, or
more accurate. The prediction is still computed exactly the same way, by
the same River online-learning model, at the same cadence. StarRocks is
purely a **write target and read surface** for the result.

The actual value is:

* **Resilience** — the dashboard degrades to "last known prediction" when
  `ml/serving` is down, instead of erroring outright.
* **A unified query surface** — predictions become just another StarRocks
  table, in principle joinable with `funnel_summary`/country names in a
  single query, or visible to any MySQL-wire client (a BI tool) with zero
  awareness that `ml/serving` exists.

## The table

`sr_local_db_sr_local_db.funnel_prediction_scoreboard` — a StarRocks
**Primary Key** table, one row per metric (not per country — the model
predicts single-series aggregate metrics, not per-country breakdowns):

```sql
CREATE TABLE funnel_prediction_scoreboard (
    metric           VARCHAR(32) NOT NULL,
    predicted_value  DOUBLE,
    confidence       DOUBLE,
    model_type       VARCHAR(64),
    samples_learned  INT,
    predicted_for    DATETIME,
    written_at       DATETIME
) PRIMARY KEY (metric)
DISTRIBUTED BY HASH(metric)
PROPERTIES ('replication_num' = '1');
```

Managed as an idempotent `on-run-start` hook in
`dbt_starrocks/dbt_project.yml`, not a dbt model — a normal dbt
materialization would `DROP`/recreate the table on every build, wiping the
row data an external process (the ML service) owns. The hook only ensures
the table exists; it never touches its contents.

A StarRocks Primary Key table's plain `INSERT INTO` is upsert-by-default —
no special `UPSERT`/`REPLACE` syntax needed, which is exactly the "partial
update" behavior that made the PK model worth demoing in the first place
(see the earlier StarRocks-features brainstorm in project history).

## The write path

`ml/serving/starrocks_writer.py` (new): a small SQLAlchemy engine
(`STARROCKS_URL`, defaults to `mysql+pymysql://root@localhost:9030/sr_local_db_sr_local_db`
for host-side execution — same pattern as the `RISINGWAVE_HOST` fix, since
`ml/serving` runs on the host, not in Docker) that upserts one row per
metric present in the `/predict` response.

Wired into `ml/serving/main.py`'s `/predict` handler (`predict_all()`) via
`_try_write_predictions()`, called right before both return points (online
mode and batch mode). Wrapped in try/except so a StarRocks outage or
network hiccup **never breaks the `/predict` response itself** — it's a
side effect, not a dependency.

## The read path

New endpoint, `modern-dashboard/backend/api.py`:
`GET /api/predictions/scoreboard` — a plain `SELECT * FROM
funnel_prediction_scoreboard`, reshaped into the same
`{metric: {value, confidence, ...}}` shape `ml/serving`'s own response
uses.

**Deliberately added as a new endpoint, not a replacement** for the
existing `/api/predictions/next`. That handler does substantial
field-mapping logic (`model_version`/heuristic-detection, `mode`/`source`
badges the frontend depends on) that the scoreboard table doesn't capture
1:1 — replacing it outright risked breaking the just-fixed Predictions tab
for no clear benefit. This mirrors the project's existing
`/api/query/funnel` vs `/api/query/funnel/cached` pattern: same data, two
paths, live vs. StarRocks-backed.

## Verified (2026-09-09)

1. `GET /predict` on `ml/serving` — response includes all 5 metrics.
2. Immediately queried `funnel_prediction_scoreboard` directly — all 5 rows
   present, values matching the `/predict` response exactly.
3. `GET /api/predictions/scoreboard` on the dashboard backend — returns the
   same 5 metrics, sourced entirely from StarRocks, no call to `ml/serving`
   involved.

## Not yet done (as of the last update before revert)

The frontend (`PredictionsTab.jsx`) still called `/api/predictions/next`
(the live path) exclusively — `/api/predictions/scoreboard` existed and
worked but wasn't wired into any UI. It was later made the *sole* path for
`/api/predictions/next` itself (see Postmortem), which is what actually
surfaced the problems that led to the revert.

## Postmortem: why this was reverted

After the initial "read-only fallback" version above, the design was
pushed further at the user's direction to make StarRocks the dashboard's
**sole** prediction source — `ml/serving` writing proactively on a
background timer, `/api/predictions/next` reading only from StarRocks
(never calling `ml/serving` over HTTP), plus a `degraded` flag and a UI
badge for staleness. Each of these was individually justified and verified
working at the time. In combination, they added real, compounding
operational cost:

1. **A staleness threshold that needed constant re-tuning.** Started at
   30s, dropped to 5s (too tight — false positives), raised to 10s, then
   20s, as actual write-timing measurements kept revealing more jitter than
   assumed. There was no single correct number — it depended on how loaded
   the host happened to be at any given moment.
2. **A write interval that became a load-management knob.** Started at 5s,
   tightened to 1s (for faster test-cycle feedback), then had to be eased
   back to 3s because the background writer plus the dashboard's own
   incidental `/predict` calls meaningfully added to an already
   heavily-loaded dev host (RisingWave, StarRocks FE+CN, Trino, Dagster,
   MinIO, Redpanda all running simultaneously).
3. **A real concurrency bug introduced by the fix for #2's jitter.**
   Offloading the blocking RisingWave/StarRocks calls to
   `asyncio.to_thread` (the technically-correct fix for event-loop
   stalling) let multiple write cycles run concurrently, which could
   interleave at the row level — the 5 metric rows ending up with visibly
   different `written_at` ages (6s/20s/9s/15s/6s in one observed case)
   instead of landing as one coherent batch. Fixable (a lock), but another
   fix stacked on top of a fix.
4. **A confounding variable that made testing unreliable.** One
   measurement showing a ~30s write gap turned out to be caused by
   host-wide CPU contention (`load average` ~20, several containers at
   80-96% CPU), not the code under test — meaning "did the last change
   help?" stopped being answerable from a single test run.
5. **Restart-order dependencies that produced confusing symptoms.** Because
   two independent processes (`ml/serving`, the dashboard backend) each
   needed a restart to pick up related-but-separate changes (a write
   fix in one, a threshold fix in the other), several "still broken after
   restart" reports during testing turned out to be "restarted the wrong
   one" or "restarted one but not both" — not new bugs, but real,
   repeated confusion in practice.

None of these individually was a reason to abandon the approach — each had
a legitimate diagnosis and fix. But the cumulative operational surface
(two processes, a background loop, a tunable interval, a tunable
threshold, a UI badge, and a new failure mode in the write path itself)
grew large for what the feature actually bought: resilience against
`ml/serving` being unreachable, which happens rarely, and a "unified query
surface" benefit that was never actually used for anything (no query ever
joined predictions with other StarRocks tables; no BI tool ever connected
to it). The user's read at the end -- "this just makes it more
complicated" -- matches that: the cost was concrete and immediate, the
benefit stayed hypothetical.

**If this is reconsidered in the future**, worth doing differently:

* Pick the write interval and staleness threshold from a real load
  measurement on the target host *first*, not iteratively during a live
  demo session.
* Decide up front whether concurrent writes need a lock, rather than
  discovering it from an observed symptom.
* Test failure scenarios (stop/restart) in a quiet environment, not while
  the whole stack is also running other work -- the confounded 30s-gap
  measurement above is the clearest example of why that matters.
* Consider whether the "unified query surface" benefit has an actual,
  planned consumer before building for it -- here it never did.
