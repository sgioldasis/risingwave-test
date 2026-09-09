---
title: StarRocks Evaluation Summary — Real Benefits and Poor Fits
description: A rigorous, evidence-based tally of where StarRocks earned its place in this architecture and where it was tried and didn't pan out
---

<!-- markdownlint-disable-file -->

## Purpose

This project has, over one extended evaluation, actually built and measured
several candidate StarRocks use cases against this stack (RisingWave,
Databricks Unity Catalog, MinIO/Lakekeeper Iceberg, and a live ML
prediction pipeline) — not just discussed them. This doc collects the
verdicts in one place: what earned a real, evidence-backed "yes," and what
was tried in good faith and reverted, with the reason why. Both halves are
included deliberately — a persuasive-only writeup is a pitch, not an
evaluation.

Every claim below links back to the doc or session finding it came from,
so none of this is asserted from memory.

## Genuine wins

### 1. Federated queries across heterogeneous storage

`dashboard_funnel_serving` (see
[SR_POC_UNIFIED_PLAN.md](SR_POC_UNIFIED_PLAN.md)) unions live data from
three separate systems in one SQL layer, with no ETL step copying data
between them:

- `risingwave.public.funnel_summary` — live, via JDBC federation
- `databricks_uc.sr_poc_external.funnel_summary_historical` — live, via
  Iceberg REST (Unity Catalog)
- Country reference data, same catalog

This is StarRocks's core value proposition and it is the actual,
currently-running architecture of this demo, not a proof of concept.

### 2. Transparent materialized-view query rewrite

Fully built, measured, and still valid after the later StarRocks storage
migration — see
[SR_POC_QUERY_REWRITE_DEMO.md](SR_POC_QUERY_REWRITE_DEMO.md). An analyst
(or BI tool) queries a **raw** Iceberg table with no knowledge that a
matching materialized view exists; StarRocks silently rewrites the query
plan to hit the pre-aggregated MV instead.

- Confirmed via `EXPLAIN`: plan flips between `OlapScanNode` (MV, rewrite
  on) and `IcebergScanNode` (raw table, rewrite off) for the *identical*
  query text.
- Measured speedup: **~20-35x** (0.05-0.15s vs 1.8-5.5s), even at trivial
  data volume (a few thousand raw rows) — the gap should only widen as
  data grows, since most of the "without rewrite" cost is Iceberg
  metadata/file-list overhead the rewrite path skips entirely.
- Caveat also documented honestly: this only works for a specific query
  shape (stable SPJG rollup, no volatile predicates). The project's other
  MV (`mv_unified_funnel_summary`, a `UNION ALL` with
  `CURRENT_TIMESTAMP()`-relative filters) is explicitly disqualified
  (`QUERY_REWRITE_STATUS: UNSUPPORTED_DEFINITION`) — rewrite is not a
  blanket capability.

### 3. Shared-data architecture (compute/storage separation) on MinIO

See [SR_POC_STARROCKS_SHARED_DATA_MINIO.md](SR_POC_STARROCKS_SHARED_DATA_MINIO.md).
Migrated the main `starrocks` service from `allin1-ubuntu` (shared-nothing)
to a real FE+CN pair backed by the stack's own MinIO — now the actual
production setup, not a side experiment.

- Confirmed working end-to-end: storage volume creation, table write/read,
  all three external catalogs, and a full `modern_dashboard_setup_job`
  rebuild, all `RUN_SUCCESS`.
- Real operational win: compute (CN nodes) now scales independently of
  storage — adding a CN doesn't require rebalancing data, unlike the old
  shared-nothing setup.
- Notably *easier* than the equivalent Azure ADLS attempt
  ([SR_POC_STARROCKS_SHARED_DATA_AZURE.md](SR_POC_STARROCKS_SHARED_DATA_AZURE.md)),
  which is confirmed feasible but blocked indefinitely on Azure credential
  limitations (ADLS2 storage volumes only support Managed/Workload
  Identity, not a plain client secret). MinIO's static access-key/secret
  model just works.

### 4. BI tool connectivity, for free

StarRocks speaks the MySQL wire protocol natively — the dashboard backend
already connects this way. Any MySQL-compatible BI tool (Power BI,
Tableau) could connect directly with zero custom integration. **Not yet
built as a demo**, but the zero-effort nature of it is itself the point:
worth a follow-up if a BI-tool story is wanted.

## Confirmed poor fits

Included deliberately, not as a weakness of the evaluation but as
evidence it's a rigorous one — every workload doesn't benefit from every
tool, and this project generated real data on where the line is.

### 1. Latency-sensitive, high-frequency point reads or writes

Tried twice, independently:

- **Predictions via StarRocks** (built, worked, then fully reverted — see
  [SR_POC_PREDICTIONS_VIA_STARROCKS.md](SR_POC_PREDICTIONS_VIA_STARROCKS.md)
  and its postmortem). Routing the dashboard's ML predictions through a
  StarRocks-backed table added a background write loop, a tunable write
  interval, a tunable staleness threshold, and a UI badge — real,
  compounding operational cost — for a resilience benefit
  (`ml/serving` briefly unreachable) that's real but rare, and a
  "unified query surface" benefit that was never actually exercised by
  anything.
- **Considered and rejected**: routing the ML prediction algorithm's
  frequent (near-1/sec) RisingWave reads through StarRocks's `risingwave`
  JDBC catalog instead of a direct `psycopg2` connection. StarRocks's
  documented external-catalog planning tax (hundreds of ms to ~2s per
  touch — the same cost visible in the zero-copy dashboard queries) would
  add that cost to every single prediction computation, directly against
  a pipeline that was independently tuned this same session for
  sub-20-second responsiveness.

**Takeaway**: StarRocks's per-query external-catalog overhead makes it a
poor fit for anything polled sub-second, or for a workload where the
whole point is minimizing latency on a single row.

### 2. Frequent small-batch streaming sinks

A native RisingWave→StarRocks sink (Stream Load-based) was tried on the
funnel data early in this project's history (see
[SR_POC_ICEBERG_COUNTRIES_MIGRATION.md](SR_POC_ICEBERG_COUNTRIES_MIGRATION.md)).
Because it flushes on every checkpoint, the frequent small-batch writes
overloaded StarRocks's compaction on the receiving table and made query
latency *worse*, not better. Confirmed relevant again later: the `funnel`
model checkpoints roughly every 4 seconds
(`barrier_interval_ms=2000 * checkpoint_frequency=2`) — a sink on it today
would very likely reproduce the same failure mode.

**Takeaway**: StarRocks (like most OLAP columnar engines) wants larger,
less-frequent batch loads, not a continuous streaming trickle.

### 3. Reflexively reaching for StarRocks as a resilience layer

The predictions-via-StarRocks episode (above) is also a caution in its
own right: "make X more resilient" is not automatically a good reason to
put StarRocks in the path. The added complexity should be justified by an
actual, exercised consumer of the resulting query surface — in that case,
there wasn't one.

## Recommendation

For a demo audience: lead with **#1 and #2** from the wins list — they
show StarRocks doing something a plain Iceberg query engine or a
single-database setup genuinely cannot (live federation across three
systems; free query-plan-level speedups with zero query changes). **#3**
(elastic shared-data compute) is strong material for an infrastructure/ops
audience specifically. Keep the poor-fits list ready for "what
*shouldn't* we use this for" — it's what makes this an evaluation and not
a sales pitch.

For future work: the BI-connectivity demo (#4) is the cheapest remaining
win to actually build. Nothing in the poor-fits list is worth revisiting
unless the underlying constraint changes (e.g., a future StarRocks release
meaningfully reduces external-catalog planning latency).
