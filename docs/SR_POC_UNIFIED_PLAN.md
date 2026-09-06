---
title: StarRocks Unified Lakehouse Dashboard Architecture Memo
description: Architecture validation and handoff record for the StarRocks hot and cold funnel serving layer
---

<!-- markdownlint-disable-file -->

## Overview

This memo records the validated architecture for the modern dashboard serving
layer. The dashboard query path reads through StarRocks rather than querying
RisingWave directly. In the resulting design, StarRocks provides a unified
serving surface that combines the hot, recent data streamed from RisingWave with
cold historical data stored in Databricks Unity Catalog, while the live Kafka
consumer and SSE stream remain unchanged for real-time updates.

## Validated state (2026-09-06)

The StarRocks-backed serving layer is functionally validated in the current
project runtime.

* `GET /api/serving/status` returned `"status": "ready"` with all required
  catalogs present (`databricks_uc`, `lakekeeper_local`, `risingwave`)
* direct StarRocks validation confirmed populated serving data for the exact
  dashboard tables used in the query path:
  * `dashboard_funnel_serving`: 13 rows in the selected 16:00-16:12 window
  * `mv_unified_funnel_summary`: 13 rows in the same window
  * `dashboard_funnel_enriched`: 13 rows in the same window
* the live SSE endpoint emitted real funnel and stats payloads, confirming the
  Kafka consumer thread and the backend data path are active
* the query endpoints returned real dashboard data from the StarRocks serving
  path rather than synthetic or stale mock output

This is a functional architecture validation, not a final production sign-off.
The stack is operating as designed for the modern dashboard path, and the
remaining work is now performance, failure-handling, and operational hardening
rather than basic functional completion.

## Remaining work

The following items remain as explicit production-readiness gates before final
acceptance:

- [ ] Confirm p50/p95/p99 latency and concurrency targets for detail and
  aggregate queries across representative time ranges and hot/cold boundary
  conditions.
- [ ] Validate restart and recovery behavior for RisingWave and StarRocks,
  including Databricks catalog outages and stale or replayed event windows.
- [ ] Exercise duplicate-boundary, late-arrival, and stale-refresh scenarios to
  verify correctness of the unified serving layer.
- [ ] Confirm operational guardrails: alerting, monitoring, restoration
  runbooks, and cost/throughput assumptions under realistic workload patterns.
- [x] **Resolved (2026-09-06):** `mv_unified_funnel_summary` was unpartitioned,
  so every one-minute refresh fully rebuilt the entire view (~8.9s at the
  time). Adding `partition_by` was attempted and rejected by StarRocks:
  `Materialized view partition column in partition exp must be base table
  partition column` -- neither base table (`hot_funnel_summary` via JDBC,
  `funnel_summary_historical` as an unpartitioned external Iceberg table) is
  itself partitioned, so partition-level incremental refresh isn't available
  without first partitioning those base tables (a larger change touching
  Part 1's Iceberg table spec). Mitigated instead by widening the refresh
  interval from 1 to 5 minutes, cutting full-rebuild frequency 5x with no
  partitioning dependency -- deployed via `dg launch --job
  dbt_starrocks_build_job`, confirmed live: `REFRESH_POLICY: EVERY(INTERVAL 5
  MINUTE)`, refresh completed in ~16.6s, `serving/status` remained `ready`
  with both hot and cold watermarks populated. The MV still fully rebuilds
  each cycle; if cold-history volume grows enough to make even a 5-minute
  full rebuild a bottleneck, revisit by partitioning the underlying Iceberg
  table first.

## POC conclusion

The proof of concept supports introducing StarRocks as the dashboard serving
layer. StarRocks successfully federates the newest RisingWave windows through
JDBC with historical Databricks Iceberg data, while the dashboard backend uses
only the StarRocks MySQL endpoint. The query-time hot overlay removes the
asynchronous materialized-view refresh from the realtime dashboard query path.

The implementation also demonstrates deduplicated cold history, country
reference joins, StarRocks-derived enrichment and health metrics, and Dagster
provisioning without requiring a Databricks SQL warehouse for the existing
historical table.

## Implemented architecture

```text
Kafka producer
    -> RisingWave sources (src_page, src_cart, src_purchase)
    -> funnel_summary (1-minute tumbling MV)
         -> sink_funnel_to_kafka       -> Kafka topic "funnel"
         -> sink_funnel_to_postgres    -> local PostgreSQL (JDBC upsert)
         -> sink_funnel_to_rw_iceberg  -> Lakekeeper Iceberg (rw_managed_funnel)

modern-dashboard/backend/api.py
    -> background Kafka consumer thread -> in-memory cache -> /api/funnel, /api/funnel/stream (SSE)
    -> one SQLAlchemy connection to StarRocks MySQL wire (port 9030)
       -> query-time hot overlay from risingwave.public.funnel_summary
       -> cold history from mv_unified_funnel_summary
```

`funnel_summary` columns: `window_start`, `window_end`, `country`, `viewers`,
`carters`, `purchasers`, `view_to_cart_rate`, `cart_to_buy_rate`.

StarRocks currently has three external catalogs and a dbt-managed hot view:

* `databricks_uc` - Iceberg REST against Unity Catalog `de_dev`
  ([starrocks/init_catalog.sh](../starrocks/init_catalog.sh))
* `lakekeeper_local` - Iceberg REST against the local Lakekeeper/MinIO stack
* `risingwave` - JDBC federation against RisingWave `public`
* `hot_funnel_summary` - normalized StarRocks view over
  `risingwave.public.funnel_summary`

## Target architecture (to-be)

```text
Kafka producer
    -> RisingWave sources
    -> funnel_summary (1-minute tumbling MV)
         -> sink_funnel_to_kafka        (existing, keep as-is)
         -> sink_funnel_to_postgres     (existing, keep as-is)
         -> sink_funnel_to_rw_iceberg   (existing, keep as-is, Lakekeeper)
         -> sink_funnel_to_databricks   (NEW: Managed Iceberg table in Unity Catalog)

StarRocks
    -> databricks_uc catalog reads the new Managed Iceberg table (historical/cold data)
    -> a RisingWave-fed hot table or catalog holds the last N minutes (live data)
    -> async materialized view stores cold history and a fallback hot union

modern-dashboard/backend/api.py
    -> Kafka consumer thread, in-memory cache, /api/funnel, /api/stats, /api/funnel/stream (SSE)  (unchanged)
  -> /api/query/funnel and /api/query/funnel/aggregate query StarRocks (MySQL wire protocol)
  -> /api/funnel/enriched and /api/funnel/health use StarRocks-derived SQL metrics
```

## Part 1: RisingWave to Databricks sink

Add a new dbt sink model, `sink_funnel_to_databricks.sql`, modeled directly
on the existing [dbt/models/sink_funnel_to_rw_iceberg.sql](../dbt/models/sink_funnel_to_rw_iceberg.sql).

**Target table type matters.** Unity Catalog's Iceberg REST Catalog (IRC)
only supports external **write** access for genuinely Managed Iceberg
tables (`USING ICEBERG`). It does not support external writes to Delta
tables, with or without UniForm — UniForm is documented as read-only from
external engines. This sidesteps the entire UniForm/HMS governance
trade-off discussed in
[SR_POC_TESTING_PLAN.md](SR_POC_TESTING_PLAN.md#external-hms-versus-unity-catalog-irc-governance-trade-off):
the target must be created as a Managed Iceberg table in Unity Catalog from
the start, not a Delta table.

Draft sink shape (to refine before implementation):

```sql
CREATE SINK IF NOT EXISTS funnel_databricks_sink
FROM {{ ref('funnel_for_iceberg') }}
WITH (
    connector = 'iceberg',
  type = 'append-only',
  force_append_only = 'true',
  catalog.type = 'rest',
  catalog.uri = 'https://<workspace>/api/2.1/unity-catalog/iceberg-rest',
  catalog.oauth2_server_uri = 'https://login.microsoftonline.com/<tenant>/oauth2/v2.0/token',
  catalog.credential = '<client-id>:<client-secret>',
  catalog.scope = '2ff814a6-3304-4ab8-85cb-cd0e6f879c1d/.default',
  warehouse.path = 'de_dev',
    database.name = 'sr_poc_external',
    table.name = 'funnel_summary_historical',
  adlsgen2.account_name = '<new-storage-account>',
  adlsgen2.tenant_id = '<tenant-id>',
  adlsgen2.client_id = '<client-id>',
  adlsgen2.client_secret = '<client-secret>',
  commit_checkpoint_interval = 20
)
```

The working RisingWave configuration embeds OAuth and ADLS data-plane
credentials directly in `CREATE SINK`. The earlier `CREATE CONNECTION` probe
was not the successful path. Use a pre-created Managed Iceberg table and do
not rely on `create_table_if_not_exists` until the write path is validated.

The sink must be append-only. Unity Catalog does not accept the Iceberg delete
files produced by a normal upsert sink. Updates must be represented as new
rows and collapsed downstream with a latest-row query or materialized view.

### Open questions for Part 1

* **Validated:** the target table must have no
  `delta.feature.catalogManaged` protocol feature. On tables where it is
  present, an administrator must run:
  `ALTER TABLE ... DROP FEATURE catalogManaged` before external IRC writes.
* **Validated:** the target schema must exactly match the RisingWave sink
  relation. A mismatch in column count, names, or timestamp semantics fails
  sink validation before any data is written.
* **Validated:** RisingWave `v3.0.3` can write with the new service principal
  and new ADLS account when the target is converted and the sink is
  append-only.
* Should the target catalog/schema be a new schema (for example
  `de_dev.sr_poc_external`) or a dedicated catalog reserved for
  RisingWave-managed tables, to keep it clearly separated from
  human-managed Databricks tables? **Still open** — a governance/ownership
  decision, not resolvable by inspecting the code.
* **Resolved (2026-09-06):** the historical Databricks table uses grain
  `(window_start, country)`. Confirmed by inspecting the deployed
  [dbt_starrocks/models/mv_unified_funnel_summary.sql](../dbt_starrocks/models/mv_unified_funnel_summary.sql),
  whose cold branch dedupes with `GROUP BY window_start, country` — matching
  `sink_funnel_to_postgres`, not the `window_start`-only key on
  `sink_funnel_to_rw_iceberg`. Note: `funnel_summary.sql` currently hardcodes
  `country = 'GR'`, so this grain hasn't yet been exercised with more than
  one country value.

### Validated write spike (2026-09-05)

The write path was reproduced on a clean RisingWave `v3.0.3` stack using only
  the new service principal and the new PoC ADLS account
  `stkznneusrpoccdddevstd/sr-poc-cont1`.

  The required steps were:

  1. Disconnect the VPN before pulling the new RisingWave image, then reconnect
    before live Unity Catalog/ADLS testing.
  2. Pin the default Compose image to
    `risingwavelabs/risingwave:v3.0.3`.
  3. Reset only the RisingWave metadata/state volumes
    `risingwave-test_hummock-fs-store` and `risingwave-test_postgres-0`.
    Reusing v3.2-alpha metadata caused v3.0.3 migration failures.
  4. Start fresh v3.0.3 `meta-node-0`, `compute-node-0`,
    `frontend-node-0`, and compactor services.
  5. Create a Managed Iceberg target in
    `de_dev.sr_poc_external` and ensure its schema exactly matches the
    RisingWave relation, including timezone-aware `TIMESTAMP` columns.
  6. Create the RisingWave UC REST connection using Azure AD OAuth2 client
    credentials. UC metadata authentication and table discovery succeeded.
  7. Create a `type = 'append-only'` sink with
    `force_append_only = 'true'` and direct `adlsgen2` service-principal
    credentials.
  8. Insert a row after the sink was active and query the target through
    Databricks SQL.

  The exact marker row `minimal-v303-20260905` appeared in
  `de_dev.sr_poc_external.rw_irc_probe_20260902`, proving the complete path:

  ```text
  RisingWave v3.0.3
    -> Azure AD OAuth2 to Unity Catalog IRC
    -> new service principal data-plane access
    -> append-only Iceberg sink
    -> Managed Iceberg table in Unity Catalog
    -> Databricks SQL read
  ```

  The disposable RisingWave source, MV, and sink objects were cleaned up after
  the successful check. The marker row remains in the existing converted probe
  table as evidence. The production funnel-to-Databricks sink has not yet been
  created.

  The attempted v3.2-alpha run was not a valid final comparison: it reused
  metadata/state that later proved incompatible with v3.0.3, accumulated stale
  probe sink actors, and included several schema-mismatched disposable targets.
  The clean v3.0.3 run removed those confounders.

### Production sink validation

The production-shaped sink was deployed successfully on the clean RisingWave
`v3.0.3` stack on 2026-09-05:

1. Rebuilt `funnel_for_iceberg` through the existing dbt/Dagster path.
2. Confirmed the exact eight-column relation schema, including timezone-aware
   `window_start` and `window_end` columns.
3. Created the Managed Iceberg target
   `de_dev.sr_poc_external.funnel_summary_historical` with the matching schema.
4. Removed `delta.feature.catalogManaged` from the target before external
   writes.
5. Added [dbt/models/sink_funnel_to_databricks.sql](../dbt/models/sink_funnel_to_databricks.sql)
   with direct Unity Catalog OAuth2 metadata credentials, direct ADLS service
   principal credentials, `append-only`, `force_append_only = 'true'`, and a
   production checkpoint interval of 20.
6. Deployed the sink through dbt. A bounded live producer run crossed the
   checkpoint threshold and produced a Databricks commit.
7. Verified 26 rows in the Unity Catalog target, spanning windows from
   `2026-09-05T04:06:00Z` through `2026-09-05T06:00:00Z`.

The intermediate verification returned zero rows because the sink had not yet
reached its checkpoint interval. The later count confirms the complete
production path:

```text
RisingWave v3.0.3
  -> dbt/Dagster sink deployment
  -> Azure AD OAuth2 to Unity Catalog IRC
  -> ADLS service-principal data-plane access
  -> append-only Managed Iceberg writes
  -> Databricks SQL verification
```

Part 1 is complete for the historical funnel table. The StarRocks hot path,
unified view, Dagster setup job, and scoped dashboard endpoint migration are
implemented. Live endpoint validation has passed through StarRocks; the
remaining validation is a full day/night hot/cold soak.

### Current implementation status (2026-09-06)

The project has now moved from the design stage into the execution stage:

* The production Databricks sink is validated and writing to the Managed Iceberg
  table `de_dev.sr_poc_external.funnel_summary_historical`.
* The StarRocks dbt project has been scaffolded at
  [dbt_starrocks/dbt_project.yml](../dbt_starrocks/dbt_project.yml),
  [dbt_starrocks/profiles.yml](../dbt_starrocks/profiles.yml),
  [dbt_starrocks/models/hot_funnel_summary.sql](../dbt_starrocks/models/hot_funnel_summary.sql),
  and [dbt_starrocks/models/mv_unified_funnel_summary.sql](../dbt_starrocks/models/mv_unified_funnel_summary.sql).
* Governed dashboard serving models are defined in
  [dbt_starrocks/models/dashboard_funnel_serving.sql](../dbt_starrocks/models/dashboard_funnel_serving.sql)
  and [dbt_starrocks/models/dashboard_funnel_enriched.sql](../dbt_starrocks/models/dashboard_funnel_enriched.sql).
* The new StarRocks project parses successfully via the adapter: the command
  `uv run --with dbt-starrocks==1.12.0 dbt ls --project-dir dbt_starrocks --profiles-dir dbt_starrocks`
  discovered `4 models, 3 operations, 3 sources, 480 macros`.

`modern_dashboard_setup_job` completed successfully on 2026-09-06 after the
existing Databricks table was validated through StarRocks. The run created the
RisingWave and StarRocks objects required by the dashboard. Live endpoint
queries also passed through StarRocks; the remaining validation is a full
day/night hot/cold soak.

The governed serving models were built successfully with `dbt-starrocks`
(`PASS=7`). The dashboard detail, aggregate, enriched, and health endpoints
now query those models through StarRocks. After a fresh Redpanda volume reset,
the `funnel` topic was recreated, the producer was started, and the backend
consumed a current event through Kafka/SSE.

The dashboard bootstrap no longer depends on Trino. The country reference asset
validates the existing `lakekeeper_local.public.iceberg_countries` table through
StarRocks, matching the dashboard serving path and leaving Trino out of the
required runtime architecture.

The serving hardening endpoint `/api/serving/status` reports catalog
availability and hot, cold, and serving watermarks. It validated the
`databricks_uc`, `lakekeeper_local`, and `risingwave` catalogs as ready.
Dashboard SQL failures now return explicit StarRocks service or timeout HTTP
errors instead of HTTP 200 responses containing an error payload.

An initial local concurrency baseline completed with 32 requests per endpoint
and eight workers, with zero errors: detail p95 `573 ms`, aggregate p95
`668 ms`, enriched p95 `676 ms`, and health p95 `957 ms`. These are baseline
measurements, not production acceptance thresholds.

An isolated StarRocks outage test also passed: the aggregate endpoint returned
HTTP `503` with `detail.source = "starrocks"`, while the real running service
remained ready and continued returning successful queries afterward.

An isolated hot-catalog failure contract test passed for all four SQL endpoint
groups. Detail, aggregate, enriched, and health responses successfully used
cold MV data and marked their responses with `degraded=true`; enriched and
health fallback emoji fields used neutral `N/A` values.

A StarRocks restart recovery test also passed. The catalog initializer exited
successfully, all three external catalogs returned, the serving status returned
`ready`, the governed serving view recovered, and aggregate queries resumed
with `degraded=false`.

A RisingWave frontend restart recovery test also passed. The frontend returned
healthy, StarRocks resumed JDBC reads from `risingwave.public.funnel_summary`,
the serving status remained `ready`, and dashboard aggregate queries resumed
with `degraded=false`.

A simulated Databricks catalog outage test passed without altering the live
catalog. Serving status returned HTTP `200` with `status=degraded` and listed
`databricks_uc` as missing; an aggregate query returned HTTP `503` because
neither hot nor cold history was available in the simulation.

The dashboard-specific Dagster preflight asset also passed in the live
container. It validated the StarRocks catalogs, the Lakekeeper country table,
the Databricks historical table, the RisingWave hot relation, and the Kafka
`funnel` topic before dashboard dbt assets were allowed to run.

All four SQL-backed dashboard endpoint groups fall back to cold MV history when
the hot RisingWave catalog is unavailable and mark the response with
`degraded=true`. Enriched and health fallback responses preserve their metric
shape, while emoji fields use the neutral `N/A` value without hot data.

The modern dashboard launcher uses the Devbox-managed Node.js 22 runtime. The
launcher explicitly prepends the Devbox Node path so Script Runner cannot fall
back to an incompatible global Homebrew Node installation.

## Part 2: Ad-hoc query endpoints read through StarRocks

Only the dashboard's on-demand query endpoints in
[modern-dashboard/backend/api.py](../modern-dashboard/backend/api.py) change.
The Kafka consumer thread (`kafka_consumer_loop`), the in-memory cache it
feeds, and the endpoints backed by that cache (`/api/funnel`, `/api/stats`,
`/api/funnel/stream` SSE) are explicitly **not modified** by this plan.

The backend now uses one SQLAlchemy engine. `STARROCKS_URL` defaults to
`mysql+pymysql://root@localhost:9030/sr_local_db_sr_local_db`. All SQL query
endpoints execute through StarRocks, combining cold history from
`mv_unified_funnel_summary` with the newest three minutes read directly from
`risingwave.public.funnel_summary` through StarRocks JDBC federation. This
keeps dashboard queries current without waiting for the asynchronous MV
refresh. Enrichment and health fields are calculated in StarRocks SQL to
preserve the existing frontend response contract without a direct RisingWave
connection.

### Part 2 implementation decisions

* The backend uses the `mysql+pymysql` SQLAlchemy dialect for StarRocks.
* Detail queries join `lakekeeper_local.public.iceberg_countries` through
  StarRocks, preserving the existing `country_name` response field.
* The three-minute hot/cold boundary is applied both in the MV and in the
  dashboard query-time overlay.
* `/api/funnel/enriched` and `/api/funnel/health` reproduce the former UDF
  classifications, scores, emojis, and health status in StarRocks SQL.
* The Kafka consumer, in-memory cache, and SSE stream are unchanged.

## Part 3: StarRocks async materialized view (hot + cold union)

### Current status (2026-09-05 — federated demo validated)

The dbt-starrocks adapter config issues have been resolved:

The initial hot model failed because the RisingWave JDBC catalog did not yet exist. The catalog is now created idempotently by the StarRocks dbt project's `on-run-start` hook.

The two source catalogs are now both validated:

**Pilot A: RisingWave JDBC External Catalog** — StarRocks reads `public.funnel_summary` from RisingWave via a JDBC external catalog. This is the live-reading pattern previously researched for Part 3 and documented in [SR_POC_TESTING_PLAN.md](SR_POC_TESTING_PLAN.md) as an unvalidated candidate. The advantage is direct MV access; the disadvantage is a new external service dependency.

**Pilot B: Reuse Databricks UC (no hot path)** — Skip the hot path altogether and use only the cold path (Databricks UC historical table) in the unified view. The disadvantage is no real-time hot data; the advantage is no new dependencies. This changes the MV from union-based to a single-table read.

**COLD-PATH BASELINE (2026-09-05):** Pilot B was deployed first as a fallback and remains useful for comparison.
  - ✅ `mv_unified_funnel_summary` materialized view created in StarRocks (`sr_local_db_sr_local_db` schema)
  - ✅ Async refresh was initially configured: `EVERY (INTERVAL 5 MINUTE)`
  - ✅ Data verified: 54 rows, 11 distinct time windows from Databricks UC `funnel_summary_historical`
  - ✅ Sample query result confirms column mappings and numeric precision

**FEDERATION OUTCOME (2026-09-05 12:11 UTC):** Pilot A is now validated on StarRocks 4.1.4.
  - ✅ Created `risingwave` JDBC catalog using the documented `jdbc_uri`, `driver_url`, `driver_class`, and `schema_resolver=postgresql` properties.
  - ✅ StarRocks discovered RisingWave catalogs `information_schema`, `pg_catalog`, `public`, and `rw_catalog`.
  - ✅ Direct query through StarRocks returned 11 rows from `risingwave.public.funnel_summary`.
  - ✅ Databricks UC direct query returned 54 rows from `databricks_uc.sr_poc_external.funnel_summary_historical`.
  - ✅ One StarRocks `UNION ALL` query across both catalogs returned 65 rows.
  - ✅ Superseded by the boundary-aware federated demo MV below.

**DEMO FEDERATION SETUP (2026-09-05 12:23 UTC):** The producer-driven test configuration is deployed and tested.
  - ✅ Hot source: `risingwave.public.funnel_summary` through the StarRocks JDBC catalog.
  - ✅ Cold source: `databricks_uc.sr_poc_external.funnel_summary_historical` through Iceberg REST.
  - ✅ Hot/cold ownership boundary: three minutes (`window_start >= CURRENT_TIMESTAMP() - INTERVAL 3 MINUTE` for RisingWave; older rows from Databricks).
  - ✅ StarRocks MV refresh: every one minute.
  - ✅ Dagster run `dbt_starrocks_build_job`: `RUN_SUCCESS`, dbt `PASS=4`, `WARN=0`, `ERROR=0`.
  - ✅ Initial post-build counts: 19 RisingWave rows, 206 Databricks rows, 183 rows visible in the unified MV.
  - ✅ Later live validation: 24 RisingWave rows, 369 Databricks rows, 369 rows visible in the unified MV.
  - ✅ Cold-branch deduplication now collapses repeated historical snapshots by `(window_start, country)` using the maximum cumulative counts and recalculated rates.
  - ✅ Deduplicated MV validation: 23 rows, 23 distinct logical keys, 0 duplicate rows.
  - ℹ️ The external Databricks Iceberg table remains append-only and unchanged; deduplication is applied at the StarRocks federated view layer.

### Cold-path continuity validation (2026-09-05 14:11 UTC)

The Databricks sink later stopped advancing because the runtime configuration
used an ADLS account name that did not match the account in the Iceberg table's
`abfss://` metadata location. The sink DDL remained valid, but OpenDAL writes
timed out against the incorrect storage endpoint.

* Corrected the Devbox account value and removed stale hardcoded account values
  from the Dagster Compose services.
* Recreated `sink_funnel_to_databricks` through the credential-populated
  `dagster-webserver` container with dbt `PASS=3`, `WARN=0`, and `ERROR=0`.
* Confirmed that the Iceberg metadata version advanced from `00015` to `00018`
  without new object-write timeout messages.
* Confirmed that the cold table advanced from 369 rows with a `09:27` watermark
  to 495 rows with an `11:10` watermark after a bounded producer run.
* Refreshed the unified MV synchronously and verified 43 rows, 43 distinct
  `(window_start, country)` keys, and an `11:11` watermark.

### Materialized view design

The materialized view unions:

* **Hot path**: the most recent three minutes of `funnel_summary`, read live
  through the validated RisingWave JDBC catalog.
* **Cold path**: `databricks_uc` catalog reading the new
  `funnel_summary_historical` Managed Iceberg table.

Deployed shape (refresh interval widened to 5 minutes on 2026-09-06; see
"Remaining questions for Part 3" below):

```sql
CREATE MATERIALIZED VIEW sr_local_db_sr_local_db.mv_unified_funnel_summary
REFRESH ASYNC EVERY (INTERVAL 5 MINUTE)
PROPERTIES (
  "query_rewrite_consistency" = "loose"
)
AS
WITH cold_deduplicated AS (
  SELECT window_start, MAX(window_end) AS window_end, country,
       MAX(viewers) AS viewers, MAX(carters) AS carters,
       MAX(purchasers) AS purchasers
  FROM databricks_uc.sr_poc_external.funnel_summary_historical
  WHERE window_start < DATE_SUB(CURRENT_TIMESTAMP(), INTERVAL 3 MINUTE)
  GROUP BY window_start, country
)
SELECT window_start, window_end, country, viewers, carters, purchasers
FROM cold_deduplicated

UNION ALL

SELECT window_start, window_end, country, viewers, carters, purchasers
FROM sr_local_db_sr_local_db.hot_funnel_summary
WHERE window_start >= DATE_SUB(CURRENT_TIMESTAMP(), INTERVAL 3 MINUTE);
```

### Remaining questions for Part 3

* **Resolved (2026-09-06):** confirmed via
  `information_schema.materialized_views` on the running StarRocks instance
  that `mv_unified_funnel_summary` was `PARTITION_TYPE: UNPARTITIONED`, so
  every refresh cycle fully rebuilds the entire unified view
  (`LAST_REFRESH_MV_REFRESH_PARTITIONS` reported the whole MV as a single
  partition; observed `LAST_REFRESH_DURATION: 8.856s` at current data
  volume). Partition-level incremental refresh across the mixed JDBC +
  external Iceberg `UNION ALL` is not happening — the MV has no
  `PARTITION BY`, so PCT (partial refresh) has nothing to partition on.
  Adding `partition_by` was attempted and rejected outright by StarRocks
  (`Materialized view partition column in partition exp must be base table
  partition column`) since neither base table is itself partitioned.
  Mitigated instead by widening the refresh interval from 1 to 5 minutes,
  deployed 2026-09-06 via `dg launch --job dbt_starrocks_build_job`
  (`REFRESH_POLICY: EVERY(INTERVAL 5 MINUTE)` confirmed live, refresh
  completed in ~16.6s, `serving/status` stayed `ready`). This bounds
  full-rebuild frequency but not per-rebuild cost — see the resolved item
  under "Remaining work" below for when to revisit.
* The materialized view itself, and the `databricks_uc`/`lakekeeper_local`
  external catalog `CREATE EXTERNAL CATALOG` statements, can be managed by
  the `dbt-starrocks` adapter (confirmed current, PyPI `dbt-starrocks`
  1.12.0, Apache-2.0, requires StarRocks >= 2.5). See Part 4 for how this
  replaces the current shell-script catalog setup
  ([starrocks/init_catalog.sh](../starrocks/init_catalog.sh)) with a
  dbt+Dagster-managed equivalent.
* **Freshness boundary**: the three-minute `window_start >= / <` split is
  applied consistently in the MV and dashboard query-time overlay. Keep both
  definitions aligned if the boundary changes.
* **Backfill/replay**: if RisingWave is restarted or replayed, duplicate
  windows could exist transiently in both hot and cold paths before the
  Databricks sink's upsert catches up. Needs a defined reconciliation
  behavior (last-write-wins on `window_start, country`, or an explicit
  dedupe in the MV).

## Part 4: dbt + Dagster orchestration for StarRocks objects

Today, StarRocks catalogs (`databricks_uc`, `lakekeeper_local`, `risingwave`)
are created by
[starrocks/init_catalog.sh](../starrocks/init_catalog.sh), a shell script run
by the `starrocks-init` container at stack startup and idempotently by dbt
`on-run-start` hooks. RisingWave objects are dbt models
materialized as `materialized_view`, `sink`, or `iceberg_table`, run through
dbt and orchestrated by Dagster's `dagster_dbt.dbt_assets`. This part brings
StarRocks objects to parity with that pattern.

The `dbt-starrocks` adapter (verified current on PyPI: v1.12.0, Apache-2.0
license, requires StarRocks >= 2.5.0) is the mechanism. Its supported
materializations cover every StarRocks object this plan needs:

| StarRocks object | dbt-starrocks mechanism |
| --- | --- |
| `databricks_uc`, `lakekeeper_local` external catalogs | `CREATE EXTERNAL CATALOG` is not itself a materialization; run via an `on-run-start` macro, the same pattern already used for `create_iceberg_connection()` in the RisingWave dbt project |
| Hot Primary Key table (fed by RisingWave) | `materialized='table'`, `table_type='PRIMARY'` |
| `mv_unified_funnel_summary` async materialized view | `materialized='materialized_view'`, `refresh_method="ASYNC EVERY (interval 1 minute)"` |
| Reading the Databricks Managed Iceberg table from `databricks_uc` | `source()` against a `sources.yml` entry, per the adapter's documented "Read From Catalog" pattern |

### Proposed project structure

Because `dbt-starrocks` and the existing `risingwave` adapter are different
dbt adapter types, they cannot share one dbt project (a dbt project's
`profile:` resolves to exactly one adapter). This plan adds a second, sibling
dbt project rather than mixing adapters in the existing one. The scaffold for
that project is now in place and successfully parsed, so the remaining work is
execution and dependency wiring rather than project bootstrap:

```text
dbt/                     (existing, unchanged) - profile: funnel_profile, type: risingwave
dbt_starrocks/           (new) - profile: starrocks_profile, type: starrocks
  dbt_project.yml
  profiles.yml           (or a new output added to the existing dbt/profiles.yml)
  models/
    catalogs/            (on-run-start macros: create_databricks_uc_catalog(), create_lakekeeper_local_catalog())
    hot_funnel_summary.sql        (materialized='table', table_type='PRIMARY')
    mv_unified_funnel_summary.sql (materialized='materialized_view', refresh_method='ASYNC ...')
  sources.yml            (declares databricks_uc.sr_poc_external.funnel_summary_historical)
```

### Dagster wiring

Mirror the existing pattern in
[orchestration/definitions.py](../orchestration/definitions.py), which already
loads one `dbt_assets` set for the RisingWave project via
`DbtProject`/`dbt_assets` and wires one-time setup operations (for example
`postgres_funnel_table` in
[orchestration/assets/postgres_sink_setup.py](../orchestration/assets/postgres_sink_setup.py),
and `databricks_uc_tables_setup` in
[orchestration/assets/casino_prd_setup.py](../orchestration/assets/casino_prd_setup.py))
as plain `@asset` dependencies ahead of the dbt models that need them:

1. Add a second `DbtProject`/`dbt_assets` definition for `dbt_starrocks/`,
   analogous to the existing one for `dbt/`.
2. Declare an explicit Dagster dependency from the StarRocks hot-table model
   to the RisingWave `funnel_summary` asset, and from the StarRocks
   materialized view to both the hot-table model and the Databricks sink
   model (`sink_funnel_to_databricks`, from Part 1) — using the same
   `meta.dagster.deps` mechanism already used across dbt models tagged
   `casino_prd_setup`, `databricks`, and `lakekeeper` in
   `CustomDagsterDbtTranslator`.
3. Keep [starrocks/init_catalog.sh](../starrocks/init_catalog.sh) aligned with
  the dbt `on-run-start` hooks so fresh stack startup and scheduled dbt runs
  create the same three catalogs.

### Open questions for Part 4

* Confirm `dbt-starrocks` supports StarRocks' `is_async: true` / `SUBMIT
  TASK` semantics cleanly alongside the `is_async` polling behavior
  described in its docs, so that dbt runs do not block for the full async
  MV refresh duration on every `dbt run`.
* **Resolved (2026-09-06):** confirmed in
  [dbt_starrocks/dbt_project.yml](../dbt_starrocks/dbt_project.yml) that all
  three `on-run-start` hooks use `CREATE EXTERNAL CATALOG IF NOT EXISTS`, not
  a drop-and-recreate. This is a pure no-op skip when the catalog already
  exists, so repeated `dbt run`s have no impact on in-flight queries against
  `databricks_uc.*`.
* Decide whether the new `dbt_starrocks/` project shares the existing
  `dbt/profiles.yml` file (as an additional named profile) or uses its own,
  and align with however the project's dbt Cloud or CLI invocation
  conventions expect multiple profiles to be organized.

## Relevant files

* [dbt/models/funnel_summary.sql](../dbt/models/funnel_summary.sql) — source MV, defines the grain and columns to preserve end-to-end
* [dbt/models/funnel_for_iceberg.sql](../dbt/models/funnel_for_iceberg.sql) — existing type-casting pattern to reuse for the new Databricks sink
* [dbt/models/sink_funnel_to_rw_iceberg.sql](../dbt/models/sink_funnel_to_rw_iceberg.sql) — template for the new `sink_funnel_to_databricks.sql`
* [dbt/dbt_project.yml](../dbt/dbt_project.yml) — on-run-start hooks and vars; the production sink should use direct OAuth/ADLS properties rather than the unsuccessful PAT-style connection probe
* [starrocks/init_catalog.sh](../starrocks/init_catalog.sh) — existing `databricks_uc`/`lakekeeper_local` catalog DDL and credentials to port into the new `dbt_starrocks/` project's `on-run-start` macros, then retire
* [orchestration/definitions.py](../orchestration/definitions.py) — existing `DbtProject`/`dbt_assets` wiring and `CustomDagsterDbtTranslator`; needs a second `dbt_assets` set for `dbt_starrocks/`
* [orchestration/assets/postgres_sink_setup.py](../orchestration/assets/postgres_sink_setup.py) — precedent pattern for a one-time setup `@asset` wired as a dbt model dependency
* [orchestration/assets/casino_prd_setup.py](../orchestration/assets/casino_prd_setup.py) — contains `databricks_uc_tables_setup`, the existing precedent for Databricks-side setup as a Dagster asset
* [modern-dashboard/backend/api.py](../modern-dashboard/backend/api.py) — Kafka consumer thread and StarRocks SQLAlchemy query endpoints
* [docs/SR_POC_TESTING_PLAN.md](SR_POC_TESTING_PLAN.md) — governance, cost, and UniForm/HMS trade-off background this plan builds on

## Sequencing (current status and next steps)

✅ **DONE (1-3):**
1. Create the production-shaped UC Managed Iceberg target and implement
  `sink_funnel_to_databricks.sql` using the validated v3.0.3 append-only
  contract. ✅ Completed; 54 rows verified in Databricks.
2. Run the production sink through dbt and Dagster, then verify multiple
  commits, row counts, timestamp semantics, and external reads through
  StarRocks/Trino. ✅ Completed; StarRocks can read 54 rows from databricks_uc.
3. Scaffold the new `dbt_starrocks/` project (Part 4) and port the
   `databricks_uc`/`lakekeeper_local` catalog DDL from
   [starrocks/init_catalog.sh](../starrocks/init_catalog.sh) into
   `on-run-start` macros. ✅ Completed; both catalogs created via on-run-start hooks.

✅ **DONE (4):**
4. Validate StarRocks federation and deploy the boundary-aware unified MV:
  - ✅ RisingWave JDBC catalog created and queried successfully
  - ✅ Databricks UC Iceberg catalog recreated and queried successfully
  - ✅ Three-minute hot/cold ownership boundary deployed
  - ✅ One-minute MV refresh deployed
  - ✅ Dagster `dbt_starrocks_build_job` completed successfully with `PASS=4`, `WARN=0`, `ERROR=0`

✅ **DONE (5):**
5. Validate append-only historical duplicates separately from hot/cold boundary
  overlap. ✅ Completed; the serving MV exposes the deduplicated latest-row
  representation and retains unique `(window_start, country)` keys.

✅ **DONE (6):**
6. Refactor all SQL-backed dashboard endpoints to query governed StarRocks
  serving models. The Kafka consumer and SSE stream remain unchanged, and the
  former RisingWave UDF outputs are reproduced in StarRocks SQL.

⏭️ **NEXT (7):**
7. Continue live endpoint validation for at least one full day/night cycle to
  exercise the hot/cold boundary and compare response totals with direct
  StarRocks queries. Basic live enriched and health endpoint queries already
  pass through StarRocks.

## Explicitly out of scope for this plan

**✅ COMPLETED (4):**
4. Deploy unified MV and wire Dagster orchestration:
  - ✅ `mv_unified_funnel_summary` federates RisingWave JDBC and Databricks UC
  - ✅ MV refreshes every minute with a three-minute ownership boundary
  - ✅ Dagster asset `starrocks_unified_dbt_assets` defined and scheduled
  - ✅ Job `dbt_starrocks_build_job` created and validated through `dg launch`
  - ✅ Pilot A (RisingWave JDBC) validated on StarRocks 4.1.4
* Changing the existing Kafka, PostgreSQL, or Lakekeeper sinks — they
⏭️ **NEXT (5-7):**
5. Implement the boundary-aware hot/cold MV using the validated RisingWave JDBC
  catalog and Databricks UC catalog. Keep the current cold-path-only MV as the
  comparison baseline until duplicate handling and freshness are validated.
  to StarRocks.
* Row-level or column-level governance on the new Databricks table beyond
  standard Unity Catalog table grants.
* Historical backfill of existing RisingWave data into the new Databricks
  table — this plan only covers the ongoing sink from this point forward.
