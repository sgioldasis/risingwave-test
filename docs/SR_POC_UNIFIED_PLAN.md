# StarRocks Unified Lakehouse Dashboard Plan

## Objective

Refactor the modern dashboard so it stops reading directly from Kafka and
RisingWave, and instead reads through StarRocks. StarRocks serves a single
unified view that combines the hot, recent data streamed from RisingWave with
the cold, historical data stored in Databricks Unity Catalog.

## Current architecture (as-is)

```text
Kafka producer
    -> RisingWave sources (src_page, src_cart, src_purchase)
    -> funnel_summary (1-minute tumbling MV)
         -> sink_funnel_to_kafka       -> Kafka topic "funnel"
         -> sink_funnel_to_postgres    -> local PostgreSQL (JDBC upsert)
         -> sink_funnel_to_rw_iceberg  -> Lakekeeper Iceberg (rw_managed_funnel)

modern-dashboard/backend/api.py
    -> background Kafka consumer thread -> in-memory cache -> /api/funnel, /api/funnel/stream (SSE)
    -> SQLAlchemy over RisingWave Postgres wire (port 4566) -> /api/query/funnel*, /api/funnel/enriched, /api/funnel/health
```

`funnel_summary` columns: `window_start`, `window_end`, `country`, `viewers`,
`carters`, `purchasers`, `view_to_cart_rate`, `cart_to_buy_rate`.

StarRocks currently has two external catalogs and no table fed by
RisingWave:

* `databricks_uc` — Iceberg REST against Unity Catalog `de_dev`
  ([starrocks/init_catalog.sh](../starrocks/init_catalog.sh))
* `lakekeeper_local` — Iceberg REST against the local Lakekeeper/MinIO stack

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
    -> async materialized view UNIONs hot + cold, refreshed on a short interval

modern-dashboard/backend/api.py
    -> replaces Kafka consumer thread and direct RisingWave SQLAlchemy queries
    -> queries StarRocks (MySQL wire protocol) for /api/funnel, /api/query/funnel*, SSE stream
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
    type = 'upsert',
    primary_key = 'window_start,country',
    connection = databricks_uc_catalog_conn,  -- new connection, see below
    database.name = 'sr_poc_external',
    table.name = 'funnel_summary_historical',
    create_table_if_not_exists = 'true',
    commit_checkpoint_interval = 30
)
```

This requires a new dbt on-run-start connection, `databricks_uc_catalog_conn`,
analogous to `lakekeeper_catalog_conn`
(created in [dbt/dbt_project.yml](../dbt/dbt_project.yml) via
`create_iceberg_connection()`), but pointed at the Unity Catalog IRC
endpoint and OAuth credentials already used by
[starrocks/init_catalog.sh](../starrocks/init_catalog.sh) for the
`databricks_uc` catalog.

### Open questions for Part 1

* Does RisingWave's `iceberg` sink connector support OAuth2 client-credential
  authentication against Unity Catalog's IRC endpoint the same way StarRocks'
  `iceberg.catalog.credential` property does? Not yet verified.
* Should the target catalog/schema be a new schema (for example
  `de_dev.sr_poc_external`) or a dedicated catalog reserved for
  RisingWave-managed tables, to keep it clearly separated from
  human-managed Databricks tables?
* Upsert primary key: `funnel_summary` is keyed by `(window_start, country)`,
  matching `sink_funnel_to_postgres`, not `sink_funnel_to_rw_iceberg` (which
  uses `window_start` alone from the country-less `funnel_for_iceberg`
  projection). Confirm which grain the historical Databricks table should
  use before implementing.

## Part 2: Dashboard reads through StarRocks

Replace the dashboard's two current data paths in
[modern-dashboard/backend/api.py](../modern-dashboard/backend/api.py):

* The Kafka consumer thread (`kafka_consumer_loop`, feeding
  `/api/funnel`, `/api/stats`, `/api/funnel/stream`) is replaced by a
  polling or push query against the new StarRocks unified view.
* The SQLAlchemy engine currently pointed at RisingWave
  (`RISINGWAVE_URL`, `postgresql://root:root@localhost:4566/dev`) is
  replaced by a StarRocks MySQL-wire connection for
  `/api/query/funnel*`, `/api/funnel/enriched`, `/api/funnel/health`.

### Open questions for Part 2

* StarRocks speaks the MySQL wire protocol, not PostgreSQL — the backend's
  SQLAlchemy dialect and connection string need to change
  (`mysql+pymysql://` or equivalent), not just the host/port.
* The SSE stream (`/api/funnel/stream`) currently pushes on every Kafka
  message. A StarRocks-backed replacement needs either polling on a short
  interval or a separate change-notification mechanism — StarRocks itself
  has no native push/subscribe API. Decide the acceptable staleness window
  before implementing.
* `/api/funnel/enriched` currently queries a RisingWave UDF-enhanced view
  directly. Confirm whether the enrichment logic moves into the StarRocks
  materialized view, stays in RisingWave with StarRocks reading the
  enriched RisingWave table instead of the raw one, or is reimplemented in
  StarRocks SQL.

## Part 3: StarRocks async materialized view (hot + cold union)

The materialized view unions:

* **Hot path**: the most recent window(s) of `funnel_summary`, fed live
  from the RisingWave sink.
* **Cold path**: `databricks_uc` catalog reading the new
  `funnel_summary_historical` Managed Iceberg table.

Draft shape (to refine before implementation):

```sql
CREATE MATERIALIZED VIEW sr_local_db.mv_unified_funnel_summary
PARTITION BY (window_start)
REFRESH ASYNC EVERY (INTERVAL 1 MINUTE)
PROPERTIES (
    "query_rewrite_consistency" = "loose",
    "mv_rewrite_staleness_second" = "30"
)
AS
SELECT window_start, window_end, country, viewers, carters, purchasers,
       view_to_cart_rate, cart_to_buy_rate
FROM sr_local_db.hot_funnel_summary
WHERE window_start >= CURRENT_TIMESTAMP() - INTERVAL 1 DAY

UNION ALL

SELECT window_start, window_end, country, viewers, carters, purchasers,
       view_to_cart_rate, cart_to_buy_rate
FROM databricks_uc.sr_poc_external.funnel_summary_historical
WHERE window_start < CURRENT_TIMESTAMP() - INTERVAL 1 DAY;
```

### Open questions for Part 3

* **How does the hot path actually reach StarRocks?** Two candidate
  designs, not yet tested:
  1. RisingWave sinks `funnel_summary` a second time into a native
     StarRocks Primary Key table (a new sink, analogous to
     `sink_funnel_to_postgres` but with a `starrocks` connector).
  2. StarRocks reads RisingWave live through a JDBC external catalog,
     analogous to Trino's proven `risingwave` PostgreSQL catalog
     documented in
     [SR_POC_TESTING_PLAN.md](SR_POC_TESTING_PLAN.md). This has not been
     verified for StarRocks specifically and needs a pilot test before
     it can be assumed to work.
* **Incremental partition refresh across mixed internal/external
  sources is unverified.** StarRocks documentation on whether partition-level
  incremental refresh is fully supported when an async MV's base tables
  span both an internal table and an external Iceberg catalog table in the
  same `UNION ALL` could not be confirmed in this session. Until verified,
  assume the MV may fall back to a full refresh on each cycle and size the
  refresh interval and cluster resources accordingly.
* The materialized view itself, and the `databricks_uc`/`lakekeeper_local`
  external catalog `CREATE EXTERNAL CATALOG` statements, can be managed by
  the `dbt-starrocks` adapter (confirmed current, PyPI `dbt-starrocks`
  1.12.0, Apache-2.0, requires StarRocks >= 2.5). See Part 4 for how this
  replaces the current shell-script catalog setup
  ([starrocks/init_catalog.sh](../starrocks/init_catalog.sh)) with a
  dbt+Dagster-managed equivalent.
* **Freshness boundary**: the `WHERE window_start >= / <` split between hot
  and cold needs to be a single source of truth (a variable or view), not
  duplicated as a literal `INTERVAL 1 DAY` in two places, to avoid a gap or
  overlap window as the boundary moves.
* **Backfill/replay**: if RisingWave is restarted or replayed, duplicate
  windows could exist transiently in both hot and cold paths before the
  Databricks sink's upsert catches up. Needs a defined reconciliation
  behavior (last-write-wins on `window_start, country`, or an explicit
  dedupe in the MV).

## Part 4: dbt + Dagster orchestration for StarRocks objects

Today, StarRocks objects (`databricks_uc`, `lakekeeper_local`) are created by
[starrocks/init_catalog.sh](../starrocks/init_catalog.sh), a shell script run
by the `starrocks-init` container at stack startup. This is outside dbt and
Dagster entirely. RisingWave objects, by contrast, are all dbt models
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
dbt project rather than mixing adapters in the existing one:

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
3. Retire [starrocks/init_catalog.sh](../starrocks/init_catalog.sh) once the
   `on-run-start` macros in the new project reliably (re)create both
   catalogs idempotently; keep it only as a fallback until that is proven
   in practice.

### Open questions for Part 4

* Confirm `dbt-starrocks` supports StarRocks' `is_async: true` / `SUBMIT
  TASK` semantics cleanly alongside the `is_async` polling behavior
  described in its docs, so that dbt runs do not block for the full async
  MV refresh duration on every `dbt run`.
* Confirm whether `CREATE EXTERNAL CATALOG` is idempotent enough to run on
  every `dbt run` via `on-run-start` (`CREATE EXTERNAL CATALOG IF NOT
  EXISTS`, or a drop-and-recreate as the current shell script does), and
  whether repeated recreation has any impact on already-running queries
  against `databricks_uc.*`.
* Decide whether the new `dbt_starrocks/` project shares the existing
  `dbt/profiles.yml` file (as an additional named profile) or uses its own,
  and align with however the project's dbt Cloud or CLI invocation
  conventions expect multiple profiles to be organized.

## Relevant files

* [dbt/models/funnel_summary.sql](../dbt/models/funnel_summary.sql) — source MV, defines the grain and columns to preserve end-to-end
* [dbt/models/funnel_for_iceberg.sql](../dbt/models/funnel_for_iceberg.sql) — existing type-casting pattern to reuse for the new Databricks sink
* [dbt/models/sink_funnel_to_rw_iceberg.sql](../dbt/models/sink_funnel_to_rw_iceberg.sql) — template for the new `sink_funnel_to_databricks.sql`
* [dbt/dbt_project.yml](../dbt/dbt_project.yml) — on-run-start hooks and vars; needs a new `databricks_uc_catalog_conn` connection
* [starrocks/init_catalog.sh](../starrocks/init_catalog.sh) — existing `databricks_uc`/`lakekeeper_local` catalog DDL and credentials to port into the new `dbt_starrocks/` project's `on-run-start` macros, then retire
* [orchestration/definitions.py](../orchestration/definitions.py) — existing `DbtProject`/`dbt_assets` wiring and `CustomDagsterDbtTranslator`; needs a second `dbt_assets` set for `dbt_starrocks/`
* [orchestration/assets/postgres_sink_setup.py](../orchestration/assets/postgres_sink_setup.py) — precedent pattern for a one-time setup `@asset` wired as a dbt model dependency
* [orchestration/assets/casino_prd_setup.py](../orchestration/assets/casino_prd_setup.py) — contains `databricks_uc_tables_setup`, the existing precedent for Databricks-side setup as a Dagster asset
* [modern-dashboard/backend/api.py](../modern-dashboard/backend/api.py) — Kafka consumer thread and RisingWave SQLAlchemy queries to replace
* [docs/SR_POC_TESTING_PLAN.md](SR_POC_TESTING_PLAN.md) — governance, cost, and UniForm/HMS trade-off background this plan builds on

## Sequencing (draft)

1. Resolve the Part 1 open questions; implement and validate
   `sink_funnel_to_databricks.sql` writes to a Managed Iceberg table,
   independent of the dashboard changes.
2. Scaffold the new `dbt_starrocks/` project (Part 4) and port the
   `databricks_uc`/`lakekeeper_local` catalog DDL from
   [starrocks/init_catalog.sh](../starrocks/init_catalog.sh) into
   `on-run-start` macros, validating catalog creation through `dbt run`
   before any table or MV models depend on it.
3. Resolve the Part 3 hot-path open question (native StarRocks sink vs.
   JDBC catalog to RisingWave) with a small pilot before committing to the
   materialized view design.
4. Implement the hot Primary Key table and the async materialized view as
   `dbt_starrocks/` models once both source paths are validated
   independently, and wire Dagster dependencies per Part 4.
5. Refactor the dashboard backend to read from StarRocks, behind a feature
   flag or parallel endpoint so the existing Kafka/RisingWave path can be
   compared side-by-side before cutover.
6. Remove the Kafka consumer thread and direct RisingWave SQLAlchemy
   queries from the dashboard only after the StarRocks path is validated
   in parallel for at least one full day/night cycle (to exercise the
   hot/cold boundary).

## Explicitly out of scope for this plan

* Changing the existing Kafka, PostgreSQL, or Lakekeeper sinks — they
  continue running unchanged.
* Row-level or column-level governance on the new Databricks table beyond
  standard Unity Catalog table grants.
* Historical backfill of existing RisingWave data into the new Databricks
  table — this plan only covers the ongoing sink from this point forward.
