# Reverse-ETL sync: a code walkthrough (Dagster side)

*Written 2026-10-16, against the code as of that date (OSS Delta extraction, Avro encoding, cost meter all in).*

This walks through the Dagster implementation of the reverse-ETL sync: how one
Databricks Delta table's changes flow to Kafka and on to RisingWave and Postgres,
and how a single config object generates the whole thing for any number of syncs.
It explains the code as it is — file by file, function by function — for someone
who has to maintain or extend it.

The other docs cover behaviour and evidence; this one covers structure:

- What the sync does and why: `REVERSE_ETL_CDF_POC_PLAN.md`
- The Debezium JDBC sink and message encodings: `REVERSE_ETL_DEBEZIUM_JDBC_SINK.md`
- Databricks cost of every piece (measured): `REVERSE_ETL_DBU_COST_NOTES.md`
- Live demo and operating notes: `REVERSE_ETL_LIVE_DEMO.md`
- Tool choices (drt/dlt/SaaS/OSS): `REVERSE_ETL_TOOL_EVALUATION.md`
- The notebook (option B) internals: the notebook at
  `/Users/s.gioldasis-si@devkaizengaming.com/reverse_etl_cdf_to_kafka` in the DEV workspace

---

## The 60-second map

| File | Role |
|---|---|
| `orchestration/defs/<label>/defs.yaml` | One small YAML per sync — the only thing you write to add a sync |
| `orchestration/components/reverse_etl_cdf_sync.py` | The Dagster component: reads the YAML, builds the config |
| `orchestration/assets/reverse_etl_config.py` | `ReverseEtlSyncConfig` — every name a sync needs, derived from a label |
| `orchestration/assets/reverse_etl_defs.py` | Turns one config into assets, jobs, checks and schedules |
| `orchestration/assets/reverse_etl_cdf_setup.py` | The heart: table setup, seed, the sync asset (both backends), event building, Kafka production |
| `orchestration/assets/reverse_etl_spark_reader.py` | The OSS Delta backend: reads storage directly, zero Databricks compute |
| `orchestration/assets/reverse_etl_change.py` | Demo changes (insert / update / delete / add column) |
| `orchestration/assets/reverse_etl_risingwave_setup.py` | The RisingWave target table + schema evolution |
| `orchestration/assets/reverse_etl_debezium_sink.py` | The Kafka Connect Debezium JDBC sink (Postgres) |
| `orchestration/assets/reverse_etl_health.py` | `sink_healthy` asset check + its 5-minute schedule |
| `orchestration/assets/reverse_etl_reset.py` | The reset job (drops everything, with a name allowlist) |
| `orchestration/assets/reverse_etl_notebook_job.py` | Option B: triggers the Databricks notebook job |
| `orchestration/assets/databricks_optimize.py` | Shared Databricks plumbing: tokens, `_submit`/`_poll`, retry |
| `orchestration/assets/databricks_cost_meter.py` | Per-run metering of Databricks compute |
| `orchestration/definitions.py` | Top-level `Definitions` — merges the base defs with every sync's defs |

Mental model in one sentence: **a frozen config object names everything
`reverse_etl_<label>_<role>`; factory functions build assets and jobs from it; the
sync asset reads a Change Data Feed from the watermark to the table's latest
version, reshapes CDF rows into Debezium envelopes, produces them to one Kafka
topic (Avro or JSON), then moves the watermark — and both targets converge by
consuming that topic independently.**

---

## 1. One config object generates everything

`orchestration/assets/reverse_etl_config.py` defines a frozen dataclass:

```python
@dataclass(frozen=True)
class ReverseEtlSyncConfig:
    sync_name: str            # e.g. "reverse_etl_avro" — also the watermark key
    catalog: str
    schema: str
    source_table: str         # "reverse_etl_avro_source"
    state_table: str          # "reverse_etl_avro_state" — the watermark
    key_column: str           # "rid" (identity column, see below)
    source_columns: tuple[str, ...]
    kafka_topic: str          # "reverse_etl_avro_topic"
    connector_name: str       # "reverse_etl_avro_sink"
    postgres_table: str       # "reverse_etl_avro_target"
    risingwave_table: str     # "reverse_etl_avro_target"
    ...
    extraction: str = "oss_spark"   # or "databricks_sql"
    encoding: str = "avro"          # or "json"
```

`ReverseEtlSyncConfig.for_name("avro", ...)` derives **every** name from the
label — tables, topic, connector, assets, jobs — so syncs never collide and the
label is visible in every system (Databricks, Kafka, Connect, Postgres,
RisingWave, Dagster). Handy properties build the FQNs and the Debezium envelope
name (`<sync_name>.<schema>.<source_table>`), and the Connect consumer group
(`connect-<connector_name>`).

Three deliberate details:

- **`key_column = "rid"`** is an identity column Databricks fills on insert. The
  business `id` is *not* the key: the source table does not enforce uniqueness on
  it (a duplicate-keyed row is a real, reachable state here — `_to_debezium_events`
  handles that, see §4). An identity column cannot be added to an existing table,
  so it must exist from creation.
- **`reset_name_prefixes` + `reset_schema`** are the reset job's safety allowlist
  (§8): the reset drops tables, a connector and a topic, and refuses to touch
  anything outside the prefix/schema.
- The config imports nothing from the asset modules — every asset module imports
  the config — so there is no cycle at this level.

### The YAML and the component

You never build the dataclass by hand. `ReverseEtlCdfSync`
(`orchestration/components/reverse_etl_cdf_sync.py`) is a Dagster component
(`dg.Component, dg.Model, dg.Resolvable`): one `defs.yaml` per sync under
`orchestration/defs/<label>/` becomes one config. The avro sync's file is the
shortest complete example:

```yaml
type: orchestration.components.reverse_etl_cdf_sync.ReverseEtlCdfSync
attributes:
  name: avro
  extraction: oss_spark      # how the sync asset reads the change feed (§6)
  encoding: avro             # Confluent wire format + staging registry
  catalog: de_dev
  schema_name: sr_poc_external
  source_columns: ["id BIGINT NOT NULL", "value STRING", "updated_at TIMESTAMP"]
  seed_statements: [...]     # optional demo rows (MERGE/UPDATE/DELETE)
```

Every attribute has a derived default; the optional overrides exist for pointing
a sync at a table that already exists. `sync_name` is the watermark key and the
Debezium envelope prefix — changing it for an existing sync restarts that sync
from the beginning.

## 2. From YAML to Dagster definitions

`build_reverse_etl_defs(cfg)` (`reverse_etl_defs.py`, ~90 lines) is the assembly
point. Per sync it builds:

- **assets**: `table_setup`, `sync` (via `build_sync_asset`, §3), `risingwave_target`,
  `jdbc_sink`, optionally `topic_setup` and `seed`, plus the `make_change` asset;
- **the setup job** (`define_asset_job` with an explicit `AssetSelection` of the
  asset *names*) — selection by `AssetKey` name, not asset objects, so the shared
  topic asset built elsewhere can still be part of the job;
- **the reset job**, **the change job** (one-asset `define_asset_job`),
  **the health check** and its 5-minute schedule.

`orchestration/definitions.py` then does
`load_defs(reverse_etl_yaml_defs, ...)` over every `defs.yaml` and merges the
result into the project's `Definitions`. Adding a sync = adding one YAML file.

## 3. The sync asset — the heart

`build_sync_asset(cfg)` (`reverse_etl_cdf_setup.py`) is a **dispatcher**:

```python
def build_sync_asset(cfg):
    if cfg.extraction == "oss_spark":
        return _build_sync_asset_oss_spark(cfg)
    return _build_sync_asset_databricks_sql(cfg)
```

Both branches build an asset with the **same name** (`cfg.sync_asset`), the same
deps, the same Kafka messages and the same watermark semantics — they differ only
in *how they read Databricks*. The skeleton both share:

1. **Read the watermark** from the state table (`last_commit_version` for this
   `sync_name`). `None` means first run.
2. **First-run policy**: if `REVERSE_ETL_BACKFILL_VERSION` is set, backfill from
   that explicit version (`_resolve_backfill_version`); else if the table's
   earliest available version is 0 (full history intact) backfill from the
   beginning; else *baseline* at the current version and sync nothing historical —
   the CDF only contains what retention kept.
3. **Read the changes** since `last_version + 1` (see the watermark note below),
   unless there are no new commits — checked *before* reading, because both
   `table_changes()` and `readChangeFeed` error past the table's latest version
   (`DELTA_CDC_START_VERSION_AFTER_LATEST`) rather than returning empty.
4. **Group CDF rows into Debezium envelopes** (`_to_debezium_events`, §4).
5. **Schema evolution**: if there are events, read the source's current columns
   and `add_missing_columns()` on RisingWave *before* producing — RisingWave only
   fills a column from messages read after the column exists.
6. **Produce to Kafka** (`_produce_to_kafka`, §5).
7. **Write the watermark** — only after produce returned success.
8. Emit run metadata (events synced, op counts, commit version, message previews,
   extraction, and the cost entries from the meter, §9).

**Watermark semantics** — the subtle part, commented in both branches:
`startingVersion` of `table_changes()`/`readChangeFeed` is **inclusive**, and
`last_version` was already fully processed by the previous run, so the next read
starts at `last_version + 1`. Resuming from `last_version` itself would re-deliver
that version's rows. On a first-run baseline with no rows, the watermark is set to
the table's *current* version so the run is still a real checkpoint.

### The `oss_spark` branch (the default, zero DBUs)

`_build_sync_asset_oss_spark` does every read through
`orchestration/assets/reverse_etl_spark_reader.py` (§6). The token minted at the
start is **control-plane only**: it resolves the two tables' storage locations via
the Unity Catalog REST API. No SQL ever runs against a warehouse — the change
feed, the watermark, the schema and the table history all come from the Delta
transaction log on storage, read by a small local Spark session
(`local[1]`, 512 MB) inside the webserver container. pyspark/delta are imported
*lazily inside the asset body* so definitions keep loading on machines without
them (and because `reverse_etl_risingwave_setup` imports this module — see §11).

### The `databricks_sql` branch (the comparison baseline)

Same skeleton, but every read is SQL on the warehouse: `_read_changes` runs
`table_changes()`, the watermark is a `MERGE`, the schema comes from
`information_schema`. All of it goes through `_run_sql` (§7) and meters as
`databricks_warehouse_seconds` — which is exactly how the zero-DBU claim in
`REVERSE_ETL_DBU_COST_NOTES.md` stays honest: flip `extraction:` in the YAML and
the same asset reports measured seconds instead of 0.

## 4. Building Debezium events — `_to_debezium_events`

CDF says an update is **two rows** (`update_preimage` + `update_postimage`,
sharing key and `_commit_version`). Consumers want one `before/after/op` envelope.
The function groups raw CDF rows by `(row key, commit version)` — by *group key*,
not by adjacency, because `table_changes()` documents no ordering — and pairs
pre/postimages with `zip_longest` *positionally*.

Why so careful: the source table has no enforced uniqueness on the business key,
so one commit can touch several physical rows sharing a key
(`UPDATE ... WHERE id = 1` matching two duplicates → two pre/postimage pairs, both
keyed `id=1`). Grouping accumulates **lists** per change type (an early dict-based
implementation silently kept only the last row of each type per commit) and pairs
positionally, reconstructing N update events for N duplicate rows.

Output shape, exactly what Debezium consumers expect:

```python
{"before": {...} | None, "after": {...} | None, "op": "c"|"u"|"d",
 "source": {"commit_version": int, "commit_timestamp": str}}
```

CDF's own `_change_type`/`_commit_*` columns move into `source` — Debezium puts
event metadata there, not in row content.

## 5. Producing to Kafka — `_produce_to_kafka`

One producer per run. Encoding is config: `avro` (Confluent wire format, schemas
registered under `<topic>-key` / `<topic>-value` on the staging Apicurio's
`/apis/ccompat/v7`, via `_avro_serializers` + `_register_avro_schemas`) or `json`
(Kafka Connect JSON with the schema embedded per message).

The delivery guard is the load-bearing detail:

```python
producer.produce(..., on_delivery=lambda err, msg: delivery_errors.append(str(err)) if err else None)
undelivered = producer.flush(timeout=120)
if undelivered or delivery_errors:
    raise RuntimeError(...)
```

`produce()` only enqueues; failures surface later in librdkafka's threads. The
callbacks + bounded `flush` fail the run **before the watermark MERGE that
follows**, so the next sync replays the same version instead of silently skipping
events that never reached the topic. Replays are harmless: both targets upsert by
key. (This was added after a broker blip proved the failure was silent — see
`REVERSE_ETL_DEBEZIUM_JDBC_SINK.md`.)

## 6. The OSS Delta backend — `reverse_etl_spark_reader.py`

The zero-DBU module. Every function mirrors a SQL helper from
`reverse_etl_cdf_setup.py` with the same output contract, so the two backends are
drop-in equivalents:

- `get_table_location(token, cfg, fqn)` — the table's `abfss` storage location via
  the **UC REST API** (`/api/2.1/unity-catalog/tables/<fqn>`). Control plane, free.
  Replaces the `DESCRIBE DETAIL` the prototypes ran on the warehouse.
- `get_spark_session(storage_account)` — a deliberately small local session:
  `local[1]`, 512 MB driver (the webserver container shares the box), UI off,
  Delta's catalog + extensions, `spark.sql.session.timeZone=UTC`, and the ABFS
  OAuth settings that let the `ADLS_*` service principal read the tables' bytes
  directly. `configure_spark_with_delta_pip` wires the Delta jars (pip-installed
  `delta-spark`, pinned); `hadoop-azure` always comes from Maven because pyspark
  does not bundle it.
- `read_changes` — `readChangeFeed` from `startingVersion`, ordered by
  `_commit_version`; every `TIMESTAMP` column rendered through `date_format` into
  a microsecond UTC string, because a collected `TimestampType` would render in
  the machine's timezone and drift from the SQL path's output.
- `read_last_version` / `write_last_version` — the state table read and the same
  idempotent `MERGE` as the SQL path. The state table must be **plain Delta**
  (option A in `REVERSE_ETL_DBU_COST_NOTES.md` §5): OSS Delta cannot commit to a
  UniForm table (`DELTA_MISSING_ICEBERG_CLASS`), while the source stays UniForm so
  StarRocks/Databricks Iceberg readers keep working.
- `get_current_version` / `get_earliest_available_version` — `DESCRIBE HISTORY`
  equivalents from `DeltaTable.forPath(...).history()`.

## 7. The SQL-warehouse plumbing — `databricks_optimize.py` + `_run_sql`

`databricks_optimize.py` holds the shared plumbing: `_get_token()` (client-credentials
against the tenant), and the Statement Execution API calls `_submit` / `_poll`
(with `_with_retry` — one retry on `ConnectionError`/`Timeout` for VPN blips; the
comment documents that a retried statement may execute twice, which every demo
statement tolerates). `_run_sql` in `reverse_etl_cdf_setup.py` wraps those in a
timed envelope that feeds the cost meter (§9) and `_with_all_chunks` pages the
result. `_rows_as_dicts` flattens a response into plain dicts.

## 8. The supporting assets and jobs

All factories take the config and return Dagster objects; all names come from it.

- **`build_table_setup_asset`** — creates the source table if absent, with the
  properties the whole design rests on: `delta.enableChangeDataFeed`, column
  mapping, deletion vectors off, UniForm/iceberg compat — plus the identity key —
  and the state table (plain Delta). Idempotent (`IF NOT EXISTS`).
- **`build_seed_asset`** — optional demo rows (the YAML's `seed_statements`,
  `{source_table}` substituted), run as the *last* step of the setup job so the
  rows wait in Databricks until the first sync.
- **`build_change_asset`** — one asset (in its own `define_asset_job`) applying a
  demo change: `insert`/`update`/`delete` target the row with max `id`; `add_column`
  `DESCRIBE`s first and only adds columns that don't exist, so re-clicking it is a
  no-op. This is an *asset* (not an op) so its run metadata — including its
  measured warehouse seconds — is queryable.
- **`build_risingwave_table_asset`** — creates the RisingWave target
  (`CREATE ... INCLUDE timestamp AS rw_event_timestamp` style upsert/delete table,
  `FORMAT DEBEZIUM ENCODE AVRO/JSON` per encoding). `add_missing_columns()` is the
  schema-evolution helper the sync calls before producing; with Avro it registers
  the new schema version *before* altering the table, because RisingWave validates
  new columns against the registry schema.
- **`build_debezium_sink_asset`** — PUTs the connector config to Kafka Connect
  (`reverse_etl_avro_sink`, Debezium JDBC sink, `tasks.max` from config), waits for
  Connect, then for the connector to reach `RUNNING`. Postgres-side schema
  evolution is the sink's own feature.
- **`build_sink_health_check` + `build_sink_health_job_and_schedule`** — an
  `@asset_check` on the sink asset: connector/task states via the Connect API and
  consumer-group lag via Kafka admin, on a 5-minute schedule. Reads no Databricks
  anything (verified in the cost audit, `REVERSE_ETL_DBU_COST_NOTES.md`).
- **`build_reset_job`** — chained ops: delete connector, drop RisingWave table,
  drop Postgres table, delete topic + consumer group, drop Databricks tables. Every
  name must match the config's `reset_name_prefixes`/`reset_schema` allowlist or
  the op raises before touching anything.
- **Option B** (`reverse_etl_notebook_job.py`) — `reverse_etl_notebook_sync_job`
  finds the `reverse_etl_notebook_sync` Databricks job by name, optionally adds
  missing RisingWave columns first, runs it with the sync's label as a parameter,
  and waits. The notebook repeats the config's Databricks-side values as widget
  defaults (it cannot import this repo) — the config is the source of truth.
  It shares the watermark through the same state table: run A *or* B, never both.

## 9. The cost meter — `databricks_cost_meter.py`

A per-process accumulator. `_run_sql` records every warehouse statement's wall
time; every instrumented asset/op calls `meter_reset()` at entry and emits
`databricks_warehouse_seconds` / `databricks_statements` (+ `databricks_compute`,
and `databricks_cluster_seconds` on the notebook path) as run metadata. The
`oss_spark` sync emits 0.0 **structurally** — it makes no compute calls. The demo
helper reads the metadata back through GraphQL and shows it as the "Databricks
cost" chip; the semantics (seconds, not DBUs) are in
`REVERSE_ETL_DBU_COST_NOTES.md`. Per-process is deliberate: multiprocess steps get
isolation for free, and the in-process setup job resets at every asset entry.

## 10. One change, end to end

| Step | Where |
|---|---|
| Demo change (e.g. `UPDATE`) commits → new source version | `reverse_etl_change.py` `make_change` |
| Sync materializes: watermark read (`last_version`) | `reverse_etl_cdf_setup.py` `cdf_to_kafka` → `read_last_version` (or `_read_last_version`) |
| CDF rows read since `last_version + 1` | `reverse_etl_spark_reader.read_changes` (or `_read_changes`) |
| Rows → Debezium envelopes | `_to_debezium_events` |
| Source schema read; RisingWave columns added | `get_row_fields` → `reverse_etl_risingwave_setup.add_missing_columns` |
| Avro schemas registered; events produced, delivery verified | `_register_avro_schemas` → `_produce_to_kafka` |
| Watermark `MERGE`d to the state table | `write_last_version` (or `_write_last_version`) |
| RisingWave applies the change (native Debezium + Avro) | RisingWave `reverse_etl_avro_target` |
| Postgres upserts/deletes via the JDBC sink | `reverse_etl_debezium_sink` (created) → connector |
| Health checked every 5 min | `reverse_etl_health.sink_healthy` |

## 11. Gotchas worth knowing before editing

- **Lazy imports are load-bearing.** `reverse_etl_risingwave_setup` imports
  `reverse_etl_cdf_setup`; the sync asset imports it back *inside the function
  body*. Same for pyspark/delta (not installed everywhere definitions load) and
  for the notebook job importing the component. Keep heavy or circular imports
  inside functions.
- **Executor choice**: the setup job and the change job run `in_process` (fast,
  shared session state); asset materializations (sync) run the default
  multiprocess executor — which is also why the meter resets per step.
- **Laptop runs**: the `dagster` CLI loads `<CWD>/.env` and overrides real env —
  run from a neutral CWD with `PYTHONPATH=<repo>` (see `REVERSE_ETL_LIVE_DEMO.md`).
- **The notebook** cannot import this repo; its widget defaults must be kept equal
  to the config it serves.
- **Watermark + inclusive `startingVersion`**: any new reader of the state table
  must resume at `last_version + 1`, or the last synced version is re-delivered
  (harmless for upserts, wrong for deletes' audit trail).
- **UniForm split**: source tables stay UniForm (Iceberg readers); the state table
  must stay plain Delta (OSS commits). The setup assets create them that way; the
  conversion script `scripts/convert_state_tables_plain_delta.py` migrated the
  first two.
