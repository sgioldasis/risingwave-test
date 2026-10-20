# Reverse-ETL sync: a code walkthrough (Dagster side)

*Written 2026-10-16, against the code as of that date (OSS Delta extraction, Avro encoding, cost meter all in). Updated 2026-10-19 for the delta-rs reconstruction backend — the OSS-Spark and SQL-warehouse branches described in the original are gone (`REVERSE_ETL_SPARK_FREE_FINDINGS.md` has the why and the measurements).*

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
| `orchestration/assets/reverse_etl_cdf_setup.py` | The heart: table setup, seed, the sync asset, event building, Kafka production |
| `orchestration/assets/reverse_etl_delta_reader.py` | The delta-rs backend: per-commit-version snapshot diffs reconstruct the change feed — no Spark, no CDF read, no Databricks compute |
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

`build_sync_asset(cfg)` (`reverse_etl_cdf_setup.py`) builds the single sync
asset — since 2026-10-19 there is one backend, the delta-rs reconstruction
(§6); the former OSS-Spark and SQL-warehouse branches are deleted
(`REVERSE_ETL_SPARK_FREE_FINDINGS.md` records why and what they measured).
The asset skeleton:

0. **Spark-free no-op pre-check**: before anything heavy, the sync reads the
   source's current version and the state table's watermark with delta-rs (no
   JVM, service-principal auth — `precheck_versions`) and
   ends the run when they are equal: a schema change is always a commit, so an
   unchanged version cannot hide one. A no-op sync costs ~8s instead of ~17s,
   whose floor was the Spark session start. The token and the storage locations
   it needs come from a small disk cache (`dagster_storage/`, TTLs 45min/10min —
   `_get_token_cached`/`get_table_location_cached`), which is also why the state
   tables are created *plain* Delta (no column
   mapping, deletion vectors off): delta-rs rejects tables carrying those
   reader features. The reset job **clears this cache** after dropping the
   tables — a drop+recreate moves them to new storage locations, and a cached
   location would point at the dead tables (a green no-op that silently skips
   everything; bit us live). The check is bounded by a 5s
   watchdog and fails open — any problem with it simply runs the full sync.
1. **Read the watermark** from the state table (`last_commit_version` for this
   `sync_name`). `None` means first run. The state table is **append-only**: every
   sync appends one row (a cheap single-commit insert — the read-modify-write
   `MERGE` this replaced cost ~2s per real sync), and the read takes the *freshest*
   row for the sync. History accumulates harmlessly — one auditable row per sync —
   and the no-op pre-check picks the max the same way.
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
5. **Schema evolution (adds)**: for every event-bearing chunk, read the source's
   columns *pinned to that chunk's top commit* (`get_row_fields(source_location,
   version=chunk_to)`) and `add_missing_columns()` on RisingWave *before*
   producing — RisingWave only fills a column from messages read after the
   column exists. Per chunk, not once for the run: a schema change between
   chunks must reach the later chunks' envelopes, or the projection silently
   drops those values (verified live 2026-10-19, findings doc §2.7). The pin
   itself is not cosmetic either: a `RENAME COLUMN` committing between the data
   read and a "live" schema read names the envelope's columns with the new
   schema while the rows still carry the value under the old name, and the
   projection silently nulls it (findings doc §2.6).
6. **Produce to Kafka** (`_produce_to_kafka`, §5).
7. **Drop & rename propagation** (`_propagate_column_drops`, before the state write): when
   `propagate_drops` is on, diff the current source columns against `last_schema`
   — the JSON column list the state row has recorded since the previous sync. A
   column present there but absent from the current read is positive evidence of
   a change (a partial or failed schema read can never trigger one). A second
   diff — the schema the events were actually enveloped with against the final
   one — catches renames and drops landing *inside* the unsynced window, after
   the last data event, which `last_schema` (recorded before those columns
   existed) can never see; the two diffs' results are merged, with the event-era
   record adjusted by the state diff's changes first so nothing propagates
   twice. Since the
   ids landed in `last_schema`, the diff is rename-aware: the state records each
   column's `delta.columnMapping.id` (`{"name": ..., "id": ...}`; the reader
   tolerates the original name-only format), and the reader fetches the
   current ids with delta-rs (`get_column_ids` — Spark's DataFrame
   schema strips field metadata, verified back when the sync ran on OSS Delta
   Spark, but delta-rs preserves it, so this is a ~0.2s
   local read rather than a Spark log-scan; the id is what survives a
   `RENAME COLUMN`). A recorded column whose id reappears under a new name is a
   **rename**: RisingWave gets `rename_columns_in_risingwave` — `ADD COLUMN` the
   new name, `UPDATE` it from the old (data survives; the column moves to the
   end), then drop the old `<col>_ts` companion and the old column. Postgres
   gets `rename_columns_in_postgres` — a native `RENAME COLUMN`, or a copy of
   the data plus a drop of the old column when the sink's schema evolution
   already created the new one (empty) while the rename was pending — followed
   by a sink connector task restart: the JDBC sink caches the table schema in
   memory, and a rename behind its back fails the next message exactly like a
   manual column drop. In the copy-and-drop case the new column stays where the
   evolution put it — last — so on both targets a raced rename ends with the
   column at the end; positions are cosmetic, everything maps by name. Both
   registrations go through `_registration_fields`,
   which keeps every column the targets still have: RisingWave rebinds the
   whole table against the registry's latest schema on every `ADD COLUMN` and
   rejects the statement while a table column is missing from it, so a schema
   built from the source's new names alone would shed a pending rename's old
   name and fail the add path in the same sync (verified live). A recorded
   column with no surviving id is a
   **drop**: dropped from RisingWave (`drop_columns_from_risingwave`, which
   takes the generated `<col>_ts` companions down first) and from Postgres
   (`drop_absent_columns`, the broader "not mirrored any more" rule, which also
   re-drops a column a sink replay might resurrect — the sink creates that table
   purely from message fields, so it has no designed extras). The
   warehouse/notebook path cannot read the ids (no column metadata in
   `information_schema.columns`), so renames stay drop + add there.
8. **Write the watermark and `last_schema`** — only after produce returned
   success, and only when the version actually moved (no-op syncs skip the
   write). The written `last_schema` carries the ids, so the next sync can tell
   a rename from a drop.
9. Emit run metadata (events synced, op counts, commit version, columns dropped
   and renamed, message previews, extraction, and the cost entries from the
   meter, §9).

**Watermark semantics** — the subtle part, commented in the asset:
the reconstruction's version range is **inclusive**, and
`last_version` was already fully processed by the previous run, so the next read
starts at `last_version + 1`. Resuming from `last_version` itself would re-deliver
that version's rows. On a first-run baseline with no rows, the watermark is set to
the table's *current* version so the run is still a real checkpoint. And when the
read comes back **empty but the version advanced**, the commits were schema-only —
the reconstruction emits no rows for an `ADD`/`DROP COLUMN` — so the watermark still
moves to the current version. (Letting it strand on the old watermark was a live
bug: the schema commit was re-read by every sync forever, and drop propagation
never saw it.)

The reads themselves are `read_changes()` in `reverse_etl_delta_reader.py` (§6):
file-set comparison per commit version, new files only for append-only commits,
full snapshot diff for rewrites. The comparison baseline for this path is the
Databricks **notebook** (`reverse_etl_notebook_sync_job`, Spark + `readChangeFeed`
inside the workspace) — the SQL-warehouse branch was deleted with the refactor,
its measurements preserved in `REVERSE_ETL_DBU_COST_NOTES.md` and
`REVERSE_ETL_SPARK_FREE_FINDINGS.md`.

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

One producer per run — created **early** (before the CDF read, only when there is
something to read) so its TLS/SASL handshake to the staging brokers (~9 s over
VPN) overlaps the Spark work instead of stalling the first produce. Encoding is
config: `avro` (Confluent wire format, schemas under `<topic>-key` /
`<topic>-value` on the staging Apicurio's `/apis/ccompat/v7`) or `json` (Kafka
Connect JSON with the schema embedded per message).

Schemas are **not re-registered when unchanged**: `_schema_already_registered`
compares the registry's latest version against the built schema (parsed, so key
order cannot false-negative), and when both subjects already hold identical
schemas the serializers run with `use.latest.version` instead of auto-registering.
On any registry error the check fails open — register anyway, exactly as before.

The delivery guard is the load-bearing detail:

```python
producer.produce(..., on_delivery=lambda err, msg: delivery_errors.append(str(err)) if err else None)
undelivered = producer.flush(timeout=120)
if undelivered or delivery_errors:
    raise RuntimeError(...)
```

`produce()` only enqueues; failures surface later in librdkafka's threads. The
callbacks + the bounded `flush` fail the run **before the watermark append that
follows**, so the next sync replays the same version instead of silently skipping
events that never reached the topic. Replays are harmless: both targets upsert by
key. (This was added after a broker blip proved the failure was silent — see
`REVERSE_ETL_DEBEZIUM_JDBC_SINK.md`.) The producer itself is tuned for volume
syncs: lz4 compression on the wire, 50ms linger so librdkafka batches, and raised
queue/batch ceilings — the final `flush` absorbs the linger, and a one-event demo
sync never notices it. The chunked sync (`flush=False` per chunk) enqueues as it
reads and calls `_flush_producer` **once, after the last chunk**: the broker
transfers while the next chunk's Spark read runs instead of every chunk paying
its own round-trip.

## 6. The delta-rs backend — `reverse_etl_delta_reader.py`

The zero-Databricks-compute module. (Live proof across all four syncs, the
performance comparison against the Spark era, and the when-to-use-which
assessment live in `REVERSE_ETL_SPARK_FREE_FINDINGS.md`.)

The reconstruction: for each commit version in the requested range, diff the
snapshot at v-1 against the snapshot at v (two column-pruned scans) and emit
CDF-shaped rows — `insert`, `update_preimage` + `update_postimage` pairs,
`delete` — ordered by `_commit_version`, which is what
`readChangeFeed`/`table_changes()` do internally for tables without the CDF
feature. Two design points worth keeping:

- **Per-version diffing is required for correctness.** A key deleted in one
  commit and reinserted in the next must emit both events; a diff over a whole
  range would net them out to nothing. The sync still chunks the range
  (`_version_chunks`, `REVERSE_ETL_CDF_CHUNK_VERSIONS`) and streams
  read → produce → read with `flush=False` and per-chunk heartbeat lines —
  that machinery lives in the sync asset body (`reverse_etl_cdf_setup.py`) and
  is unchanged from the Spark days.
- **Rewrite windows diff only the rewritten file pairs** (2026-10-19, after the
  full-snapshot wall was measured). A commit's log names exactly which files it
  removed and added; with deletion vectors off (enforced — the reader refuses
  DV tables) the changed rows live only in those pairs, so the rewrite path
  reads the removed files as the pre-image and the added files as the
  post-image and diffs those — memory and I/O sized by the touched files, not
  the table. Rows in untouched files cannot have changed, compaction's pairs
  diff to nothing, and a pre-image needs no carry-forward state: the removed
  files already hold the rows as of one version earlier. Partitioned tables
  and no-Azure-filesystem runs keep the full-snapshot diff as the fallback
  (correct at any shape). Verified live: update and delete windows emit the
  same events the full-snapshot diff emitted (findings doc §6).
- **Schema changes need no bisection.** Snapshot reads do not error on
  incompatible schema changes the way CDF reads do: an added or dropped column
  reads as `None` on the other side and never false-positives an event (the
  row-diff compares values, treating an absent column as null). Drop
  propagation onto the targets is unchanged (evidence-gated via the state
  table's `last_schema`, `get_column_ids` for renames).

Function contract, mirroring the deleted Spark module:

- `read_changes(cfg, location, since_version, until_version)` — the
  reconstruction; the caller pins `until_version` to the source's current
  version so the read range matches the watermark exactly. Every `TIMESTAMP`
  column is rendered into a microsecond UTC string before comparison, so a
  datetime-vs-formatted-string mismatch can never read as a change.
- `read_state` — the state table read (watermark + `last_schema`, freshest row
  wins), only the fail-open fallback when the delta-rs pre-check could not run.
  The watermark WRITE is the one small SQL statement `_write_state()` in
  `reverse_etl_cdf_setup.py`, deliberately: this Databricks runtime creates
  every warehouse-written table with a v7 protocol whose writer-feature list
  (rowTracking, domainMetadata) delta-rs 1.6 refuses to commit to, so a table
  cannot have both writers — delta-rs keeps the reads, one SQL insert the
  write. The state table stays **plain Delta** (option A in
  `REVERSE_ETL_DBU_COST_NOTES.md` §5).
- `get_current_version` / `get_earliest_available_version` — `DeltaTable
  .version()` for the latest; a bisection on `load_as_version()` for the
  earliest, where "loadable" means exactly what the reconstruction needs: a
  version readable as a snapshot, through commit files or a checkpoint. Any
  load failure that does not look like an unavailable version re-raises — a
  wrong "earliest" would silently corrupt the first-run backfill decision.
- `get_row_fields(location, version=None)` / `get_column_ids(location,
  version=None)` — the schema reads, current by default. The sync passes the
  version it is reading: schema and data must come from the same commit, or a
  rename landing mid-run nulls values under the renamed column (findings doc
  §2.6).

Every versioned read goes through `_version_handle(location, version)` — a
fresh `DeltaTable` constructed *at* that version, so schema, add actions and
data are consistent by construction (a reused handle across `load_as_version()`
jumps is one less thing to reason about, at the cost of one handle construction
per version; log replay is checkpoint-bounded). Per-version commit timestamps —
the envelope's `source.commit_timestamp` — come from a `{version: timestamp}`
map built once per read from the *current* handle's full `history()`, keyed by
each entry's version field: on a version-pinned handle, `history()` mislabels
entries (the version field shifts by the pin, and `history(1)` reports the
newest commit's fields under the pinned version's label — findings doc §2.5).

One verified delta-rs finding shapes the read path: **1.6.6 does not apply the
columnMapping physical→logical rename for warehouse-written column-mapped
sources** (columnMapping as a writer feature, no reader features — verified on
both the UniForm v2 and the plain column-mapped flavor) — `to_pyarrow_table()`
returns the logical schema with every value None. The reader detects the
all-None scan and re-reads the snapshot's parquet files directly, renaming via
`delta.columnMapping.physicalName` with pyarrow's `AzureFileSystem` (the same
service principal), and refuses sources with deletion vectors, which file reads
cannot apply. The earlier claim that "OSS Delta reads column-mapped tables
fine" held for plain column-mapped tables and is wrong for this UniForm v2
shape — hence the fallback.

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
  and the state table (plain Delta with column mapping; every demo table takes
  `DROP COLUMN`). Idempotent (`IF NOT EXISTS`).
- **`build_seed_asset`** — optional demo rows (the YAML's `seed_statements`,
  `{source_table}` substituted), run as the *last* step of the setup job so the
  rows wait in Databricks until the first sync.
- **`build_change_asset`** — one asset (in its own `define_asset_job`) applying a
  demo change: `insert`/`update`/`delete` target the row with max `id`; `add_column`
  `DESCRIBE`s first and only adds columns that don't exist, so re-clicking it is a
  no-op; `rename_column` flips the highest `new_col_<n>` to `renamed_col_<n>` (and
  clicking it again renames that back) so the demo cycle add → rename → drop composes;
  `drop_column` drops the highest-numbered `new_col_<n>` or `renamed_col_<n>`. The
  configured columns are never touched by the rename/drop ops. This is an *asset*
  (not an op) so its run metadata — including its measured warehouse seconds and the
  exact SQL it ran (`databricks_change_sql`, which the demo page shows under the
  result) — is queryable.
  The ops stay inside what Delta natively supports, which is narrower than it
  looks: **adding a column** is always allowed (metadata-only; existing rows read
  `NULL`); **dropping, renaming, and reordering columns require column mapping**
  (without it Delta refuses `DROP COLUMN` — legacy readers match columns by name
  and would break). With mapping (`delta.columnMapping.mode = name`, set on every
  demo table) a drop or rename is a logical schema edit: each column carries a
  stable `delta.columnMapping.id` and physical name, so old Parquet files stay
  readable and the change touches no data. **Type changes** are widening-only
  (`byte→short→int→long`, `float→double`, recent versions); anything lossy would
  need a table rewrite and has no `ALTER`.
- **`build_risingwave_table_asset`** — creates the RisingWave target
  (`CREATE ... INCLUDE timestamp AS rw_event_timestamp` style upsert/delete table,
  `FORMAT DEBEZIUM ENCODE AVRO/JSON` per encoding). `add_missing_columns()` is the
  schema-evolution helper the sync calls before producing; with Avro it registers
  the new schema version *before* altering the table, because RisingWave validates
  new columns against the registry schema — and the registration keeps every column
  the table still has (`_registration_fields`), since RisingWave rebinds the whole
  table against the registry's latest on every `ADD COLUMN` (a pending rename's old
  name or a pending drop must not be shed mid-flight). Registration must make the
  built schema the registry's *latest*, not just present: registries dedupe by
  content, so re-registering a schema whose bytes already exist as an older version
  (the demo's second add → rename → drop → add cycle) is a no-op that leaves the
  latest behind and RisingWave rejects the column. `_register_avro_schemas` decides
  from the register's returned schema id against a snapshot read BEFORE the write —
  the registry's latest-version read lags its writes by seconds, so a post-write
  verification raced its own register (seen live); when the returned id matches an
  older version, the versions after it are soft-deleted and the register repeats,
  landing the dedup on that version as the latest. Soft delete keeps the content
  resolvable by id, so messages already on the topic still deserialize (all
  verified live 2026-10-19; findings doc §2.8).
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
delta-rs sync emits `0.0` for reads **structurally** — it makes no compute calls
(only the watermark write meters a second or two). The demo
helper reads the metadata back through GraphQL and shows it as the "Databricks
cost" chip; the semantics (seconds, not DBUs) are in
`REVERSE_ETL_DBU_COST_NOTES.md`. Per-process is deliberate: multiprocess steps get
isolation for free, and the in-process setup job resets at every asset entry.

## 10. One change, end to end

| Step | Where |
|---|---|
| Demo change (e.g. `UPDATE`) commits → new source version | `reverse_etl_change.py` `make_change` |
| Sync materializes: watermark + `last_schema` read | delta-rs `precheck_versions` (reused); SQL `_read_state` only as the fail-open fallback |
| CDF rows read since `last_version + 1` | `reverse_etl_delta_reader.read_changes` (reconstructed: file sets per commit, new files only for appends) |
| Rows → Debezium envelopes | `_to_debezium_events` |
| Source schema read (pinned to the chunk's top commit); RisingWave columns added | `get_row_fields(location, version=chunk_to)` (delta-rs) → `reverse_etl_risingwave_setup.add_missing_columns` |
| Avro schemas registered; events produced, delivery verified | `_register_avro_schemas` → `_produce_to_kafka` |
| Watermark + `last_schema` written to the state table | `_write_state` (the one small SQL statement; delta-rs cannot commit to the v7 state table) |
| RisingWave applies the change (native Debezium + Avro) | RisingWave `reverse_etl_avro_target` |
| Postgres upserts/deletes via the JDBC sink | `reverse_etl_debezium_sink` (created) → connector |
| Health checked every 5 min | `reverse_etl_health.sink_healthy` |

## 11. Gotchas worth knowing before editing

- **Deletion vectors must stay OFF on the source.** With DVs enabled (this
  runtime defaults new plain-Delta tables to `'delta.enableDeletionVectors' =
  'true'`), a 1-row DELETE makes the Change Data Feed emit the *whole touched
  file*: verified live — two single-row deletes on 100k-row files produced
  200,000 delete + 100,000 insert CDF rows, the sync relayed them verbatim, and
  the targets lost 100,000 legitimate rows (900,002 instead of 1,000,000). The
  source DDL sets DVs off; re-enable them and every delete sync becomes
  file-scale. (The bloat is invisible on small files — the seed rows' deletes
  looked clean all along.)
- **Never read the source schema "live" inside the sync.** The read range is
  pinned from the pre-check; the schema used to name the envelope and detect
  renames must be pinned to the same commit (`get_row_fields(location,
  version=...)`, `get_column_ids(location, version=...)`). A `RENAME COLUMN`
  committing between the two reads names the envelope's columns with the new
  schema while the rows carry the value under the old name — the projection
  nulls it, and because the state row then records the new schema, no later
  sync can even detect the rename. Verified live 2026-10-19: a populated value
  was lost exactly this way, and recovered only by a new populate event after
  the fix.
- **delta-rs `history()` lies on version-pinned handles.** Pinned below the
  latest version, every entry's `version` field shifts by the pin and
  `history(1)` carries the newest commit's operation and timestamp. Per-version
  commit metadata must come from a current handle's full `history()`, keyed by
  each entry's version field.
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
