# Plan: decouple the reverse-ETL component from the demo

*Written 2026-10-25 against `main` after PR #1. **Status: steps 1 (config split), 2 (hooks seam), 3 (core
signature), 4 (demo assets moved), 5 (preconditions) and 6 (two components) are implemented on
`feature/component-decoupling`; the rest is a proposal.** Line numbers refer to
`orchestration/assets/reverse_etl_cdf_setup.py` as of the writing date.*

## 1. Goal

Today one component, `ReverseEtlCdfSync`, builds the sync **and** everything the demo needs around it. A team that
wants reverse ETL for its own table should not get any of the demo: no table creation, no topic creation, no RisingWave
table, no Debezium JDBC sink, no reset job.

| | Reusable component (for teams) | Demo (this repo's PoC) |
|---|---|---|
| Does | Reads the changes of an **existing** Delta table and writes them to an **existing** Kafka topic | Everything else the PoC shows |
| Databricks | Read-only (Unity Catalog lookup of the table location, then delta-rs reads of storage) | Creates and changes demo tables (setup, seed, make-change, reset) |
| Kafka | Produces to a topic that already exists | Creates and deletes topics |
| Targets | None. Consumers of the topic are the team's own | RisingWave table, Debezium JDBC sink to Postgres, health check |

The test of success: a team's `defs.yaml` using the component needs **no** `risingwave`, `http` or write access to
Databricks, and the component's Dagster definitions contain exactly one asset.

## 2. Where the two are tangled today

The sync asset (`cdf_to_kafka`, lines 1309 to 1662) is the core, but it reaches into the demo in five places:

| # | Coupling | Where | Why it is a problem |
|---|---|---|---|
| 1 | Takes `risingwave` and `http` resources | signature, lines 1323 to 1331 | A team would have to register RisingWave and Kafka Connect resources |
| 2 | Creates RisingWave columns before producing | `add_missing_columns`, lines 1476 and 1524 | Writes to RisingWave. Ordering matters: RisingWave only fills a column from messages read after the column exists |
| 3 | Propagates drops and renames to RisingWave and Postgres, then restarts the sink task | `_propagate_column_drops`, lines 1594 and 1628 | Writes to two target databases and Kafka Connect. Must run after delivery and before the watermark is saved |
| 4 | Declares upstream dependencies on demo assets | `deps=[table_setup_asset, topic_asset]`, line 1313 | The sync cannot exist without the demo's setup assets |
| 5 | Relies on the demo creating the watermark table | `table_setup` creates `reverse_etl_sync_state`, line 1278 | The sync reads and writes a table only the demo creates |

Further coupling is in the config and the component:

- `ReverseEtlSyncConfig` mixes sync fields (`kafka_topic`, `source_table`, `encoding`) with demo fields
  (`source_columns`, `seed_statements`, `connector_name`, `postgres_table`, `risingwave_table`, `sink_tasks_max`,
  `uniform`, `reset_*`, `create_topic_asset`, `propagate_drops`).
- `build_reverse_etl_defs` always builds the demo assets and jobs (`orchestration/assets/reverse_etl_defs.py`).
- `reverse_etl_cdf_setup.py` (1,694 lines) holds both the sync and the demo's `table_setup` and `seed` assets.

The good news from reading the code: the sync reads Databricks **read-only** already. The token is used for the
control-plane lookup only, and reads go through delta-rs, so the Databricks side needs no change.
The fields the sync actually uses from the config are few: `sync_name`, `kafka_topic`, `source_table` (and
catalog/schema through `source_fqn`), `key_column`, `encoding`, `schema_registry_url`, `envelope_schema_name`,
`key_subject`, `value_subject`, `group_name`, `sync_asset`.

## 3. Target design

### 3.1 Two components

- **`ReverseEtlCdfSync` (the reusable one).** Builds one asset: the sync. Attributes: only what the sync needs.
- **`ReverseEtlCdfDemo` (new).** Keeps today's full attribute set. Builds the sync asset **through the same builder**,
  with demo hooks attached, and adds all demo assets and jobs.

The five existing `defs.yaml` files that use today's `ReverseEtlCdfSync` change their `type:` line to
`ReverseEtlCdfDemo`. Their attributes stay as they are.

### 3.2 The reusable component's attributes

| Attribute | Required | Notes |
|---|---|---|
| `name` | yes | Label. Derives the sync name (the watermark key and Debezium envelope prefix) and the asset name, as today |
| `catalog`, `schema_name`, `source_table` | yes | The existing table. Today `source_table` is derived from `name`; for teams it is given explicitly |
| `key_column` | yes | No default. `rid` is the demo's surrogate key and means nothing for a real table |
| `kafka_topic` | yes | Must already exist |
| `encoding` | no | `json` (default) or `avro` |
| `schema_registry_url` | no | Needed for `avro` |
| `group_name`, `sync_asset` | no | Overrides |
| `deps` | no | Optional upstream asset keys, so a team can order the sync after its own pipeline |

Not in it: `source_columns`, `seed_statements`, `connector_name`, `postgres_table`, `risingwave_table`,
`sink_tasks_max`, `uniform`, `propagate_drops`, `create_topic_asset`, `reset_*`, the job and asset name overrides for
demo assets.

### 3.3 Config split

Introduce a small `CdfSyncConfig` holding exactly the fields listed in section 2. `ReverseEtlSyncConfig` becomes a
subclass that adds the demo fields (declared keyword-only so field order is not a problem). Effects:

- Core functions are typed to `CdfSyncConfig`. The demo keeps passing a `ReverseEtlSyncConfig`, so **no demo module
  changes** (`reset`, `change`, `jobotron`, `notebook_job`, `drt` and the targets setup keep working untouched).
- `for_name()` and every derived name stay on the demo config, so existing names (and therefore asset keys, topics,
  tables, connector names, the watermark key) do not change.

### 3.4 Schema-change hooks (the one real design decision)

Couplings 2 and 3 are not removable, because the demo needs them, and they depend on **when** they run inside the sync.
Splitting them into separate downstream assets would break the ordering. So the core gets a narrow extension point
and the demo supplies the RisingWave and Postgres behaviour:

```
class SyncHooks(Protocol):
    def before_produce(self, context, cfg, row_fields) -> dict:
        """Called once per event-bearing chunk, after the chunk's schema is resolved and before its
        messages are produced. Demo: add_missing_columns on RisingWave. Returns metadata to add to the
        run (list values merge across chunks, others overwrite)."""
    def after_delivery(self, context, cfg, schema) -> dict:
        """Called once after the final flush succeeded and before the watermark is saved. `schema`
        carries the final fields and column ids, the previously recorded schema, and the schema the
        events were produced with (fields, ids, version), which is what the drop and rename diff needs.
        Demo: the drop/rename propagation and the sink restart. Returns metadata."""
    def idle_metadata(self) -> dict:
        """Metadata keys for a no-op run, so Dagster shows the same columns whether or not anything moved."""
```

*(Implemented as written in step 2 — see `SyncHooks` in `reverse_etl_cdf_setup.py` and
`reverse_etl_demo_hooks.py`; the ordering contract is pinned by `tests/sync_asset/test_sync_hooks.py`.)*

- With no hooks (the reusable component), nothing is called and none of the demo metadata keys
  (`risingwave_columns_added`, `columns_dropped`, `columns_renamed`) are emitted.
- The hooks reach resources through `context.resources`. The builder takes an `extra_resource_keys` argument so the
  demo declares `risingwave` and `http` on the asset; the reusable component declares neither.
- **Ordering contract, kept exactly as today and covered by tests:** `before_produce` runs before the chunk is
  produced; `after_delivery` runs after the flush succeeded and before the watermark is written. A failing hook
  therefore fails the run before the watermark moves, and the next run repeats it (every hook operation is already
  idempotent).

### 3.5 Watermark state

The sync stores its watermark and last-seen schema in the Postgres table `reverse_etl_sync_state` and reads it with
the `postgres` resource. That table is created today by the demo's `table_setup` asset. Decision D1 below covers where
the state lives for teams. The recommendation is to keep it in Postgres but stop creating the table in the core: the
core checks that it exists and raises an error that contains the `CREATE TABLE` statement, and the demo's setup keeps
creating it. The table name, columns and meaning do not change, so existing syncs keep their watermark.

### 3.6 Topic

Today the topic asset runs before the sync and creates the topic. The core stops depending on it: before producing it
checks that the topic exists and fails with a clear message if not. The demo's topic asset is passed to the core as a
`deps` entry, so the demo's graph looks the same as today.

## 4. Where each piece goes

| Piece | Today | After |
|---|---|---|
| Sync asset (`cdf_to_kafka`) | demo + core mixed | **Core**, built by one builder used by both components |
| Delta reader, version chunks, Debezium events, Connect JSON and Avro messages, producer, state read and write | core code in `reverse_etl_cdf_setup.py` / `reverse_etl_delta_reader.py` | **Core** (unchanged) |
| `table_setup` (creates source and state tables) | `reverse_etl_cdf_setup.py` | **Demo** |
| `seed` | `reverse_etl_cdf_setup.py` | **Demo** |
| Topic asset | `reverse_etl_defs.py` | **Demo** |
| RisingWave table asset, Debezium JDBC sink asset, sink health check, job and schedule | assets modules | **Demo** (unchanged) |
| `make_change` asset and job | `reverse_etl_change.py` | **Demo** (unchanged) |
| Setup job, reset job | `reverse_etl_defs.py`, `reverse_etl_reset.py` | **Demo** (unchanged) |
| `add_missing_columns`, `_propagate_column_drops`, the sink restart | called from the sync asset | **Demo hooks** module |
| Notebook trigger job (`reverse_etl_notebook_job.py`) | demo | **Demo** (unchanged; it is the Spark-notebook alternative) |
| Jobotron and drt components | their own components | **Out of scope** |

Module layout: keep the names `reverse_etl_cdf_setup.py` (core) and the other module names as they are, and add
`reverse_etl_demo_assets.py` (table setup and seed) and `reverse_etl_demo_hooks.py` (the hooks). A rename of the
core module is optional and left to the end (D8), so the first steps do not churn imports across the tests and the
mutation script.

## 5. The reusable component's contract (for the team guide)

**Prerequisites the team provides.** A Delta table in Unity Catalog; a key column that identifies a row; Kafka and a
topic that exists; the Postgres state table (or whatever D1 decides); for Avro, a schema registry.

**Resources it needs.** `databricks` (control-plane lookup and storage read, no SQL), `kafka` (producer and, for the
topic check, admin), `postgres` (state), `registry` (Avro only). Not `risingwave`, not `http`.

**What it does not do.** Create or alter tables, create topics, write to any database other than its own state row,
or change anything in Databricks.

**What it writes.** Debezium-style events to the topic (Connect JSON or Avro); one state row per run that moved.
For Avro it registers the key and value schemas under `<topic>-key` and `<topic>-value` (D6).

## 6. Tests

| Area | Change |
|---|---|
| `tests/components/cdf_sync/` | Becomes the tests of the **reusable** component: its attributes (the smaller set), that it builds exactly one asset, and that it needs only the four resources |
| `tests/components/cdf_demo/` (new) | Moves today's attribute, definitions and YAML tests (they are about the demo's attribute set), plus the existing `reverse_etl_*` YAML files |
| `tests/sync_asset/` (91 tests) | Keep, running the sync **with the demo hooks attached** so every existing scenario (rename, drop, add, backlog, avro) still holds |
| New: core without hooks | The same sync scenarios with a resource set that raises on any access to `risingwave` or `http`, which proves the decoupling. Plus: the demo-only metadata keys are absent |
| New: hook contract | Call order (`before_produce` before produce, `after_delivery` after flush and before the watermark), once per chunk or per run as specified, a failing hook leaves the watermark unmoved, no hook call on an empty run |
| New: preconditions | Missing state table gives the DDL in the error; missing topic fails before any message is produced |
| `test_propagation.py`, `test_risingwave_setup.py`, `test_postgres_setup.py` | Unchanged (demo side) |
| `tests/mutation_check.py` | Paths for moved code are updated in the same step that moves it; new entries for the hook ordering and the two preconditions |
| Coverage | Report the reusable component's modules on their own, with a high bar |

## 7. Steps

Each step is its own commit and leaves the whole suite, the lint and the mutation script green. None of them touches a
production system. Live checks use the DEV demo as before.

1. **Config split.** Add `CdfSyncConfig`; make `ReverseEtlSyncConfig` its subclass; type the core functions to the
   base. No behaviour change.
2. **Hooks seam.** Add `SyncHooks` and call it at the two points. Move `add_missing_columns` and
   `_propagate_column_drops` calls behind `reverse_etl_demo_hooks.py`; the demo passes them in. Existing tests stay
   green through the demo wiring. Add the hook contract tests.
3. **Core signature.** Drop `risingwave` and `http` from the sync asset's parameters; add `extra_resource_keys` and
   `deps` to the builder. Add the core-without-hooks tests.
4. **Move demo assets.** `table_setup` and `seed` go to `reverse_etl_demo_assets.py`. `build_reverse_etl_defs` is
   renamed to the demo's builder and keeps building everything it builds today.
5. **Preconditions.** Core checks the state table and the topic (D1, D5); the demo keeps creating both.
6. **Two components.** New core `ReverseEtlCdfSync` (section 3.2) and `ReverseEtlCdfDemo` (today's attributes). The
   five `defs.yaml` files change `type:` only. Component tests move as in section 6.
7. **Docs.** A team guide ("use the component on your table") with an example `defs.yaml`, the prerequisites and the
   resource list; update `REVERSE_ETL_CODE_WALKTHROUGH.md` and `tests/README.md`.
8. **Optional: rename** the core module and update the imports.
9. **Verify live on DEV:** the demo's setup job, a sync with a rename in a backlog (the live test), and a run of one
   real-table sync (`company`) before and after, comparing messages and the watermark.

## 8. Risks and how they are contained

| Risk | Containment |
|---|---|
| The hook ordering is subtle (the RisingWave column must exist before its first message; propagation must run before the watermark is saved) | The ordering is a written contract with tests and mutations; all 91 sync-asset scenarios run against it unchanged |
| Asset keys and lineage change in Dagster | Names do not change. The core asset loses its upstream demo assets, and the demo restores them through `deps`, so the demo graph is identical |
| Existing syncs lose their place | The state table, its columns and the watermark key are unchanged; the first run after the change must read the old watermark (checked in step 9) |
| Two components drift apart | One builder for the sync asset; the demo only adds to it. The notebook-parity tests keep covering the notebook path |
| Teams hit the hook seam as a public API | It is only passed by the demo component. Not exposed as a YAML attribute |
| Wide diff | Nine small commits in the order above; steps 1 to 5 change no behaviour and no YAML |

## 9. Decisions for you

| # | Question | Recommendation |
|---|---|---|
| D1 | Where does a team's watermark live? (A) Postgres table that the team creates and the core checks. (B) Core creates it if missing. (C) In Dagster, as asset metadata | **A.** It keeps "no infrastructure created", keeps existing watermarks, and avoids losing state when Dagster's storage is reset (C). B is a small flag if A is too much friction |
| D2 | Hooks (section 3.4) or separate downstream assets for the demo's RisingWave and Postgres work? | **Hooks.** Separate assets cannot keep the "column exists before its first message" ordering |
| D3 | Which component keeps the name `ReverseEtlCdfSync`? | **The reusable one**, so teams get the obvious name; the demo becomes `ReverseEtlCdfDemo` |
| D4 | Config split by subclassing, a structural type, or composition? | **Subclassing.** No change to any demo module |
| D5 | Should the core fail fast when the topic does not exist? | **Yes**, with a clear message; it never creates it |
| D6 | Does the core keep registering Avro schemas in the registry? | **Yes.** Registering is part of publishing with Avro; it does not create infrastructure beyond the subjects for its own topic |
| D7 | Should the component offer a schedule? | **No.** One asset; teams attach their own schedule or sensor, the guide shows how |
| D8 | Rename the core module `reverse_etl_cdf_setup.py` (it no longer sets anything up)? | **Later and optional**, in its own commit |
| D9 | Do composite keys matter for teams? The code passes a list of key columns but the config has one | **Out of scope here**; note it in the team guide as a limit |
| D10 | Where does the team guide live? | `docs/poc/REVERSE_ETL_COMPONENT_GUIDE.md`, linked from the walkthrough |

## 10. Out of scope

- The Jobotron and drt components, and the Databricks notebook path (it stays part of the demo).
- Changing what the sync reads or writes (the Delta reader, the message formats, the Avro schemas).
- Any production system. Everything here is the local stack and the DEV Databricks workspace.
