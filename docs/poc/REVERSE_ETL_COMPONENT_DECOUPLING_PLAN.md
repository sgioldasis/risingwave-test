# Plan: decouple the reverse-ETL component from the demo

*Written 2026-10-25 against `main` after PR #1. **Status: steps 1-7 (config split, hooks seam, core signature,
demo assets moved, preconditions, two components, docs) are implemented and step 9's live verification (demo
reset+setup, rename-in-backlog live test, company no-op run with its watermark untouched) passed on
`feature/component-decoupling`; step 8 (module rename) is left optional. Sections 11-13 (explicit
infrastructure names, pluggable state and target backends, the sync/downstream split) are designed and
their decisions D11-D19 accepted as recommended on 2026-10-25: section 11 is implemented (commit bf223a6);
section 12 is implemented (steps 1-2, the state interface and DuckDB backend, live-verified on 2026-10-25 —
the company sync ran on a host-visible DuckDB state file, browsed from DBeaver, then moved back to
Postgres; step 3, the target selection with the DuckDB mirror, in commit 5cef099; step 4, docs);
section 13 is implemented (steps 1-2, the targets builder and the `sync:`/`targets:` blocks, with the
section 11 names landing on the split shape and docs; live-verified on 2026-10-25 with company and
delta setup runs on the split YAMLs — the delta run also caught the envelope-name seam leak, fixed
as a seam field, see 13.2).** Line
numbers refer to `orchestration/assets/reverse_etl_cdf_setup.py` as of the writing date.*

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

---

## 11. Plan: explicit infrastructure names, label-derived Dagster names (proposal, 2026-10-25, rev. 2)

*Written after steps 1-9 shipped. Revision 2: the label `name` stays, but only for the Dagster
objects. **D11-D13 accepted as recommended; implemented 2026-10-25 (five YAMLs, golden-names test,
live reset+setup verified with identical names).***

### 11.1 Goal

Today a `defs.yaml` gives a short label and the code *assumes* the rest — including the names that
land in **external systems**: `name: delta` becomes `reverse_etl_delta` in the watermark and the
Debezium envelope, `reverse_etl_delta_source` in Databricks, `reverse_etl_delta_topic` in Kafka,
`reverse_etl_delta_sink` in Kafka Connect, `reverse_etl_delta_target` in Postgres and RisingWave.
Reading the YAML does not tell you which tables, topics and connectors a sync touches, and
overriding one of them means knowing which others silently stayed derived.

The refactor, split by audience:

- **Infrastructure names — explicit and required in the YAML.** The watermark key, source and
  state tables, topic, connector and both target tables. These land in real systems, are painful
  to rename, and a typo in a derivation is only visible at run time.
- **Dagster object names — keep deriving from the label `name`** (`reverse_etl_<name>_to_kafka`,
  `_setup_job`, ...), overridable per name exactly as today. They exist only inside Dagster, are
  trivially renamable, and the label keeps five YAMLs short and uniform.

### 11.2 Inventory

| Name | Kind | Today | After |
|---|---|---|---|
| `sync_name` (watermark key, Debezium envelope prefix) | external | `reverse_etl_<name>` | explicit, required |
| `source_table`, `state_table`, `kafka_topic`, `connector_name`, `postgres_table`, `risingwave_table` | external | derived from the label | explicit, required (already overridable today; becomes required) |
| `group_name`, `table_setup_asset`, `sync_asset`, `risingwave_asset`, `sink_asset`, `topic_asset`, `seed_asset`, `setup_job`, `reset_job` | Dagster | derived from the label | **unchanged** — still derived from `name`, still overridable per name |
| make-change asset/job, sink-health job/schedule (`<sync_name>_make_change`, ...) | Dagster | derived from `sync_name` | **unchanged** — Dagster-internal, no new config fields |
| reset op names (`<sync_name>_delete_connector`, ...) | Dagster op names | derived inside the reset job | **unchanged** — internal to one job, invisible outside Dagster |
| `reset_name_prefixes`, `reset_schema` | external (what reset may drop) | constant / copied from `schema` | explicit — the YAML states them (already fields today) |
| `name` (the label) | — | drives everything | **stays**, but now drives only the Dagster object names |

The reusable component mirrors the split with its smaller set: `sync_name` becomes explicit and
required (the watermark key is the team's); `source_table`, `key_column` and `kafka_topic` are
already explicit; `group_name` and `sync_asset` keep deriving from `name`, overridable as today.

### 11.3 Steps

Each step is its own commit and leaves the suite, the lint and the mutation script green. No
effective name changes anywhere — the five YAMLs state exactly today's derived values, so topics,
connectors, tables and watermark keys do not move.

1. **Demo component.** `to_config()` stops calling `for_name()` for the infrastructure names: the
   seven external names (`sync_name`, `source_table`, `state_table`, `kafka_topic`,
   `connector_name`, `postgres_table`, `risingwave_table`) become required pydantic attributes and
   are mapped straight into the config; the Dagster names keep deriving from `name` exactly as
   today. A missing external name fails at load with the field named. `for_name()` stays as a
   **test helper only** — production paths stop deriving.
2. **The five YAMLs.** Each gains the seven external names written out with today's exact values.
   A golden-names test pins each file's expected external names, so a typo or an accidental rename
   fails the suite. The existing no-collision and reset-allowlist YAML tests stay as is.
3. **Reusable component.** `sync_name` becomes explicit and required alongside the already-explicit
   `source_table`/`key_column`/`kafka_topic`; `name` stays for the Dagster names. Component tests
   and the guide's example updated.
4. **Docs.** Guide, walkthrough and this plan's status line.

### 11.4 Risks and containment

- **A typo in an explicit external name** collides or orphans an object — caught by the
  golden-names test, the no-collision test and the reset-allowlist check that already run per YAML
  file.
- **Two names that must agree** (`name` and `sync_name` may but need not coincide) — the guide
  states the rule: the label names the Dagster objects, `sync_name` is the watermark key; changing
  `sync_name` for an existing sync restarts it.
- **The notebook trigger** still matches instances by `name` — unchanged, since `name` stays.
- **The demo UI** launches jobs by name and reads nothing component-typed — unaffected; verified
  after step 2 with one reset+setup cycle (names identical before/after).

### 11.5 Decisions for you

| # | Question | Recommendation |
|---|---|---|
| D11 | Split the name space as proposed — external names explicit, Dagster names label-derived? | **Yes** — it puts the YAML's verbosity where the risk is (external systems) and keeps it where the value is (Dagster conventions) |
| D12 | Should `sync_name` also keep deriving from the label (with an override)? | **No** — the watermark key is the one name a replay silently depends on; write it down |
| D13 | Keep `name` as a required label? | **Yes, required** — it is what the Dagster names derive from, and the notebook lookup matches on it |

*(Revision 1 of this section proposed full explicitness including the Dagster names, with `name`
removed; revision 2 keeps the label for the Dagster objects per review.)*

### 11.6 Out of scope

- Renaming any existing object (topics, tables, connectors, watermark keys stay as they are).
- Composite key columns, the Jobotron and drt components, and anything the earlier plan already
  leaves out (section 10).

---

## 12. Plan: pluggable state and target backends (proposal, 2026-10-25)

*Extends section 11 — the YAML already states every name; here it also states **where** state and
targets live. **D14-D16 accepted as recommended 2026-10-25; not yet implemented.***

### 12.1 Goal

Two things are hardwired today that a team may want to choose:

- **The state database.** The watermark lives in Postgres `reverse_etl_sync_state` (D1). The sync's
  read, write, precondition and the delta-rs pre-check all speak psycopg2 directly. A team without
  Postgres — or wanting a zero-infra file — has no alternative.
- **The target.** The component's contract is "writes to Kafka and the state table, and nothing
  else" — targets are Kafka consumers built by the demo (RisingWave table, Debezium JDBC sink into
  Postgres). Which target stack to build is not a choice; it is the demo's shape.

### 12.2 The state backend (well-defined, recommend doing)

A small `SyncState` interface replaces the direct psycopg2 calls: `require()` (the precondition,
with the backend's DDL in the error), `read(sync_name)`, `write(sync_name, version, schema)`, and
`create()` for the demo's setup. Two implementations:

| | `postgres` (default, today's behaviour) | `duckdb` |
|---|---|---|
| Where | the `postgres` resource, explicit `state_table` | a file path (`state_path`), same table/columns as today |
| Watermark semantics | unchanged: append-only, freshest-row-wins | same |
| Precondition | `to_regclass` check, DDL in the error | file+table check, `CREATE TABLE` in the error |
| The `postgres` resource | required | **not required** — the sync's required resources become backend-dependent (computed at build time) |
| Reset | clears the sync's rows | same |
| delta-rs pre-check | unchanged | same, through the interface |

`duckdb` is already a project dependency (the Jobotron scans run on it), so the only new code is
one small implementation. The tests already run against real local Delta tables; the DuckDB state
tests use a real temp file the same way.

Flat YAML attributes (matching section 11's style — no nesting):

```yaml
state_backend: duckdb          # default: postgres
state_table: reverse_etl_sync_state   # postgres only
state_path: /var/lib/sync/state.duckdb   # duckdb only
```

### 12.3 The target (one real fork — D14)

**Option A — targets stay consumers of the topic (recommended).** The component keeps its honest
contract: it produces to Kafka and touches only its state. Which *downstream* stack exists becomes
a YAML choice in the **demo** component (`targets: ["risingwave", "postgres"]` — the demo builds
only the selected target assets and their health check), and a DuckDB target arrives as one more
consumer asset in the demo's pattern (like the Jobotron consume asset: read the topic, upsert by
key into a `.duckdb` file). Teams bring their own consumer; a local DuckDB file becomes one
deployable target without the sync growing a write path.

**Option B — the sync writes the target directly.** `target: {backend: duckdb|postgres|none}` on
the reusable component: after producing, the run upserts the same rows into the target before the
watermark moves (replay-safe like everything else). One run, one transaction boundary — but the
component grows a write path per backend, the "writes to Kafka and the state table and nothing
else" promise dies, and it duplicates what a consumer of the topic already does. The hooks
(`SyncHooks`) remain the right seam for bespoke target care either way.

Recommendation: **A now, B only if a team actually needs in-run delivery** — the hooks can carry a
B-style target later without any YAML-shape change (a hooks implementation is already a component
input in everything but name).

### 12.4 Steps

1. **State interface.** Extract `SyncState` (postgres implementation = today's code moved); the
   sync asset, pre-check and reset go through it. Postgres-only behaviour, no YAML change yet.
2. **DuckDB state.** The second implementation; `state_backend`/`state_path` on the config and the
   reusable component; the demo's `table_setup` creates the chosen backend's state (Postgres table
   or DuckDB file+table); required resources computed per backend. Tests per backend, and the sync
   asset suite parameterised over both (it already runs two storage modes and two encodings).
3. **Target selection (option A).** `targets` on the demo component gates which target assets the
   demo builds; a `build_duckdb_target_asset` joins the demo's assets; the demo's health check
   follows the selected targets. The reusable component stays Kafka-only, guide updated.
4. **Docs.** Guide (state backends, the DuckDB target), walkthrough, plan status.

### 12.5 Risks and containment

- **Two state backends drifting** (different upsert/replay semantics) — the sync-asset suite runs
  against both; the property tests' invariants (chunking, watermark) are backend-agnostic by
  construction once the interface lands.
- **DuckDB single-writer** — the state file must not be shared by two syncs concurrently; the
  guide states it, and the path is explicit in the YAML so collisions are visible.
- **Target-selection halves the demo graph** — the demo component's tests assert each selection's
  exact asset set, the same way the reusable component's one-asset test does.

### 12.6 Decisions for you

| # | Question | Recommendation |
|---|---|---|
| D14 | Target as a consumer selection (A) or a direct write in the sync (B)? | **A** — keeps the component's contract, one new consumer asset gives DuckDB |
| D15 | Should the DuckDB state file be created on first run, or check-only like Postgres (error carries the DDL)? | **Create on first run** — it is a local file, not shared infrastructure; Postgres keeps check-only |
| D16 | Ship DuckDB state together with the explicit-names refactor (section 11) or after it? | **After** — section 11 touches the same YAML fields; landing 11 first keeps each step reviewable |

---

## 13. Plan: separate the sync from everything downstream (proposal, 2026-10-25)

*Extends sections 11 and 12 — explicit names name the seam; the target backends land on the
downstream side of it. **D17-D19 accepted as recommended 2026-10-25; implemented same day
(live-verified, see the status header).***

### 13.1 What already holds

After steps 1-9 the reusable component is already exactly what this section asks for: it reads
Databricks and writes Kafka, plus its own state row, and nothing else. Its config
(`CdfSyncConfig`) has no field for any connector, target table or warehouse; its asset needs only
the postgres (state), databricks, kafka and registry resources. The 14 references to connectors
and targets in the config module all live in the demo subclass.

What still mixes the concerns is the **demo layer**: one `ReverseEtlCdfDemo` YAML carries the sync's
fields *and* the connector's, target tables' and health check's; one builder composes both sides;
and the target names sit in the same YAML as the source's. The split below is about that layer.

### 13.2 The seam: the message contract

The only thing the two sides share is what the sync puts on the topic:

| The sync decides | The downstream sees |
|---|---|
| the topic (`kafka_topic`) | the same topic — its only input |
| the encoding (`json` / `avro`) | how to decode: Connect JSON with the schema embedded, or the registry's `<topic>-key` / `<topic>-value` subjects |
| the key column | the message key (and `primary.key.fields` for the sink, the RisingWave primary key) |
| the envelope name (`<sync_name>.<schema>.<source_table>`) | the Avro schemas' namespace: the RisingWave registration must byte-match the sync's schemas, so it receives the name as opaque text (`envelope_schema_name` on the targets config — found live 2026-10-25, when the split made the RW asset's Avro registration fail without it) |

The downstream component therefore needs **no Databricks resource, no source table, no catalog** —
it cannot know where the events came from. Conversely the sync needs **no connector, no target
tables, no warehouse** — it cannot know what happens after the topic. Target-side schema care
(add missing column before its first message, drop/rename propagation) already runs through the
demo's hooks inside the sync run, so it stays a *composition* input, not a component feature.

### 13.3 What moves where

| Concern | Owns | Component |
|---|---|---|
| The sync | source table, key column, topic, encoding, **its own state** (section 12's state backend), the sync asset | `ReverseEtlCdfSync` — as today |
| The targets | connector, Postgres target, RisingWave target, sink `tasks.max`, the **sink health check/job/schedule**, and section 12's target backends (a DuckDB target is one more downstream asset) | **new: the targets component** |
| Demo infrastructure | table setup, topic creation, seed, make-change, reset (reset spans both sides — it drops what the demo created, so it stays an infrastructure concern that owns the full name set) | the demo component, composing the two |

### 13.4 Structure options

**Option A — two `defs.yaml` per sync** (`reverse_etl_delta/` becomes `reverse_etl_delta_sync/` +
`reverse_etl_delta_targets/`): the truest separation — the targets file cannot even name the
source table. Costs: ten YAML files where there were five, and the demo's setup/reset jobs and
shared topic asset need a home (a third, infrastructure-level instance per sync, or the targets
instance doubling as the demo host).

**Option C — one demo YAML, two config objects** (recommended): `ReverseEtlCdfDemo`'s attributes
become a `sync:` block and a `targets:` block, each mapping onto its own config object; the demo's
`build_defs` composes the sync asset (with the demo hooks), the target assets and the demo
infrastructure as today. The separation is real at the config and builder level — the sync builder
takes only the sync config, the targets builder only the targets config — while the five YAMLs and
every job name stay exactly as they are. A team later promotes the `sync:` block to its own
`ReverseEtlCdfSync` YAML verbatim.

Either way, ordering is explicit: the target assets declare an upstream dep on the sync asset by
its name (already explicit under section 11), and the setup job sequences infrastructure → sync →
targets as it does today.

### 13.5 Steps

1. **Targets builder.** Extract the RisingWave, sink and health builders behind
   `build_target_defs(targets_cfg)` taking only the seam fields plus the target names; the demo's
   builder calls it with the targets half of its config. No YAML change yet — the demo's existing
   attributes fill both halves.
2. **The `sync:` / `targets:` blocks.** The demo component's attributes split into the two blocks
   (one file, option C); validation errors name the block. The five YAMLs are rewritten into the
   blocks with today's exact values — no name moves.
3. **Section 11 lands on the split shape.** The sync block's external names are `sync_name`,
   `source_table`, `state_table`, `kafka_topic`; the target names (`connector_name`,
   `postgres_table`, `risingwave_table`) become the targets block's explicit attributes. The
   golden-names test covers both blocks.
4. **Docs.** Guide (the seam, and how a team composes targets), walkthrough, plan status.

### 13.6 Risks and containment

- **The seam leaks** (a target field needed "just this once" on the sync side) — the sync builder's
  signature only accepts the sync config, so a leak cannot compile; the fake-contract tests already
  pin which resources each asset may touch.
- **Splitting the YAML breaks the demo jobs** — step 1 keeps every job name and selection identical
  (verified by the component tests' job assertions and one live reset+setup cycle).
- **Hooks straddle the seam** (they write to targets inside the sync run) — documented as the
  demo's composition choice; the reusable component's hook contract already defines the only two
  points where that is safe.

### 13.7 Decisions for you

| # | Question | Recommendation |
|---|---|---|
| D17 | Option C (one demo YAML with `sync:` / `targets:` blocks) or option A (separate YAML per concern)? | **C** — real config separation at half the file count; promoting the sync block later is mechanical |
| D18 | One targets component (RisingWave + JDBC sink together) or separate per-target components? | **One** for now — they share the seam fields and the health check; splitting per target is easy later if a team wants only one |
| D19 | Where does the sink health check live? | **The targets side** — it checks the connector and the lag, not the sync; it moves with step 1 |

---

## 14. Follow-up (not scheduled): make the first-run replay cheaper

*Observed 2026-10-25: the company sync's migration replay (fresh state table) processed its 9 source
versions as 9 chunks at ~10 s each — ~90 s of fixed per-chunk I/O (a delta-rs ADLS read, the demo hook's
RisingWave call, Avro registry + Kafka produce/flush) for 1-2 rows per chunk. Normal incremental runs pay
the fixed cost once: one chunk, a few seconds.*

The per-version chunking is the correctness mechanism (schema evolution replays in order), so treat both
options below as design changes, not cleanups:

- **Batch consecutive schema-stable versions** into one chunk on backfill — keeps the replay order and
  every rename/drop, collapses only the runs where the schema did not change.
- **Baseline on first run** (like the aged-out-history path already does) — one chunk, but the replay no
  longer carries the source's rename/drop history to the targets.

If replays become routine (state migrations, backend switches), plan this properly before anyone touches
the chunk loop.
