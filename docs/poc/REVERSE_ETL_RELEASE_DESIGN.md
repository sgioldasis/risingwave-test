# Reverse-ETL release design: the sync outside the demo

Status: **proposed** (2026-10-26), not implemented. This is the design for releasing the
reverse-ETL sync to another team working in a different project. It builds on the completed
decoupling plan (`REVERSE_ETL_COMPONENT_DECOUPLING_PLAN.md`) and reuses its decisions: the
sync/targets seam (section 13), the state backends (section 12, D15), the health check's home
on the targets side (D19). It **revises** the plan's naming rule (section 11) for the release
components — see the derivation policy below; the demo keeps the explicit blocks.

## Goal

A team in another project wants one Databricks table's changes on a Kafka topic — and,
optionally, the prebuilt targets. They get it by dropping YAML files into their Dagster `defs/`
directory; the project's defs loader (`load_from_defs_folder`) merges them with no glue code.
Nothing they load may import demo code.

## Naming policy: minted names are derived, owned names are typed

The plan's section-11 rule ("external names explicit") is replaced by a sharper line for the
release components — **who mints the name**:

- **Typed — names of things that exist independently of this component**: the label `name`,
  `catalog`, `schema_name`, `source_table`, `key_column`, `state_backend`, a topic the team's
  platform already provisioned, the DuckDB mirror's file path. We cannot know these; typing
  them is the contract. (`state_backend` is a decision rather than a name, but it is the
  author's to make — a closed enum, load-time validated, so explicitness costs nothing.)
- **Derived — names this component mints**, everything `reverse_etl_<name>[_<role>]`:
  `sync_name`, `kafka_topic` (when the component creates it), `connector_name`,
  `postgres_table`, `risingwave_table`, the envelope schema name, and all Dagster object names.
  A hand-typed minted name is how typos mint accidental identities; derivation makes the whole
  family consistent by construction. Evidence the defaults are right: every one of the demo's
  five YAMLs types exactly what `reverse_etl_<name>[_<role>]` produces.

Every derived field keeps an explicit override — not for daily use, but for migrations
("rename the label, keep the watermark") and for fitting an existing estate. Two instances
must not share a `name` (their derived names would collide) — the same constraint the demo's
five distinct labels already live under.

Labels are the only thing a rename touches, and renames are handled per field: Dagster-internal
names follow the label freely; minted external names are covered by the watermark guard below;
recreating a connector or target table under a new derived name is recoverable (the new object
backfills or is created fresh; the old one lingers until deleted).

## The shape: one YAML per concern

Four components. The sync component builds exactly one thing: the sync. Each target is its own
component, so a team takes only what it wants, and the presence of a YAML is the target
selection.

### The sync component

`ReverseEtlCdfSync`, evolved in place from today's flat component. The minimal YAML types six
facts and nothing else:

```yaml
type: orchestration.components.reverse_etl_cdf_sync.ReverseEtlCdfSync
attributes:
  name: company
  catalog: de_dev
  schema_name: their_schema
  source_table: dim_company        # must already exist; never created by the component
  key_column: Company_ID           # required, no default outside the demo
  state_backend: postgres          # or duckdb — required: the author's decision, not a default
```

The state's connection is deliberately absent from this file: the library's connection function
reads the Postgres variables from the environment (the env contract below), and the
deployment's Vault supplies their values. The YAML alone is the whole story — nothing to
create, no file to write. The chain: YAML names the backend → the connection function reads
env (`POSTGRES_HOST`/`PORT`/`USER`/`PASSWORD`/`DB`) → Vault supplies the values.

Everything else defaults or derives:

| Field | Default / derivation |
|---|---|
| `sync_name` | `reverse_etl_<name>` — watermark key + Debezium envelope prefix |
| `kafka_topic` | `reverse_etl_<name>_topic`; override + `create_topic: false` to produce to an existing topic |
| `create_topic` | `true` |
| `encoding` | `json` (avro registers schemas under `<topic>-key`/`-value`) |
| `schema_registry_url` | the staging registry URL |
| `state_table` | shared `reverse_etl_sync_state`, keyed by sync_name; override for a per-sync table |
| `state_path` | required when `state_backend: duckdb` (a local file needs no postgres connection) |
| `group_name`, `sync_asset`, job names | `reverse_etl_<name>[_<role>]` |

Builds the sync asset, the topic asset (when `create_topic`), and a small bootstrap job
(topic → sync). Deliberately absent — demo scaffolding, not sync concerns: `source_columns`,
`seed_statements`, `uniform`, `propagate_drops`, the table-setup/seed/make-change assets, the
reset job, `reset_name_prefixes`, `reset_schema`.

**The state.** The state configuration names *where* the watermark lives; the watermark itself
(`last_commit_version`, `last_schema`) is runtime data in that store, never config — the YAML
is read once at code-location load, and the watermark advances every run. The backend is the
sync author's explicit choice (consequence-bearing: restart semantics, a postgres dependency,
the DuckDB single-writer rule); which Postgres *instance* is never the YAML's business — the
library's connection function reads the instance from the environment, and Vault supplies the
values (the env contract below). Each run:
`require()` (Postgres fails with the DDL if the table is absent; DuckDB creates its file),
`read(sync_name)` (empty = first run, which starts from the table's current version — no
history replay), emit only versions after it to Kafka, `write` the new one. A run that dies
before the write leaves the watermark unmoved, so the next run replays the same versions —
safe because targets upsert by key. The targets have no state at all; the watermark is
exclusively sync-side.

**The watermark guard (renames must fail loudly).** Deriving `sync_name` from `name` has one
dangerous case: a label rename (or an override typo) changes the watermark key, `read()`
returns empty, and the sync silently restarts from the table's current version — skipping
every change since. Hand-typing does not prevent this; a typo causes the same silent restart.

Guarding it needs an identity that does *not* move on rename — and `label` is not one: both
`sync_name` and `label` derive from `name`, so a rename changes both and no stored row matches
either new value. The stable anchor is the **source table**: the state store records
`source_fqn` (and `label`, for the message) alongside `sync_name`, and when `read()` comes back
empty the run looks up rows for the same `source_fqn`:

| this run's `sync_name` | rows found for the same source, under another key | result |
|---|---|---|
| derived | also derived-named | **error** — the rename message below |
| derived | explicitly named | **error** — "this derived name collides with a deliberate sync; set `sync_name` explicitly" |
| explicit (pinned) | anything | **warn and proceed** — a deliberate identity; covers a pinned rename and a legitimate second sync on one table |
| either | none | proceed — genuinely fresh |

> watermark exists for `de_dev.sr_poc_external.dim_company` under
> `sync_name='reverse_etl_company'` (label `company`), but this instance derives
> `reverse_etl_company_v2` — the label was renamed. Pin `sync_name` in the YAML to keep the
> watermark, or delete the old rows to start fresh.

Whether `sync_name` was derived or pinned is config knowledge: the component records
`sync_name_explicit` on the config. The schema change is additive — nullable `source_fqn` and
`label` columns; legacy rows (NULL) match nothing, so no false positives, and the next sync run
repopulates them. With the guard, derivation is strictly safer than typing.

### The target components (one per target)

**RisingWave** — the only target that composes the envelope name, and the only one allowed
`source_fqn` (its CREATE reads the source's columns and types once — the seam's single
composition input):

```yaml
type: orchestration.components.reverse_etl_targets.ReverseEtlRisingwaveTarget
attributes:
  name: company
  source_fqn: de_dev.sr_poc_external.dim_company
  key_column: Company_ID
```

Derived/matched: `kafka_topic` (`reverse_etl_<name>_topic`, same derivation as the sync — they
match by construction), `sync_name` (derivation input for the envelope), `risingwave_table`
(`reverse_etl_<name>_target`), `envelope_schema_name` =
`f"{sync_name}.{schema}.{table}"` composed from `sync_name` + the last two components of
`source_fqn` — the same formula the sync side uses, so the Avro registration byte-matches by
arithmetic, not discipline. Overrides: `kafka_topic`, `sync_name`, `risingwave_table`,
`encoding`, `schema_registry_url` — override on one side only if you override on both.
`deps: [...]` is optional ordering. `scan.startup.mode='earliest'` covers message order — it
does not cover topic *existence*: materialize the sync's bootstrap once before a target's first
materialization, or add the topic asset to `deps`. Builds the RisingWave table asset.

**Postgres** — the Debezium JDBC sink plus the sink health check with its job and schedule
(D19: the check watches the connector and its lag, so it exists only when this YAML does):

```yaml
type: orchestration.components.reverse_etl_targets.ReverseEtlPostgresTarget
attributes:
  name: company
  key_column: Company_ID
```

Derived: `connector_name` (`reverse_etl_<name>_sink`), `postgres_table`
(`reverse_etl_<name>_target`), `kafka_topic` (same derivation as the sync), `sink_tasks_max`
(1). The health job/schedule names derive from `name`. The connector's coordinates come from
the environment only (`HOST_POSTGRES_URL`/`POSTGRES_USER`, password `${env:POSTGRES_PASSWORD}`
resolved by the Connect worker — see the Postgres section below; no JDBC fields in this YAML).
`deps` optional. The check's cadence
is optional `health_cron` (default: the library's 5-minute cron). One schema check rides with
implementation: the connector maps the table via `collection.name.format` with no schema, so
verify a schema-qualified `postgres_table` (`myschema.my_table`) flows through cleanly —
otherwise this YAML grows a `postgres_schema` field. *(Verified live 2026-10-26: a
schema-qualified `collection.name.format` creates the table in the named schema with rows
flowing — connector 3.7.0.Final on Connect 4.3.1 — so no `postgres_schema` field; pinned by
`test_a_schema_qualified_table_flows_through_collection_name_format_verbatim`.)*

**DuckDB** — the stateless local mirror (drains the topic, latest event per key, tombstones on
`op=="d"`, table named after the topic):

```yaml
type: orchestration.components.reverse_etl_targets.ReverseEtlDuckdbTarget
attributes:
  name: company
  key_column: Company_ID
  duckdb_target_path: /workspace/company_mirror.duckdb
```

The path is typed — it is a deployment decision, not a minted name (the default mount in this
repo is `/workspace`, but a team's project mounts its own). Consumer group:
`<name>-duckdb-target` — rename-safe, because the mirror reads from `earliest` and never
commits offsets. `deps` optional. It is a dev mirror with a stated ceiling: it re-reads the
whole topic every run (O(events ever), no committed offsets), so point it at PoC-volume topics
— not a high-rate production topic.

## The wire contract

Values that must agree across files, and how agreement is guaranteed:

- **Matched by construction** (both sides derive from the same `name`): `sync_name`,
  `kafka_topic`, the envelope schema name (same formula over `sync_name` + `source_fqn`), and
  the defaults of `encoding` / `schema_registry_url`. A seam test pins envelope-formula
  equality across the two sides.
- **Must agree, typed on each side**: `key_column`. And `kafka_topic`, `encoding`,
  `schema_registry_url` when either side overrides them — override on one side, override on
  both; the repetition is the contract made visible.
- **One-sided**: `source_fqn` (RisingWave only). The postgres and duckdb targets carry no
  source knowledge at all.

What the per-target split deletes from today's merged shape: the `targets: [...]` selection
list and its validation (YAML presence *is* the selection); `sync_asset`/`topic_asset` from
the targets config (the builders' hardcoded `deps=[topic, sync]` lines become each YAML's
`deps` field, so a target no longer knows the sync exists); `sync_name` from the postgres and
duckdb targets — the builders touch it only for Dagster-internal naming, which moves to
`label`. The RisingWave target regains `sync_name` — as a
derivation input for the envelope, not an identity of its own.

## Which Postgres does the postgres target write to?

No Dagster resource. The sink asset declares no resource keys — it POSTs a connector config to
Kafka Connect (its REST URL read from env); the connector itself opens the JDBC connection. The
coordinates come from
environment variables (`reverse_etl_debezium_sink.py`): `connection.url` from `HOST_POSTGRES_URL`,
`connection.username` from `POSTGRES_USER`, and `connection.password` stays the literal
`${env:POSTGRES_PASSWORD}`, resolved by the Connect worker — the password never passes through
Dagster. The state's Postgres is likewise env-fed — the design has no resource layer at all (see the env
contract).

Decision (reversed after checking the destination's conventions): the coordinates stay
env-only — no `jdbc_url`/`jdbc_user` in the YAML. One defs.yaml deploys to both staging and
production (the workspace copies the matching `container_context.<env>.yaml` per target), so an
environment-specific value must never live in a component YAML; a filled-in URL would make a
branch deploy write to production's database. The YAML's silence is the contract: the
database's identity is a deployment fact, and the workspace's home for those is env and Vault.
If a code location ever needs multiple postgres targets, the workspace-aligned pattern is a
logical name resolved against a root map (the `databricks-workspaces.yaml` pattern), not
literal URLs.

## The env contract

The components declare **no Dagster resources at all** — matching the destination, where no
project binds any. Connections resolve through library-internal, env-fed functions
(`reverse_etl_connections.py`): the sync asset, the target builders, and the hooks take what
they need from functions that read `os.environ`, and the assets declare no
`required_resource_keys`. The environment is the only place coordinates live, because it is
the only layer that differs per deployment: the same defs.yaml deploys to staging and
production, and the workspace provisions env per environment (Vault → External Secrets
Operator → `container_context.<env>.yaml`).

| Consumer | Env the connection functions read (verified in code where noted) |
|---|---|
| Sink connector assembly | `KAFKA_OUTPUT_BOOTSTRAP` (**required** — the assembly raises without it), optional `KAFKA_OUTPUT_SASL_USERNAME`/`_MECHANISM`/`_PASSWORD` |
| Connector JDBC coordinates | `HOST_POSTGRES_URL`, `POSTGRES_USER`; the password stays `${env:POSTGRES_PASSWORD}`, resolved by the Connect worker |
| delta-rs ADLS reads | `DATABRICKS_AZURE_CLIENT_ID` / `DATABRICKS_AZURE_TENANT_ID` / `DATABRICKS_AZURE_CLIENT_SECRET` — bare key names; service principal, never a SAS token |
| State Postgres (the sync's watermark) | `POSTGRES_HOST`, `POSTGRES_PORT`, `POSTGRES_DB`, `POSTGRES_USER`, `POSTGRES_PASSWORD` — the names in today's `get_postgres_connection` (`postgres_sink_setup.py:26-56`). The released function keeps these names but drops the `/.dockerenv` sniffing: `POSTGRES_HOST` becomes **required** (verified: `host.docker.internal` doesn't resolve on the macOS host, so no single default serves both sides) — the demo sets it per-context via compose on the dagster services, and the other four are explicit in the demo's `.env` (step 7) |
| RisingWave, Dagster-side Kafka clients, Databricks API, Kafka Connect REST | the remaining variables, enumerated per connection function as each is converted — the full table is the library README's env contract |

Everything env-adjacent that is *not* a credential stays in YAML or config: the registry URL
is a config field with an org-specific default, the DuckDB path is typed, topic names are
derived. The rule is the same one the YAMLs follow: names and decisions in files, values in
the environment.

**Why zero resources, given `Definitions.merge`.** The merge raises when two merged
`Definitions` carry different objects under the same resource key — component-shipped defaults
could neither be overridden by a team nor coexist across our own components. Env-direct
sidesteps the machinery entirely: nothing to merge, nothing to conflict, nothing for a team to
learn.

**A team's Python footprint: zero files.** Adoption is exactly the ingestion components'
story: drop the YAMLs, and SRE provisions the environment. Customization is env-shaped —
different instances mean different variable values, not different classes. The project layout:

```
their_project/
  definitions.py                  # @definitions + load_from_defs_folder — untouched, ~8 lines
  src/<team>/defs/
    reverse_etl_company_sync/defs.yaml
    reverse_etl_company_risingwave/defs.yaml
```

Note the asymmetry between the two Postgres writers — both env-fed, through different
processes: the state's Postgres is reached by the Dagster run's own process (the sync asset's
connection function), while the sink target's Postgres is reached by the Kafka Connect worker
via the connector's JDBC config. Two different writers, two different env scopes, neither
passing through YAML or a resource layer.

## Home in the production workspace

The component's destination is a shared library in `dagster-workspace` — the same shape as its
existing `kaizen-dagster-databricks` and `kaizen-dagster-dbt`: `libraries/kaizen-dagster-reverse-etl/`
with the components under `src/kaizen_dagster_reverse_etl/components/`, consumed by team
locations as `type: kaizen_dagster_reverse_etl.components.…` YAMLs in
`src/<team>/defs/<name>/defs.yaml`. The workspace boundary rule ("official `dagster-*` packages
provide integration mechanics; internal libraries provide Kaizen-specific behavior") is the
import-hygiene guarantee stated as policy.

- **Wiring.** Workspace projects load with `@definitions` + `load_from_defs_folder`; this repo
  assembles `Definitions.merge` + `load_defs`. The components are wiring-agnostic —
  `build_defs()` runs identically under either — so the demo keeps its wiring and a
  compatibility test (below) pins the production load path.
- **Versions.** The demo develops on dagster 1.13.26; the workspace floor is 1.13.17 (its
  libraries pin `dagster>=1.13.17,<2`). The release library pins the same range; no version
  work is needed. Python: the demo venv is 3.13, workspace locations run 3.14 — the library
  supports both.
- **Validation.** The workspace runs `dg check defs` against the real systems; the components'
  load-time validation (unknown fields, unknown targets, missing required attributes) fits it,
  and a
  future custom check could verify topic existence at configure time.
- **Component YAML conventions.** One folder per component instance; dg generates a `$schema:`
  header per defs.yaml (pre-commit regenerates from the component definitions) which the team
  commits; and the `type:` strings in this doc's examples are this repo's — in the destination
  they read `kaizen_dagster_reverse_etl.components.…`. The governing rule for values: **no
  environment-specific value in a component YAML** — one defs.yaml deploys to stg and prd, and
  per-environment facts (hosts, credentials) live in `container_context.<env>.yaml` and Vault,
  with per-env hosts resolved through root maps keyed by logical names
  (`databricks-workspaces.yaml` + `workspace: newport`). The reverse-ETL components are
  env-only for Databricks (no `workspace:` field; the connection functions read env — the same
  pattern as this workspace's own libraries) — at extraction, consider adopting the
  `databricks-workspaces.yaml` root map so per-env hosts converge on one mechanism.
  Catalog/schema/source names must be stable across environments
  (`target_catalog: de_bronze` hardcoded in the ingestion YAMLs is the precedent); a team whose
  names differ per environment needs the root-map treatment.
- **Infrastructure prerequisites (the open gap).** The workspace's required-infrastructure list
  covers Dagster+, GitLab/Harbor, Vault and Databricks — no Kafka brokers, schema registry,
  Kafka Connect, RisingWave or Postgres. The sync needs Kafka and a registry; the postgres
  target additionally needs a Kafka Connect deployment with the Debezium JDBC plugin; the
  RisingWave target needs production RisingWave. These are platform prerequisites (SRE scope),
  not code; until they land, the sync-only shape is the first release and targets follow their
  infrastructure.

## The port surface

The library is a specific, enumerated module set (verified against today's imports) — not
"everything under `orchestration/assets`":

`reverse_etl_config` (the base `CdfSyncConfig` and helpers), `reverse_etl_state`,
`reverse_etl_cdf_setup`, `reverse_etl_delta_reader`, `databricks_cost_meter`,
`reverse_etl_topic` (new, step 1), `reverse_etl_risingwave_setup`, `reverse_etl_debezium_sink`,
`reverse_etl_health`, `reverse_etl_duckdb_target`, `reverse_etl_connections` (the env-fed
connection functions that replace today's resource classes), plus the components
(`reverse_etl_cdf_sync`, the new `reverse_etl_targets`) and the slim target configs. Staying
behind: `reverse_etl_defs`, `reverse_etl_demo_assets`, `reverse_etl_demo_hooks`,
`reverse_etl_change`, `reverse_etl_reset`, `reverse_etl_notebook_job`, the drt modules.

Two structural notes. First, the config split is the port's axis: the sync asset's body reads
only base-config fields (verified attribute-by-attribute); the one demo-only access
(`cfg.propagate_drops`) lives in `_propagate_column_drops`, whose only callers are the demo
hooks — so the sync asset retypes to `CdfSyncConfig` (step 2) and the propagation helper moves
to the demo-hooks side. Second, the test kit (`tests/fakes.py`,
`tests/sync_asset/conftest.py`) is the executable proof of the connection seam — after the
env-direct conversion the fakes are injected by patching the connection functions — and ships
with the library as `kaizen_dagster_reverse_etl.testing`, so teams can reuse the fakes in their
own tests.

Optional step 0: group the surface into a subpackage (`orchestration/reverse_etl/`) so the
port is a directory move and the hygiene test is structural. Mechanical import churn across
`definitions.py`, the demo builder, drt and tests; if zero churn is preferred, the step-8 grep
test stands in — but the enumeration above is the checklist either way.

## Implementation steps

Each step is its own commit on a branch cut from the one carrying this doc, and leaves the
suite, the lint on touched files, and the mutation check green — the decoupling plan's
discipline. Steps 1–2 split what the outline called "sync component"; the order respects the
dependencies (builders' configs before components, components before connections).

1. **Topic bootstrap on the sync component.** Move `_build_topic_asset` out of
   `reverse_etl_defs.py` into a demo-free home (`reverse_etl_topic.py`); `build_demo_defs`
   imports it from there, unchanged in behavior. `ReverseEtlCdfSync` gains
   `create_topic: bool = True`; its `build_defs` appends the topic asset and a bootstrap job
   (topic → sync, `in_process_executor`), named via `dagster_names` (which gains a
   `bootstrap_job` entry — check the anchors referencing the dict). The topic asset exposes
   `topic_partitions`/`topic_config` with `cleanup.policy=compact` as the documented default —
   a Debezium-style change topic wants compaction; the broker default (`delete`) risks
   retention dropping events a lagging consumer has not read. Tests: the component defs
   test asserts the topic asset and job appear, and that `create_topic: false` builds neither.
   Mutation anchors: "the component skips the topic", "create_topic is ignored", "the topic
   config is dropped". Live check:
   rerun a demo setup job through the GraphQL path — the demo now exercises the moved builder.

2. **Derivation policy and the config-class correction.** `sync_name` and
   `kafka_topic` become optional attributes (omitted → `reverse_etl_<name>` /
   `reverse_etl_<name>_topic`; given → verbatim); `key_column` stays required;
   `state_backend` becomes **required** (`postgres` | `duckdb`, validated at load).
   The new config fields — `label` and the guard's `sync_name_explicit` —
   go on **`CdfSyncConfig`**, the class the released component constructs, not
   `ReverseEtlSyncConfig`; and the sync asset retypes from `ReverseEtlSyncConfig` to
   `CdfSyncConfig` (its body is already base-clean — verified attribute-by-attribute; the one
   demo-only access, `cfg.propagate_drops`, lives in `_propagate_column_drops`, which moves to
   the demo-hooks side in this step). (The sync asset keeps its resource keys for now — the
   demo hooks still reach their connections through them; they drop with the env-direct
   conversion in step 7.) The base keeps its
   `state_backend` default for the demo path; the component does not. Tests: the derivation
   matrix (omitted → derived, given → verbatim, `state_backend` missing → load error,
   `state_backend: duckdb` without `state_path` → load error);
   a static pinning test that every `cfg.<attr>`
   accessed in `reverse_etl_cdf_setup.py` exists on `CdfSyncConfig`; and the ripple audit —
   every construction site supplies the new fields (the demo component's `to_config`,
   `tests/sync_asset/conftest.py`'s `make_config`, `tests/fakes.py`, the drt path, which
   inherits through the demo component). Mutation anchors: "sync_name is ignored",
   "state_backend requiredness sneaks back", "the asset reads a demo-only field".

3. **The watermark guard (source-anchored).** `state_table_ddl` gains nullable `source_fqn` and
   `label` columns; both backends' `write()` persist `cfg.source_fqn` and `cfg.label`; a
   `rows_for_source(source_fqn)` lookup lands on the state interface; the sync run's
   precondition implements the decision table above (derived/derived → the pinned rename
   error, derived/explicit → the collision error, explicit → warn and proceed), with the error
   wording pinned for the mutation check. Migration is check-only plus migrate: `require()`
   also validates the new columns and its failure carries the exact
   `ALTER TABLE … ADD COLUMN IF NOT EXISTS …` statements (D15-consistent — a standalone sync
   materialization on a pre-guard table fails with the fix in hand instead of
   `UndefinedColumn` at write time); `create()` performs the migration, so the demo's next
   setup run upgrades its tables. DuckDB files are created fresh with the new DDL — old demo
   state files are recreated, not migrated (disposable, single-writer). Legacy NULL rows match
   nothing and are repopulated by the next run. Tests: both backends × {fresh, legacy row,
   rename error, explicit-overlap warn, collision error}; the wording pinned.

4. **Per-target configs.** Three slim dataclasses — `RisingwaveTargetConfig`,
   `PostgresTargetConfig`, `DuckdbTargetConfig` — each carrying only its target's fields,
   names derived from `label` via `dagster_names`, `deps: tuple[str, ...]`, and (RW only) the
   envelope composed from `sync_name` + the last two components of `source_fqn`. The four
   builders (`reverse_etl_risingwave_setup`, `reverse_etl_debezium_sink`, `reverse_etl_health`,
   `reverse_etl_duckdb_target`) retype to the slim configs; the hardcoded
   `deps=[AssetKey(cfg.topic_asset), AssetKey(cfg.sync_asset)]` lines become `cfg.deps`;
   health job/schedule and duckdb asset/consumer-group naming moves to `label` with slim
   naming `reverse_etl_<label>_<role>`. **The demo bridge**: `build_target_defs` composes the
   slim configs from the merged one passing
   `label = sync_name.removeprefix("reverse_etl_")` (all five demo sync_names conform) and
   `deps=(cfg.topic_asset, cfg.sync_asset)` — so the demo's health job/schedule and duckdb
   asset names stay byte-identical and its target assets keep their deps. One accepted demo
   delta: the duckdb consumer group becomes `<label>-duckdb-target` (was
   `<sync_name>-duckdb-target`) — rename-safe, the mirror never commits offsets.
   `targets_config()` and the five demo YAMLs stay untouched, and `test_targets_defs`'s
   check-spec assertions are the no-rename gate. Tests: per-config field and
   derivation tests; envelope-formula equality (the sync-side property equals the RW-side
   composition given the same inputs); existing builder call sites updated. Mutation anchors
   move to the slim configs ("a derived name is ignored", "the envelope drops the schema
   component", "the bridge strips the wrong prefix").

5. **Target components.** `orchestration/components/reverse_etl_targets.py`: three components
   with minimal required attributes — RW: `name`, `source_fqn`, `key_column`; postgres:
   `name`, `key_column`; duckdb: `name`, `key_column`, `duckdb_target_path` — derived fields
   with overrides (`kafka_topic`, `encoding`, `schema_registry_url`, connector/table names,
   `sink_tasks_max`, `health_cron`), the `deps` field, and the health check + job + schedule
   travelling with the postgres component. Tests mirror the demo component's attribute/defs tests: required
   attributes parametrized, per-component asset sets, golden derived names. Mutation group per
   component ("a derived name is ignored", "the health check is dropped").

6. **Connector schema check (JDBC fields stay out).** The YAML carries no JDBC coordinates —
   reversed after the destination's per-environment rule (see the Postgres section); the
   assembly keeps its env reads (`HOST_POSTGRES_URL`/`POSTGRES_USER`, the password untouched as
   `${env:POSTGRES_PASSWORD}`). Verify a schema-qualified `postgres_table` flows through
   `collection.name.format`; if it does not, add `postgres_schema` (default: the connector's
   default schema) to the config and component. *(Outcome: verified live 2026-10-26 — it does;
   no `postgres_schema` field was added.)* Tests: coordinates assembled from env only;
   the password never interpolated from config. Mutation anchors: "the schema qualifier is
   dropped", "the password is interpolated".

7. **Env-direct connections.** Convert the resource layer into `reverse_etl_connections.py` —
   one env-fed function per connection (`os.environ` only, explicit variable names, no
   `/.dockerenv` sniffing). Verified during design: the sniffing **is load-bearing** —
   `host.docker.internal` does not resolve on the macOS host, so one explicit value cannot
   serve both sides. The resolution: the state-postgres function takes `POSTGRES_HOST` as
   **required** (no default); the demo satisfies it per-context — compose sets
   `host.docker.internal` for the dagster services, whose `extra_hosts` maps it — and `.env`
   gains the remaining four (`POSTGRES_USER`/`_PASSWORD`/`_DB`/`_PORT`, matching today's
   effective defaults) so the helpers stop guessing them; the entrypoint keeps owning
   `HOST_POSTGRES_URL` (it bakes the resolved host IP into the connector config). The sync
   asset, the target builders, and the demo hooks drop `required_resource_keys` /
   `context.resources` entirely and call the functions directly — and so do the
   staying-behind demo modules that inject today's resource classes as Dagster parameters
   (`reverse_etl_drt`, `reverse_etl_notebook_job`, `reverse_etl_change`, `reverse_etl_demo_assets`,
   the demo defs' Kafka asset): with the bindings gone, Dagster refuses to build an asset whose
   annotated resource parameter has no resource under that key, so they convert in the same
   step and `reverse_etl_resources.py` dissolves (the hygiene test asserts nothing imports it).
   The demo's six inline bindings (`orchestration/definitions.py:1042-1048`) are deleted. The cost meter becomes an
   optional capability: when its env is unset the sync degrades to unmetered (metadata
   omitted) instead of raising. The env contract lands as the module docstring — including
   the sync's own storage credentials (`DATABRICKS_AZURE_CLIENT_ID`/`TENANT_ID`/`CLIENT_SECRET`,
   which delta-rs needs and no resource carries today; service principal, never SAS) and the
   connector's (`KAFKA_OUTPUT_BOOTSTRAP` is required — the assembly raises without it — plus
   the optional SASL variables). Gate: the existing suite unchanged except the fakes (tests
   switch from resource-binding to patching the connection functions
   `reverse_etl_connections.<fn>`) and the connection-default tests
   (`test_postgres_setup.py`'s defaults assertions flip to the required-`POSTGRES_HOST`
   contract) — plus a definitions-build smoke.

8. **Hygiene and compatibility.** The import-hygiene test — a static source scan (import
   lines) over the port surface asserting the component-reachable modules import no
   `reverse_etl_demo_*`/`reverse_etl_change`/`reverse_etl_reset` — is the
   extractability guarantee, and a scan so the mutation check can mutate it. The compatibility
   test builds a minimal dg project in a tmp dir
   (pyproject with `[tool.dg]`, a sync component YAML and a target component YAML) and
   loads it through `load_from_defs_folder`, asserting the assets and the health check all
   resolve with no resources configured — the exact production load path; its fixtures include a target
   YAML whose `deps` reference the sync component's topic asset and a variant with a dangling
   dep, proving both load.

9. **Docs.** "Releasing to a team" in `REVERSE_ETL_COMPONENT_GUIDE.md` (the naming policy, the
   minimal YAMLs, the wire contract, the env contract, the
   no-environment-values-in-YAML rule and the generated `$schema` header); the DuckDB
   mirror's scale ceiling and the library's org-specific defaults (the staging registry URL)
   documented where teams will read them; walkthrough rows
   for the new modules; and the library README skeleton (env contract + boundary rule) so the
   eventual extraction is a move, not a rewrite.

## Deferred

- **Demo migration to per-target YAMLs and derived names** — the demo keeps its merged
  `targets:` block, `build_target_defs` and explicit names (its setup job and reset span both
  sides). It can migrate later to prove the released shape. The demo also keeps its
  `Definitions.merge` wiring; production-style loading is pinned by the compatibility test,
  not by rewiring the demo.
- **Packaging** — this repo stays the source of truth; the destination shape is known (a
  `kaizen-dagster-reverse-etl` library in `dagster-workspace`, consumed per team as shown
  above), and extraction moves there once a real consumer exists. The import-hygiene test keeps
  that cheap.
- **Reset/teardown for teams** — demo scaffolding; add only if a team asks.
