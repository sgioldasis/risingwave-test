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
directory; `load_defs` merges them into their project with no glue code. Nothing they load may
import demo code.

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
  # state_resource: postgres       # which of the project's resource bindings carries this
                                   # connection (default key "postgres"). Credentials are
                                   # still never in this file — the binding holds those.
```

Everything else defaults or derives:

| Field | Default / derivation |
|---|---|
| `sync_name` | `reverse_etl_<name>` — watermark key + Debezium envelope prefix |
| `kafka_topic` | `reverse_etl_<name>_topic`; override + `create_topic: false` to produce to an existing topic |
| `create_topic` | `true` |
| `encoding` | `json` (avro registers schemas under `<topic>-key`/`-value`) |
| `schema_registry_url` | the staging registry URL |
| `state_table` | shared `reverse_etl_sync_state`, keyed by sync_name; override for a per-sync table |
| `state_path` | required when `state_backend: duckdb` (a local file needs no postgres resource) |
| `state_resource` | which of the project's resource bindings carries the state connection; default key `postgres`; only valid with `state_backend: postgres` |
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
YAML only names which of the project's bindings to use (`state_resource`, default `postgres`),
and the binding itself — class, instance, credentials — lives in the project, like every other
entry in the contract below. Each run:
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
So the state store records the **label** alongside `sync_name`, and a run that finds no
watermark for its derived key but does find rows whose label matches fails with:

> watermark exists under `sync_name='reverse_etl_company'` for label `company`, but this
> instance derives `reverse_etl_company_v2` — the label was renamed. Pin `sync_name` in the
> YAML to keep the watermark, or delete the old rows to start fresh.

The schema change is additive: a nullable `label` column; legacy rows (NULL label) predate the
guard and are reported as such. With the guard, derivation is strictly safer than typing.

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
`deps: [...]` is optional ordering (narrative only — `scan.startup.mode='earliest'` reads the
topic from the beginning regardless of creation order). Builds the RisingWave table asset.

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
(1). The health job/schedule names derive from `name`. `jdbc_url`/`jdbc_user` default to the
`HOST_POSTGRES_URL`/`POSTGRES_USER` env vars (see below). `deps` optional.

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
commits offsets. `deps` optional.

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
duckdb targets (neither builder reads it). The RisingWave target regains `sync_name` — as a
derivation input for the envelope, not an identity of its own.

## Which Postgres does the postgres target write to?

Not a Dagster resource. The sink asset requires only `http` — it POSTs a connector config to
Kafka Connect; the connector itself opens the JDBC connection. Today the coordinates come from
environment variables (`reverse_etl_debezium_sink.py`): `connection.url` from `HOST_POSTGRES_URL`,
`connection.username` from `POSTGRES_USER`, and `connection.password` stays the literal
`${env:POSTGRES_PASSWORD}`, resolved by the Connect worker — the password never passes through
Dagster. Dagster's `postgres` resource serves other tenants (the watermark state, the demo
hooks, reset cleanup), not this data path.

Decision: the release component promotes `jdbc_url` and `jdbc_user` to explicit YAML fields
(defaulting to the env vars, so the demo does not move). The password stays `${env:...}` —
explicit never beats secret-free, and "which database" belongs in the same file as
`connector_name`.

## Resource contract

The components require these resource keys:

| Component | Required resource keys |
|---|---|
| Sync asset | `databricks`, `kafka`, `registry`; the state's key (`state_resource`, default `postgres`) when `state_backend: postgres` |
| Topic asset | `kafka` |
| RisingWave target | `risingwave`, `databricks`, `registry` |
| Postgres sink + health | `http`; `http`, `kafka` for the check |
| DuckDB target | `kafka`, `registry` |

The JDBC coordinates for the sink are env vars (see above), not resources.

**Keys and interfaces, not classes or connections.** The component hard-codes two things and no
more: the resource *key names* in the table (the state's is choosable — the sync YAML's
`state_resource`), and the small interface it calls on each bound
object. Everything else — the resource class, the env-var names, the defaults, the host
resolution — belongs to the consuming project, because each team/system points these keys at
its own instances. The assets declare the keys as `required_resource_keys` (a project missing
one fails at definitions load, not at run time) and duck-type the interface:

| Key | The interface the component calls |
|---|---|
| the state's key (`state_resource`, default `postgres`) | `.connect()` — nothing else |
| `risingwave` | `.connect()` |
| `registry` | `.client(schema_registry_url)` |
| `kafka` | `.producer()`, `.consumer(group, opts)`, `.admin_client()` |
| `databricks` | `.require_env()`, `.token()` / `.cached_token()`, `.table_location_cached(token, cfg, source_fqn)` (demo-path helpers call more) |
| `http` | `.get(...)`, `.put(...)` — Kafka Connect REST |

**Why the components ship no bindings.** `Definitions.merge` raises when two merged
`Definitions` carry different objects under the same resource key — so a component cannot ship
env-fed defaults for a team to override, and our own tests merge fakes under the same keys. The
library instead exports the *default bindings*, and each code location includes them exactly
once, in a file it owns:

```python
# kaizen_dagster_reverse_etl/resources.py (library)
def default_resources() -> Definitions:
    """Env-fed bindings for every key the components require."""
    return Definitions(resources={
        "postgres": PostgresResource(), "risingwave": RisingWaveResource(),
        "databricks": DatabricksResource(), "kafka": KafkaResource(),
        "registry": SchemaRegistryResource(), "http": HttpResource(),
    })
```

**A team's Python footprint: one file per code location.** The production workspace
(`dagster-workspace`) binds no Dagster resources anywhere — credentials flow Vault → External
Secrets Operator → `container_context.<env>.yaml` → plain `os.environ`. The one deliberate
Python file is the bindings module, autoloaded from the team's `defs/` folder:

```python
# src/<team>/defs/reverse_etl_resources.py (the team's)
from dagster import Definitions, definitions
from kaizen_dagster_reverse_etl.resources import default_resources

@definitions
def reverse_etl_resources() -> Definitions:
    return default_resources()
```

No credentials in it — Vault provides the env, the classes read it. A team needing a custom
binding edits *their own file* — swap one line for their class, no merge conflict because the
defaults are theirs, not the component's:

```python
class MetadataPostgres(ConfigurableResource):
    dsn: str                      # their env/secret conventions

    def connect(self):
        return psycopg2.connect(self.dsn)
```

For release, the library's default resource classes are cleaned up: one `connect()` per class,
explicit env names, none of this repo's `/.dockerenv` sniffing or the
`connect()`/`connect_with_timeout()` split. The env contract — which variables each default
class reads — is documented in the library README, because that is exactly what Vault must
provision. The team's project layout:

```
their_project/
  definitions.py                  # @definitions + load_from_defs_folder — untouched, ~8 lines
  src/<team>/defs/
    reverse_etl_resources.py      # the bindings module (above)
    reverse_etl_company_sync/defs.yaml
    reverse_etl_company_risingwave/defs.yaml
```

Bindings cannot live in the component YAMLs — they are Python-side by design, which is why no
host, credential, or driver detail can leak into the files the sync authors write.

One binding serves most projects, and `state_table` distributes across *tables* — but a sync
whose state belongs to a different system points `state_resource` at that binding instead, so
a different Postgres per team/system needs nothing hard-coded anywhere. Note the asymmetry with the sink
target: the state's postgres goes through this resource layer, while the sink's postgres
bypasses it entirely (Kafka Connect + its own JDBC config) — two different writers, Dagster
writing the watermark and Connect writing the rows.

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
  work is needed.
- **Validation.** The workspace runs `dg check defs` against the real systems; the components'
  load-time validation (unknown fields, unknown targets, missing bindings) fits it, and a
  future custom check could verify topic existence at configure time.
- **Infrastructure prerequisites (the open gap).** The workspace's required-infrastructure list
  covers Dagster+, GitLab/Harbor, Vault and Databricks — no Kafka brokers, schema registry,
  Kafka Connect, RisingWave or Postgres. The sync needs Kafka and a registry; the postgres
  target additionally needs a Kafka Connect deployment with the Debezium JDBC plugin; the
  RisingWave target needs production RisingWave. These are platform prerequisites (SRE scope),
  not code; until they land, the sync-only shape is the first release and targets follow their
  infrastructure.

## Implementation outline

Each step its own commit, suite + lint + mutation check green, as in the decoupling plan.

1. **Sync component**: add `create_topic` (default true), the topic asset and a bootstrap job
   to `ReverseEtlCdfSync`; move `_build_topic_asset` out of `reverse_etl_defs.py` into a
   demo-free home both the demo and the component import. Apply the derivation policy to the
   component (`sync_name`, `kafka_topic`, Dagster names: derived with overrides), make the
   state's binding choosable (`state_resource`, default `postgres`, driving the sync asset's
   required keys), and update the component tests and the mutation group.
2. **Watermark guard**: add the nullable `label` column to the state schema
   (`reverse_etl_state.py`: DDL, both backends' read/write) and the rename detection to the
   sync run's precondition. Legacy NULL-label rows are reported, never fatal.
3. **Per-target configs**: split `ReverseEtlTargetsConfig` into three slim dataclasses feeding
   the unchanged builders, each carrying only its target's fields plus the derived-name
   machinery. The demo's merged config, `targets_config()` and `build_target_defs` stay for
   the demo's single setup job; the seam's golden field-set test is re-pinned deliberately in
   this step.
4. **Target components**: `orchestration/components/reverse_etl_targets.py` with the three
   components wrapping the builders; the `deps` field; the health check travelling with the
   postgres target; the envelope composed from `sync_name` + `source_fqn`.
5. **Connection fields**: `jdbc_url`/`jdbc_user` on the postgres target, env-var defaults.
6. **Resource bindings**: clean the resource classes into the library's defaults (one
   `connect()` per class, explicit env names, no demo sniffing), export `default_resources()`,
   swap the demo's six inline bindings (`orchestration/definitions.py:1042-1048`) for the
   helper, and document the env contract.
7. **Seam and hygiene tests**: envelope-formula equality across the two sides; a test pinning
   that the sync and target component modules' transitive imports contain no demo module
   (`reverse_etl_demo_*`, `reverse_etl_change`, `reverse_etl_reset`) — the extractability
   guarantee; and a compatibility test loading a minimal defs-folder project (one component
   YAML plus the `@definitions` bindings module) through `load_from_defs_folder` — the exact
   production load path.
8. **Docs**: a "Releasing to a team" section in `REVERSE_ETL_COMPONENT_GUIDE.md` (the naming
   policy, the resource contract and bindings module, the env contract, the wire contract, the
   minimal YAMLs) and walkthrough rows for the new module.

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
