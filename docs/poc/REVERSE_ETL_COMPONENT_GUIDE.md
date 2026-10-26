# The reverse-ETL component: put your table's changes on Kafka

*Written 2026-10-25, updated 2026-10-26 for the release design
(docs/poc/REVERSE_ETL_RELEASE_DESIGN.md — the naming policy, the env contract and the
component split). Part of the component decoupling
(docs/poc/REVERSE_ETL_COMPONENT_DECOUPLING_PLAN.md); the internals are in
docs/poc/REVERSE_ETL_CODE_WALKTHROUGH.md.*

`ReverseEtlCdfSync` (`orchestration/components/reverse_etl_cdf_sync.py`) is a Dagster component for
one job: read an existing Databricks Delta table's changes and write them to an existing Kafka topic
as Debezium-style events. It builds exactly **one** Dagster asset. It creates nothing — not the
table, not the topic, not the watermark state table, not any target.

This guide is for a team that wants reverse ETL for its own table. The demo's variant
(`ReverseEtlCdfDemo`, orchestration/components/reverse_etl_cdf_demo.py) additionally creates the
source table, the topic, the targets you select (see Targets below), and the reset/make-change
jobs; that is the PoC's business, not the component's.

## What it does

- **Reads** new, changed and deleted rows since the last run by diffing Delta table versions with
  delta-rs — no Spark, no Databricks compute, no warehouse scans, and the table does not need
  Change Data Feed enabled (docs/poc/REVERSE_ETL_DBU_COST_NOTES.md, sections 4-5). A backlog is
  read in version chunks, so it streams through the driver instead of collecting in memory.
- **Produces** Debezium-style events to the topic — one per row change, keyed by the key column —
  in Kafka Connect JSON (schema embedded in every message) or Confluent Avro (key and value schemas
  registered under `<topic>-key` and `<topic>-value`).
- **Keeps the watermark** and the last-seen schema in the sync's state — one append-only row per
  run that moved. A failed run moves nothing, so the next run replays the same versions — upsert
  your targets by the key column and a replay is safe. The state lives behind a small backend
  interface (`orchestration/assets/reverse_etl_state.py`): Postgres by default, or a local DuckDB
  file (see The watermark state below).

## What you provide

| Thing | Notes |
|---|---|
| A Delta table in Unity Catalog | must exist. The component never creates or alters it |
| A key column | identifies a row downstream. One column — see Limits |
| A Kafka topic | must exist before the first sync. The sync checks and fails fast otherwise; it never creates one |
| The watermark state | the shared Postgres table `reverse_etl_sync_state` (DDL below) by default — checked at the start of every sync, a missing one fails with the DDL in the error. With `state_backend: duckdb` instead: nothing, the state file is created on first run |
| A schema registry | only for `encoding: avro` |
| The environment | no Dagster resources — the components declare none. Connections come from env-fed functions (`reverse_etl_connections.py`), read at call time; see **The env contract** below and the full table in that module's docstring |

```sql
CREATE TABLE IF NOT EXISTS reverse_etl_sync_state (
    sync_name TEXT NOT NULL,
    last_commit_version BIGINT NOT NULL,
    last_schema TEXT,
    source_fqn TEXT,
    label TEXT,
    updated_at TIMESTAMPTZ NOT NULL DEFAULT now()
)
```

The `source_fqn`/`label` columns are the watermark guard's anchor (release design
step 3): on an empty read the sync checks whether the same source table already has a
watermark under another key — a renamed label must fail loudly, never silently restart.
A pre-guard table is migrated by the next setup run; a standalone sync's precondition
error carries the `ALTER TABLE … ADD COLUMN IF NOT EXISTS` statements.

## The attributes

Required: `name`, `catalog`, `schema_name`, `source_table`, `key_column`, `state_backend`.

Optional (each has a derivation — see the naming policy in **Releasing to a team**):

| Attribute | Default | Meaning |
|---|---|---|
| `sync_name` | `reverse_etl_<name>` | the watermark key and Debezium envelope prefix — derived; override only to keep a watermark across a rename or fit an existing estate |
| `kafka_topic` | `reverse_etl_<name>_topic` | the topic the sync produces to — derived; override when your platform provisioned a differently named topic (then set `create_topic: false`) |
| `create_topic` | `true` | build the topic asset and the bootstrap job (topic → sync). `false`: the topic is your platform's, and the component builds only the sync |
| `encoding` | `json` | `json` or `avro` |
| `schema_registry_url` | the project's staging registry | where the Avro subjects live |
| `group_name` | `reverse_etl_<name>` | the asset's Dagster group |
| `sync_asset` | `reverse_etl_<name>_to_kafka` | the asset's name |
| `deps` | none | upstream asset keys, so the sync can be ordered after your own pipeline's assets |
| `state_table` | `reverse_etl_sync_state` | Postgres backend only: the state table. One per sync is possible; each must exist |
| `state_path` | — | DuckDB backend only: the state file's path, required |

## The watermark state

Both backends keep the same thing — one append-only row per run that moved the
watermark, freshest row wins, a failed run writes nothing — so replay semantics
are identical; only the storage differs.

- **`postgres`** (default): the shared table `reverse_etl_sync_state`, keyed by
  `sync_name`, DDL under What you provide. Set `state_table` to give a sync its
  own table (the demo's syncs each get one).
- **`duckdb`**: a local DuckDB file at `state_path`, table `reverse_etl_sync_state`,
  created on first run. Needs no `postgres` resource — a sync that only produces
  to Kafka then runs without any Postgres at all.

Caveats: DuckDB allows **one writer** — never point two syncs at one file, and
keep external readers (DBeaver) read-only or short-lived. And `state_path` is a
container path: under the Dagster containers it must land on a writable,
persistent mount (the demo mounts `./sync_state` to `/workspace/sync_state`;
`./data` is read-only and `dagster_storage` is not host-visible).

`name` derives the default Dagster object names as `reverse_etl_<name>_<...>` (overridable
per name). `sync_name` is explicit and is the watermark key — the one name a replay silently
depends on; changing it for an existing sync restarts that sync from the beginning.

## Example

`orchestration/defs/company_changes/defs.yaml`:

```yaml
type: orchestration.components.reverse_etl_cdf_sync.ReverseEtlCdfSync
attributes:
  name: company_changes
  catalog: de_dev
  schema_name: my_schema
  source_table: dim_company
  key_column: company_id
  state_backend: postgres
  kafka_topic: company_changes_cdc   # a topic your platform owns: override + create_topic: false
  create_topic: false
```

With the default `create_topic: true` the component builds three things: the sync asset
(`reverse_etl_company_changes_to_kafka`), the topic asset (`reverse_etl_<name>_topic_setup`,
creating the topic with `cleanup.policy=compact` — a Debezium-style change topic wants
compaction so retention cannot drop events a lagging consumer has not read), and a small
bootstrap job running topic → sync. The component adds no schedule and no sensor — attach
your own to the job (or select the assets in one of yours).

## Targets (the demo's variant)

The demo sync's `targets` attribute selects which target assets get built — the targets are
**consumers of the sync's topic**; the sync itself writes nothing but the topic and its watermark.

| Target | What gets built |
|---|---|
| `risingwave` (default) | the RisingWave target table, kept ahead of the messages by the demo hooks |
| `postgres` (default) | the Debezium JDBC sink into Postgres, plus the sink health check, its job and schedule |
| `duckdb` | a mirror asset draining the topic into a local DuckDB file at `duckdb_target_path` (required with this target) |

The DuckDB mirror is the topic's latest event per key: `c`/`u`/`r` upsert the `after` payload, a
`d` tombstone removes the key. It is stateless — every run re-reads the whole topic (demo
volumes) and rebuilds the mirror, draining against the broker's watermarks. The table is named
after the topic, its columns are the union of the fields the events carried (a field no event has
seen yet is added when one arrives), all `VARCHAR` — a JSON event carries no types. Like the
DuckDB state backend it allows one writer: one mirror per file, external readers read-only.

## The seam (how the two sides meet)

The demo YAML splits at the sync/targets seam (decoupling plan section 13, option C): the `sync:`
block is the sync's own configuration, the `targets:` block the downstream's, and the top level
keeps what spans both sides — the `name` label the Dagster object names derive from, their
overrides, and the reset.

```yaml
type: orchestration.components.reverse_etl_cdf_demo.ReverseEtlCdfDemo
attributes:
  name: company
  sync:                       # promotes verbatim into a team's ReverseEtlCdfSync YAML
    sync_name: reverse_etl_company
    source_table: reverse_etl_company_source
    state_table: reverse_etl_company_state
    kafka_topic: reverse_etl_company_topic
    catalog: de_dev
    schema_name: sr_poc_external
    key_column: Company_ID
    encoding: avro
  targets:
    connector_name: reverse_etl_company_sink
    postgres_table: reverse_etl_company_target
    risingwave_table: reverse_etl_company_target
```

The seam is the message contract and nothing else: the targets see the topic, the encoding, the
key column and the envelope name — never the source table (a target cannot know where the events
came from; the RisingWave table's one schema read is an explicit composition argument, not a
config field). For your own pipeline the same rule applies: your `ReverseEtlCdfSync` YAML is the
`sync:` block verbatim, and your targets are your own assets consuming the topic — declare them
downstream of the sync asset by its name and decode per the contract above.

## What it does not do

- Create or alter any table, topic, connector, or anything else in Databricks, Kafka or Postgres —
  with one exception: the optional topic asset creates the change topic itself (compacted), and
  only when you did not set `create_topic: false`.
- Write to any database other than its own state row.
- Add schedules, checks or sensors beyond the bootstrap job — bring your own automation.
- Care for targets. Adding a missing column to a target before its first message, or propagating
  drops and renames, is target-side work the demo does in hooks around the sync's two extension
  points (one per event-bearing chunk before producing, one after the final flush and before the
  watermark). If your targets need that, either manage the target schema upstream of the topic, or
  follow the hooks pattern: `orchestration/assets/reverse_etl_demo_hooks.py`, with the ordering
  contract pinned by `tests/sync_asset/test_sync_hooks.py`.

## Releasing to a team

The release design (docs/poc/REVERSE_ETL_RELEASE_DESIGN.md) splits the component family so a
team adopts it with **zero Python files**: drop YAMLs into your `src/<team>/defs/` folders and
SRE provisions the environment. The components declare no Dagster resources — one defs.yaml
deploys to staging and production unchanged, and the environment is the only layer that
differs (Vault → External Secrets Operator → `container_context.<env>.yaml`).

**The naming policy: who mints the name.** Typed are the names of things that exist
independently of the component — the label `name`, `catalog`, `schema_name`, `source_table`,
`key_column`, `state_backend`, a topic your platform provisioned, the DuckDB mirror's file
path; we cannot know these, so typing them is the contract. Derived are the names the
component mints — everything `reverse_etl_<name>[_<role>]`: `sync_name`, `kafka_topic` (when
the component creates the topic), `connector_name`, `postgres_table`, `risingwave_table`,
the envelope schema name, and all Dagster object names. Every derived field keeps an
explicit override for migrations, but a hand-typed minted name is how typos mint accidental
identities — derivation makes the family consistent by construction. Two instances must not
share a `name`.

**The minimal YAMLs.** The sync types six facts and nothing else:

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

Each target is its own component, so a team takes only what it wants — the presence of a
YAML *is* the target selection:

```yaml
# RisingWave: the only target allowed source_fqn (its CREATE reads the source's schema once)
type: orchestration.components.reverse_etl_targets.ReverseEtlRisingwaveTarget
attributes:
  name: company
  source_fqn: de_dev.their_schema.dim_company
  key_column: Company_ID

# Postgres: the Debezium JDBC sink plus the sink health check, its job and its schedule
type: orchestration.components.reverse_etl_targets.ReverseEtlPostgresTarget
attributes:
  name: company
  key_column: Company_ID

# DuckDB: the stateless local mirror (dev volumes — see the ceiling below)
type: orchestration.components.reverse_etl_targets.ReverseEtlDuckdbTarget
attributes:
  name: company
  key_column: Company_ID
  duckdb_target_path: /workspace/company_mirror.duckdb
```

`deps: [...]` on any of them is optional ordering — a target YAML whose `deps` name the
sync's `reverse_etl_<name>_topic_setup` asset loads as-is (a dangling dep loads too; ordering
is your declaration, pinned by tests/components/compat/test_defs_folder_load.py).

**The wire contract.** Values that must agree across files, and how agreement is
guaranteed: matched by construction (both sides derive from the same `name`) are `sync_name`,
`kafka_topic` and the envelope schema name (the same formula over `sync_name` + the last two
components of `source_fqn` — the Avro registration byte-matches by arithmetic, not
discipline). `key_column` must agree and is typed on each side. `kafka_topic`, `encoding`
and `schema_registry_url` agree by derivation too — override on one side only if you
override on both. One-sided: `source_fqn` (RisingWave only); the postgres and duckdb targets
carry no source knowledge at all.

**The env contract.** No project binds resources, so nothing conflicts on merge and nothing
waits to be learned. Connections resolve through env-fed functions
(`orchestration/assets/reverse_etl_connections.py`), each reading `os.environ` **at call
time** — never import time. The essentials: the state Postgres needs `POSTGRES_HOST`
(**required, no default** — `host.docker.internal` does not resolve on the macOS host, so one
value cannot serve both the containers and the host), `POSTGRES_PORT`/`_DB`/`_USER`/
`_PASSWORD`; the sync's Databricks reads use the service principal trio
(`DATABRICKS_AZURE_TENANT_ID`/`_CLIENT_ID`/`_CLIENT_SECRET` — service principal, never a SAS
token); the connector assembly needs `KAFKA_OUTPUT_BOOTSTRAP` (required) with optional
`KAFKA_OUTPUT_SASL_*`; the connector's JDBC coordinates come from `HOST_POSTGRES_URL` +
`POSTGRES_USER`, and the password stays the literal `${env:POSTGRES_PASSWORD}` — resolved by
the Connect worker, never passing through Dagster. The full table is the module docstring.

**No environment-specific value in a component YAML.** One defs.yaml deploys to staging and
production; a filled-in URL would make a branch deploy write to production's database. The
YAML's silence is the contract: names and decisions in files, values in the environment.
Everything env-adjacent that is *not* a credential stays in YAML or config — the registry
URL is a config field (default: this project's staging registry), the DuckDB path is typed,
topic names are derived.

**The `$schema` header.** dg generates a `$schema:` line per defs.yaml from the component
definitions (pre-commit regenerates it); commit it — it is what gives YAML autocomplete and
load-time validation of unknown fields. The `type:` strings above are this repo's; in the
destination library they read `kaizen_dagster_reverse_etl.components.…` (see
docs/poc/REVERSE_ETL_LIBRARY_README.md).

**The DuckDB mirror's ceiling.** It re-reads the whole topic every run — O(events ever), no
committed offsets, stateless by design (rename-safe, replay-safe). Point it at PoC-volume
topics, not a high-rate production topic; the postgres target is the production path.

## Limits

- **One key column.** The sync code carries a list, but the component exposes one.
- **No schedule or sensor built in.** The bootstrap job and (optionally) the topic asset; bring
  your own automation beyond that.
- **The `reverse_etl_` prefix** rides along in every minted name, so in the watermark key and
  the Debezium envelope prefix.
- **Two instances must not share a `name`** — their derived names would collide.
