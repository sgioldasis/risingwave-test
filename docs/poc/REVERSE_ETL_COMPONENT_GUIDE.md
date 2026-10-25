# The reverse-ETL component: put your table's changes on Kafka

*Written 2026-10-25. Part of the component decoupling
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
| The Dagster resources | `databricks` (control-plane lookups and storage reads), `kafka`, `postgres` for the default state backend, and `registry` for Avro — bound under those names, as the project's resource setup does. Not `risingwave`, not `http` (`postgres` is not needed with `state_backend: duckdb`) |

```sql
CREATE TABLE IF NOT EXISTS reverse_etl_sync_state (
    sync_name TEXT NOT NULL,
    last_commit_version BIGINT NOT NULL,
    last_schema TEXT,
    updated_at TIMESTAMPTZ NOT NULL DEFAULT now()
)
```

## The attributes

Required: `name`, `catalog`, `schema_name`, `sync_name`, `source_table`, `key_column`, `kafka_topic`.

Optional:

| Attribute | Default | Meaning |
|---|---|---|
| `encoding` | `json` | `json` or `avro` |
| `schema_registry_url` | the project's staging registry | where the Avro subjects live |
| `group_name` | `reverse_etl_<name>` | the asset's Dagster group |
| `sync_asset` | `reverse_etl_<name>_to_kafka` | the asset's name |
| `deps` | none | upstream asset keys, so the sync can be ordered after your own pipeline's assets |
| `state_backend` | `postgres` | `postgres` or `duckdb` — where the watermark lives (see below) |
| `state_table` | `reverse_etl_sync_state` | Postgres backend only: the state table. One per sync is possible; each must exist |
| `state_path` | — | DuckDB backend only: the state file's path, required |
| `group_name`, `sync_asset` | derived from `name` | the Dagster object names, overridable |

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
  sync_name: company_changes          # the watermark key and Debezium envelope prefix
  source_table: dim_company
  key_column: company_id
  kafka_topic: company_changes_cdc
```

Materialize the `reverse_etl_company_changes_to_kafka` asset. The component adds no schedule and no
sensor — attach your own to the asset (or select it in a job).

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

- Create or alter any table, topic, connector, or anything else in Databricks, Kafka or Postgres.
- Write to any database other than its own state row.
- Add jobs, schedules, checks or sensors.
- Care for targets. Adding a missing column to a target before its first message, or propagating
  drops and renames, is target-side work the demo does in hooks around the sync's two extension
  points (one per event-bearing chunk before producing, one after the final flush and before the
  watermark). If your targets need that, either manage the target schema upstream of the topic, or
  follow the hooks pattern: `orchestration/assets/reverse_etl_demo_hooks.py`, with the ordering
  contract pinned by `tests/sync_asset/test_sync_hooks.py`.

## Limits

- **One key column.** The sync code carries a list, but the component exposes one.
- **No schedule or sensor built in.** One asset; bring your own automation.
- **The `reverse_etl_` prefix** rides along in the sync name, so in the watermark key and the
  Debezium envelope prefix.
