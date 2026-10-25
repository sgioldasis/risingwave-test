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
source table, the topic, the RisingWave and Postgres targets, and the reset/make-change jobs; that
is the PoC's business, not the component's.

## What it does

- **Reads** new, changed and deleted rows since the last run by diffing Delta table versions with
  delta-rs — no Spark, no Databricks compute, no warehouse scans, and the table does not need
  Change Data Feed enabled (docs/poc/REVERSE_ETL_DBU_COST_NOTES.md, sections 4-5). A backlog is
  read in version chunks, so it streams through the driver instead of collecting in memory.
- **Produces** Debezium-style events to the topic — one per row change, keyed by the key column —
  in Kafka Connect JSON (schema embedded in every message) or Confluent Avro (key and value schemas
  registered under `<topic>-key` and `<topic>-value`).
- **Keeps the watermark** and the last-seen schema in the Postgres table `reverse_etl_sync_state`:
  one append-only row per run that moved. A failed run moves nothing, so the next run replays the
  same versions — upsert your targets by the key column and a replay is safe.

## What you provide

| Thing | Notes |
|---|---|
| A Delta table in Unity Catalog | must exist. The component never creates or alters it |
| A key column | identifies a row downstream. One column — see Limits |
| A Kafka topic | must exist before the first sync. The sync checks and fails fast otherwise; it never creates one |
| The Postgres state table | `reverse_etl_sync_state`, DDL below. Checked at the start of every sync; a missing table fails with the DDL in the error |
| A schema registry | only for `encoding: avro` |
| The Dagster resources | `databricks` (control-plane lookups and storage reads), `kafka`, `postgres`, and `registry` for Avro — bound under those names, as the project's resource setup does. Not `risingwave`, not `http` |

```sql
CREATE TABLE IF NOT EXISTS reverse_etl_sync_state (
    sync_name TEXT NOT NULL,
    last_commit_version BIGINT NOT NULL,
    last_schema TEXT,
    updated_at TIMESTAMPTZ NOT NULL DEFAULT now()
)
```

## The attributes

Required: `name`, `catalog`, `schema_name`, `source_table`, `key_column`, `kafka_topic`.

Optional:

| Attribute | Default | Meaning |
|---|---|---|
| `encoding` | `json` | `json` or `avro` |
| `schema_registry_url` | the project's staging registry | where the Avro subjects live |
| `group_name` | `reverse_etl_<name>` | the asset's Dagster group |
| `sync_asset` | `reverse_etl_<name>_to_kafka` | the asset's name |
| `deps` | none | upstream asset keys, so the sync can be ordered after your own pipeline's assets |

`name` derives the sync name — the watermark key and the Debezium envelope prefix — and the default
Dagster names as `reverse_etl_<name>_<...>`. Changing the label of an existing sync restarts it
from the beginning.

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
  kafka_topic: company_changes_cdc
```

Materialize the `reverse_etl_company_changes_to_kafka` asset. The component adds no schedule and no
sensor — attach your own to the asset (or select it in a job).

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
