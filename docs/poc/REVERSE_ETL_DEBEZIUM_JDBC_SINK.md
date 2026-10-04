# Reverse-ETL CDF POC: Debezium JDBC sink into Postgres

Date: 2026-10-02 (updated 2026-10-04). Since the first version: a **single topic** read by both RisingWave and
the Debezium sink (sections 2, 3, 10.2), a surrogate **`rid` key** (4.6), real **timestamp** types (4.3), a
**reset job** (14.3), a **Databricks notebook** version of the sync (10.3), tested behaviour for column
drops and type changes (10.4), and, since 2026-10-04, a **reusable Dagster component** with uniform
`reverse_etl_<label>_<role>` names, a **seed asset** and a **second sync** (10.5, 14.4). Names in sections that
describe earlier runs may be the older ones; section 14.4 has the current names.
Branch: `feature-sr`
Related: [`REVERSE_ETL_CDF_POC_PLAN.md`](REVERSE_ETL_CDF_POC_PLAN.md) (APR-233), which describes the
Databricks Change Data Feed (CDF) -> Kafka half of this pipeline. This document covers the second half:
consuming that Kafka topic with the **Debezium JDBC sink connector** on **Kafka Connect** and upserting /
deleting the changes in a **host PostgreSQL** table.

---

## 1. Goal and outcome

**Goal.** Take the Debezium-shaped change events that the reverse-ETL POC already publishes to Kafka and
apply them to a Postgres table, using the real Debezium JDBC sink (not a hand-rolled consumer and not a
RisingWave sink).

**Outcome.** Working end to end. After a Databricks table change and one run of the Dagster asset
`reverse_etl_cdf_to_kafka` (or the equivalent notebook, section 10.3), the change appears in host Postgres
table `reverse_etl_cdf_target` and in the RisingWave table within seconds. Inserts, updates and deletes are all
applied, and added columns flow through to both targets.

Evidence from a live run (host Postgres after reset, setup, seeding and syncing, 2026-10-03):

```
 rid | id |     value      |         updated_at
-----+----+----------------+----------------------------
   1 |  1 | first          | 2026-10-03 17:16:35.578+03
   2 |  2 | second-updated | 2026-10-03 17:16:38.324+03
```

Before any live run, the same connector configuration was verified locally (section 9) against a scratch
Postgres and a local Redpanda topic, covering insert, update and delete.

---

## 2. Architecture

```
 Databricks (de_dev.sr_poc_external.reverse_etl_cdf_source, CDF enabled)
        |
        |  Dagster asset reverse_etl_cdf_to_kafka  OR  the Databricks notebook of the same name
        |  (BATCH, manual / on demand; section 10.3)
        |  reads table_changes() since the stored watermark,
        |  groups CDF rows into Debezium before/after/op events
        v
 External Kafka cluster (SASL_SSL)    <-- KAFKA_OUTPUT_BOOTSTRAP
   '-- reverse_etl_cdf_topic   Debezium JSON + embedded Connect schema   (ONE topic, two readers)
         |                                |
         |                                |  kafka-connect container
         v                                |  consumer.override.* -> external SASL_SSL cluster
   RisingWave table                       |  worker internal topics -> LOCAL Redpanda
   reverse_etl_cdf_target            v
   (FORMAT DEBEZIUM ENCODE JSON,    Host PostgreSQL (host.docker.internal:5432, db "postgres")
    typed columns, PK: rid)         table: public.reverse_etl_cdf_target   (PK: rid)
```

Key properties:

- **Batch half, streaming half.** Databricks -> Kafka only moves when the Dagster asset (or the notebook) runs. Kafka ->
  Postgres is continuous: the connector is always running and applies messages within seconds of arrival.
- **One topic, two readers.** Every event is produced once, with an embedded Connect schema (section 4).
  RisingWave and the Debezium sink both read it. (It started as two topics, one schemaless for RisingWave and one
  with a schema for the sink; consolidated on 2026-10-03 after confirming RisingWave reads the embedded-schema
  format, including typed columns. The topic was later renamed `reverse_etl_cdf_topic`.)
- **Split Kafka usage in Connect.** The Connect worker's own bookkeeping topics live on the *local* Redpanda;
  only the connector's consumer talks to the external cluster. This means Connect does not need
  topic-creation rights on the external cluster.
- **Target is host Postgres, not `postgres-0`.** The compose service `postgres-0` is RisingWave's metadata
  store and must not be written to. The target is the devbox / host Postgres reached through
  `host.docker.internal`, the same one the existing `sink_funnel_to_postgres` model uses.

---

## 3. Decision log (how we got here)

| Question | Decision | Why |
|---|---|---|
| Is "Debezium consuming Kafka" a thing? | Use the **Debezium JDBC sink connector** | Debezium proper captures DB changes *into* Kafka. Reading Kafka and writing to a database is the JDBC **sink** connector, which runs on Kafka Connect. |
| Existing alternatives | Rejected for this task | RisingWave already has a JDBC sink (see `dbt/models/sink_funnel_to_postgres.sql`) and would need no new infrastructure, but the requirement was the real Debezium JDBC sink. |
| Redpanda Connect (Benthos) | Rejected | A separate Go stream processor; cannot host Kafka Connect (Java) plugins, so it cannot run the Debezium JDBC sink. |
| Redpanda Connectors (Kafka Connect packaged by Redpanda) | Rejected | The self-managed image bundles only the MirrorMaker2 connectors, no JDBC sink and no Debezium. Custom plugins can be mounted via `CONNECT_PLUGIN_PATH`, but that is the same setup as Debezium's own image on a less common base. (Source: Redpanda docs for the Connectors Docker image, partly the 24.2 version; the current page is titled "Deploy Kafka Connect in Docker".) |
| Runtime | `quay.io/debezium/connect:3.7.0.Final` plus the JDBC plugin | 3.7.0.Final is the current release on Maven Central and quay.io as of 2026-10-02. |
| Message format | **JSON with embedded schema** (`{"schema": ..., "payload": ...}`) | The sink requires schema information on every record. The original topic was a hand-rolled, schemaless envelope, which the sink would reject. Avro would need a schema registry reachable from the external cluster; embedded JSON needs nothing extra. |
| Change the existing topic or add one? | **Initially a second topic; consolidated to one on 2026-10-03** | At first the existing schemaless topic fed a verified RisingWave `FORMAT DEBEZIUM` table, and re-encoding it risked breaking that parser, so the events went to a new `_jdbc` topic as well. A local test then showed RisingWave parses the embedded-schema messages (insert, update, delete, and typed `DOUBLE` / `INT` / `BOOLEAN` columns), so the schemaless topic was retired and RisingWave reads the `_jdbc` topic. This removes the second produce, the non-atomic double write, and the `VARCHAR`-only restriction for RisingWave columns. |
| Distributed vs standalone Connect | Distributed, with internal topics on **local Redpanda** | Avoids needing topic-create ACLs on the external cluster while keeping the standard Debezium image behaviour. |
| How to reach the external SASL_SSL cluster | Per-connector `consumer.override.*` | Requires `connector.client.config.override.policy=All` on the worker. |
| Secrets | `${env:NAME}` references via Kafka's `EnvVarConfigProvider` | Secret values never appear in the stored connector config or the REST API. |
| Key | Surrogate identity column **`rid`**, not the business `id` | The source table does not enforce unique ids, and a delete by key removed a different row that shared the id (section 4.6). Delta row tracking was tested and cannot be used (its row id is not in the change feed). |
| `TIMESTAMP` columns | Sent as `io.debezium.time.ZonedTimestamp` strings | Gives `timestamp with time zone` in Postgres and `TIMESTAMPTZ` in RisingWave instead of text (section 4.3). |
| Trigger for the sync | Dagster asset by default; a PySpark notebook as an alternative | The notebook needs a classic cluster that can reach Kafka and does not add RisingWave columns (section 10.3). |
| Column drops and type changes | Not propagated; documented and tested | The sink only adds columns, and Databricks needs an opt-in before it allows either (sections 10.4, 14.4). |

---

## 4. Message format

### 4.1 What the Debezium JDBC sink requires

Verified in the Debezium source (`KafkaDebeziumSinkRecord` in the `debezium-sink` module):

- A record is treated as a Debezium message if its **value schema name** is a Debezium envelope name
  (following the `<server>.<schema>.<table>.Envelope` convention), **or** the schema has no name but has an
  `op` field.
- A record is a delete when `op` is `"d"`. For a delete the row is read from `before`; otherwise from `after`.
- The record **key** must be a primitive or a `Struct`. With `primary.key.mode=record_key` the key struct
  supplies the primary key columns.
- Deletes are applied only when `delete.enabled=true` **and** the primary key mode is `record_key`,
  `record_value` or `record_header`.

### 4.2 Schema names used

Built from the sync's config (`ReverseEtlSyncConfig.envelope_schema_name` =
`<sync_name>.<schema>.<source_table>`), shown here for the POC:

```
reverse_etl_cdf.sr_poc_external.reverse_etl_cdf_source.Envelope   value (the envelope)
reverse_etl_cdf.sr_poc_external.reverse_etl_cdf_source.Value      before / after row struct
reverse_etl_cdf.sr_poc_external.reverse_etl_cdf_source.Key        key struct
```

### 4.3 Row columns

The row schema is **derived at sync time** from the source table's current columns, not hardcoded:
`_get_row_fields()` runs

```sql
SELECT column_name, data_type, is_nullable
FROM de_dev.information_schema.columns
WHERE table_schema = 'sr_poc_external' AND table_name = 'reverse_etl_cdf_source'
ORDER BY ordinal_position
```

each time `reverse_etl_cdf_to_kafka` produces events, and builds the Connect schema from the result. A column
added in Databricks therefore reaches the message schema, and from there Postgres, with no code change
(section 10.1). If `information_schema` returns no columns the asset fails rather than guessing.

At the time of writing the table has these columns:

| Column | Connect type | Optional | Resulting Postgres type |
|---|---|---|---|
| `rid` | `int64` | no | `bigint` (primary key; identity column, see 4.6) |
| `id` | `int64` | no | `bigint` (not unique) |
| `value` | `string` | yes | `text` |
| `updated_at` | `string`, named `io.debezium.time.ZonedTimestamp` | yes | `timestamp with time zone` |

Databricks type to Connect type mapping (`_CONNECT_TYPE_BY_DATABRICKS_TYPE`):

| Databricks type | Connect JSON type | Postgres type created by the sink (verified) |
|---|---|---|
| `TINYINT` / `SMALLINT` | `int8` / `int16` | not tested |
| `INT` / `INTEGER` | `int32` | `integer` |
| `BIGINT` / `LONG` | `int64` | `bigint` |
| `FLOAT` | `float` | not tested |
| `DOUBLE` | `double` | `double precision` |
| `BOOLEAN` | `boolean` | `boolean` |
| `TIMESTAMP` | `string` with schema name `io.debezium.time.ZonedTimestamp` (ISO-8601 with timezone) | `timestamp with time zone` |
| anything else (`STRING`, `DATE`, `DECIMAL`, ...) | `string` | `text` |

Kafka Connect's JSON schema type names are `float` and `double`, not `float32` / `float64`. An earlier draft of
the mapping used the latter and the converter rejected the message (`Unknown schema type: float64`); this was
caught by the local end-to-end test before any live use.

RisingWave reads the same messages, so its table uses real types too. Connect type to RisingWave type
(`_RISINGWAVE_TYPE_BY_CONNECT_TYPE` in `reverse_etl_risingwave_setup.py`):

| Connect type | RisingWave type | Tested |
|---|---|---|
| `int8` / `int16` | `SMALLINT` | no |
| `int32` | `INT` | yes |
| `int64` | `BIGINT` | yes |
| `float` | `REAL` | no |
| `double` | `DOUBLE PRECISION` | yes |
| `boolean` | `BOOLEAN` | yes |
| `string` | `VARCHAR` | yes |
| `string` named `io.debezium.time.ZonedTimestamp` (internal marker `zoned_timestamp`) | `TIMESTAMPTZ` | yes |

The Statement Execution API returns every value as a string, so values are cast to the declared Connect type
(`_coerce()`) before they are written into the payload. Row payloads contain exactly the schema's fields, in
schema order. `source.commit_version` is cast to an integer as well (in the first version it was sent as a string
into an `int64` field, which Connect's JSON converter reads as 0; the sink ignores `source`, so it went unnoticed).

`TIMESTAMP` columns are sent as ISO-8601 strings with a timezone (for example `2026-10-03T03:41:08.345Z`), and
the schema field carries the name `io.debezium.time.ZonedTimestamp`, which the Debezium sink maps to
`timestamp with time zone`. The RisingWave column is `TIMESTAMPTZ`. Verified live on 2026-10-03 after reset and
setup: both targets have `timestamp with time zone` and hold identical instants (displayed in each session's
timezone). Earlier versions sent `updated_at` as a plain string, giving `text` / `VARCHAR`; switching needed the
tables recreated. `TIMESTAMP_NTZ` and `DATE` are still carried as plain strings (not tested as typed).
In the code the marker is `ZONED_TIMESTAMP`; `_connect_field()` (and its copy in the notebook) turns it into the
named schema field.

### 4.4 Example: an insert (`op = "c"`)

Key:

```json
{
  "schema": {
    "type": "struct",
    "name": "reverse_etl_cdf.sr_poc_external.reverse_etl_cdf_source.Key",
    "optional": false,
    "fields": [{"field": "rid", "type": "int64", "optional": false}]
  },
  "payload": {"rid": 1}
}
```

Value (schema abbreviated to the field list):

```json
{
  "schema": {
    "type": "struct",
    "name": "reverse_etl_cdf.sr_poc_external.reverse_etl_cdf_source.Envelope",
    "optional": false,
    "fields": [
      {"field": "before", "type": "struct", "optional": true, "name": "...Value", "fields": ["rid", "id", "value", "updated_at"]},
      {"field": "after",  "type": "struct", "optional": true, "name": "...Value", "fields": ["rid", "id", "value", "updated_at"]},
      {"field": "op",     "type": "string", "optional": false},
      {"field": "source", "type": "struct", "optional": true,
       "fields": [{"field": "commit_version", "type": "int64"}, {"field": "commit_timestamp", "type": "string"}]}
    ]
  },
  "payload": {
    "before": null,
    "after": {"rid": 1, "id": 1, "value": "first", "updated_at": "2026-10-02T04:18:23.009Z"},
    "op": "c",
    "source": {"commit_version": 5, "commit_timestamp": "..."}
  }
}
```

- **Update (`op = "u"`):** both `before` and `after` are populated; the sink upserts `after`.
- **Delete (`op = "d"`):** `before` holds the deleted row, `after` is `null`; the sink issues a `DELETE` by key.
- The message **key** is built from `after` (insert/update) or `before` (delete), so a delete still carries the
  key it needs. Per-key ordering is preserved because the key determines the partition.

### 4.5 Where the events come from

`_to_debezium_events()` (unchanged) turns Databricks CDF rows into one logical event per change: CDF's
`update_preimage` + `update_postimage` pair collapses into a single `u` event, and CDF's four `_change_type`
values map to Debezium's `c`/`u`/`d`. The key column is cast to an integer because the Databricks Statement Execution
API returns numerics as JSON strings. See `REVERSE_ETL_CDF_POC_PLAN.md` section "3c" for the full reasoning.

### 4.6 Key: why `rid` and not `id`

The source table does not enforce uniqueness of `id` (Unity Catalog primary keys are informational only), so two
rows can share an `id`. Keyed by `id`, that loses data downstream: deleting one of two rows with `id = 14` sent a
delete for key 14, which removed the *other* row from Postgres and RisingWave while Databricks still had it
(seen live, 2026-10-03).

The source table therefore has `rid BIGINT GENERATED ALWAYS AS IDENTITY`: Databricks assigns it on insert and
never changes it, and it appears in the change feed like any column. Everything is keyed on it. `id` is just a
column that may repeat, as in the source.

Alternatives tested and rejected: Delta **row tracking** gives each physical row a stable `_metadata.row_id`, but
neither the SQL `table_changes()` nor the Spark `readChangeFeed` reader exposes it, so it cannot be a key; a
**duplicate-key check** would only turn the loss into a failed sync.

Verified live after reset, setup and seed: inserting two rows with `id = 14` (`rid` 4 and 5) and deleting one by
value left the other present and identical in Databricks, Postgres and RisingWave.

---

## 5. What was changed in the repo

All changes are on branch `feature-sr` (see `git log`). The first rows are a change log: module-level constants
they mention (`KAFKA_TOPIC`, `KEY_COLUMN`, `ENVELOPE_SCHEMA_NAME`, `SYNC_NAME`, ...) were later replaced by fields
of `ReverseEtlSyncConfig`, and the last rows describe the current structure.

| File | Change |
|---|---|
| `orchestration/assets/kafka_topics_setup.py` | `OUTPUT_TOPICS` no longer lists the reverse-ETL topic: each sync now creates its own (section 14.4). It originally listed `rw_poc_reverse_etl_cdf_out_jdbc`, which the original `rw_poc_reverse_etl_cdf_out` had replaced. |
| `orchestration/assets/reverse_etl_cdf_setup.py` | The sync now writes to the single topic. Added `ENVELOPE_SCHEMA_NAME`, `_get_row_fields()` (live column list from `information_schema`), `_coerce()` / `_coerce_row()`, `_connect_row_schema()` and `_build_connect_json_message()`; `_produce_to_kafka()` produces each event once, in that format. Removed the old schemaless `_build_kafka_message()` and the `NUMERIC_ROW_COLUMNS` cast of `id` (now handled by `_coerce()`). `_message_previews()` shows each message's payload only; `_raw_messages()` returns the first three messages complete with their embedded schema. |
| `Dockerfile.debezium-connect` (new) | `FROM quay.io/debezium/connect:3.7.0.Final`, downloads `debezium-connector-jdbc-3.7.0.Final-plugin.tar.gz` from Maven Central, verifies its **sha512**, extracts it into `$KAFKA_CONNECT_PLUGINS_DIR`. |
| `docker-compose.yml` | New `kafka-connect` service (section 6). |
| `orchestration/assets/reverse_etl_risingwave_setup.py` | The RisingWave table now reads the single topic. `reverse_etl_cdf_risingwave_target` creates it from the live Databricks columns with real types (`_create_table_sql()`), so no column is added after the table starts reading. `add_missing_columns()` (called by `reverse_etl_cdf_to_kafka` before it produces) adds later-appearing columns with their types. |
| `orchestration/assets/reverse_etl_debezium_sink.py` (new) | Dagster asset `reverse_etl_cdf_jdbc_sink` that registers the connector via the Connect REST API and waits until it is `RUNNING` (section 7). |
| `orchestration/definitions.py` | First imported the new asset and added it to the setup job. Later the reverse-ETL assets and jobs moved out: it now merges `load_defs(...)` of `orchestration/defs/` (section 14.4). |
| `orchestration/assets/reverse_etl_cdf_setup.py` (later changes) | Added `KEY_COLUMN = "rid"` (the identity column, section 4.6): the source table is created with `rid BIGINT GENERATED ALWAYS AS IDENTITY` and the sync's `key_columns` is `[KEY_COLUMN]`. Added `_raw_messages()` and the `kafka_messages_raw` output metadata (first three messages with their embedded schema). `TIMESTAMP` columns use the `ZONED_TIMESTAMP` marker and `_connect_field()` (section 4.3). `reverse_etl_risingwave_setup.py` and `reverse_etl_debezium_sink.py` import `KEY_COLUMN` for the RisingWave primary key and `primary.key.fields`; the RisingWave type map gains `TIMESTAMPTZ`. |
| `orchestration/assets/reverse_etl_reset.py` (new) | Op job `reverse_etl_cdf_reset_job` (section 14.3), built by `build_reset_job(cfg)`. |
| `notebooks/reverse_etl_cdf_to_kafka.py` (new) | PySpark version of the sync for Databricks (section 10.3). |
| `orchestration/assets/reverse_etl_config.py` (new) | `ReverseEtlSyncConfig`: the one place that names a sync (Databricks catalog, schema, source and watermark tables, key column, Kafka topic, connector name, Postgres and RisingWave table names, plus derived values such as the consumer group and envelope schema name). The sync, RisingWave, connector, reset and topic modules read it. It also names the Dagster assets, jobs and group built for the sync, the source table's column DDL, and the reset job's safety allowlist (`reset_name_prefixes`, `reset_schema`). `ReverseEtlSyncConfig.for_name(label, ...)` derives every name as `reverse_etl_<label>_<role>` so two syncs cannot collide. |
| `orchestration/assets/reverse_etl_defs.py` (new) | `build_reverse_etl_defs(cfg)` returns `Definitions` with the four assets, the setup job and the reset job for one sync (plus its own topic asset if `create_topic_asset` is set). The `ReverseEtlCdfSync` component calls it for each YAML instance (section 14.4). |
| Asset factories (refactor) | In `reverse_etl_cdf_setup.py`, `reverse_etl_risingwave_setup.py`, `reverse_etl_debezium_sink.py` and `reverse_etl_reset.py` the functions take the config as a parameter and the assets and the reset job are built by `build_*` factories. Dependencies between assets use `AssetKey` from the config, so a sync can depend on the shared `kafka_output_topics_setup` without importing it. The reset job's op names start with the sync name (`reverse_etl_cdf_delete_connector`, ...) because Dagster needs unique op names across jobs; the names of the POC's assets and jobs have since changed with the uniform naming (section 14.4). |
| `orchestration/components/reverse_etl_cdf_sync.py`, `orchestration/defs/` (new) | The `ReverseEtlCdfSync` Dagster component and the folder of its YAML instances: `defs/reverse_etl_cdf/` (the POC) and `defs/reverse_etl_orders/` (the second sync). `definitions.py` merges `load_defs(...)` of that folder (section 14.4). |
| `build_seed_asset` in `reverse_etl_cdf_setup.py` (new) | The optional `reverse_etl_<label>_seed` asset, the last step of a sync's setup job, running the `seed_statements` from its YAML. It replaced `scripts/reverse_etl_poc_seed.py`, `bin/3_run_reverse_etl_seed.sh` and the script-runner entry "Seed Reverse-ETL POC", which were deleted. |
| `orchestration/assets/kafka_topics_setup.py` (later) | The reverse-ETL topic was removed from `OUTPUT_TOPICS`; `reverse_etl_<label>_topic_setup` creates each sync's topic. |
| `docker-compose.yml` (later) | `dagster-webserver` memory limit raised from `1G` to `1536M` after the webserver restarted itself once during a code-location reload (cause not found; it was not OOM-killed). |

Verification of the factory refactor: the constants, generated RisingWave DDL, connector config, topic list and message bytes are identical to before the change, the definitions load with the same POC asset and job names, and a second dummy sync merges into the same `Definitions` with no name collisions. A live reset, setup, seed and sync run after this change gave the expected result in both targets (ids 1 and 2, id 3 deleted, same instants in Postgres and RisingWave). There is no second real sync, so two syncs running side by side were not tested live. Dagster allows only one module-level `Definitions`, so `definitions.py` builds `defs` as one `Definitions.merge(Definitions(...), load_defs(...))` call (later changes: section 14.4).

---

## 6. The `kafka-connect` compose service

Built from `Dockerfile.debezium-connect`; container name `kafka-connect`.

| Setting | Value | Purpose |
|---|---|---|
| Port | `127.0.0.1:8083:8083` | Connect REST API, **localhost only** (the API can create connectors and read their config). |
| `BOOTSTRAP_SERVERS` | `redpanda:9092` | Worker internal topics use the **local** Redpanda. |
| `GROUP_ID` | `debezium-connect` | Connect cluster group. |
| Internal topics | `_connect_configs`, `_connect_offsets`, `_connect_statuses` | Created by Connect on local Redpanda (1 / 25 / 5 partitions). Replication factor 1 because Redpanda is single-node. |
| `KAFKA_HEAP_OPTS` | `-Xms256m -Xmx640m` | Heap bounded below the container limit. |
| Memory limit | `1G` | Consistent with the other services. |
| `CONNECT_CONNECTOR_CLIENT_CONFIG_OVERRIDE_POLICY` | `All` | Lets a connector override its consumer's bootstrap and SASL settings. |
| `CONNECT_CONFIG_PROVIDERS` | `env` | Enables `${env:NAME}` references in connector configs. |
| `CONNECT_CONFIG_PROVIDERS_ENV_CLASS` | `org.apache.kafka.common.config.provider.EnvVarConfigProvider` | The env provider implementation. |
| `CONNECT_CONFIG_PROVIDERS_ENV_PARAM_ALLOWLIST_PATTERN` | `^(KAFKA_OUTPUT_SASL_USERNAME\|KAFKA_OUTPUT_SASL_PASSWORD\|POSTGRES_PASSWORD)$` | Least privilege: a connector config can only read these three env vars. (`$$` in the compose file escapes the `$`.) |
| `KAFKA_OUTPUT_SASL_USERNAME`, `KAFKA_OUTPUT_SASL_PASSWORD` | passed through from the host shell | Names only in the compose file; values come from the environment (devbox loads `.env`). |
| `POSTGRES_PASSWORD` | `${POSTGRES_PASSWORD:-}` | Empty by default (host Postgres is trust-auth for local connections). |
| `extra_hosts` | `host.docker.internal:host-gateway` | Reach the host Postgres, same as the Dagster services. |
| Healthcheck | `curl -f http://localhost:8083/connectors`, `start_period: 60s` | Healthy after about a minute. |
| `depends_on` | `redpanda` (healthy) | Needs the local broker for its internal topics. |

---

## 7. The connector and the Dagster asset

Asset: `reverse_etl_cdf_jdbc_sink` (group `reverse_etl_cdf`), dependencies
`reverse_etl_cdf_topic_setup` and `reverse_etl_cdf_to_kafka`.

Behaviour:

1. Waits for the Connect REST API (`KAFKA_CONNECT_URL`, default `http://kafka-connect:8083`) for up to 120 s.
2. `PUT /connectors/reverse_etl_cdf_sink/config` with the generated config. PUT is idempotent: it creates
   the connector or updates it. A non-2xx response raises with the response text (truncated).
3. Polls `/status` for up to 90 s until the connector **and** its task are `RUNNING`. If anything reports
   `FAILED`, the asset fails with the first 1500 characters of the trace.
4. Records connector name, topic, target table and states as run metadata.

### Connector configuration

| Key | Value | Notes |
|---|---|---|
| `connector.class` | `io.debezium.connector.jdbc.JdbcSinkConnector` | |
| `tasks.max` | `1` | |
| `topics` | `reverse_etl_cdf_topic` | |
| `connection.url` | `HOST_POSTGRES_URL`, default `jdbc:postgresql://host.docker.internal:5432/postgres` | Same default as the existing dbt Postgres sink. |
| `connection.username` | `POSTGRES_USER`, default `postgres` | |
| `connection.password` | `${env:POSTGRES_PASSWORD}` | Resolved inside the Connect container. |
| `insert.mode` | `upsert` | |
| `primary.key.mode` | `record_key` | Key struct supplies the primary key. |
| `primary.key.fields` | `rid` | The surrogate identity column (section 4.6), not the business `id`. |
| `delete.enabled` | `true` | `op = "d"` becomes a `DELETE`. |
| `schema.evolution` | `basic` | The sink creates the table (with the primary key) on first use and adds columns if the schema grows. No separate table-creation asset is needed. |
| `collection.name.format` | `reverse_etl_cdf_target` | Target table name. Without it the table would be named after the topic. |
| `key.converter` / `value.converter` | `org.apache.kafka.connect.json.JsonConverter` | |
| `key.converter.schemas.enable` / `value.converter.schemas.enable` | `true` | Schema is read from each message. |
| `consumer.override.bootstrap.servers` | `KAFKA_OUTPUT_BOOTSTRAP` | Points the consumer at the **external** cluster. |
| `consumer.override.auto.offset.reset` | `earliest` | So messages produced before the connector was created are still consumed. |
| `consumer.override.security.protocol` | `SASL_SSL` | Only set if `KAFKA_OUTPUT_SASL_USERNAME` is set. |
| `consumer.override.sasl.mechanism` | `KAFKA_OUTPUT_SASL_MECHANISM`, default `SCRAM-SHA-512` | |
| `consumer.override.sasl.jaas.config` | `ScramLoginModule` (or `PlainLoginModule` when the mechanism is `PLAIN`) with `username="${env:KAFKA_OUTPUT_SASL_USERNAME}" password="${env:KAFKA_OUTPUT_SASL_PASSWORD}"` | The credentials are references, not values. |

The consumer group is `connect-reverse_etl_cdf_sink`.

---

## 8. Security and compliance notes

- **No secrets in config.** SASL and Postgres credentials are `${env:...}` references; the stored connector
  config and `GET /connectors/<name>/config` show the references, not the values. No credentials are written to
  any file in the repo.
- **Env var allowlist.** The Connect worker's env provider can only resolve the three variables listed in
  section 6.
- **REST API bound to localhost.** The Connect API can create connectors and so is not exposed on the network.
- **Supply chain.** The JDBC plugin is downloaded from Maven Central and pinned by sha512
  (`b0596cc6...2600b95`). The pin was compared against Maven Central's published `.sha512` and matched, and the
  Docker build re-verifies it (`sha512sum -c`). A mismatch fails the build.
- **Notebook credentials.** The Databricks notebook reads the Kafka credentials from a Databricks secret scope
  (`rw_poc`), never from the notebook text. The values are copied from the shell environment straight into the
  scope (section 10.3), so they are not printed or typed into a command line.
- **Data minimisation.** The POC table holds synthetic sandbox rows in `de_dev.sr_poc_external`, not real E&A
  or customer data.
- **Environments.** This is a local/staging POC. Nothing here targets production infrastructure; any move
  beyond this needs the normal change-management reference.

---

## 9. How it was verified

### 9.1 Build-time

- `python3 -m py_compile` on all changed Python files.
- `docker compose config -q` (compose file valid).
- `docker compose build kafka-connect`: the in-build checksum check printed `OK`.

### 9.2 Worker smoke test (local Redpanda + `kafka-connect`)

- Container reached `healthy`.
- `GET /connector-plugins` listed `io.debezium.connector.jdbc.JdbcSinkConnector` (sink, 3.7.0.Final).
- Worker log showed the config-provider, allowlist and override-policy settings applied; no errors.
- `_connect_configs`, `_connect_offsets`, `_connect_statuses` created on local Redpanda.

### 9.3 End-to-end connector test with no external systems

Used a throwaway `postgres:17-alpine` container (trust auth) on the compose network and a local Redpanda topic,
with the **exact config the asset generates** (only topic, table name and Kafka bootstrap changed for the test):

1. Generated three insert events with the real `_to_debezium_events()` + `_build_connect_json_message()`, produced
   them with `rpk`, registered the connector. Result: connector and task `RUNNING`; table auto-created as
   `id bigint not null` (primary key at that time; the key is now `rid`, section 4.6), `value text`,
   `updated_at text` (since changed to a timestamp, section 4.3); three rows present. This also proved that
   `${env:POSTGRES_PASSWORD}` resolves when the variable is empty.
2. Produced an update (id 2 -> `two-v2`) and a delete (id 3). Result: id 2 updated, id 3 removed.
3. Cleaned up afterwards: connector deleted, test topic deleted, scratch Postgres removed, containers stopped.

### 9.4 Live run

The first live run (2026-10-02) ran the seed script and then the Dagster job; it exercised the external SASL_SSL
cluster, which the local tests could not. Later live verification is recorded where each feature is described:
the `rid` key (4.6), schema evolution (10.1), the notebook (10.3), column drops and type changes (10.4), and
the reset job (14.3).

---

## 10. How to run the demo

### One-time

1. Start the script runner from a **devbox shell** so the environment is loaded: `./bin/0_script_runner.sh`
   (port 4001). The shell must export `KAFKA_OUTPUT_SASL_USERNAME` and `KAFKA_OUTPUT_SASL_PASSWORD`, because
   compose passes them to the Connect container, and it starts host Postgres.
2. Script runner: **Start Services** (`1_up.sh`). Tick **Offline** if on VPN; this skips the rebuild and reuses
   the locally built images, including `risingwave-test-kafka-connect`. Wait about a minute for `kafka-connect`
   to be healthy (`docker compose ps kafka-connect`).
3. Dagster (http://localhost:3000), Jobs, `reverse_etl_cdf_setup_job`, Launch. This creates the Databricks
   source and watermark tables, creates the topic, runs a first sync (empty at this point, so it only records the
   table version as the watermark), creates the RisingWave table, registers and starts the connector, and last runs
   the seed asset `reverse_etl_cdf_seed` (insert, update, delete in Databricks; the rows are not synced yet).
4. Dagster: materialize `reverse_etl_cdf_to_kafka` (or run the notebook, section 10.3) to sync the seed rows.

### Every time after

1. Change the Databricks table (insert / update / delete).
2. Materialize **only** `reverse_etl_cdf_to_kafka` in Dagster (or run the notebook, section 10.3; use one or
   the other per change, since they share a watermark). The connector is already running and consumes the new
   messages continuously, so the whole job does not need to run again.

Nothing runs on a schedule for this pipeline (the existing schedules and sensor are for dbt and ML). The
Databricks -> Kafka step is manual.

### Starting a new demo from a known state

Run, in this order:

1. Dagster: `reverse_etl_cdf_reset_job` (tears everything down, section 14.3).
2. Dagster: `reverse_etl_cdf_setup_job` (recreates the Databricks tables with the original columns, the topic,
   the RisingWave table and the connector). Its first sync runs against the empty source table, finds no
   changes, and only records the current table version as the watermark. Its last step, `reverse_etl_cdf_seed`,
   writes the demo rows to Databricks.
3. Dagster: materialize `reverse_etl_cdf_to_kafka`.

The schema is then always `rid`, `id`, `value`, `updated_at`, whatever columns earlier demos added. The Postgres
table `reverse_etl_cdf_target` does **not** exist after steps 1 and 2: the sink creates it when it receives the
first message, so it appears after step 3. A database client that still lists it is showing a cached tree
(refresh it); querying it before step 3 fails with "relation does not exist", which is expected.

### Seeing the Kafka messages

In Dagster, open the latest materialization of `reverse_etl_cdf_to_kafka` (Assets, Events tab, or the run's
log). Its metadata has `kafka_messages` (payloads of up to 50 messages) and `kafka_messages_raw` (the first
three messages exactly as sent, with the embedded `schema` listing the table's columns and types, and the
`payload`). `kafka_messages_raw` is empty on a run with no changes.

### Checking results

```bash
# Connector and task state
curl -s localhost:8083/connectors/reverse_etl_cdf_sink/status

# Rows in the target
psql -h localhost -U postgres -d postgres -c "select * from reverse_etl_cdf_target order by rid"
```

### Images needed (VPN note)

No new pulls are needed once the image is built. A rebuild (`docker compose build kafka-connect` or
`up --build`) pulls `quay.io/debezium/connect:3.7.0.Final` and downloads the plugin from Maven Central, so
do it off VPN.

### 10.1 Schema evolution demo (add a column in Databricks)

Because the message schema is derived from the table's current columns on every sync (section 4.3), a column
added in Databricks appears in Postgres without touching any code or connector config.

**Before you start:** the asset code changed, so make Dagster pick it up (Deployment, then Reload definitions in
the UI, or `docker compose restart dagster-webserver dagster-daemon`). The Debezium connector itself needs no
restart or change.

**Steps** (Databricks SQL editor or UI, then Dagster, then `psql`):

```sql
-- 1. Databricks: add a column (additive change, supported by Change Data Feed)
ALTER TABLE de_dev.sr_poc_external.reverse_etl_cdf_source ADD COLUMN region STRING;

-- 2. Databricks: write a row that uses it (explicit column list)
INSERT INTO de_dev.sr_poc_external.reverse_etl_cdf_source (id, value, updated_at, region)
VALUES (10, 'evolved', current_timestamp(), 'eu');
```

3. Dagster: materialize `reverse_etl_cdf_to_kafka`.
4. Postgres:

```bash
psql -h localhost -U postgres -d postgres -c '\d reverse_etl_cdf_target' \
     -c 'select * from reverse_etl_cdf_target order by rid'
```

Expected: a new `region text` column; rows that existed before show `NULL` in it; the new row shows `eu`.
Other column types work the same way: a `DOUBLE`, `INT` or `BOOLEAN` column becomes `double precision`,
`integer` or `boolean`, in both Postgres and RisingWave.

**Cautions**
- The `ALTER TABLE` changes your sandbox table permanently. Dropping a column or changing its type later is a
  *non-additive* change: Databricks refuses it until an opt-in is enabled, a change feed read that spans it fails,
  and the targets do not follow it. What was tested is in section 10.4. Treat the new column as permanent, or use
  the reset and setup jobs to start over.
- **Running the notebook instead of the asset?** It cannot reach RisingWave, so run
  `ALTER TABLE reverse_etl_cdf_target ADD COLUMN <name> <type>` there first (section 10.3).
- The seed statements (in `defs.yaml`) use explicit column lists, so they keep working after the `ALTER`.
- **The RisingWave table picks the column up too, with the matching type.** Before producing events,
  `reverse_etl_cdf_to_kafka` calls `add_missing_columns()` (in `reverse_etl_risingwave_setup.py`), which compares
  the live Databricks columns with `reverse_etl_cdf_target` and runs `ALTER TABLE ... ADD COLUMN <name>
  <type>` for each one the table lacks (type mapping in section 4.3). It does nothing if the table doesn't exist
  yet. Check with `psql -h localhost -p 4566 -U root -d dev -c "describe reverse_etl_cdf_target"`; the run's
  Dagster metadata also lists `risingwave_columns_added`.
  - **Typed, as in Postgres.** RisingWave reads the same typed, schema-embedded messages as the sink, so a
    `DOUBLE` column is `double precision` in both. (Before the single-topic change it had to be `VARCHAR`,
    because the old schemaless topic carried values as strings.)
  - **It must happen before the events are produced.** RisingWave fills a column only from messages it reads
    after the column exists; a row ingested earlier keeps `NULL` even if its message carried a value. That is
    why the sync runs inside `reverse_etl_cdf_to_kafka`, ahead of the produce step, so the everyday "materialize
    just that asset" flow stays correct.
  - **Coupling.** When there are events to send, the asset now needs RisingWave reachable (errors propagate
    rather than being swallowed). Without RisingWave, Postgres would not be updated either.

**What was verified**
- Locally, with the real message builder and a real connector, against a scratch Postgres: rows with three
  columns, then rows with four extra columns (`string`, `double`, `int`, `boolean`). The sink issued the
  `ALTER TABLE` itself, created `text`, `double precision`, `integer` and `boolean` columns, left earlier rows
  `NULL`, applied a delete from the same batch, and the connector stayed `RUNNING`.
- Live, read-only: `_get_row_fields()` run in the Dagster container returns the table's three columns with the
  expected types.
- RisingWave, on the local instance with a throwaway Kafka-connector table (`FORMAT DEBEZIUM ENCODE JSON`):
  messages carrying fields the table has no column for are ingested normally (the extra fields are ignored);
  `ALTER TABLE ... ADD COLUMN` works on such a table; messages read after the `ALTER` fill the new column while
  already-ingested rows keep `NULL`; deletes still apply. `add_missing_columns()` itself was run from the
  Dagster container against a throwaway table: it added the missing columns, was a no-op on a second call and on
  a missing table, and handled a column name needing quotes (`my col`).
- Single-topic consolidation, local only (throwaway RisingWave tables and a local Redpanda topic; the real
  RisingWave table and connector were not touched):
  - RisingWave's `FORMAT DEBEZIUM ENCODE JSON` parsed the schema-embedded messages: inserts, an update and a
    delete applied; typed `DOUBLE PRECISION`, `INT` and `BOOLEAN` columns filled from the payload.
  - From the Dagster container, with the production `_produce_to_kafka()` pointed at the local topic: the table
    created by `_create_table_sql()` had `double precision` / `integer` / `boolean` columns and
    `add_missing_columns()` found nothing to add (no race); after two new source columns appeared,
    `add_missing_columns()` added `country varchar` and `weight double precision` before the next produce, the new
    values arrived typed, an update upserted, and a delete removed the row.
  - Live migration (2026-10-03, local stack): Dagster definitions reloaded, the real
    `reverse_etl_cdf_target` dropped (nothing depended on it) and recreated by materializing
    `reverse_etl_cdf_risingwave_target` (run succeeded). The new table reads `reverse_etl_cdf_topic`, was
    created with `region` from the start, and its four rows (ids 1, 2, 10, 11, including `region` = `eu` / `us` on
    10 and 11) matched the Postgres table exactly. The Debezium connector stayed `RUNNING` throughout.
  - Live new-column test on the single topic (2026-10-03): in Databricks, `ALTER TABLE ... ADD COLUMN country
    STRING`, a `MERGE` of ids 20 (`GR`) and 21 (`DE`), and an `UPDATE` of id 10 to `FR`; then `reverse_etl_cdf_to_kafka`
    was materialized. The run read the change feed across the `ALTER`, logged `Added column(s) to the RisingWave
    table: ['country']` *before* producing, and produced 3 events (2 creates, 1 update). Result: `country` appeared
    as `text` in Postgres and `character varying` in RisingWave; ids 20, 21 and 10 carried `GR`, `DE` and `FR` in
    both tables; every older row had `NULL`; the connector stayed `RUNNING`. This was a `STRING` column; typed
    (`DOUBLE` / `INT` / `BOOLEAN`) columns were verified locally only.
- **Live, end to end (2026-10-02):** `ALTER TABLE ... ADD COLUMN region STRING` and an insert of ids 10 and 11
  (`evolved-eu`, `evolved-us`) were run in Databricks, then `reverse_etl_cdf_to_kafka` was materialized. Postgres
  got the new `region` column with `eu` / `us` on those rows, so Change Data Feed carried the added column
  through as the Databricks documentation says (an additive change; batch reads use the latest table schema).
  `add_missing_columns()` then added `region` to the RisingWave table (logged as `Added column(s) to the
  RisingWave table: ['region']`), and after the fix-up below the RisingWave rows showed the values as well.

**Gotcha seen during the live run: rows ingested before the column exists stay `NULL` in RisingWave.**
The first sync of ids 10 and 11 ran on an earlier code version that had the Postgres schema logic but not
`add_missing_columns()`. RisingWave ingested those two rows (07:20) while its table had no `region` column, and
it does not re-read old messages for a column added later (RisingWave `_rw_timestamp` showed 07:20 for ids 10/11;
the column was added by a later run at 07:29). Postgres was unaffected because the sink adds columns itself. With
the current code this cannot happen for a *new* column, because the RisingWave column is added before the events
carrying it are produced. To repair rows that were ingested too early, make a real change to them so fresh events
are produced, then materialize `reverse_etl_cdf_to_kafka`:

```sql
UPDATE de_dev.sr_poc_external.reverse_etl_cdf_source
SET region = region, updated_at = current_timestamp()
WHERE id IN (10, 11);
```

Alternatively, rebuild the RisingWave table, which fills every row from the topic (section 10.4).
`updated_at` is changed on purpose: an update that changes nothing may not produce a change event. The same
situation would arise if a recreated table were created with fewer columns than the topic carries: with
`scan.startup.mode = 'earliest'` it starts re-reading the topic immediately, so a column added a moment later
misses the messages already read. The asset therefore creates the table with all current columns up front
(section 10.2), which avoids this.

### 10.2 Migrating an existing RisingWave table to the single topic

A RisingWave table's Kafka topic is fixed when the table is created, so a `reverse_etl_cdf_target` created
before the consolidation still reads the retired schemaless topic and will no longer receive new events. To
move it:

1. Reload Dagster's definitions (or restart the Dagster containers) so the new asset code is loaded.
2. Drop the table: `psql -h localhost -p 4566 -U root -d dev -c "DROP TABLE reverse_etl_cdf_target"`.
3. Materialize `reverse_etl_cdf_risingwave_target` (or run `reverse_etl_cdf_setup_job`). It recreates the table
   from the live Databricks columns, with real types, reading the topic from the beginning.

Notes:
- The Debezium connector and the Postgres table are unaffected: they already read that topic.
- RisingWave rebuilds its state from that topic's contents, which is also exactly where Postgres' contents came
  from, so the two stay consistent. Anything that only ever existed on the retired topic does not reappear.
- The retired topic `rw_poc_reverse_etl_cdf_out` is left in place on the staging cluster, unused; it is no longer
  produced to or created by anything.
- Create the table with all columns up front (the asset now does) rather than adding columns after it starts
  reading, for the reason given in section 10.1.
- The same drop-and-recreate is the repair for rows left `NULL` by a column added late (section 10.4).

### 10.3 Running the sync as a Databricks notebook

`notebooks/reverse_etl_cdf_to_kafka.py` is a PySpark version of `reverse_etl_cdf_to_kafka`, in Databricks
source format. It reads the change feed since the watermark, builds the same Debezium-style events and Connect
JSON messages with the embedded schema, writes them with Spark's Kafka writer, and advances the watermark. A
local test showed the events and message bytes equal the Dagster asset's for inserts, updates and deletes.

Verified live on 2026-10-03: an insert with a new `country` value (row 13) went Databricks -> notebook -> Kafka
-> Postgres and RisingWave, the watermark moved from version 8 to 9, and timestamps matched the Dagster format.

**Where it runs.** Import it into workspace `adb-1608121643336927` (the one the pipeline uses; Unity Catalog
`de_dev.sr_poc_external` is readable there) and attach a **classic cluster**:
- **Serverless compute cannot resolve the staging Kafka hostname** (`gaierror: Name or service not known`,
  and `No resolvable bootstrap urls` from the Kafka client), in either workspace.
- **The `databri-pltf-stg` workspace (`adb-2241475393894655`) was only tried on serverless**, where it failed
  the same way: no DNS for Kafka, and HTTP 403 `AuthorizationFailure` from the `de_dev` storage account (probably
  because its firewall does not allow serverless). Whether a classic cluster there works was not tested.

**Setup.**
1. Import the notebook (UI: Create, Import, or `databricks workspace import ... --format SOURCE --language PYTHON`).
2. Create the secret scope `rw_poc` in that workspace, with the Kafka credentials copied from your shell
   environment so the values never appear on screen or in a command line:
   ```bash
   databricks --profile personal secrets create-scope rw_poc
   printenv KAFKA_OUTPUT_SASL_USERNAME | tr -d '\n' | databricks --profile personal secrets put-secret rw_poc kafka_output_username
   printenv KAFKA_OUTPUT_SASL_PASSWORD | tr -d '\n' | databricks --profile personal secrets put-secret rw_poc kafka_output_password
   ```
   (`tr -d '\n'` matters: a trailing newline would be stored in the value.)
3. Run it: Run all. Widgets at the top (catalog, schema, tables, topic, bootstrap, secret names, optional
   `backfill_from_version`) default to the POC's values.

**Differences from the Dagster asset.**
- It does **not** add new columns to the RisingWave table (a notebook cannot reach the local RisingWave). After
  adding a Databricks column, run `ALTER TABLE reverse_etl_cdf_target ADD COLUMN <name> <type>` in
  RisingWave *before* the notebook, or RisingWave leaves the column `NULL` for the new rows.
- The source and state tables must exist (`reverse_etl_cdf_table_setup`); the notebook does not create them.
- A failed Kafka write raises before the watermark moves, so the next run re-sends (the sink and RisingWave
  upsert by key, so replays are harmless). The Dagster asset advances the watermark even if a delivery failed.
- **Do not run both for the same change:** they share one watermark (`sync_name`) and one topic.

### 10.4 Dropping a column or changing a type (tested)

Tested on 2026-10-03 in two steps. The Databricks behaviour (the tables below) was tested on throwaway tables in
`de_dev.sr_poc_external` with Change Data Feed enabled, dropped afterwards. The effect on the pipeline was then
tested live on the demo table, which was restored with the reset and setup jobs.

| Operation | Result |
|---|---|
| `DROP COLUMN` on a plain table | Refused: `DELTA_UNSUPPORTED_DROP_COLUMN`; column mapping (`delta.columnMapping.mode = 'name'`) must be enabled first |
| Enable column mapping, then `DROP COLUMN` | Works (two separate table versions) |
| `INT` to `BIGINT` without type widening | Refused: `DELTA_UNSUPPORTED_ALTER_TABLE_CHANGE_COL_OP` |
| `STRING` to `INT` | Refused, even with type widening |
| Enable type widening (`delta.enableTypeWidening`), then `INT` to `BIGINT` | Works; values beyond the old `INT` range can then be inserted |

**Change feed reads across the change.** For a drop at version D:

| `table_changes()` range | Result |
|---|---|
| Ends before D | Works; the dropped column is still returned with its old values |
| Starts before D and spans it | **Fails**: `DELTA_CHANGE_DATA_FEED_INCOMPATIBLE_SCHEMA_CHANGE` |
| Starts at D or later | Works; the column is gone |

A type change behaves the same way (a read spanning it fails with the same error). The information schema
then reports the widened type (`LONG` for `BIGINT`), which `_CONNECT_TYPE_BY_DATABRICKS_TYPE` already maps to
`int64`.

**What this means for the sync.** The sync reads from `last_commit_version + 1`, and the watermark only advances
when a sync reads data rows. Enabling column mapping or type widening is a table commit with no data, so it
does not move the watermark, and the drop or retype that follows is then still ahead of the next read's
starting version. Confirmed on a scratch table: with the watermark at version 1, a read starting at version 2
(the no-data upgrade commit, before a drop at 3) fails with `DELTA_CHANGE_DATA_FEED_INCOMPATIBLE_DATA_SCHEMA`,
while a read starting at the drop (3) works. If the sync fails this way it fails on every run until the
watermark is moved to the change's version, which skips any earlier unsynced changes. Safe order: **enable
column mapping / type widening, make one data change, sync (the watermark now passes the upgrade), make the
schema change, sync again.**

**Live run on the demo table (2026-10-03), using that order.** Column mapping was enabled and a row inserted,
then synced; then `region` was dropped, a row inserted and another updated, then synced. The sync succeeded and
the connector stayed healthy. The targets kept the column and disagreed afterwards:

| Row | Databricks | Postgres `region` | RisingWave `region` |
|---|---|---|---|
| Written before the drop (id 16) | column gone | `eu` | `eu` |
| Updated after the drop (id 15) | column gone | `eu` (stale; the upsert did not write the missing field) | `NULL` (the upsert replaced the row) |
| Inserted after the drop (id 17) | column gone | `NULL` | `NULL` |

The same run for a type change: `score INT` was added and synced (Postgres `integer`, RisingWave `integer`),
then type widening enabled, a row synced, `score` widened to `BIGINT`, and a row with `score = 9999999999`
inserted. The Dagster sync reported success, but:
- **Postgres:** the sink's insert failed because the value did not fit the `integer` column. The connector
  reported `RUNNING` while its **task was `FAILED`**, and no later message was applied. The connector state alone
  looks healthy; check `tasks[].state` in `GET /connectors/<name>/status`.
- **RisingWave:** the row arrived with `score = NULL`, silently (no error).
- **Recovery for Postgres:** `ALTER TABLE reverse_etl_cdf_target ALTER COLUMN score TYPE bigint`, then
  `POST /connectors/reverse_etl_cdf_sink/tasks/0/restart`; the task resumed from its offset and the row
  appeared with the right value.
- **RisingWave cannot be repaired in place** (no column type change): the table must be recreated, in practice
  with the reset job and then setup, which also restores the original schema and removes the column-mapping
  and type-widening upgrades from the demo table.

**Why the targets keep a dropped column.** This is the Debezium JDBC sink's design, not a bug. Its only schema
operation is `ALTER TABLE ... ADD COLUMN` (`schema.evolution=basic`; the only other value, `none`, does nothing),
because dropping or retyping destroys data. It also could not tell that a drop happened: it receives records, not
DDL events, and the message schema describes the shape of that one record, which after a drop simply lacks the
field (our schema was correct: it is built from the live columns). Debezium source connectors publish DDL
changes on a separate schema-change topic, which the JDBC sink does not read and this pipeline does not
produce. RisingWave is not a Debezium component: it ignores fields it does not know, and our code only adds
missing columns (`add_missing_columns()`).

**Repairing rows that have `NULL` after a late column add.** `ALTER TABLE ... ADD COLUMN` in RisingWave fills the
column only from messages read afterwards, so rows already ingested stay `NULL`. Either update those rows in
Databricks (new change events carry the value) or rebuild the table: `DROP TABLE reverse_etl_cdf_target`,
then materialize `reverse_etl_cdf_risingwave_target`. It recreates the table with all live columns and re-reads
the topic from the start, so every row is filled. Verified live on 2026-10-03 (rows 10 and 11 had `region`
only in Postgres; after the rebuild both targets matched). The drop does not cascade, so it fails if anything
depends on the table; during the rebuild the table is briefly empty or partial.

### 10.5 Adding another sync

The second sync, `orders`, was added this way (see 14.4 for the names it gets):

1. Create `orchestration/defs/reverse_etl_<label>/defs.yaml` with a `ReverseEtlCdfSync` instance: `name` (the
   label), `catalog`, `schema_name` (a scratch schema), `source_columns` (used only if the setup creates the
   table), and optional `seed_statements` (SQL using `{source_table}`).
2. Reload the Dagster code location (no restart needed; the containers mount `./orchestration`). The group
   `reverse_etl_<label>`, its assets and `reverse_etl_<label>_setup_job` / `_reset_job` appear.
3. Run `reverse_etl_<label>_setup_job`: it creates the Databricks tables (with `rid` and CDF), the topic, the
   RisingWave table and the connector, and seeds if statements are given.
4. Materialize `reverse_etl_<label>_to_kafka`.

To start over, run `reverse_etl_<label>_reset_job`, then the setup job. To remove a sync, run its reset job first,
then delete its folder; deleting the folder alone leaves the objects behind. The notebook serves one sync per
widget set (set `source_table`, `state_table`, `sync_name`, `kafka_topic`); it was not run against `orders`.
For a real, existing table: the setup only creates a table that does not exist, and the table needs a `rid`
identity column from creation (section 13). `source_columns` is still a required field in the YAML even then; it is
unused when the table exists (making it optional is not done).

---

## 11. Operating notes

- **Which asset to run when.** After any change to the Databricks table, materialize **only**
  `reverse_etl_cdf_to_kafka`: it reads the change feed since its watermark, adds any new column to the
  RisingWave table, and produces the events; the connector and the RisingWave table pick them up within
  seconds. Nothing is scheduled, so the sync only runs when triggered. The other assets in
  `reverse_etl_cdf_setup_job` are one-time setup (plus `reverse_etl_cdf_table_setup`, `reverse_etl_cdf_topic_setup`
  and the demo seed `reverse_etl_cdf_seed`, which do not move data either):
  - `reverse_etl_cdf_risingwave_target` creates the RisingWave table that reads the topic. Run it again only
    after dropping that table or changing its topic.
  - `reverse_etl_cdf_jdbc_sink` registers the connector with Kafka Connect, which then runs it on its own.
    Run it again only if Connect loses its state (for example its local Redpanda volume is wiped) or the
    connector configuration changes.
  Neither moves data; both sit after the sync in the job only for ordering.
- **Where the sync runs.** `reverse_etl_cdf_to_kafka` runs as Python in the Dagster container. Databricks is used
  only through the Statement Execution API (SQL, authenticated with the service principal) to read the change
  feed, the watermark table and `information_schema`; the grouping into events, the RisingWave column sync and
  the Kafka produce all happen in the container, which therefore needs a network path to the Kafka cluster (VPN
  today). A PySpark notebook that does the same work exists and was verified live (section 10.3); it is an
  alternative trigger, not a replacement, and the Dagster asset remains the default.
- **Starting over.** Run `reverse_etl_cdf_reset_job`, then `reverse_etl_cdf_setup_job` (which seeds), then
  the sync (section 10, "Starting a new demo from a known state"). To repair only the RisingWave table, rebuild it (section 10.4).
- **Which Kafka.** The data topic lives on Kaizen's staging Kafka cluster (SASL_SSL, SCRAM-SHA-512), named in
  `REVERSE_ETL_CDF_POC_PLAN.md` and read from `KAFKA_OUTPUT_BOOTSTRAP` at run time. The local Redpanda only holds
  Kafka Connect's own bookkeeping topics; no pipeline data passes through it.
- **Updating the Debezium version.** Change `DEBEZIUM_VERSION` in `Dockerfile.debezium-connect` and update
  `JDBC_PLUGIN_SHA512` to the value from
  `https://repo1.maven.org/maven2/io/debezium/debezium-connector-jdbc/<version>/debezium-connector-jdbc-<version>-plugin.tar.gz.sha512`.
  Keep the base image and plugin versions aligned.
- **Re-running the asset.** `reverse_etl_cdf_jdbc_sink` is idempotent; running it again updates the
  connector config in place.
- **Delivery semantics.** At-least-once from Kafka; upserts and deletes by primary key are idempotent, so a
  redelivered message gives the same end state.
- **Replaying from the start.** Stop the connector, reset its offsets with the Kafka Connect offsets API (or
  delete the connector and the `connect-reverse_etl_cdf_sink` consumer group), then restart. Because
  `auto.offset.reset=earliest`, a new consumer group re-reads the whole topic.
- **Removing the sink.**
  `curl -X DELETE localhost:8083/connectors/reverse_etl_cdf_sink`, then
  `drop table reverse_etl_cdf_target;` in host Postgres if the data should go too.

---

## 12. Troubleshooting

| Symptom | Likely cause and fix |
|---|---|
| Postgres table missing or empty after the job | The topic was empty: the CDF watermark had already advanced and there were no new table changes. Seed or change the Databricks table and re-run `reverse_etl_cdf_to_kafka`. Check the asset logged `Produced N event(s)` rather than `No new commits`. |
| Asset `reverse_etl_cdf_jdbc_sink` fails with "Kafka Connect not reachable" | `kafka-connect` is not up or not healthy yet. `docker compose ps kafka-connect`, `docker logs kafka-connect`. |
| Connector `FAILED` with an authentication / SASL error | `KAFKA_OUTPUT_SASL_USERNAME` / `KAFKA_OUTPUT_SASL_PASSWORD` were not exported in the shell that ran compose, or the mechanism is wrong (`KAFKA_OUTPUT_SASL_MECHANISM`). Fix the environment, recreate the container (`docker compose up -d kafka-connect`), re-run the asset. |
| Connector config rejected with a config-provider / "not allowed" error | The variable is not in the allowlist pattern. Only `KAFKA_OUTPUT_SASL_USERNAME`, `KAFKA_OUTPUT_SASL_PASSWORD` and `POSTGRES_PASSWORD` can be referenced. |
| Connector `FAILED` connecting to Postgres | Host Postgres is not running (start it via `devbox shell`), or `HOST_POSTGRES_URL` / `POSTGRES_USER` / `POSTGRES_PASSWORD` are wrong. `pg_isready -h localhost -p 5432`. |
| Messages rejected: "schema" / not a Struct errors | The message was not produced in the schema-embedded format (for example someone produced to the topic by hand with plain JSON). Only `_build_connect_json_message()` output is valid. |
| RisingWave table `reverse_etl_cdf_target` stops updating, or is missing new columns | It was created before the single-topic change and still reads the retired schemaless topic. Migrate it (section 10.2). |
| Deletes not applied | `delete.enabled` must be `true` and `primary.key.mode` must be `record_key`, and the message must have `op = "d"` with the row in `before`. |
| Connector shows `RUNNING` but rows stop arriving | Check the **task** state, not just the connector: `GET /connectors/reverse_etl_cdf_sink/status`. A `FAILED` task usually means a value the Postgres column cannot hold (for example a widened integer, section 10.4). Fix the column, then `POST /connectors/reverse_etl_cdf_sink/tasks/0/restart`. |
| Sync fails with `DELTA_CHANGE_DATA_FEED_INCOMPATIBLE_SCHEMA_CHANGE` | A column was dropped or retyped and the change feed read starts before it. Section 10.4: it fails every run until the watermark is moved past the change; or use the reset job. |
| Postgres table does not exist right after setup or reset | Expected: the sink creates it on the first message, so it appears after the first sync that has changes (section 10). |
| `INSERT` into the source table fails after reset and setup | The table now has an identity column `rid`; list the columns explicitly (`INSERT INTO t (id, value, ...) VALUES (...)`). |
| Notebook fails with `No resolvable bootstrap urls` / `Name or service not known` | The cluster cannot resolve the staging Kafka host. Use a **classic** cluster in the pipeline workspace, not serverless (section 10.3). |
| New column is `NULL` for rows ingested before RisingWave had the column | `ALTER` does not backfill. Rebuild the table or update those rows (section 10.4). |
| Port 8083 already in use | Another process is bound to 8083; stop it or change the host port mapping in compose. |

---

## 13. Known limitations and open items

- **Delivery errors are not inspected.** `_produce_to_kafka()` produces each event and flushes once; a failed
  delivery is not surfaced, and the watermark is still advanced afterwards (pre-existing behaviour).
- **Schema evolution is additive only.** Adding a column works end to end (section 10.1). Dropping a column or
  widening a type was run live (section 10.4): both need an opt-in on the Databricks side, and the sync only
  works if run in the safe order given there. A dropped column stays in both targets and they disagree on rows
  updated afterwards; a widened value that no longer fits fails the Postgres sink task and becomes `NULL` in
  RisingWave. Treat added columns as permanent, use a new column name for repeated demos, and use the reset and
  setup jobs to return to the original schema.
- **Type coverage.** Only the types in the mapping table (section 4.3) become typed Postgres columns; anything
  else (`DATE`, `TIMESTAMP_NTZ`, `DECIMAL`, ...) arrives as `text`. `TINYINT`, `SMALLINT` and `FLOAT` mappings
  were not exercised end to end.
- **Key column is fixed.** `key_column` (default `"rid"`, a component attribute) is
  the key for the Kafka message, the RisingWave primary key and the connector's `primary.key.fields`; the
  notebook repeats it as `KEY_COLUMNS`. A different key means changing the config and the notebook and
  recreating the source table.
- **The notebook repeats the config's Databricks-side values.** It runs inside Databricks and cannot import
  `reverse_etl_config.py`, so its widget defaults and `KEY_COLUMNS` must be kept equal to the names the config derives
  for `defs/reverse_etl_cdf/defs.yaml` by hand (there is no automated check).
- **Two syncs have run together, no more.** The POC (`cdf`) and one more sync (`orders`, section 14.4) ran side by
  side on the single Kafka Connect worker. The notebook's `sync_name` watermark convention and the second sync's
  schema changes and reset were not exercised.
- **`rid` must exist from table creation.** An identity column cannot be added to an existing table, so this
  needs the reset job, then the setup job. Inserts into the source table must list their columns
  (`INSERT INTO t (id, value, ...)`); a column-less `INSERT ... VALUES` does not work, and Databricks restricts
  concurrent writers on tables with identity columns.
- **Schema repeated in every message.** JSON with embedded schema is larger than Avro with a registry; fine for
  a POC, worth revisiting at volume.
- **Single task.** `tasks.max=1`; throughput scaling would need more tasks, and the topic has 15 partitions so
  there is room.
- **Manual trigger.** No schedule or sensor drives the Databricks -> Kafka step.
- **Reset is destructive and total.** It drops the Databricks source table and its history, not only the
  downstream copies; it is meant for the sandbox schema `sr_poc_external` only.
- **Table created by the sink.** `schema.evolution=basic` auto-creates the table; there is no explicit DDL or
  migration for it.
- **Internal topics on local Redpanda.** The Connect worker depends on the local Redpanda; if its volume is
  wiped, the connector registration is lost and the asset must be re-run (the consumer group offsets on the
  external cluster remain, so a re-registered connector resumes where it left off).
- **Retired topic remains.** `rw_poc_reverse_etl_cdf_out` still exists on the staging cluster, unused.

---

## 14. Investigations and designs not adopted

### 14.1 Schema registry (Avro) instead of embedded JSON schema

Evaluated, not adopted: the POC keeps JSON with the schema embedded in each message (section 4).

- **Local Redpanda registry** is Confluent-API compatible (default `BACKWARD` compatibility): adding an optional
  field is accepted, adding a required field without a default is rejected with HTTP 409.
- **Staging Apicurio 2.6.8** exposes the Confluent-compatible API at `/apis/ccompat/v7` and its native v2 API,
  with anonymous read and write. Subjects qualified by a group (such as `bigdata/reverse-etl`) cannot be
  resolved through the ccompat API, so Confluent-style clients would need the native API or an unqualified name.
- **Connect image:** it ships the Apicurio 3.2.5 converters but no Confluent `AvroConverter`, so Avro would need
  an extra image layer.
- **Test artifact (cleaned up):** a probe registered `bigdata/reverse-etl` in Apicurio staging on 2026-10-02
  (contentId 445, globalId 2635). It was unused and was deleted on 2026-10-03
  (`DELETE .../apis/registry/v2/groups/bigdata/artifacts/reverse-etl`, HTTP 204; a follow-up GET returns 404).

### 14.2 Kafka delete permissions

Checked with throwaway objects only (a `..._permtest` topic and a `rw-poc-permtest-*` consumer group, never the
real ones). The principal can create and delete topics and delete consumer groups on the staging cluster. Deletes
are asynchronous: the topic stayed listed for about 6 seconds. The reset job (14.3) relies on this.

### 14.3 Reset job (built, verified live)

`reverse_etl_cdf_reset_job` (`orchestration/assets/reverse_etl_reset.py`) is a Dagster op job, not assets,
because it destroys state instead of materializing it. It runs these steps in order:

1. Delete the connector from Kafka Connect.
2. Drop the RisingWave table `reverse_etl_cdf_target`.
3. Drop the Postgres table `reverse_etl_cdf_target`.
4. Delete the consumer group `connect-reverse_etl_cdf_sink` (retried while the connector's consumer
   leaves it) and the topic `reverse_etl_cdf_topic`, then wait until the topic is gone from the
   metadata, since topic deletes are asynchronous.
5. Drop the Databricks source and watermark tables (`reverse_etl_cdf_source`, `reverse_etl_cdf_state`).

Dropping the Databricks tables, rather than only resetting the watermark, is what removes columns added during
a demo: `reverse_etl_cdf_table_setup` recreates them with `CREATE TABLE IF NOT EXISTS` and the original
columns. Every step tolerates a missing object, so a half-finished reset can be run again. A guard refuses to
run unless every name it drops starts with `reverse_etl_` and the schema equals the sync's `schema_name` (`sr_poc_external`). Each sync has its own reset job, with op names prefixed by its `sync_name`.

Verified live on 2026-10-03: reset, then setup (the first sync on the empty table succeeded), then seed and a
sync produced matching rows in Postgres and RisingWave. After the reset, the connector list, topic, consumer
group and both target tables were all absent, as expected.

### 14.4 Discussed and not built, and the Dagster component (built)

- **Propagating column drops to the targets.** In `reverse_etl_cdf_to_kafka`, after reading the live columns and
  before producing, compare them with each target's columns and run `ALTER TABLE ... DROP COLUMN` for any extra
  one (never the key). The Postgres side must first wait until the connector has no lag: older Kafka messages
  still carry the column, and the sink would add it straight back through `schema.evolution`. RisingWave has no
  such race (it ignores unknown fields) but dropping a column on this Kafka-backed table is untested. Guard it
  behind an explicit setting and report `columns_dropped` in the asset metadata. Renaming the column to
  `_dropped_<name>` instead keeps the data. The notebook cannot do this (it cannot reach Postgres or RisingWave).
  Type changes are harder: Postgres can widen in place, RisingWave cannot change a column type.
- **A separate column-sync job** (Dagster, which reaches both Databricks and RisingWave) that adds missing
  RisingWave columns, run before the notebook, on a schedule, or from a sensor watching the source table's history
  for `ADD COLUMNS`. Rows that arrive before the column exists still end up `NULL` until the table is rebuilt.
- **A one-click rebuild job** doing the two-step RisingWave rebuild above.
- **Enabling column mapping and type widening in `reverse_etl_cdf_table_setup`.** Not done on purpose: today
  Databricks refuses a drop or retype, which protects the pipeline from changes the targets cannot follow. They are
  also one-way table upgrades. If wanted for a deliberate schema-change demo, make it an optional switch.
- **Typed `DATE` / `TIMESTAMP_NTZ` columns** (still sent as plain strings, see section 4.3).
- **A Dagster component for the whole pipeline** (built; both syncs run through it). `ReverseEtlCdfSync`
  (`orchestration/components/reverse_etl_cdf_sync.py`) is a regular `Component`, not state-backed, because the
  columns are read at run time. One instance per sync under `orchestration/defs/<folder>/defs.yaml`:

  ```yaml
  type: orchestration.components.reverse_etl_cdf_sync.ReverseEtlCdfSync
  attributes:
    name: orders            # the label; every name below is derived from it
    catalog: de_dev
    schema_name: sr_poc_external
    source_columns: ["id BIGINT NOT NULL", "description STRING", "total DOUBLE", "updated_at TIMESTAMP"]
    # key_column: rid   (default); seed_statements: see defs/reverse_etl_orders/defs.yaml
  ```

  It calls `build_reverse_etl_defs(ReverseEtlSyncConfig.for_name(...))`. Every name is `reverse_etl_<label>_<role>`,
  shown here for the two syncs (`cdf` is the POC, `orders` the second):

  | Object | `cdf` | `orders` |
  |---|---|---|
  | Databricks source / watermark table | `reverse_etl_cdf_source` / `reverse_etl_cdf_state` | `reverse_etl_orders_source` / `reverse_etl_orders_state` |
  | Postgres and RisingWave table | `reverse_etl_cdf_target` | `reverse_etl_orders_target` |
  | Kafka topic | `reverse_etl_cdf_topic` | `reverse_etl_orders_topic` |
  | Connector (consumer group `connect-<connector>`) | `reverse_etl_cdf_sink` | `reverse_etl_orders_sink` |
  | Setup / reset job | `reverse_etl_cdf_setup_job` / `reverse_etl_cdf_reset_job` | `reverse_etl_orders_setup_job` / `reverse_etl_orders_reset_job` |
  | Assets (group `reverse_etl_<label>`) | `reverse_etl_cdf_table_setup`, `_topic_setup`, `_to_kafka`, `_risingwave_target`, `_jdbc_sink`, `_seed` | same with `orders` |
  | `sync_name` (watermark key, Debezium envelope prefix) | `reverse_etl_cdf` | `reverse_etl_orders` |

  `definitions.py` loads the `defs/` folder with `load_defs(..., project_root=...)` and merges it with the rest.
  The optional override fields (`source_table`, `sync_name`, `state_table`, `kafka_topic`, `connector_name`,
  `postgres_table`, `risingwave_table`, `group_name`, the asset and job names, `topic_asset`,
  `create_topic_asset`, `reset_name_prefixes`, `reset_schema`) replace a derived name, for example to point
  `source_table` at an existing table; neither current instance uses any. Notes and limits:
  - **The POC has one definition.** `defs/reverse_etl_cdf/defs.yaml`, including its seed. There is no Python copy
    of the POC's names. `sync_name` is the watermark key, so changing it restarts that sync.
  - **Seeding.** `seed_statements` (a list of SQL strings, each may use `{source_table}`) makes the component add
    `reverse_etl_<label>_seed`, the last asset of the setup job. It runs after the first sync, so the rows wait in
    Databricks until `reverse_etl_<label>_to_kafka` runs. Both syncs use it (a MERGE, an UPDATE of id 2 and a DELETE
    of id 3). It replaces `scripts/reverse_etl_poc_seed.py`, which was deleted. Leave it unset for a real table.
  - **Each sync owns its topic.** `reverse_etl_<label>_topic_setup` creates the topic (15 partitions, replication
    1). The shared `kafka_output_topics_setup` no longer lists the POC topic.
  - **Layout.** Only this part is on the `dg` layout; the other assets stay in `definitions.py`. `project_root` is
    passed explicitly because the Dagster containers mount only `./orchestration`, so `load_defs` finds no
    `pyproject.toml` there (without it the code location failed to load).
  - **`dg` tooling does not see it.** `orchestration` is not an installed package, so `dg list components` and
    `dg scaffold defs` do not list the type; write the `defs.yaml` by hand. Runtime loading is unaffected.
  - **Field name.** `schema_name`, not `schema`, which would shadow a pydantic attribute.
  - **Reset allowlist.** The reset job refuses to run unless every name it drops starts with `reverse_etl_` and
    the schema equals `schema_name` (the config's `reset_name_prefixes` and `reset_schema`).
  - **History of the names.** The names above replaced earlier ones through three rounds of "reset under the old
    names, change the config, re-run setup", for example `reverse_etl_cdf_poc_source`, `reverse_etl_cdf_poc`,
    `reverse_etl_cdf_poc_current`, `rw_poc_reverse_etl_cdf_out_jdbc`, `reverse_etl_cdf_jdbc_sink` and
    `reverse_etl_poc_setup_job`. Older sections that describe past runs may still show some of them.
  - **Not exposed.** Kafka connection settings, a schedule and an asset check (the connection is still the
    `KAFKA_OUTPUT_*` env vars).
  - **The orders table.** Its columns are `rid`, `id`, `description`, `total`, `updated_at` (in that order, which is
    the order of `source_columns` after the key). Verified live (2026-10-04) after reset, setup and a sync: both
    targets show ids 1 and 2 with those columns (`description` as text, `updated_at` as a timestamp with time
    zone in Postgres and `TIMESTAMPTZ` in RisingWave, id 3 deleted), and the connector and its task were
    `RUNNING`. Column order is fixed when the table is created, so changing `source_columns` needs the
    reset and setup jobs.
  - **Verified live** after the last rename: both syncs' reset jobs, setup jobs and syncs ran, an insert, update and
    delete on the orders table and the seeds reached Postgres and RisingWave in each, both connectors were
    `RUNNING`, and no objects under the old names remained (Databricks, Postgres, RisingWave, Connect). Not
    tested: the notebook against the orders sync, or a column change on it.

---

## 15. References

- Debezium JDBC sink connector documentation (`debezium.io`, JDBC connector reference).
- Debezium source: `debezium-sink` module, `KafkaDebeziumSinkRecord` (envelope detection, delete handling).
- Maven Central: `io.debezium:debezium-connector-jdbc` (plugin archive and published checksums).
- Redpanda docs: "Deploy Kafka Connect in Docker" / "Deploy Redpanda Connectors in Docker".
- In this repo: `docs/poc/REVERSE_ETL_CDF_POC_PLAN.md`, `dbt/models/sink_funnel_to_postgres.sql`,
  `orchestration/assets/postgres_sink_setup.py`.
