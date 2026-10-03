# Reverse-ETL CDF POC: Debezium JDBC sink into Postgres

Date: 2026-10-02 (updated 2026-10-03: the pipeline now uses a **single topic** read by both RisingWave and the
Debezium sink; see sections 2, 3 and 10.2)
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
`reverse_etl_cdf_to_kafka`, the change appears in host Postgres table `reverse_etl_cdf_poc` within seconds.
Inserts, updates and deletes are all applied.

Evidence from the live run (host Postgres after seeding and syncing):

```
 id |     value      |        updated_at
----+----------------+--------------------------
  1 | first          | 2026-10-02T04:18:23.009Z
  2 | second-updated | 2026-10-02T04:18:33.963Z
```

Before any live run, the same connector configuration was verified locally (section 9) against a scratch
Postgres and a local Redpanda topic, covering insert, update and delete.

---

## 2. Architecture

```
 Databricks (de_dev.sr_poc_external.reverse_etl_cdf_poc_source, CDF enabled)
        |
        |  Dagster asset: reverse_etl_cdf_to_kafka        (BATCH, manual / on demand)
        |  reads table_changes() since the stored watermark,
        |  groups CDF rows into Debezium before/after/op events
        v
 External Kafka cluster (SASL_SSL)    <-- KAFKA_OUTPUT_BOOTSTRAP
   '-- rw_poc_reverse_etl_cdf_out_jdbc   Debezium JSON + embedded Connect schema   (ONE topic, two readers)
         |                                |
         |                                |  kafka-connect container
         v                                |  consumer.override.* -> external SASL_SSL cluster
   RisingWave table                       |  worker internal topics -> LOCAL Redpanda
   reverse_etl_cdf_poc_current            v
   (FORMAT DEBEZIUM ENCODE JSON,    Host PostgreSQL (host.docker.internal:5432, db "postgres")
    typed columns, PK: id)          table: public.reverse_etl_cdf_poc   (PK: id)
```

Key properties:

- **Batch half, streaming half.** Databricks -> Kafka only moves when the Dagster asset runs. Kafka ->
  Postgres is continuous: the connector is always running and applies messages within seconds of arrival.
- **One topic, two readers.** Every event is produced once, with an embedded Connect schema (section 4).
  RisingWave and the Debezium sink both read it. (It started as two topics, one schemaless for RisingWave and one
  with a schema for the sink; consolidated on 2026-10-03 after confirming RisingWave reads the embedded-schema
  format, including typed columns. The `_jdbc` suffix in the name is historical.)
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

Built from the existing constants in `reverse_etl_cdf_setup.py`
(`SYNC_NAME`, `SCHEMA`, `SOURCE_TABLE`):

```
reverse_etl_cdf_poc.sr_poc_external.reverse_etl_cdf_poc_source.Envelope   value (the envelope)
reverse_etl_cdf_poc.sr_poc_external.reverse_etl_cdf_poc_source.Value      before / after row struct
reverse_etl_cdf_poc.sr_poc_external.reverse_etl_cdf_poc_source.Key        key struct
```

### 4.3 Row columns

The row schema is **derived at sync time** from the source table's current columns, not hardcoded:
`_get_row_fields()` runs

```sql
SELECT column_name, data_type, is_nullable
FROM de_dev.information_schema.columns
WHERE table_schema = 'sr_poc_external' AND table_name = 'reverse_etl_cdf_poc_source'
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
| `updated_at` | `string` | yes | `text` |

Databricks type to Connect type mapping (`_CONNECT_TYPE_BY_DATABRICKS_TYPE`):

| Databricks type | Connect JSON type | Postgres type created by the sink (verified) |
|---|---|---|
| `TINYINT` / `SMALLINT` | `int8` / `int16` | not tested |
| `INT` / `INTEGER` | `int32` | `integer` |
| `BIGINT` / `LONG` | `int64` | `bigint` |
| `FLOAT` | `float` | not tested |
| `DOUBLE` | `double` | `double precision` |
| `BOOLEAN` | `boolean` | `boolean` |
| anything else (`STRING`, `TIMESTAMP`, `DATE`, `DECIMAL`, ...) | `string` | `text` |

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

The Statement Execution API returns every value as a string, so values are cast to the declared Connect type
(`_coerce()`) before they are written into the payload. Row payloads contain exactly the schema's fields, in
schema order. `source.commit_version` is cast to an integer as well (in the first version it was sent as a string
into an `int64` field, which Connect's JSON converter reads as 0; the sink ignores `source`, so it went unnoticed).

`updated_at` is deliberately a string (`TIMESTAMP` falls into the "anything else" row), matching the RisingWave
table's `VARCHAR`: the hand-rolled envelope does not follow real Debezium connectors' temporal encodings, so
declaring a native temporal type risked a parse mismatch. It can be cast downstream.

### 4.4 Example: an insert (`op = "c"`)

Key:

```json
{
  "schema": {
    "type": "struct",
    "name": "reverse_etl_cdf_poc.sr_poc_external.reverse_etl_cdf_poc_source.Key",
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
    "name": "reverse_etl_cdf_poc.sr_poc_external.reverse_etl_cdf_poc_source.Envelope",
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

All changes are on branch `feature-sr` (see `git log`).

| File | Change |
|---|---|
| `orchestration/assets/kafka_topics_setup.py` | `OUTPUT_TOPICS` lists `rw_poc_reverse_etl_cdf_out_jdbc` (created by the existing `kafka_output_topics_setup` asset: 15 partitions, replication 1, like the others). The original `rw_poc_reverse_etl_cdf_out` was removed from the list. |
| `orchestration/assets/reverse_etl_cdf_setup.py` | `KAFKA_TOPIC` is now the single `_jdbc` topic. Added `ENVELOPE_SCHEMA_NAME`, `_get_row_fields()` (live column list from `information_schema`), `_coerce()` / `_coerce_row()`, `_connect_row_schema()` and `_build_connect_json_message()`; `_produce_to_kafka()` produces each event once, in that format. Removed the old schemaless `_build_kafka_message()` and the `NUMERIC_ROW_COLUMNS` cast of `id` (now handled by `_coerce()`). `_message_previews()` shows each message's payload only; `_raw_messages()` returns the first three messages complete with their embedded schema. |
| `Dockerfile.debezium-connect` (new) | `FROM quay.io/debezium/connect:3.7.0.Final`, downloads `debezium-connector-jdbc-3.7.0.Final-plugin.tar.gz` from Maven Central, verifies its **sha512**, extracts it into `$KAFKA_CONNECT_PLUGINS_DIR`. |
| `docker-compose.yml` | New `kafka-connect` service (section 6). |
| `orchestration/assets/reverse_etl_risingwave_setup.py` | The RisingWave table now reads the single `_jdbc` topic. `reverse_etl_cdf_risingwave_table` creates it from the live Databricks columns with real types (`_create_table_sql()`), so no column is added after the table starts reading. `add_missing_columns()` (called by `reverse_etl_cdf_to_kafka` before it produces) adds later-appearing columns with their types. |
| `orchestration/assets/reverse_etl_debezium_sink.py` (new) | Dagster asset `reverse_etl_debezium_jdbc_sink` that registers the connector via the Connect REST API and waits until it is `RUNNING` (section 7). |
| `orchestration/definitions.py` | Imported the new asset, added it to `reverse_etl_poc_setup_job` and to the `Definitions` asset list, and updated the job description. |
| `orchestration/assets/reverse_etl_cdf_setup.py` (later changes) | Added `KEY_COLUMN = "rid"` (the identity column, section 4.6): the source table is created with `rid BIGINT GENERATED ALWAYS AS IDENTITY` and the sync's `key_columns` is `[KEY_COLUMN]`. Added `_raw_messages()` and the `kafka_messages_raw` output metadata (first three messages with their embedded schema). `reverse_etl_risingwave_setup.py` and `reverse_etl_debezium_sink.py` import `KEY_COLUMN` for the RisingWave primary key and `primary.key.fields`. |
| `orchestration/assets/reverse_etl_reset.py` (new) | Op job `reverse_etl_poc_reset_job` (section 14.3), registered in `definitions.py`. |
| `notebooks/reverse_etl_cdf_to_kafka.py` (new) | PySpark version of the sync for Databricks (section 10.3). |

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

Asset: `reverse_etl_debezium_jdbc_sink` (group `reverse_etl_poc`), dependencies
`kafka_output_topics_setup` and `reverse_etl_cdf_to_kafka`.

Behaviour:

1. Waits for the Connect REST API (`KAFKA_CONNECT_URL`, default `http://kafka-connect:8083`) for up to 120 s.
2. `PUT /connectors/reverse_etl_cdf_jdbc_sink/config` with the generated config. PUT is idempotent: it creates
   the connector or updates it. A non-2xx response raises with the response text (truncated).
3. Polls `/status` for up to 90 s until the connector **and** its task are `RUNNING`. If anything reports
   `FAILED`, the asset fails with the first 1500 characters of the trace.
4. Records connector name, topic, target table and states as run metadata.

### Connector configuration

| Key | Value | Notes |
|---|---|---|
| `connector.class` | `io.debezium.connector.jdbc.JdbcSinkConnector` | |
| `tasks.max` | `1` | |
| `topics` | `rw_poc_reverse_etl_cdf_out_jdbc` | |
| `connection.url` | `HOST_POSTGRES_URL`, default `jdbc:postgresql://host.docker.internal:5432/postgres` | Same default as the existing dbt Postgres sink. |
| `connection.username` | `POSTGRES_USER`, default `postgres` | |
| `connection.password` | `${env:POSTGRES_PASSWORD}` | Resolved inside the Connect container. |
| `insert.mode` | `upsert` | |
| `primary.key.mode` | `record_key` | Key struct supplies the primary key. |
| `primary.key.fields` | `rid` | The surrogate identity column (section 4.6), not the business `id`. |
| `delete.enabled` | `true` | `op = "d"` becomes a `DELETE`. |
| `schema.evolution` | `basic` | The sink creates the table (with the primary key) on first use and adds columns if the schema grows. No separate table-creation asset is needed. |
| `collection.name.format` | `reverse_etl_cdf_poc` | Target table name. Without it the table would be named after the topic. |
| `key.converter` / `value.converter` | `org.apache.kafka.connect.json.JsonConverter` | |
| `key.converter.schemas.enable` / `value.converter.schemas.enable` | `true` | Schema is read from each message. |
| `consumer.override.bootstrap.servers` | `KAFKA_OUTPUT_BOOTSTRAP` | Points the consumer at the **external** cluster. |
| `consumer.override.auto.offset.reset` | `earliest` | So messages produced before the connector was created are still consumed. |
| `consumer.override.security.protocol` | `SASL_SSL` | Only set if `KAFKA_OUTPUT_SASL_USERNAME` is set. |
| `consumer.override.sasl.mechanism` | `KAFKA_OUTPUT_SASL_MECHANISM`, default `SCRAM-SHA-512` | |
| `consumer.override.sasl.jaas.config` | `ScramLoginModule` (or `PlainLoginModule` when the mechanism is `PLAIN`) with `username="${env:KAFKA_OUTPUT_SASL_USERNAME}" password="${env:KAFKA_OUTPUT_SASL_PASSWORD}"` | The credentials are references, not values. |

The consumer group is `connect-reverse_etl_cdf_jdbc_sink`.

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
   `id bigint not null` (primary key at that time; the key is now `rid`, section 4.6), `value text`, `updated_at text`; three rows present. This also proved that
   `${env:POSTGRES_PASSWORD}` resolves when the variable is empty.
2. Produced an update (id 2 -> `two-v2`) and a delete (id 3). Result: id 2 updated, id 3 removed.
3. Cleaned up afterwards: connector deleted, test topic deleted, scratch Postgres removed, containers stopped.

### 9.4 Live run

Run from the script runner and Dagster: seed script, then the Dagster job. The Postgres query output shown in
section 1 is the result; it exercised the external SASL_SSL cluster, which the local tests could not.

---

## 10. How to run the demo

### One-time

1. Start the script runner from a **devbox shell** so the environment is loaded: `./bin/0_script_runner.sh`
   (port 4001). The shell must export `KAFKA_OUTPUT_SASL_USERNAME` and `KAFKA_OUTPUT_SASL_PASSWORD`, because
   compose passes them to the Connect container, and it starts host Postgres.
2. Script runner: **Start Services** (`1_up.sh`). Tick **Offline** if on VPN; this skips the rebuild and reuses
   the locally built images, including `risingwave-test-kafka-connect`. Wait about a minute for `kafka-connect`
   to be healthy (`docker compose ps kafka-connect`).
3. Script runner: **Seed Reverse-ETL POC** (`3_run_reverse_etl_seed.sh`). Run this **before** the job: it makes
   Databricks table changes, and the new `_jdbc` topic is empty until a sync sees changes.
4. Dagster (http://localhost:3000), Jobs, `reverse_etl_poc_setup_job`, Launch. This syncs the changes to both
   topics, creates the RisingWave table, and registers and starts the connector.

### Every time after

1. Change the Databricks table (insert / update / delete).
2. Materialize **only** `reverse_etl_cdf_to_kafka` in Dagster. The connector is already running and consumes
   the new messages continuously, so the whole job does not need to run again.

Nothing runs on a schedule for this pipeline (the existing schedules and sensor are for dbt and ML). The
Databricks -> Kafka step is manual.

### Starting a new demo from a known state

Run, in this order:

1. Dagster: `reverse_etl_poc_reset_job` (tears everything down, section 14.3).
2. Dagster: `reverse_etl_poc_setup_job` (recreates the Databricks tables with the original columns, the topic,
   the RisingWave table and the connector). Its first sync runs against the empty source table, finds no
   changes, and only records the current table version as the watermark.
3. Script runner: **Seed Reverse-ETL POC**.
4. Dagster: materialize `reverse_etl_cdf_to_kafka`.

The schema is then always `id`, `value`, `updated_at`, whatever columns earlier demos added. The Postgres
table `reverse_etl_cdf_poc` does **not** exist after steps 1 and 2: the sink creates it when it receives the
first message, so it appears after step 4. A database client that still lists it is showing a cached tree
(refresh it); querying it before step 4 fails with "relation does not exist", which is expected.

### Seeing the Kafka messages

In Dagster, open the latest materialization of `reverse_etl_cdf_to_kafka` (Assets, Events tab, or the run's
log). Its metadata has `kafka_messages` (payloads of up to 50 messages) and `kafka_messages_raw` (the first
three messages exactly as sent, with the embedded `schema` listing the table's columns and types, and the
`payload`). `kafka_messages_raw` is empty on a run with no changes.

### Checking results

```bash
# Connector and task state
curl -s localhost:8083/connectors/reverse_etl_cdf_jdbc_sink/status

# Rows in the target
psql -h localhost -U postgres -d postgres -c "select * from reverse_etl_cdf_poc order by id"
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
ALTER TABLE de_dev.sr_poc_external.reverse_etl_cdf_poc_source ADD COLUMN region STRING;

-- 2. Databricks: write a row that uses it (explicit column list)
INSERT INTO de_dev.sr_poc_external.reverse_etl_cdf_poc_source (id, value, updated_at, region)
VALUES (10, 'evolved', current_timestamp(), 'eu');
```

3. Dagster: materialize `reverse_etl_cdf_to_kafka`.
4. Postgres:

```bash
psql -h localhost -U postgres -d postgres -c '\d reverse_etl_cdf_poc' \
     -c 'select * from reverse_etl_cdf_poc order by id'
```

Expected: a new `region text` column; rows that existed before show `NULL` in it; the new row shows `eu`.
Other column types work the same way: a `DOUBLE`, `INT` or `BOOLEAN` column becomes `double precision`,
`integer` or `boolean`, in both Postgres and RisingWave.

**Cautions**
- The `ALTER TABLE` changes your sandbox table permanently. Dropping or renaming a column later is a
  *non-additive* change, which Databricks documents as able to break batch Change Data Feed reads across that
  version range, so treat the new column as permanent.
- `scripts/reverse_etl_poc_seed.py` uses explicit column lists, so it keeps working after the `ALTER`.
- **The RisingWave table picks the column up too, with the matching type.** Before producing events,
  `reverse_etl_cdf_to_kafka` calls `add_missing_columns()` (in `reverse_etl_risingwave_setup.py`), which compares
  the live Databricks columns with `reverse_etl_cdf_poc_current` and runs `ALTER TABLE ... ADD COLUMN <name>
  <type>` for each one the table lacks (type mapping in section 4.3). It does nothing if the table doesn't exist
  yet. Check with `psql -h localhost -p 4566 -U root -d dev -c "describe reverse_etl_cdf_poc_current"`; the run's
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
    `reverse_etl_cdf_poc_current` dropped (nothing depended on it) and recreated by materializing
    `reverse_etl_cdf_risingwave_table` (run succeeded). The new table reads `rw_poc_reverse_etl_cdf_out_jdbc`, was
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
UPDATE de_dev.sr_poc_external.reverse_etl_cdf_poc_source
SET region = region, updated_at = current_timestamp()
WHERE id IN (10, 11);
```

`updated_at` is changed on purpose: an update that changes nothing may not produce a change event. The same
situation would arise if a recreated table were created with fewer columns than the topic carries: with
`scan.startup.mode = 'earliest'` it starts re-reading the topic immediately, so a column added a moment later
misses the messages already read. The asset therefore creates the table with all current columns up front
(section 10.2), which avoids this.

### 10.2 Migrating an existing RisingWave table to the single topic

A RisingWave table's Kafka topic is fixed when the table is created, so a `reverse_etl_cdf_poc_current` created
before the consolidation still reads the retired schemaless topic and will no longer receive new events. To
move it:

1. Reload Dagster's definitions (or restart the Dagster containers) so the new asset code is loaded.
2. Drop the table: `psql -h localhost -p 4566 -U root -d dev -c "DROP TABLE reverse_etl_cdf_poc_current"`.
3. Materialize `reverse_etl_cdf_risingwave_table` (or run `reverse_etl_poc_setup_job`). It recreates the table
   from the live Databricks columns, with real types, reading the `_jdbc` topic from the beginning.

Notes:
- The Debezium connector and the Postgres table are unaffected: they already read the `_jdbc` topic.
- RisingWave rebuilds its state from that topic's contents, which is also exactly where Postgres' contents came
  from, so the two stay consistent. Anything that only ever existed on the retired topic does not reappear.
- The retired topic `rw_poc_reverse_etl_cdf_out` is left in place on the staging cluster, unused; it is no longer
  produced to or created by `kafka_output_topics_setup`.
- Create the table with all columns up front (the asset now does) rather than adding columns after it starts
  reading, for the reason given in section 10.1.

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
  adding a Databricks column, run `ALTER TABLE reverse_etl_cdf_poc_current ADD COLUMN <name> <type>` in
  RisingWave *before* the notebook, or RisingWave leaves the column `NULL` for the new rows.
- The source and state tables must exist (`reverse_etl_poc_table_setup`); the notebook does not create them.
- A failed Kafka write raises before the watermark moves, so the next run re-sends (the sink and RisingWave
  upsert by key, so replays are harmless). The Dagster asset advances the watermark even if a delivery failed.
- **Do not run both for the same change:** they share one watermark (`sync_name`) and one topic.

---

## 11. Operating notes

- **Which asset to run when.** After any change to the Databricks table, materialize **only**
  `reverse_etl_cdf_to_kafka`: it reads the change feed since its watermark, adds any new column to the
  RisingWave table, and produces the events; the connector and the RisingWave table pick them up within
  seconds. Nothing is scheduled, so the sync only runs when triggered. The other two assets in
  `reverse_etl_poc_setup_job` are one-time setup:
  - `reverse_etl_cdf_risingwave_table` creates the RisingWave table that reads the topic. Run it again only
    after dropping that table or changing its topic.
  - `reverse_etl_debezium_jdbc_sink` registers the connector with Kafka Connect, which then runs it on its own.
    Run it again only if Connect loses its state (for example its local Redpanda volume is wiped) or the
    connector configuration changes.
  Neither moves data; both sit after the sync in the job only for ordering.
- **Where the sync runs.** `reverse_etl_cdf_to_kafka` runs as Python in the Dagster container. Databricks is used
  only through the Statement Execution API (SQL, authenticated with the service principal) to read the change
  feed, the watermark table and `information_schema`; the grouping into events, the RisingWave column sync and
  the Kafka produce all happen in the container, which therefore needs a network path to the Kafka cluster (VPN
  today). A PySpark notebook that does the same
  work exists and was verified live (section 10.3); it is an alternative trigger, not a replacement, and
  the Dagster asset remains the default.
- **Which Kafka.** The data topic lives on Kaizen's staging Kafka cluster (SASL_SSL, SCRAM-SHA-512), named in
  `REVERSE_ETL_CDF_POC_PLAN.md` and read from `KAFKA_OUTPUT_BOOTSTRAP` at run time. The local Redpanda only holds
  Kafka Connect's own bookkeeping topics; no pipeline data passes through it.
- **Updating the Debezium version.** Change `DEBEZIUM_VERSION` in `Dockerfile.debezium-connect` and update
  `JDBC_PLUGIN_SHA512` to the value from
  `https://repo1.maven.org/maven2/io/debezium/debezium-connector-jdbc/<version>/debezium-connector-jdbc-<version>-plugin.tar.gz.sha512`.
  Keep the base image and plugin versions aligned.
- **Re-running the asset.** `reverse_etl_debezium_jdbc_sink` is idempotent; running it again updates the
  connector config in place.
- **Delivery semantics.** At-least-once from Kafka; upserts and deletes by primary key are idempotent, so a
  redelivered message gives the same end state.
- **Replaying from the start.** Stop the connector, reset its offsets with the Kafka Connect offsets API (or
  delete the connector and the `connect-reverse_etl_cdf_jdbc_sink` consumer group), then restart. Because
  `auto.offset.reset=earliest`, a new consumer group re-reads the whole topic.
- **Removing the sink.**
  `curl -X DELETE localhost:8083/connectors/reverse_etl_cdf_jdbc_sink`, then
  `drop table reverse_etl_cdf_poc;` in host Postgres if the data should go too.

---

## 12. Troubleshooting

| Symptom | Likely cause and fix |
|---|---|
| Postgres table missing or empty after the job | The topic was empty: the CDF watermark had already advanced and there were no new table changes. Seed or change the Databricks table and re-run `reverse_etl_cdf_to_kafka`. Check the asset logged `Produced N event(s)` rather than `No new commits`. |
| Asset `reverse_etl_debezium_jdbc_sink` fails with "Kafka Connect not reachable" | `kafka-connect` is not up or not healthy yet. `docker compose ps kafka-connect`, `docker logs kafka-connect`. |
| Connector `FAILED` with an authentication / SASL error | `KAFKA_OUTPUT_SASL_USERNAME` / `KAFKA_OUTPUT_SASL_PASSWORD` were not exported in the shell that ran compose, or the mechanism is wrong (`KAFKA_OUTPUT_SASL_MECHANISM`). Fix the environment, recreate the container (`docker compose up -d kafka-connect`), re-run the asset. |
| Connector config rejected with a config-provider / "not allowed" error | The variable is not in the allowlist pattern. Only `KAFKA_OUTPUT_SASL_USERNAME`, `KAFKA_OUTPUT_SASL_PASSWORD` and `POSTGRES_PASSWORD` can be referenced. |
| Connector `FAILED` connecting to Postgres | Host Postgres is not running (start it via `devbox shell`), or `HOST_POSTGRES_URL` / `POSTGRES_USER` / `POSTGRES_PASSWORD` are wrong. `pg_isready -h localhost -p 5432`. |
| Messages rejected: "schema" / not a Struct errors | The message was not produced in the schema-embedded format (for example someone produced to the `_jdbc` topic by hand with plain JSON). Only `_build_connect_json_message()` output is valid. |
| RisingWave table `reverse_etl_cdf_poc_current` stops updating, or is missing new columns | It was created before the single-topic change and still reads the retired schemaless topic. Migrate it (section 10.2). |
| Deletes not applied | `delete.enabled` must be `true` and `primary.key.mode` must be `record_key`, and the message must have `op = "d"` with the row in `before`. |
| Port 8083 already in use | Another process is bound to 8083; stop it or change the host port mapping in compose. |

---

## 13. Known limitations and open items

- **Delivery errors are not inspected.** `_produce_to_kafka()` produces each event and flushes once; a failed
  delivery is not surfaced, and the watermark is still advanced afterwards (pre-existing behaviour).
- **Schema evolution is additive only.** Adding a column works end to end (section 10.1). Renames, drops and type
  changes are not handled: Databricks documents that batch Change Data Feed reads can fail across non-additive
  schema changes, and both the sink's `schema.evolution=basic` and `add_missing_columns()` only add columns. A
  column dropped in Databricks stays in Postgres and RisingWave. What a drop would do was reasoned through, not
  tested: Databricks only allows `DROP COLUMN` on a table with column mapping enabled (a one-way table protocol
  upgrade); if the change-feed read still works, messages simply stop carrying the column, so Postgres would
  probably keep stale values in rows updated afterwards (its upsert writes only the fields present) while
  RisingWave would probably set them to `NULL` (an upsert replaces the row), leaving the two targets disagreeing;
  if the read fails, every sync fails until the watermark is reset by hand. Treat added columns as permanent and
  use a new column name for repeated demos.
- **Type coverage.** Only the types in the mapping table (section 4.3) become typed Postgres columns; anything
  else arrives as `text`. `TINYINT`, `SMALLINT` and `FLOAT` mappings were not exercised end to end.
- **Key column is fixed.** `KEY_COLUMN = "rid"` (in `reverse_etl_cdf_setup.py`) is the key for the Kafka message,
  the RisingWave primary key and the connector's `primary.key.fields`; the notebook repeats it as `KEY_COLUMNS`.
  A different key means changing the constant, the notebook and recreating the source table.
- **`rid` must exist from table creation.** An identity column cannot be added to an existing table, so this
  needs the reset job, then the setup job. Inserts into the source table must list their columns
  (`INSERT INTO t (id, value, ...)`); a column-less `INSERT ... VALUES` does not work, and Databricks restricts
  concurrent writers on tables with identity columns.
- **`updated_at` is text** in Postgres, not `timestamp`/`timestamptz`.
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

`reverse_etl_poc_reset_job` (`orchestration/assets/reverse_etl_reset.py`) is a Dagster op job, not assets,
because it destroys state instead of materializing it. It runs these steps in order:

1. Delete the connector from Kafka Connect.
2. Drop the RisingWave table `reverse_etl_cdf_poc_current`.
3. Drop the Postgres table `reverse_etl_cdf_poc`.
4. Delete the consumer group `connect-reverse_etl_cdf_jdbc_sink` (retried while the connector's consumer
   leaves it) and the topic `rw_poc_reverse_etl_cdf_out_jdbc`, then wait until the topic is gone from the
   metadata, since topic deletes are asynchronous.
5. Drop the Databricks source and watermark tables (`reverse_etl_cdf_poc_source`, `reverse_etl_cdf_poc_state`).

Dropping the Databricks tables, rather than only resetting the watermark, is what removes columns added during
a demo: `reverse_etl_poc_table_setup` recreates them with `CREATE TABLE IF NOT EXISTS` and the original
columns. Every step tolerates a missing object, so a half-finished reset can be run again. A guard refuses to
run unless every name it drops starts with a POC prefix and the schema is `sr_poc_external`.

Verified live on 2026-10-03: reset, then setup (the first sync on the empty table succeeded), then seed and a
sync produced matching rows in Postgres and RisingWave. After the reset, the connector list, topic, consumer
group and both target tables were all absent, as expected.

---

## 15. References

- Debezium JDBC sink connector documentation (`debezium.io`, JDBC connector reference).
- Debezium source: `debezium-sink` module, `KafkaDebeziumSinkRecord` (envelope detection, delete handling).
- Maven Central: `io.debezium:debezium-connector-jdbc` (plugin archive and published checksums).
- Redpanda docs: "Deploy Kafka Connect in Docker" / "Deploy Redpanda Connectors in Docker".
- In this repo: `docs/poc/REVERSE_ETL_CDF_POC_PLAN.md`, `dbt/models/sink_funnel_to_postgres.sql`,
  `orchestration/assets/postgres_sink_setup.py`.
