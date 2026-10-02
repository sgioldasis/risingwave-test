# Reverse-ETL CDF POC: Debezium JDBC sink into Postgres

Date: 2026-10-02
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
   |-- rw_poc_reverse_etl_cdf_out        schemaless Debezium JSON      -> RisingWave table (FORMAT DEBEZIUM)
   '-- rw_poc_reverse_etl_cdf_out_jdbc   JSON + embedded Connect schema -> Debezium JDBC sink   (NEW)
                                                        |
                                                        |  kafka-connect container (NEW)
                                                        |  consumer.override.* -> external SASL_SSL cluster
                                                        |  worker internal topics -> LOCAL Redpanda
                                                        v
                                   Host PostgreSQL (host.docker.internal:5432, db "postgres")
                                   table: public.reverse_etl_cdf_poc   (PK: id)
```

Key properties:

- **Batch half, streaming half.** Databricks -> Kafka only moves when the Dagster asset runs. Kafka ->
  Postgres is continuous: the connector is always running and applies messages within seconds of arrival.
- **Two topics, same events.** The original topic is unchanged so the already-verified RisingWave path is not
  touched. The new `_jdbc` topic carries the same events with an embedded schema (section 4).
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
| Message format | **JSON with embedded schema** (`{"schema": ..., "payload": ...}`) | The sink requires schema information on every record. The existing topic is a hand-rolled, schemaless envelope, which the sink would reject. Avro would need a schema registry reachable from the external cluster; embedded JSON needs nothing extra. |
| Change the existing topic or add one? | **Add a second topic** | The existing topic feeds a verified RisingWave `FORMAT DEBEZIUM` table. Re-encoding it risked breaking that parser, so the same events are published to a new topic in the new encoding. |
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
| `id` | `int64` | no | `bigint` (primary key) |
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
    "fields": [{"field": "id", "type": "int64", "optional": false}]
  },
  "payload": {"id": 1}
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
      {"field": "before", "type": "struct", "optional": true, "name": "...Value", "fields": ["id", "value", "updated_at"]},
      {"field": "after",  "type": "struct", "optional": true, "name": "...Value", "fields": ["id", "value", "updated_at"]},
      {"field": "op",     "type": "string", "optional": false},
      {"field": "source", "type": "struct", "optional": true,
       "fields": [{"field": "commit_version", "type": "int64"}, {"field": "commit_timestamp", "type": "string"}]}
    ]
  },
  "payload": {
    "before": null,
    "after": {"id": 1, "value": "first", "updated_at": "2026-10-02T04:18:23.009Z"},
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
values map to Debezium's `c`/`u`/`d`. `id` is cast to an integer because the Databricks Statement Execution
API returns numerics as JSON strings. See `REVERSE_ETL_CDF_POC_PLAN.md` section "3c" for the full reasoning.

---

## 5. What was changed in the repo

Nothing was committed as part of this work; all changes are in the working tree on `feature-sr`.

| File | Change |
|---|---|
| `orchestration/assets/kafka_topics_setup.py` | Added `rw_poc_reverse_etl_cdf_out_jdbc` to `OUTPUT_TOPICS`, so the existing `kafka_output_topics_setup` asset creates it (15 partitions, replication 1, like the others). |
| `orchestration/assets/reverse_etl_cdf_setup.py` | Added `KAFKA_JDBC_TOPIC`, `ENVELOPE_SCHEMA_NAME`, `_get_row_fields()` (live column list from `information_schema`), `_coerce()` / `_coerce_row()`, `_connect_row_schema()` and `_build_connect_json_message()`. `_produce_to_kafka()` now also produces each event, in the new encoding, to `KAFKA_JDBC_TOPIC`. The existing topic's message format is untouched. |
| `Dockerfile.debezium-connect` (new) | `FROM quay.io/debezium/connect:3.7.0.Final`, downloads `debezium-connector-jdbc-3.7.0.Final-plugin.tar.gz` from Maven Central, verifies its **sha512**, extracts it into `$KAFKA_CONNECT_PLUGINS_DIR`. |
| `docker-compose.yml` | New `kafka-connect` service (section 6). |
| `orchestration/assets/reverse_etl_risingwave_setup.py` | Added `add_missing_columns()`, called by `reverse_etl_cdf_to_kafka` before it produces, so new Databricks columns are added to the RisingWave table as `VARCHAR`. The table and its asset are otherwise unchanged. |
| `orchestration/assets/reverse_etl_debezium_sink.py` (new) | Dagster asset `reverse_etl_debezium_jdbc_sink` that registers the connector via the Connect REST API and waits until it is `RUNNING` (section 7). |
| `orchestration/definitions.py` | Imported the new asset, added it to `reverse_etl_poc_setup_job` and to the `Definitions` asset list, and updated the job description. |

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
| `primary.key.fields` | `id` | |
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
   `id bigint not null` (primary key), `value text`, `updated_at text`; three rows present. This also proved that
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
`integer` or `boolean`.

**Cautions**
- The `ALTER TABLE` changes your sandbox table permanently. Dropping or renaming a column later is a
  *non-additive* change, which Databricks documents as able to break batch Change Data Feed reads across that
  version range, so treat the new column as permanent.
- `scripts/reverse_etl_poc_seed.py` uses explicit column lists, so it keeps working after the `ALTER`.
- **The RisingWave table picks the column up too, as `VARCHAR`.** Before producing events,
  `reverse_etl_cdf_to_kafka` calls `add_missing_columns()` (in `reverse_etl_risingwave_setup.py`), which compares
  the live Databricks columns with `reverse_etl_cdf_poc_current` and runs `ALTER TABLE ... ADD COLUMN ... VARCHAR`
  for each one the table lacks. It does nothing if the table doesn't exist yet. Check with
  `psql -h localhost -p 4566 -U root -d dev -c "describe reverse_etl_cdf_poc_current"`; the run's Dagster
  metadata also lists `risingwave_columns_added`.
  - **Always `VARCHAR`, whatever the Databricks type.** The original topic's values arrive as JSON strings (only
    `id` is cast to a number), and RisingWave's Debezium JSON parser drops a message whose string value it can't
    coerce into a numeric column. So a `DOUBLE` column is `double precision` in Postgres but `VARCHAR` here.
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
situation can arise if the RisingWave table is dropped and recreated: the asset creates it with its three base
columns and `scan.startup.mode = 'earliest'` starts re-reading the topic immediately, so a column added a moment
later misses the messages already read. Prefer the in-place `ALTER` path over recreating the table.

---

## 11. Operating notes

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
| Deletes not applied | `delete.enabled` must be `true` and `primary.key.mode` must be `record_key`, and the message must have `op = "d"` with the row in `before`. |
| Port 8083 already in use | Another process is bound to 8083; stop it or change the host port mapping in compose. |

---

## 13. Known limitations and open items

- **Not atomic across topics.** `_produce_to_kafka()` writes each event to both topics and then flushes once.
  A failure in the middle can leave the two topics with different contents. Delivery errors from `flush()` are
  not inspected (this matches the pre-existing behaviour of the original topic).
- **Schema evolution is additive only.** Adding a column works end to end (section 10.1). Renames, drops and type
  changes are not handled: Databricks documents that batch Change Data Feed reads can fail across non-additive
  schema changes, and both the sink's `schema.evolution=basic` and `add_missing_columns()` only add columns. A
  column dropped in Databricks stays in Postgres and RisingWave.
- **Type coverage.** Only the types in the mapping table (section 4.3) become typed Postgres columns; anything
  else arrives as `text`. `TINYINT`, `SMALLINT` and `FLOAT` mappings were not exercised end to end.
- **Key column is hardcoded.** `key_columns = ["id"]` is still fixed in `reverse_etl_cdf_to_kafka`, and the
  integer cast of `id` in the original topic's rows (`NUMERIC_ROW_COLUMNS`) is unchanged.
- **`updated_at` is text** in Postgres, not `timestamp`/`timestamptz`.
- **Schema repeated in every message.** JSON with embedded schema is larger than Avro with a registry; fine for
  a POC, worth revisiting at volume.
- **Single task.** `tasks.max=1`; throughput scaling would need more tasks, and the topic has 15 partitions so
  there is room.
- **Manual trigger.** No schedule or sensor drives the Databricks -> Kafka step.
- **Table created by the sink.** `schema.evolution=basic` auto-creates the table; there is no explicit DDL or
  migration for it.
- **Internal topics on local Redpanda.** The Connect worker depends on the local Redpanda; if its volume is
  wiped, the connector registration is lost and the asset must be re-run (the consumer group offsets on the
  external cluster remain, so a re-registered connector resumes where it left off).
- **Not committed.** All changes are uncommitted on `feature-sr`.

---

## 14. References

- Debezium JDBC sink connector documentation (`debezium.io`, JDBC connector reference).
- Debezium source: `debezium-sink` module, `KafkaDebeziumSinkRecord` (envelope detection, delete handling).
- Maven Central: `io.debezium:debezium-connector-jdbc` (plugin archive and published checksums).
- Redpanda docs: "Deploy Kafka Connect in Docker" / "Deploy Redpanda Connectors in Docker".
- In this repo: `docs/poc/REVERSE_ETL_CDF_POC_PLAN.md`, `dbt/models/sink_funnel_to_postgres.sql`,
  `orchestration/assets/postgres_sink_setup.py`.
