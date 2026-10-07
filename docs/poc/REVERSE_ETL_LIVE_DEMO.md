# Reverse-ETL live demo: Databricks -> Kafka -> Postgres and RisingWave

Date: 2026-10-05. Branch: `feature-sr`.

A script for demonstrating the reverse-ETL pipeline live, in about 10 minutes, driven from the Dagster UI.
Changes made to a Delta table in Databricks (inserts, updates, deletes, a new column) arrive in a Postgres table
(through the Debezium JDBC sink) and in a RisingWave table, with the messages in Kafka in between.

The steps use the **`avro` sync**, which sends Avro messages with the schemas in a registry. To demo the **JSON**
version instead, use the `cdf` label everywhere `avro` appears (asset `reverse_etl_cdf_to_kafka`, tables
`reverse_etl_cdf_target`, jobs `reverse_etl_cdf_*`) and skip the registry steps. Background and test results:
[`REVERSE_ETL_DEBEZIUM_JDBC_SINK.md`](REVERSE_ETL_DEBEZIUM_JDBC_SINK.md) (sections 10, 14.1, 14.1.1).

Each step was run separately while building the sync; this script was not rehearsed as one continuous pass, so do
a dry run first.

## What the audience sees

```
Databricks Delta table (Change Data Feed)
   -> Dagster asset or Databricks notebook: reads the changes since the last watermark
   -> Kafka topic reverse_etl_avro_topic (Debezium-style before/after/op messages, Avro)
        -> Debezium JDBC sink (Kafka Connect) -> Postgres table reverse_etl_avro_target
        -> RisingWave table reverse_etl_avro_target (FORMAT DEBEZIUM ENCODE AVRO)
   schemas: shared staging Apicurio registry (reverse_etl_avro_topic-key / -value)
```

## Before you start

- The local stack is up (`./bin/1_up.sh`) and `kafka-connect` was built with the Avro converter
  (`docker compose build kafka-connect`; this needs the VPN off, then `docker compose up -d kafka-connect`).
- Credentials for Databricks and the Kafka cluster are in `.env`, as for the other syncs.
- Dagster is open at http://localhost:3000.
- Two shell shortcuts:
  ```
  alias pg='psql -h localhost -U postgres -d postgres'
  alias rw='psql -h localhost -p 4566 -U root -d dev'
  ```
- A Databricks SQL editor open on the DEV workspace, for the table changes.
- Optional: a DB client (for example DBeaver) on both databases. If it shows `updated_at_ts` with only three
  fractional digits, that is its display format; see "Showing microseconds" below.

## 1. Start clean (skip the first time)

Run **`reverse_etl_avro_reset_job`**. It drops the connector, both target tables, the topic, the Databricks
source and watermark tables, and the two registry subjects. It only touches objects named `reverse_etl_*` in the
scratch schema `sr_poc_external`.

## 2. Setup

Run **`reverse_etl_avro_setup_job`**. It creates the Databricks source and watermark tables (Change Data Feed on,
with an identity `rid` key), the topic, the RisingWave table and the Debezium sink, and last seeds three rows into
Databricks (an insert, an update and a delete).

Show:
- The registry has the schema: `curl -s http://staging-schema-registry.kaizengaming.net/apis/ccompat/v7/subjects/reverse_etl_avro_topic-value/versions`
  returns `[1]`.
- Both targets are empty: `select * from reverse_etl_avro_target;` in `pg` and in `rw`. The rows are waiting in
  Databricks; nothing has been synced yet.

## 3. First sync

Materialize the asset **`reverse_etl_avro_to_kafka`**.

Show:
- `select * from reverse_etl_avro_target order by rid;` in `pg` and in `rw`: two rows, the updated one and the
  inserted one. The deleted row never appears.
- In RisingWave, `updated_at_ts` is a real `timestamptz` with microseconds (generated from the Avro string).
- In the asset's run metadata, `kafka_messages_raw` shows the messages as hex: a zero byte, the 4-byte schema id,
  then the Avro payload. `kafka_messages` shows the same events as readable payloads.

## 4. Change data

In the Databricks SQL editor:

```sql
UPDATE de_dev.sr_poc_external.reverse_etl_avro_source
SET value = 'changed', updated_at = current_timestamp() WHERE id = 1;
```

Run the sync asset again and query both targets: the update is applied in both. A delete works the same way
(`DELETE FROM ... WHERE id = 1`): the row disappears from both.

## 5. Add a column

```sql
ALTER TABLE de_dev.sr_poc_external.reverse_etl_avro_source ADD COLUMN country STRING;
UPDATE de_dev.sr_poc_external.reverse_etl_avro_source
SET country = 'GR', updated_at = current_timestamp() WHERE id = 2;
```

Run the sync asset. The registry versions become `[1,2]`, and `country` appears in Postgres and in RisingWave
with its value. Worth saying: the sink adds the column by itself; RisingWave only accepts a new column once the
registry schema has it, so the order is registry, then RisingWave, then messages, and the asset does that for you.
Dropping a column or changing a type is not propagated (see section 10.4 of the sink doc).

## 6. Run the sync from Databricks (optional)

The same sync as a Databricks notebook, triggered from Dagster. Make one more change in the source table, then run
the Dagster job **`reverse_etl_notebook_sync_job`** with this config:

```yaml
ops:
  reverse_etl_trigger_notebook_sync:
    config:
      label: avro
```

It triggers the DEV job `reverse_etl_notebook_sync` with `label=avro` and `encoding=avro`. The change lands in both
targets. The registry stays at its current version, because the notebook builds the identical schema.
(The job only exists in the DEV workspace; STG has the notebook but no job yet.)

## 7. Clean up

Run **`reverse_etl_avro_reset_job`** again. It also removes the two registry subjects, so nothing is left in the
shared staging registry.

## Talking points

- **Why Kafka in the middle:** one stream feeds several consumers (here Postgres and RisingWave), and each keeps
  its own place in the topic.
- **Deletes and updates are real:** the messages carry `op` (`c`, `u`, `d`) with `before` and `after`, so both
  targets apply upserts and deletes rather than appending history.
- **Size (Avro vs JSON):** for one four-column update event, JSON is 1,726 bytes and Avro is 137, about 12.6 times
  smaller. This is computed offline from one event, not measured on the topic; say so if asked.
- **What Avro costs:** a registry dependency (the shared, anonymous staging Apicurio), harder debugging, and a
  timestamp workaround in RisingWave (a `VARCHAR` column plus a generated `timestamptz` one). The recommendation
  in the docs is still that `cdf` and `orders` stay on JSON; `avro` is a trial.
- **Not shown or tested:** throughput at volume, a backfill, billions of rows, column drops with Avro, and the STG
  workspace.

## Showing microseconds

A DB client may display `updated_at_ts` as `2026-10-05 10:09:16.675 +0300`. The stored value has microseconds (the
`updated_at` string next to it shows `...16.675967Z`); the client is formatting to milliseconds.
- Quick check: `select updated_at_ts::text from reverse_etl_avro_target;`.
- DBeaver: Settings (`⌘,` on a Mac) -> Editors -> Data Editor -> Data Formats, then tick "Use native date/time
  format" or set the Timestamp pattern to `yyyy-MM-dd HH:mm:ss.ffffff`; re-run the query.
- `psql` shows the full value. The `+0300` is just the session time zone.

## If something goes wrong

- **Is the sink alive?** Each sink asset has a `sink_healthy` check (Checks tab of `reverse_etl_avro_jdbc_sink`),
  run every 5 minutes; it fails if the connector or a task is not RUNNING. Good to show at the end of the demo.
- **Sink connector FAILED:** `docker exec kafka-connect curl -s localhost:8083/connectors/reverse_etl_avro_sink/status`
  shows the cause. Two failures were seen: a schema registered in the wrong string form (`The given schema does
  not match any schema under the subject ...`), fixed in the code, where a reset plus setup recovers; and a
  transient DNS error (`No resolvable bootstrap urls`), fixed by `POST /connectors/<name>/tasks/0/restart` on Connect.
- **A Dagster run fails at the Databricks step:** check the VPN and the credentials in `.env`.
- **A column did not arrive in RisingWave:** it only fills a column from messages read after the column exists;
  run the sync asset again after the column is added, and see section 10.1 of the sink doc.
