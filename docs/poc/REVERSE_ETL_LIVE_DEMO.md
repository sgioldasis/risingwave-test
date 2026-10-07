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

## The components

Names are for the `avro` sync. Every name is `reverse_etl_<label>_<role>`, so for `cdf` or `orders` replace `avro`.

### Diagram

```
 DATABRICKS (DEV workspace)
 +-----------------------------------------------------------------+
 | reverse_etl_avro_source  Delta table, Change Data Feed, key rid |
 | reverse_etl_avro_state   watermark: last table version synced   |
 +--------------------------------+--------------------------------+
                                 | changes since the watermark
                                 v
 +-----------------------------------------------------------------+
 | THE SYNC, run by either                                         |
 |   - Dagster asset    reverse_etl_avro_to_kafka                  |
 |   - Databricks job   reverse_etl_notebook_sync (a notebook)     |
 | builds before/after/op events, produces Avro messages, then     |
 | moves the watermark                                             |
 +-------------+-----------------------------------+---------------+
               | messages                          | registers schemas
               v                                   v
 +------------------------------+     +-------------------------------+
 | STAGING KAFKA                |     | APICURIO REGISTRY (staging)   |
 | topic reverse_etl_avro_topic |     | reverse_etl_avro_topic-key    |
 | (15 partitions)              |     | reverse_etl_avro_topic-value  |
 +---------+-------------+------+     +-------------------------------+
           | reads       | reads         (both readers look up each
           v             v                message's schema here, by id)
 +-----------------+  +--------------------------------+
 | KAFKA CONNECT   |  | RISINGWAVE (local)             |
 | connector       |  | table reverse_etl_avro_target  |
 | reverse_etl_    |  | FORMAT DEBEZIUM ENCODE AVRO    |
 |   avro_sink     |  +--------------------------------+
 +--------+--------+
          | upserts / deletes
          v
 +--------------------------------+
 | POSTGRES (host)                |
 | table reverse_etl_avro_target  |
 +--------------------------------+
```

The two readers of the topic are independent: each keeps its own place in it, so one can fall behind or stop
without the other noticing (the demo in section 7 uses this).

### Databricks (DEV workspace, `de_dev.sr_poc_external`)

| Object | What it is |
|---|---|
| `reverse_etl_avro_source` | The Delta table you change in the demo. Change Data Feed is on, and `rid` is an identity column Databricks assigns on insert; it is the key everywhere downstream. |
| `reverse_etl_avro_state` | The watermark table: one row per sync with the last table version already synced. The next run reads from the version after it. |
| Notebook `reverse_etl_cdf_to_kafka` | The sync as a PySpark notebook, one notebook for every label (the `label` and `encoding` widgets choose the sync and the format). |
| Databricks job `reverse_etl_notebook_sync` | Runs that notebook, with `label` and `encoding` as job parameters. It runs on the author's cluster and exists only in the DEV workspace. |

### Dagster (http://localhost:3000)

| Object | Kind | What it does |
|---|---|---|
| `reverse_etl_avro_table_setup` | asset | Creates the source and watermark tables if they do not exist. |
| `reverse_etl_avro_topic_setup` | asset | Creates the Kafka topic if it does not exist. |
| `reverse_etl_avro_to_kafka` | asset | **The sync.** Reads the change feed since the watermark, makes the Debezium-style events, registers the schemas, produces Avro messages, moves the watermark, and adds new columns to the RisingWave table first. Run this after every change. |
| `reverse_etl_avro_risingwave_target` | asset | Creates the RisingWave table that reads the topic. |
| `reverse_etl_avro_jdbc_sink` | asset | Registers the Kafka Connect connector and waits until it is RUNNING. |
| `reverse_etl_avro_seed` | asset | Writes the three demo rows (insert, update, delete) to Databricks, as the last setup step. |
| `sink_healthy` | asset check, on `reverse_etl_avro_jdbc_sink` | Reads the connector and task states from Connect and the sink's lag from Kafka (section 7). |
| `reverse_etl_avro_setup_job` | job | Runs the assets above in order. Section 2. |
| `reverse_etl_avro_reset_job` | job | Removes the connector, both target tables, the topic, the two registry subjects and the Databricks tables. Section 1 and 8. |
| `reverse_etl_avro_sink_health_job` | job | Runs only the `sink_healthy` check. |
| `reverse_etl_avro_sink_health_schedule` | schedule | Runs the health job every 5 minutes. |
| `reverse_etl_notebook_sync_job` | job (shared by all syncs) | Adds any missing RisingWave columns, then triggers the Databricks job for the label in its run config. Section 6. |

### Kafka, registry and Connect

| Object | What it is |
|---|---|
| Topic `reverse_etl_avro_topic` | On Kaizen's staging Kafka cluster. Each message is one change: the key is `{rid}`, the value has `before`, `after`, `op` (`c`, `u` or `d`) and `source`, in Avro. |
| Apicurio registry | The shared staging registry (anonymous read and write, used by other teams). Holds the schemas `reverse_etl_avro_topic-key` and `reverse_etl_avro_topic-value`, through its Confluent-compatible API (`/apis/ccompat/v7`), in the default group. Messages carry only a schema id; readers look the schema up. |
| Container `kafka-connect` | Kafka Connect with the Debezium JDBC sink plugin and the Confluent Avro converter, running locally. Its own bookkeeping topics are on the local Redpanda. |
| Connector `reverse_etl_avro_sink` | Reads the topic (consumer group `connect-reverse_etl_avro_sink`) and upserts or deletes rows in Postgres, one task. Created by the `reverse_etl_avro_jdbc_sink` asset. |

### Targets (local)

| Object | What it is |
|---|---|
| Postgres, table `reverse_etl_avro_target` | On the host Postgres (database `postgres`). Created by the sink on the first message; the sink also adds new columns. |
| RisingWave, table `reverse_etl_avro_target` | Reads the topic itself, with `FORMAT DEBEZIUM ENCODE AVRO`. The timestamp arrives as text in `updated_at`, and a generated `updated_at_ts` column is the real `timestamptz`. New columns are added by the sync asset before the messages that carry them. |

### Where the code is

| Piece | File |
|---|---|
| The sync's definition (name, columns, seed, `encoding: avro`) | `orchestration/defs/reverse_etl_avro/defs.yaml` |
| The sync asset, schema building, producer | `orchestration/assets/reverse_etl_cdf_setup.py` |
| Connector registration | `orchestration/assets/reverse_etl_debezium_sink.py` |
| RisingWave table and column handling | `orchestration/assets/reverse_etl_risingwave_setup.py` |
| Reset job | `orchestration/assets/reverse_etl_reset.py` |
| Health check, job and schedule | `orchestration/assets/reverse_etl_health.py` |
| Notebook trigger job | `orchestration/assets/reverse_etl_notebook_job.py` |
| The notebook | `notebooks/reverse_etl_cdf_to_kafka.py` |
| Connect image (Debezium plugin, Avro converter) | `Dockerfile.debezium-connect` |

### Which run does what

| You want to | Run |
|---|---|
| Sync a change from Databricks | `reverse_etl_avro_to_kafka` (or the notebook through `reverse_etl_notebook_sync_job`; use one trigger per change, they share a watermark) |
| Build everything from scratch | `reverse_etl_avro_setup_job` |
| Throw everything away | `reverse_etl_avro_reset_job`, then the setup job to start again |
| Check the sink is alive | `reverse_etl_avro_sink_health_job` (or wait for the schedule) |

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

## 7. Show the sink health check (optional, about 5 minutes)

Kafka Connect reports a connector as RUNNING even when its task has failed, and does not restart it, so a dead
sink does not fail any Dagster run. Each sync has an asset check, `sink_healthy`, on its sink asset
(`reverse_etl_avro_jdbc_sink`) that reads the connector and task states from Connect and the sink's lag from
Kafka. A schedule runs it every 5 minutes through `reverse_etl_avro_sink_health_job`.

**Where to see it in the Dagster UI** (http://localhost:3000):
- **Asset page, Checks tab.** Left sidebar: **Catalog** (called **Assets** in some versions), search for
  `jdbc_sink` and click `reverse_etl_avro_jdbc_sink`, then the **Checks** tab. It lists `sink_healthy` with the latest
  result, severity and description; click it for the history.
- **Lineage.** Left sidebar: **Lineage**, search `key:"reverse_etl_avro_jdbc_sink"`. The check is not a node of its
  own: it shows as the **Asset checks** row on the sink asset's node (a status once the check has run, a dash if it
  has not or the page is stale). Click the node for the side panel; **View in Asset Catalog** opens the asset page
  with the Checks tab.
- **Run page.** Open any run of `reverse_etl_avro_sink_health_job`: the result is an "asset check evaluation" entry
  in the event log.
- **A stale page shows a dash.** The page may have been loaded before the check existed: hard-refresh the browser tab,
  or click **Reload definitions** (top right) and refresh.

**Steps**
1. **Show it healthy.** Launch `reverse_etl_avro_sink_health_job` (Jobs), or wait for the schedule, then open the Checks
   tab: `sink_healthy` passed, "reverse_etl_avro_sink is healthy, lag 0".
2. **Break the sink.** Pause the connector (a safe stand-in for a failed task):
   ```
   docker exec kafka-connect curl -s -X PUT localhost:8083/connectors/reverse_etl_avro_sink/pause
   ```
3. **Change data and sync, the key part of the demo.** Update a row of `reverse_etl_avro_source` in Databricks (as in
   section 4) and run the sync asset `reverse_etl_avro_to_kafka`. **The run is green.** Compare the targets:
   RisingWave has the change (it has its own consumer) and Postgres does not. The two disagree and nothing in the
   sync run says so.
4. **Let the check catch it.** Launch `reverse_etl_avro_sink_health_job` again, or wait up to 5 minutes. The check is
   now red with an ERROR, "connector is PAUSED; task 0 is PAUSED". The job run itself still succeeds: a failed check
   shows on the asset, it does not fail runs.
5. **Make it healthy again.** Resume the connector:
   ```
   docker exec kafka-connect curl -s -X PUT localhost:8083/connectors/reverse_etl_avro_sink/resume
   ```
   Confirm both states are `RUNNING`:
   ```
   docker exec kafka-connect curl -s localhost:8083/connectors/reverse_etl_avro_sink/status
   ```
   The sink resumes from where it stopped, so Postgres applies what was produced while it was paused within a few
   seconds (`select * from reverse_etl_avro_target order by rid`, compare with RisingWave). Then launch
   `reverse_etl_avro_sink_health_job` again (or wait for the schedule): the check is green again.

**If the task is `FAILED` rather than paused** (for example the one-off DNS error `No resolvable bootstrap urls`),
restart the task instead of resuming the connector:
```
docker exec kafka-connect curl -s -X POST localhost:8083/connectors/reverse_etl_avro_sink/tasks/0/restart
```
Do not try to cause a real failure on the demo machine; pausing shows the same thing.

**What to say**
- This happened for real on 2026-10-07: a one-off DNS error left the `cdf` and `orders` sink tasks FAILED while
  Connect still reported the connectors as RUNNING, and nobody noticed. The check found it on its first run.
- The asset's own metadata is a snapshot of its last materialization: the setup run's `connector_state: RUNNING`
  stays on the asset page even while the connector is paused. The check reads live from Connect, which is why it is needed.
- The check only shows in the Dagster UI. Nobody is notified until an alert destination (Slack, email or incident.io)
  is added, which is not built.
- It also warns when the sink's lag is above 10,000 messages. That needs a large backlog, so it is not part of the
  demo.

## 8. Clean up

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
  run every 5 minutes; it fails if the connector or a task is not RUNNING (section 7 shows it and how to recover).
- **Sink connector FAILED:** `docker exec kafka-connect curl -s localhost:8083/connectors/reverse_etl_avro_sink/status`
  shows the cause. Two failures were seen: a schema registered in the wrong string form (`The given schema does
  not match any schema under the subject ...`), fixed in the code, where a reset plus setup recovers; and a
  transient DNS error (`No resolvable bootstrap urls`), fixed by `POST /connectors/<name>/tasks/0/restart` on Connect.
- **A Dagster run fails at the Databricks step:** check the VPN and the credentials in `.env`.
- **A column did not arrive in RisingWave:** it only fills a column from messages read after the column exists;
  run the sync asset again after the column is added, and see section 10.1 of the sink doc.
