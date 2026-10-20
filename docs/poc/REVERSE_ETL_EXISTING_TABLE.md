# Reverse ETL from an existing Delta table in Databricks

The demo syncs all create their own source tables (the setup asset's `CREATE IF
NOT EXISTS`). Pointing the same pipeline at a table that already exists changes
only the definition and the first run — the sync machinery is identical. This
note records the steps and the traps. Written 2026-10-20.

## 1. Verify the table meets the reader's requirements

- **Change Data Feed is NOT needed** — that is the whole design: the delta-rs
  reader reconstructs changes from version diffs (append fast path,
  rewritten-file-pair diff, full-snapshot fallback; see
  [REVERSE_ETL_CODE_WALKTHROUGH.md](REVERSE_ETL_CODE_WALKTHROUGH.md) §6 and
  [REVERSE_ETL_SPARK_FREE_FINDINGS.md](REVERSE_ETL_SPARK_FREE_FINDINGS.md) §6).
- **Deletion vectors must be off.** The reader refuses DV tables outright: a
  row hidden by a DV inside a Parquet file would silently corrupt the diff.
  Check the table's `delta.deletionVectors` property; if it is enabled,
  disabling it (and rewriting any files that carry DVs) is a table-owner
  change.
- **UniForm is irrelevant** — the sync is UniForm-agnostic end to end (the
  `delta` sync exists to prove exactly that).
- **Column mapping** — only needed if you want drop/rename propagation. Without
  it, keep `propagate_drops: false`; column adds still flow.
- **Pick the key.** The reader diffs keyed on one column; choose the table's
  real primary key (unique, non-null). The demo's `rid` identity column will
  not exist on a real table.
- **Storage access.** The service principal needs direct read on the table's
  ADLS location (the same mechanism the demo syncs already use — no warehouse,
  no DBUs).

### The checks, as SQL (run in a Databricks SQL editor)

```sql
-- Table properties: what to look for —
--   * NO 'delta.deletionVectors.deletionVectorsEnabled' = 'true'
--     (and no 'delta.feature.deletionVectors' = 'enabled'/'required')
--   * 'delta.columnMapping.mode' = 'name' present only if you want drop/rename
--     propagation
--   * Change Data Feed is irrelevant: the reader does not use it
SHOW TBLPROPERTIES de_dev.my_schema.customers_raw;

-- Protocol versions and layout at a glance
DESCRIBE DETAIL de_dev.my_schema.customers_raw;
```

The reader enforces the DV rule itself: it inspects the table's Delta protocol
features (not the tblproperties) and fails the sync with a clear `RuntimeError`
if `deletionVectors` is among them — the SQL checks above just predict that.

```sql
-- The key must be unique and non-null: the reader diffs rows keyed on it
SELECT COUNT(*)                          AS total_rows,
       COUNT(DISTINCT customer_id)       AS distinct_keys,
       COUNT(*) - COUNT(customer_id)     AS null_keys
FROM de_dev.my_schema.customers_raw;
```

```sql
-- Find the table's storage location, then make sure the service principal
-- can read it (this is what the sync's delta-rs reads go through)
DESCRIBE EXTENDED de_dev.my_schema.customers_raw;
GRANT READ FILES ON EXTERNAL LOCATION `<location-url>` TO `<service-principal>`;
```

### Remediation SQL (only if a check fails)

```sql
-- Deletion vectors present? Disable, then rewrite the files that still carry
-- them. Both statements create commits — the first sync will simply read them
-- as rewrite windows and emit no events (the rows are unchanged).
ALTER TABLE de_dev.my_schema.customers_raw
  SET TBLPROPERTIES ('delta.enableDeletionVectors' = 'false');
REORG TABLE de_dev.my_schema.customers_raw APPLY (PURGE);

-- Want drop/rename propagation on a table without column mapping?
-- Metadata-only, but it bumps the protocol: pre-DBR-12 readers stop working.
ALTER TABLE de_dev.my_schema.customers_raw SET TBLPROPERTIES (
  'delta.columnMapping.mode' = 'name',
  'delta.minReaderVersion'   = '2',
  'delta.minWriterVersion'   = '5'
);

-- Optional, only if another engine (e.g. StarRocks) must read the table
-- through UniForm's Iceberg metadata — the sync itself does not care:
ALTER TABLE de_dev.my_schema.customers_raw
  SET TBLPROPERTIES ('delta.universalFormat.enabledFormats' = 'iceberg');
```

## 2. Define the sync (one YAML)

```yaml
# orchestration/defs/reverse_etl_customers/defs.yaml
type: orchestration.components.reverse_etl_cdf_sync.ReverseEtlCdfSync

attributes:
  name: customers            # label — all object names derive from it
  catalog: de_dev
  schema_name: my_schema     # the existing table's schema
  source_table: customers_raw   # OVERRIDE: point at the existing table (bare name)
  key_column: customer_id       # the table's real PK, not the demo's rid
  encoding: avro                # or json
  propagate_drops: false        # true only if the source has column mapping
  sink_tasks_max: 4             # raise for the initial backfill
  # NO seed_statements — leave unset for a real table (component docstring)
```

`source_table` is a bare table name within `catalog.schema` (the config builds
`catalog.schema.source_table`). Optional overrides if the derived names clash:
`kafka_topic`, `postgres_table`, `risingwave_table`, `connector_name`,
`group_name`.

## 3. Restart Dagster and run the setup job

`docker compose restart dagster-webserver dagster-daemon` (the code server must
reload definitions), then run `reverse_etl_customers_setup_job`. It creates
everything *around* the table:

- the watermark state table (`reverse_etl_customers_state`, deliberately plain
  Delta — no column mapping, DVs off, single SQL writer) — note it lands in the
  same `catalog.schema` as the source, clearly namespaced;
- the Kafka topic, the RisingWave target, and the Debezium JDBC sink connector;
- the registry subjects, when `encoding: avro`.

The source creation is `IF NOT EXISTS` — the existing table is untouched and
its properties are not altered.

The state table the job creates (exactly what to expect in your schema):

```sql
CREATE TABLE IF NOT EXISTS de_dev.my_schema.reverse_etl_customers_state (
    sync_name STRING, last_commit_version BIGINT, last_schema STRING
) USING DELTA
TBLPROPERTIES ('delta.enableDeletionVectors' = 'false')
```

It is deliberately plain Delta, single-writer: the sync's delta-rs no-op
pre-check reads the watermark from it, and delta-rs cannot commit to
Databricks-written v7 protocol tables — so the only writer is one small SQL
`INSERT` per sync (below). Verify after the job:

```sql
SHOW TABLES IN de_dev.my_schema LIKE 'reverse_etl_customers*';
```

## 4. First sync = initial backfill

The watermark starts at 0, so the first sync walks the table's history and
emits its current state as a stream of `c` events — Postgres and RisingWave
fill up through Kafka. This is the documented O(history) first sync, a one-off;
`REVERSE_ETL_CDF_CHUNK_VERSIONS` chunks it and the per-chunk heartbeats show
progress in the run metadata. With `sink_tasks_max` raised, the JDBC sink keeps
up with the load.

Watching it from Databricks — the watermark, and the source's current version.
After a completed sync, `last_commit_version` equals the source's max version:

```sql
-- The watermark (append-only table; take the freshest row for the sync)
SELECT sync_name, last_commit_version
FROM de_dev.my_schema.reverse_etl_customers_state
WHERE sync_name = 'customers'
ORDER BY last_commit_version DESC
LIMIT 1;

-- The source's current version (top of the commit history)
DESCRIBE HISTORY de_dev.my_schema.customers_raw LIMIT 5;
```

For reference, the one SQL statement the sync itself runs on Databricks at the
end of every run (append-only; readers take the freshest row per `sync_name`):

```sql
INSERT INTO de_dev.my_schema.reverse_etl_customers_state
VALUES ('customers', <new_version>, '<last_schema-json>');
```

## 5. Schedule it

The same pattern as the demo's recurring job
([REVERSE_ETL_DBU_COST_NOTES.md](REVERSE_ETL_DBU_COST_NOTES.md) §6): a
scheduled Dagster run of the sync job. Every subsequent sync reads only the
commits since the watermark — change-proportional, warehouse-free, zero DBUs.

The periodic health check is the same pair of queries from §4: the state
table's `last_commit_version` tracks the source's top `DESCRIBE HISTORY`
version, and `last_schema` reflects the source's current columns (that is the
drop-propagation evidence). If the watermark stalls while `DESCRIBE HISTORY`
grows, the sync runs are failing — check Dagster before Databricks.

## Safety notes

- **The reset job refuses to run for this sync.** `_assert_reset_allowed`
  requires every name it drops (including `source_table`) to start with the
  config's `reset_name_prefixes` (default `reverse_etl_`) and to live in
  `reset_schema`. A real table's name fails that check → `RuntimeError`, by
  design. Never widen the allowlist to include a real table.
- **Backfill volume** — every existing row flows through Kafka once; size topic
  retention, message size limits, and sink batching accordingly.
- **Schema changes** — column adds propagate automatically (the targets alter,
  and with Avro the registry gets a new schema version *before* the alter).
  Drops/renames propagate only if the source has column mapping on and
  `propagate_drops: true`.
- **Do not run two syncs against the same source table** — each sync owns its
  watermark; two syncs on one table would both emit the same changes to their
  own topics (which may be what you want for two independent targets, but it
  doubles the reads).

## See also

- [REVERSE_ETL_CODE_WALKTHROUGH.md](REVERSE_ETL_CODE_WALKTHROUGH.md) — the
  sync's read paths, schema flow, and setup/reset assets.
- [REVERSE_ETL_SPARK_FREE_FINDINGS.md](REVERSE_ETL_SPARK_FREE_FINDINGS.md) —
  why no CDF/Spark, the reader's scaling behavior (§6).
- [REVERSE_ETL_DBU_COST_NOTES.md](REVERSE_ETL_DBU_COST_NOTES.md) — measured
  costs, the recurring-job audit (§6).
- [REVERSE_ETL_LIVE_DEMO.md](REVERSE_ETL_LIVE_DEMO.md) — operating the demo
  chain these steps reuse.
