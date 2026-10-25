# Reverse ETL from a copy of a Databricks view

The reverse-ETL pipeline reads a Delta *table*, not a view, so to sync a view
(here `de_dev.kaizen_wars.dim_company`) you first materialize a copy of it as a
table next to the demo sources, then point a sync at that copy. The sync
machinery is identical to
[REVERSE_ETL_EXISTING_TABLE.md](REVERSE_ETL_EXISTING_TABLE.md); this note
records the view-specific steps and checks. Written 2026-10-25.

## 1. What the view is

`de_dev.kaizen_wars.dim_company` is a view over two tables (read from the
Databricks table metadata):

| Source | Type | Role |
|---|---|---|
| `de_dev.voperdb.mcompany` | VIEW (`select * from de_dev.metadatadb.mcompany`) | Base company list |
| `de_dev.masterconfigdb.dim_company_metadata` | MANAGED table | Left-joined on `comCompanyID = CompanyId` |

The view adds derived columns (area code, release date, active flag, country
grouping, brand) with `CASE` logic keyed on the company ID. `Company_ActiveFL`
depends on `current_date()`, so a copy is a snapshot that drifts until it is
refreshed.

## 2. Where the table goes, and the properties

- **Schema:** `de_dev.sr_poc_external`, where the reverse-ETL source tables
  live (not `de_dev.sr_poc`).
- **Name:** `reverse_etl_<name>_source`. The reset job only touches names with
  the `reverse_etl_` prefix in the configured schema, and with `name: company`
  every derived name (`_source`, `_state`, `_topic`, `_target`, `_sink`) lines
  up with no `source_table` override.
- **Properties:** none. Change Data Feed is not needed (the delta-rs reader
  diffs table versions) and Databricks' defaults are fine, including deletion
  vectors. `USING DELTA` is optional: Delta is the default table format.

## 3. The key

The sync needs one column that is the table's real primary key: **unique and
non-null**. The key is written to Kafka as a non-optional key field and used as
`primary.key.fields` by the Debezium JDBC sink, so duplicates or nulls break
the pipeline downstream.

Check on the view before creating the copy:

```sql
SELECT COUNT(*)                       AS total_rows,
       COUNT(DISTINCT Company_ID)     AS distinct_keys,
       COUNT(*) - COUNT(Company_ID)   AS null_keys
FROM de_dev.kaizen_wars.dim_company;
```

Result on 2026-10-25 (serverless starter warehouse): 32 / 32 / 0, so
`Company_ID` is a valid key. The risk to re-check later is the left join to
`dim_company_metadata`: more than one row per company there would duplicate
`Company_ID`.

## 4. The process

### Step 1 — Create the source table (SQL editor or a serverless warehouse)

```sql
CREATE TABLE de_dev.sr_poc_external.reverse_etl_company_source
AS SELECT * FROM de_dev.kaizen_wars.dim_company;
```

### Step 2 — Define the sync

`orchestration/defs/reverse_etl_company/defs.yaml`:

```yaml
type: orchestration.components.reverse_etl_cdf_sync.ReverseEtlCdfSync

attributes:
  name: company
  catalog: de_dev
  schema_name: sr_poc_external
  key_column: Company_ID
  encoding: avro
  propagate_drops: false
```

`source_columns` is left unset. It is optional: it is only the column DDL for
when the setup asset has to *create* the source table, as the demos do. With
it unset the setup asset treats the table as existing: it checks the table is
there (a Unity Catalog lookup, no warehouse) and fails if it is not, rather
than creating it from the key alone. The `make_change` demo helper builds its SQL
from `source_columns` and a business key called `id`, so a sync defined this way gets no `make_change` asset or job
(since 2026-10-25); change the source table with SQL instead. `seed_statements` stays unset.

### Step 3 — Restart Dagster

```bash
docker compose restart dagster-webserver dagster-daemon
```

Wait about 30 seconds for `localhost:3000`, then confirm the
`reverse_etl_company_*` assets and `reverse_etl_company_setup_job` appear.

### Step 4 — Run the setup job

Dagster UI → Jobs → `reverse_etl_company_setup_job` → Launch run. It creates
the state table, the Kafka topic and Avro subjects, and the RisingWave target,
runs the first sync (a backfill, expected to emit 32 insert events), and
creates the Debezium JDBC sink to Postgres.

### Step 5 — Verify

```bash
psql -h localhost -U postgres -d postgres -c "SELECT COUNT(*) FROM reverse_etl_company_target;"
psql -h localhost -p 4566 -U root -d dev -c "SELECT COUNT(*) FROM reverse_etl_company_target;"
```

Both should return 32.

### Step 6 — Refresh later

Change the table with `MERGE` or `UPDATE`, then materialize
`reverse_etl_company_to_kafka`. `CREATE OR REPLACE` on the source keeps the
table history and is safe. Do not drop and recreate the source while the state
table survives: the source's version numbers restart at 0, below the
watermark, and every later sync reads zero changes (see "The drop+recreate
trap" in [REVERSE_ETL_EXISTING_TABLE.md](REVERSE_ETL_EXISTING_TABLE.md)).
Never `CREATE OR REPLACE` the state table.

### Step 7 — Schedule (optional)

Add a cron automation to the `reverse_etl_company_to_kafka` asset (Dagster UI
→ Automation).

### Permissions: let the reset job drop the copy

You created the copy by hand, so you own it, and the reset job runs as the service principal, which has
`ALL PRIVILEGES` on the schema but not `MANAGE` on your table. Without `MANAGE` the job fails at
`<name>_drop_databricks_tables` with `PERMISSION_DENIED ... does not have MANAGE on Table ...`. Grant it once,
after creating the table (a recreated table starts with no grants). For service principals the grant takes
the **application ID**, not the display name, and the principal name needs back quotes:

```sql
GRANT MANAGE ON TABLE de_dev.sr_poc_external.reverse_etl_company_source
TO `27a78a40-69f4-40e0-9768-ba39d58a6a55`;   -- sp-stkznneusrpoccdddevstd-contributor
```

Find the ID with `databricks service-principals list --profile personal`.

### If a reset fails halfway

The reset clears this sync's watermark row in `reverse_etl_sync_state` **before** it touches Databricks (changed
2026-10-25; it used to clear it after the drops). Earlier, a failed drop left the old watermark behind, and
once the source was recreated at version 0 the next sync found nothing new and loaded nothing, silently:
Dagster showed a green run with `events_synced` empty, and the targets stayed empty. If you ever see that,
delete the row by hand and sync again:

```sql
DELETE FROM reverse_etl_sync_state WHERE sync_name = 'reverse_etl_company';
```

## 5. The same copy through drt

The drt comparison sync for this table is two files; the first points at the existing `company` sync for the
source table and key:

- `orchestration/defs/reverse_etl_company_drt/defs.yaml` (`ReverseEtlDrtSync`, `name: company`)
- `orchestration/drt_demo/syncs/reverse_etl_company_drt.yml` (source `SELECT * FROM
  de_dev.sr_poc_external.reverse_etl_company_source`, target `reverse_etl_company_drt_target`, `upsert_key:
  [Company_ID]`, mirror mode with the tracked strategy). The drt asset refuses to run if this file's names
  disagree with the sync config.

To run it: restart Dagster, run `reverse_etl_company_drt_setup_job` (creates the Postgres table with a `date`
column), then materialize `reverse_etl_company_drt_to_postgres`. Reset with `reverse_etl_company_drt_reset_job`.
Mirror mode scans the whole table on every run and deletes rows drt synced earlier that are gone from the source.
drt reads through a SQL warehouse, so it uses DBUs; the Debezium pipeline reads storage directly and uses none.
Verified 2026-10-25: 32 rows, identical to the Debezium target; an update and a delete on the source were
applied on the next run, and the restore too.

### Timing comparison (2026-10-25, 32 rows, VPN, warm serverless warehouse)

Dagster step durations, single runs:

| Scenario | drt | Debezium pipeline (`to_kafka` step) |
|---|---|---|
| First full load | 6.9 s | 14-16 s |
| Re-run, nothing changed | 2.7-3.2 s (still a full scan) | 1.4-1.9 s (watermark pre-check) |
| Incremental, 2-4 changed rows | 3.1-3.9 s | 25-45 s (about 10-12 s per Delta commit read) |

End to end, one single-commit change, from launching the run until the value shows in the target (two trials):

| | drt, Postgres | Debezium, Postgres | Debezium, RisingWave |
|---|---|---|---|
| Change | 11.9 s | 22.2 s | 23.7 s |
| Restore | 11.5 s | 21.2 s | 25.0 s |

Both include about 8 s of Dagster startup. The sink and RisingWave hops add only seconds: the value reached
Postgres a few seconds before Dagster marked the run successful. The incremental cost of the Debezium path is
per Delta commit, so one change per commit (as in this test) costs a chunk each; a batch of changes in one commit
costs about one. drt's cost grows with table size, the Debezium path's with the number of commits and changed files,
so on a large table the gap would shrink or flip. Two trials, one row, one table: indicative only.

## 6. Types and names in the targets

- **`DATE`** is carried as a real date: `Company_ReleaseDate` is `date` in both Postgres and RisingWave (see the
  `DATE columns` paragraph in [REVERSE_ETL_DEBEZIUM_JDBC_SINK.md](REVERSE_ETL_DEBEZIUM_JDBC_SINK.md)).
- **Column case.** The sink runs with `quote.identifiers = true`, so `Company_ID` and the other mixed-case names
  keep their case in Postgres. Quote them when querying (`"Company_ID"`); RisingWave keeps the case too.
  Without it Postgres folds names to lowercase.
- **Timestamps** are `timestamptz` in Postgres; RisingWave on Avro reads them as `varchar` plus a generated
  `<column>_ts`. This table has none.
- **Rebuild after a type or quoting change.** Existing targets keep their old column types and names. Run
  `reverse_etl_company_reset_job` (it drops the source copy too), recreate the source copy (step 1), then run the
  setup job again.
- **Snapshot drift.** The copy does not follow the view until it is refreshed (step 6).
