# Reverse-ETL POC in risingwave-test: Databricks CDF → Kafka STG

## Context

This continues the APR-233 evaluation (Kaizen's "Explore & Standardize Batch
Reverse ETL" epic). We established that **drt** (the leading OSS reverse-ETL
candidate) has no Kafka destination and, by its own architecture (ADR 0004),
deliberately does not do CDC-based extraction. Databricks' own **Change Data
Feed (CDF)**, read in **batch** mode (`table_changes()`, not Structured
Streaming), is the right mechanism to sync new/changed/deleted rows without
adopting a third-party tool.

A first cut was scaffolded as a throwaway sandbox project
(`~/Projects/dagster-test/dagster-poc/reverse-etl`) to prove the design:
watermark-owned-by-the-caller, a safe first-run baseline (no full-table
backfill unless explicitly requested), and CDF's `_change_type` /
`_commit_version` columns carried into the Kafka payload.

`~/Projects/risingwave-test` was identified as the better long-term home: it
already has a live Dagster orchestration graph, Databricks Unity Catalog
conventions, a **verified real staging Kafka cluster**
(`stg-ocp-kfk01-bootstrap.kaizengaming.net:9096`, SASL_SSL/SCRAM-SHA-512)
already serving other output topics, and a real Apicurio schema registry
integration (currently read-only).

Decisions made:
- **New, dedicated Databricks table + new Kafka topic** for this POC — not
  touching any real E&A table or data.
- **JSON payload, no schema registry** for this first cut.
- **Scope: build + unit-test only.** No live run against real staging
  Kafka/Databricks/Apicurio without a separate, explicit go-ahead.

**Live-checked against the real workspace** (`adb-1608121643336927...`, via
`databricks schemas/tables list --profile personal`):
- `de_dev.rw_poc` — real, exists, owned by the pipeline's own service
  principal. This is what `casino_prd_setup.py` / `databricks_optimize.py` /
  `landing_to_bronze.py` hardcode. `.env`'s `DATABRICKS_SCHEMA=risingwave_poc`
  is a red herring — that's a separate, unrelated schema also owned by an
  admin group, not used by any of these assets.
- `de_dev.sr_poc_external` — also real, **owned by the author**, already a
  personal scratch schema for one-off POC tables (dated/probe-suffixed names
  like `rw_funnel_uc_probe_20260905`, `trino_probe_poc`, etc.). **This is
  where the new reverse-ETL POC table goes** — lower friction than the
  shared, service-principal-owned `rw_poc`, and matches existing usage of
  this schema.
- The pipeline's own service-principal auth (`DATABRICKS_AZURE_CLIENT_ID` /
  `DATABRICKS_AZURE_CLIENT_SECRET`, required by the existing `_get_token`
  helper in `databricks_optimize.py`) is **not currently set in `.env`** —
  those two vars are simply absent, so `casino_prd_setup.py`'s assets
  couldn't run from this laptop today either, independent of this change.
  The working credential right now is the personal OAuth CLI profile
  (`databricks auth login --profile personal`), which is a different auth
  path than the code's existing `requests`-based client-secret flow. See
  "Auth for the live run" below.

## Approach

### 1. New Databricks POC table, CDF enabled at creation

New module `orchestration/assets/reverse_etl_cdf_setup.py`, following the
`_UC_TABLES` / `databricks_uc_tables_setup` *pattern* in
`orchestration/assets/casino_prd_setup.py` (lines ~30–153) — idempotent
probe-then-create — but targeting the personal scratch schema, not the
shared one:

```sql
CREATE TABLE IF NOT EXISTS de_dev.sr_poc_external.reverse_etl_cdf_poc_source (
    id BIGINT NOT NULL,
    value STRING,
    updated_at TIMESTAMP
) USING DELTA
TBLPROPERTIES ('delta.enableChangeDataFeed' = 'true')
```

Because CDF is enabled **at creation**, this table has no "pre-existing
table" bootstrap problem — version 0 already carries real CDF data, so the
POC can read from version 0 safely.

**Revised after live testing** (see "Live run results" below): rather than
hard-coding "skip backfill by default, require an explicit override" — which
turned out to be the wrong default for a table like this one — the asset
auto-detects whether a full backfill is safe, via `DESCRIBE HISTORY`'s
earliest available version: `0` means the full commit history back to
table creation is intact (true for both a freshly created table like this
one, and a real pre-existing table whose retention has always comfortably
covered its whole lifetime), so the first run backfills everything and every
run after that is a cheap delta. Only when the earliest available version is
`> 0` (some history has already aged out of `delta.logRetentionDuration`) does
it fall back to baselining at the current version with no historical rows
synced — because a full backfill in that case would silently reconstruct an
*incomplete* picture (Databricks' own fallback: pre-CDF-enablement versions
return as synthetic full-table inserts) rather than error, which is worse
than declining. An explicit `REVERSE_ETL_BACKFILL_FROM_VERSION` env var still
overrides both, e.g. to deliberately start from the exact version CDF was
enabled at. This one asset now handles new and pre-existing tables without
the caller needing to know or declare which case applies.

A small bookkeeping table, isolated from user data (same shape as drt's own
tracked-mirror state tables):

```sql
CREATE TABLE IF NOT EXISTS de_dev.sr_poc_external.reverse_etl_cdf_poc_state (
    sync_name STRING, last_commit_version BIGINT
)
```

New asset `reverse_etl_poc_table_setup`: creates both tables if absent,
using the same `SELECT 1 ... LIMIT 0` existence-probe idiom as
`databricks_uc_tables_setup` (`casino_prd_setup.py` lines ~113–152).

### 2. New Kafka STG topic

`orchestration/assets/kafka_topics_setup.py`: add `"rw_poc_reverse_etl_cdf_out"`
to the existing `OUTPUT_TOPICS` list (lines 6–12). No new asset — the
existing `kafka_output_topics_setup` asset already creates/verifies every
topic in that list against `KAFKA_OUTPUT_BOOTSTRAP` / `KAFKA_OUTPUT_SASL_*`.

### 3. CDF batch read → Kafka produce asset

New asset `reverse_etl_cdf_to_kafka` in `reverse_etl_cdf_setup.py`,
`deps=[reverse_etl_poc_table_setup, kafka_output_topics_setup]`:

- **Databricks side**: `SELECT * FROM table_changes('de_dev.sr_poc_external.reverse_etl_cdf_poc_source', <since_version>)`,
  executed via the Statement Execution API and parsed from its
  `result.data_array` + `manifest.schema.columns` response shape — the one
  real adaptation vs. the dagster-poc version, which used a DB-API cursor.
  See "3b. Auth for the live run" below for which client issues this call.
- **Kafka side**: `confluent_kafka.Producer` (already a repo dependency)
  against `KAFKA_OUTPUT_BOOTSTRAP` + `KAFKA_OUTPUT_SASL_*`, matching
  `kafka_topics_setup.py`'s `_admin_client()` credential convention exactly
  — deliberately *not* the `KAFKA_SASL_*` / `KAFKA_SECURITY_PROTOCOL` naming
  used by `scripts/produce_protobuf_casino_rounds.py`, to avoid introducing
  a third env-naming scheme for what is the same "topics this project
  writes to" credential set.
- **Payload — revised to a Debezium-style envelope** (see "3c. Debezium
  envelope" below): originally plain JSON per raw CDF row (`_change_type` /
  `_commit_version` / `_commit_timestamp` inline). Kept for reference since
  `_next_watermark` still consumes raw rows in this shape; the produced
  Kafka messages no longer look like this.
- **Watermark logic**: the pure, client-agnostic function from
  `dagster-poc/reverse-etl/src/reverse_etl/defs/cdf_to_kafka.py`
  (`_next_watermark`) ported unchanged — no Databricks/Kafka client
  dependency. `_read_last_version` / `_write_last_version` / `_read_changes` /
  `_get_current_version` are rewritten against `_run_sql`'s REST response
  shape instead of a DB-API cursor. The original `_build_kafka_message` and
  raw-CDF `_summarize_change_types` were superseded by the Debezium-envelope
  versions below.

### 3b. Auth for the live run

**Update after first live attempt:** the original plan here used
`databricks-sdk`'s `WorkspaceClient(profile="personal")`, reasoning that the
personal CLI profile (`~/.databrickscfg`) required zero new credentials.
That worked from a laptop shell but **failed inside the Dagster container**
(`ValueError: default auth: cannot configure default credentials`) — the
container has no `~/.databrickscfg`, and its `DATABRICKS_AUTH_TYPE=azure-client-secret`
env var conflicts with profile-based resolution. By the time this was hit,
`.env` already had real `DATABRICKS_AZURE_CLIENT_ID`/`DATABRICKS_AZURE_CLIENT_SECRET`
values (populated after the plan was written), so the fix was to **drop the
`databricks-sdk` dependency entirely** and reuse `_get_token`/`_submit`/`_poll`
from `databricks_optimize.py` — the same Azure AD service-principal +
Statement Execution API flow `casino_prd_setup.py` already uses successfully
in this exact container. One auth path, no new dependency, matches the
originally-flagged "reuse what's there" preference.

### 3c. Debezium envelope (revised payload)

Prompted by two questions while figuring out how to *show* the results: (1)
would a plain JSON-per-CDF-row payload be easy for StarRocks (or anything
else) to consume, and (2) why couldn't a RisingWave materialized view do
*real* deletes reconstructing current state from that payload. Both answers
pointed the same direction: RisingWave's ingestion layer only recognizes two
conventions for "this message is a delete" — a null-value message (`FORMAT
UPSERT`) or a `"op": "d"` envelope (`FORMAT DEBEZIUM`) — and our raw
CDF-row JSON was neither, so the only way to get "current state" out of it
was a `ROW_NUMBER() ... WHERE _change_type != 'delete'` view that has to
retain the *entire* change history per key forever, not a real compacted
upsert table.

Switched `_build_kafka_message` (now fed by a new `_to_debezium_events`
grouping step) to emit `{"before": {...} | null, "after": {...} | null,
"op": "c" | "u" | "d", "source": {"commit_version", "commit_timestamp"}}`
per logical change instead of one message per raw CDF row:

- CDF's `update_preimage` + `update_postimage` pair (same key, same
  `_commit_version`, no guaranteed adjacency in `table_changes()`'s output —
  grouped by `(key, commit_version)` rather than assumed-adjacent) collapses
  into one event's `before`/`after`, instead of two separate messages. No
  information is lost — `before` still carries the old value, `after` the
  new one — it's the same content restructured, not fewer facts.
- `_change_type`'s four raw CDF values collapse to Debezium's three-value
  `op` vocabulary (`c`/`u`/`d`). `_commit_version`/`_commit_timestamp` move
  into `source`, matching Debezium's own convention of separating
  change-event metadata from row content.
- This is what lets **RisingWave** ingest the topic as a genuine
  `FORMAT DEBEZIUM ENCODE JSON` upsert table (see "7. RisingWave table"
  below) — real physical deletes, not a derived view — and is a far more
  portable shape for **StarRocks** (Routine Load's own `__op` convention is
  the same idea) or any other CDC-aware consumer than the original
  Delta-specific `_change_type` field.
- Verified offline (pure logic, no I/O) with synthetic rows deliberately
  out of preimage/postimage order, confirming the `(key, commit_version)`
  grouping doesn't depend on `table_changes()` row ordering (which it does
  not guarantee).

### 4. Dagster wiring

`orchestration/definitions.py`: import `reverse_etl_poc_table_setup`,
`reverse_etl_cdf_to_kafka`, and `reverse_etl_cdf_risingwave_table` (see
"7. RisingWave table" below) alongside the existing `casino_prd_setup`
import block, add all three to the `Definitions` asset list, group_name
`reverse_etl_poc`.

### 5. Synthetic traffic (minimal)

A small script, `scripts/reverse_etl_poc_seed.py`, issuing a handful of
INSERT/UPDATE/DELETE statements against
`de_dev.sr_poc_external.reverse_etl_cdf_poc_source` (same client as the main
asset — see "3b. Auth for the live run") — just enough for the POC to have
something to sync. Not a configurable load generator like
`scripts/wallet_producer.py`.

Wired into the script runner (`./bin/0_script_runner.sh`, port 4001) so it's
runnable from the UI rather than the CLI. `scripts/script_runner.py` lists
scripts from a hardcoded `SCRIPTS` tuple (lines 38–64), not directory
scanning, so this needs two additions matching the existing one-shot style
of `3_run_dbt.sh` (no background/pattern entry needed — this seed script
runs a handful of statements and exits, unlike the long-running
`wallet_producer.py`):

- `bin/3_run_reverse_etl_seed.sh` — thin wrapper, same shape as
  `bin/3_run_wallet_producer.sh`: `exec uv run python scripts/reverse_etl_poc_seed.py`.
- One new entry in `scripts/script_runner.py`'s `SCRIPTS` list:
  `("3_run_reverse_etl_seed.sh", "🔁 Seed Reverse-ETL POC", "Insert/update/delete a few rows in the reverse-ETL CDF POC table")`.

### 6. Tests

This repo has **no existing pytest coverage for any `orchestration/assets/*.py`
setup-style asset** (confirmed: only `scripts/test_ml_training.py` exists,
no pytest config or `conftest.py` anywhere else). Matching that convention,
verification will be via `dagster asset materialize` (same as
`casino_prd_setup`'s existing assets), not a new test suite — unless the
pure watermark/transform functions should be unit-tested the way
`dagster-poc/reverse-etl` already does, in which case a small `tests/`
module should be added and flagged explicitly as a deviation from this
repo's current convention.

### 7. RisingWave table + one-click setup job

New module `orchestration/assets/reverse_etl_risingwave_setup.py`, asset
`reverse_etl_cdf_risingwave_table`, following `risingwave_countries_table.py`
/ `risingwave_udfs.py`'s exact `psycopg2` connection convention
(`RISINGWAVE_HOST`/`PORT`/`DB`/`USER`/`PASSWORD`):

```sql
CREATE TABLE IF NOT EXISTS reverse_etl_cdf_poc_current (
    id BIGINT PRIMARY KEY,
    value VARCHAR,
    updated_at VARCHAR
)
WITH (
    connector = 'kafka',
    topic = 'rw_poc_reverse_etl_cdf_out',
    properties.bootstrap.server = '<KAFKA_OUTPUT_BOOTSTRAP>',
    scan.startup.mode = 'earliest',
    properties.security.protocol = 'SASL_SSL',
    properties.sasl.mechanism = '<KAFKA_OUTPUT_SASL_MECHANISM>',
    properties.sasl.username = '<KAFKA_OUTPUT_SASL_USERNAME>',
    properties.sasl.password = '<KAFKA_OUTPUT_SASL_PASSWORD>'
)
FORMAT DEBEZIUM ENCODE JSON
```

Because the topic now carries Debezium-shaped `before`/`after`/`op` events
(see "3c. Debezium envelope"), RisingWave applies them as real upserts and
deletes against this table's own storage, keyed by `id` — not a
`ROW_NUMBER()`-over-append-log view. `updated_at` is typed `VARCHAR` rather
than `TIMESTAMP`: this hand-rolled envelope doesn't follow real Debezium
connectors' epoch-micros temporal encoding, so declaring it as a native
temporal type risked a parse mismatch; safest for a POC, castable downstream
if needed.

`deps=[kafka_output_topics_setup, reverse_etl_cdf_to_kafka]` — ordered after
the CDF sync purely for job-run narrative (confirms messages exist by
materialize time), not a functional requirement:
`scan.startup.mode = 'earliest'` reads from the topic's beginning regardless
of creation order.

**One-click job**, `reverse_etl_poc_setup_job` in `orchestration/definitions.py`,
following `wallet_pipeline_setup_job` / `kafka_topics_setup_job`'s exact
pattern (`define_asset_job` + `in_process_executor`, avoiding the
multiprocess-executor per-step reimport cost those jobs' own comments
document): selects all four assets — `reverse_etl_poc_table_setup`,
`kafka_output_topics_setup`, `reverse_etl_cdf_to_kafka`,
`reverse_etl_cdf_risingwave_table` — so one run creates every object *and*
performs an initial sync, rather than materializing four separate assets by
hand.

## Explicitly not doing (per scope decision)

- No live execution against real staging Kafka, Apicurio, or the real
  Databricks workspace as part of this change. `CREATE TABLE`, topic
  creation, and the CDF read/produce step only run when the assets are
  explicitly materialized later.
- No Apicurio schema registration (Debezium JSON payload, no schema
  registry).
- No changes to any existing E&A table, topic, or asset.

## Verification (once a live run is separately approved)

1. `uv run dagster asset materialize --select reverse_etl_poc_table_setup` —
   confirms the source + state tables exist in `de_dev.sr_poc_external`.
2. `uv run dagster asset materialize --select kafka_output_topics_setup` —
   confirms `rw_poc_reverse_etl_cdf_out` exists on `KAFKA_OUTPUT_BOOTSTRAP`.
3. Run the seed script — via the script runner UI (`🔁 Seed Reverse-ETL POC`
   button at http://localhost:4001) or directly with
   `uv run python scripts/reverse_etl_poc_seed.py` — to insert/update/delete
   a few rows.
4. `uv run dagster asset materialize --select reverse_etl_cdf_to_kafka` —
   confirms rows land on the topic (verify via `kcat` / Redpanda console) and
   `reverse_etl_cdf_poc_state.last_commit_version` advances.
5. Re-materialize step 4 with no new writes — confirms it's a no-op (0 rows,
   unchanged watermark), proving delta-only behavior.
6. `reverse_etl_poc_setup_job` (Dagster UI or
   `uv run dagster job execute -j reverse_etl_poc_setup_job -m orchestration.definitions`),
   then confirm `SELECT * FROM reverse_etl_cdf_poc_current` in RisingWave
   (`psql -h localhost -p 4566`) reflects current state — present rows for
   inserts/updates, absent for deletes — without needing a reconstruction
   view. **Done, see item 7 in "Live run results" below.**

## Live run results — ✅ end-to-end verified, including the Debezium envelope + RisingWave table + one-click job (2026-09-27)

All five verification steps above were run live against the real workspace
and staging Kafka on 2026-09-25. Three real issues surfaced and were fixed
in the process (beyond the auth swap in "3b. Auth for the live run" above):

1. **Warehouse permission gap.** The `.env`-configured service principal
   (`27a78a40-...`, display name `sp-stkznneusrpoccdddevstd-contributor` —
   an ADLS/storage-scoped identity, not one previously used for Databricks
   SQL) had zero grants on warehouse `4d06eca1e71a9ccc`, despite already
   having `ALL_PRIVILEGES` on `de_dev.sr_poc_external` itself. Fixed by
   granting it `CAN_USE` on the warehouse (a real, explicitly-approved
   change to shared workspace infrastructure — not something automated).
2. **Off-by-one in watermark resumption.** `table_changes()`'s
   `startingVersion` is inclusive, but the code originally resumed from
   `last_version` itself rather than `last_version + 1`, which would have
   redelivered the last-synced version's rows on every subsequent run.
   Fixed in `reverse_etl_cdf_to_kafka`.
3. **`DELTA_CDC_START_VERSION_AFTER_LATEST`.** Once caught up, the very next
   run's `next_version = last_version + 1` can exceed the table's current
   latest version — `table_changes()` errors in that case instead of
   returning empty. This is the common case for any daily batch once past
   its initial backlog, not an edge case. Fixed by checking
   `_get_current_version()` before querying and treating
   `next_version > current_version` as "nothing new" rather than a query.
4. **String-typed REST responses.** The Statement Execution API's JSON
   response returns every column value as a string regardless of SQL type
   (confirmed directly: `_commit_version` came back as `"6"`, not `6`).
   `_next_watermark()`'s `max()` over uncast strings would sort
   lexicographically rather than numerically, and also failed
   `MetadataValue.int()`'s type check. Fixed by casting explicitly.

Confirmed via direct consumption of `rw_poc_reverse_etl_cdf_out` (throwaway
consumer group, no offset commits): all 8 rows across commit versions 4–6
(insert ×2, update_preimage/update_postimage ×2, delete ×1 — plus the
duplicate insert from running the seed script twice) landed correctly,
keyed by `id`, carrying `_change_type` / `_commit_version` /
`_commit_timestamp`. The core question this POC was built to answer —
*can Databricks CDF, read in batch, cheaply sync new/changed/deleted rows to
Kafka without a third-party reverse-ETL tool* — is answered: **yes.**

5. **First-run design flaw, found after the fact — and fixed + re-verified
   live.** The original "skip backfill by default" behavior meant this POC's
   *own* first run (versions 1–3: the seed script's initial
   insert/update/delete) never made it to Kafka — only the second run's
   changes (4–6) did. That's not what a first run should do on a table like
   this one, where backfilling everything is completely safe. Fixed per
   "1. New Databricks POC table" above (auto-detect via `DESCRIBE HISTORY`'s
   earliest available version), then verified live: reset
   `reverse_etl_cdf_poc_state` (deleted the `reverse_etl_cdf_poc` row) to
   force a genuine first run, re-materialized `reverse_etl_cdf_to_kafka`, and
   confirmed the log line `"...full history is intact (earliest available
   version is 0), backfilling from the beginning"` followed by all 14 rows
   across versions 1–6 (`{'update_preimage': 3, 'update_postimage': 3,
   'insert': 6, 'delete': 2}`) — matching the seed script's two runs exactly.
   Watermark correctly settled at `6`. Re-consuming the topic showed 22 total
   messages (8 from the earlier delta-only run + 14 from this backfill,
   versions 1–6 all present) — Kafka is at-least-once by design here, so the
   overlap between the two test runs is expected, not a bug; a real
   downstream consumer would key on `id` + `_commit_version`.

6. **Payload switched to Debezium envelope, RisingWave table + one-click job
   added.** All of steps 1–5 above ran against the *original*
   flat-JSON-per-CDF-row payload. After discussing how to show results
   (RisingWave/StarRocks table? which format is easiest to consume? why
   can't a view do real deletes?), the payload was redesigned as a
   Debezium-style `before`/`after`/`op` envelope (see "3c. Debezium
   envelope") and a `reverse_etl_cdf_risingwave_table` asset +
   `reverse_etl_poc_setup_job` were added (see "7. RisingWave table").
   `_to_debezium_events`'s grouping logic was verified offline first against
   synthetic CDF rows (including out-of-order preimage/postimage).

7. **Live-verified end-to-end, two more real bugs found and fixed in the
   process:**
   - **Numeric columns arrive as JSON strings, and RisingWave's Debezium
     parser does not coerce them.** First live run of the new payload:
     `reverse_etl_cdf_poc_current` stayed empty despite `reverse_etl_cdf_to_kafka`
     reporting events produced. RisingWave's compute-node log had the exact
     answer: `` failed to parse message, skipping error=Cannot parse value
     `1` with type `string` into expected type `Int64` `` — every message
     was silently dropped because `id` was JSON `"1"` (a string, same root
     cause as the earlier `_commit_version` string-typing issue) against a
     declared `BIGINT` column. Fixed by explicitly casting `id` to `int` in
     `_row_without_cdf_columns` (`NUMERIC_ROW_COLUMNS`).
   - **Event order across commits wasn't guaranteed, and it mattered.**
     After the type fix, the table populated but `id=2` was stuck on
     `"second"` instead of the true current value `"second-updated"`.
     `_read_changes()` issued no `ORDER BY` on `table_changes()` (documented
     as a known gap in "3c. Debezium envelope" for preimage/postimage
     pairing, but not fixed for cross-commit-version order) — so Databricks
     could return a later commit's row before an earlier one's, and RisingWave
     (applying before/after/op events strictly in delivery order) ended up
     one version behind for that key. Fixed by adding `ORDER BY _commit_version`
     to the `table_changes()` query. Confirmed self-correcting: a *second*
     watermark-reset + backfill with the fix in place, appended after the
     first (wrongly-ordered) one, converged `id=2` to the correct final value
     without needing to touch the Kafka topic or drop the RisingWave table —
     each key's row is just overwritten by whatever arrives last.
   - **Final confirmed state**, `SELECT * FROM reverse_etl_cdf_poc_current`:
     `id=1 → "first"`, `id=2 → "second-updated"`, no `id=3` row — exactly
     matching the Databricks source table's true current state (`id=3` was
     deleted). RisingWave is doing real upserts and deletes against its own
     table storage, sourced directly from the Debezium-shaped Kafka topic —
     no reconstruction view involved.

8. **Duplicate-key discovery, a third grouping bug, and the actual limit of
   "mirroring" a table Databricks doesn't enforce uniqueness on.** After
   manually running an ad-hoc insert/delete/update against the Databricks
   table, its row count stopped matching RisingWave's: Databricks showed
   3 rows (`id=1` appearing *twice*), RisingWave showed 2. Root cause of the
   duplicate: `scripts/reverse_etl_poc_seed.py` did a blind `INSERT`, no
   uniqueness constraint stops a second run from inserting `id=1`/`id=2`
   again as fresh physical rows — so having run the seed script twice during
   earlier testing left real duplicate-keyed rows sitting in the source
   table. Two distinct fixes came out of chasing this:
   - **Explored: can Databricks enforce a real PRIMARY KEY the way RisingWave
     does?** Checked current Databricks docs rather than assume. Answer:
     no — Unity Catalog PK/FK/UNIQUE constraints are **informational only**,
     by permanent design ("Databricks does not enforce uniqueness during
     writes to avoid the massive performance overhead"), unlike `NOT NULL`/
     `CHECK`, which *are* enforced. `ALTER TABLE ... ADD CONSTRAINT ...
     PRIMARY KEY` would not have stopped the duplicates. Databricks' own
     documented recommendation for real uniqueness: enforce it in the write
     path via `MERGE INTO`, not a database-level constraint.
   - **Fixed the actual duplicates**: deleted both `id=1` rows and
     re-inserted a single fresh one (a plain `DELETE WHERE id=1` cannot
     target "just one" of two value-identical rows — no distinguishing
     column exists — so delete-both-then-reinsert-one is the practical fix).
     Databricks and RisingWave now match exactly.
   - **Fixed `scripts/reverse_etl_poc_seed.py`** to `MERGE INTO` (upsert) for
     the insert step instead of blind `INSERT`, so re-running it can no
     longer recreate this class of duplicate.
   - **Found and fixed a third real bug in `_to_debezium_events` while
     investigating**: a duplicate-keyed source row is not just a source-data
     hygiene issue, it's a *sync* correctness issue too. A single commit
     touching two physical rows sharing the same key (e.g. `UPDATE ... WHERE
     id = 1` hitting both duplicates) produces two `update_preimage` +
     two `update_postimage` rows, all sharing the same key and
     `_commit_version` — confirmed live via a direct `table_changes()` query.
     The grouping code kept a single dict entry per `(key, commit_version,
     change_type)`, so the second occurrence silently overwrote the first,
     collapsing 2 real row-level changes into 1 emitted event. It happened
     not to produce a visible discrepancy so far only because every
     duplicate pair in this POC's test data always carried identical values.
     Fixed by accumulating a *list* per change type instead of a single row,
     and pairing `update_preimage`/`update_postimage` positionally
     (`itertools.zip_longest`) rather than assuming exactly one of each per
     key per commit. Verified offline against a reconstruction of the exact
     live scenario (2 duplicate deletes + 2 duplicate update pairs sharing
     commit versions) — all 4 events now correctly preserved instead of
     collapsing to 2.

9. **Kafka messages surfaced in the Dagster UI, and a metadata-surfacing bug
   found and fixed in the process (2026-09-28).** To make it possible to
   inspect what a `reverse_etl_cdf_to_kafka` run actually produced without
   going to `kcat`/Redpanda console, both `reverse_etl_poc_table_setup` and
   `reverse_etl_cdf_to_kafka` were changed from `return {...}` to
   `context.add_output_metadata({...})` — the repo's existing convention
   (`databricks_optimize.py`, `landing_to_bronze.py`, etc.). Returning a
   plain dict from an `@asset` function does not surface it as UI metadata;
   Dagster instead treats it as the asset's own output value and silently
   pickles it to `/workspace/storage/<asset_name>` via the default IO
   manager — confirmed live: the op-count/event-count metadata added
   earlier was never visible in the Dagster UI, only the pickled-output
   `path` was.
   - `reverse_etl_cdf_to_kafka` now also reports `kafka_messages`: the
     decoded `(key, value)` pairs actually produced this run (via a new
     `_message_previews()` helper), capped at `MESSAGE_PREVIEW_LIMIT = 50`
     with a `kafka_messages_truncated` boolean flag, so a full backfill
     doesn't dump unbounded data into run metadata.
   - **First-run gotcha, not a bug**: materializing `reverse_etl_poc_setup_job`
     against a freshly-created source table produced 0 events and left both
     the Kafka topic and `reverse_etl_cdf_poc_current` empty — the setup
     job only creates the tables/topic and runs an initial (empty) sync, it
     does not seed any data itself. Running `bin/3_run_reverse_etl_seed.sh`
     followed by re-materializing `reverse_etl_cdf_to_kafka` produced real
     events, visible via the new `kafka_messages` metadata, and
     `reverse_etl_cdf_poc_current` populated correctly in RisingWave —
     confirmed live.
