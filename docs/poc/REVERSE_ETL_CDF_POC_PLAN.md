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
  See "Auth for the live run" below for which client issues this call.
- **Kafka side**: `confluent_kafka.Producer` (already a repo dependency)
  against `KAFKA_OUTPUT_BOOTSTRAP` + `KAFKA_OUTPUT_SASL_*`, matching
  `kafka_topics_setup.py`'s `_admin_client()` credential convention exactly
  — deliberately *not* the `KAFKA_SASL_*` / `KAFKA_SECURITY_PROTOCOL` naming
  used by `scripts/produce_protobuf_casino_rounds.py`, to avoid introducing
  a third env-naming scheme for what is the same "topics this project
  writes to" credential set.
- **Payload**: plain JSON per row, including CDF's `_change_type` /
  `_commit_version` / `_commit_timestamp` columns.
- **Watermark logic**: the pure, client-agnostic functions from
  `dagster-poc/reverse-etl/src/reverse_etl/defs/cdf_to_kafka.py`
  (`_summarize_change_types`, `_next_watermark`, `_build_kafka_message`)
  ported unchanged — they have no Databricks/Kafka client dependency.
  `_read_last_version` / `_write_last_version` / `_read_changes` /
  `_get_current_version` are rewritten against `_run_sql`'s REST response
  shape instead of a DB-API cursor.

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

### 4. Dagster wiring

`orchestration/definitions.py`: import `reverse_etl_poc_table_setup` and
`reverse_etl_cdf_to_kafka` alongside the existing `casino_prd_setup` import
block (~lines 29–35), add both to the `Definitions` asset list, group_name
`reverse_etl_poc`.

### 5. Synthetic traffic (minimal)

A small script, `scripts/reverse_etl_poc_seed.py`, issuing a handful of
INSERT/UPDATE/DELETE statements against
`de_dev.sr_poc_external.reverse_etl_cdf_poc_source` (same client as the main
asset — see "Auth for the live run") — just enough for the POC to have
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

## Explicitly not doing (per scope decision)

- No live execution against real staging Kafka, Apicurio, or the real
  Databricks workspace as part of this change. `CREATE TABLE`, topic
  creation, and the CDF read/produce step only run when the assets are
  explicitly materialized later.
- No Apicurio schema registration (JSON payload, no schema registry).
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

## Live run results — ✅ end-to-end verified

All five verification steps above were run live against the real workspace
and staging Kafka on 2026-09-25. Three real issues surfaced and were fixed
in the process (beyond the auth swap in "Auth for the live run" above):

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
