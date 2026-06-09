# Landing → Bronze Architecture

This document describes the raw-bytes landing layer and the re-processing pipeline that decodes those bytes into a typed bronze table. The pattern was implemented as part of the Brazil PoC and demonstrates how RisingWave and Databricks can collaborate to handle Protobuf schema evolution without data loss.

---

## Overview

```
Kafka topic (Protobuf bytes)
       │
       ├─── RisingWave ENCODE BYTES source ──► MV ──► Iceberg sink ──► Databricks rw_casino_landing
       │                                                               (raw bytes, append-only)
       └─── RisingWave ENCODE PROTOBUF source ──► MV ──► Iceberg sink ──► Databricks rw_casino_transactions
                                                                          (decoded, live bronze)

rw_casino_landing  ──► Dagster job ──► Databricks notebook (from_protobuf) ──► rw_casino_landing_bronze
                        (landing_to_bronze_job)                                (re-processed bronze)
```

The landing layer stores raw Kafka payload bytes permanently. When the Protobuf schema changes, the bronze table can be fully rebuilt from landing without replaying Kafka. The Dagster job is a full overwrite — re-running it is idempotent.

---

## 1. Landing Sources (RisingWave)

Two parallel Kafka sources read the same topics as the existing PROTOBUF sources, but using `ENCODE BYTES`. Each uses a distinct consumer group so they don't interfere with the live decode path.

### Casino (`dbt/models/casino_prd/src_casino_prd_landing.sql`)

```sql
CREATE TABLE IF NOT EXISTS {{ this }} (
    payload BYTEA
)
APPEND ONLY
INCLUDE KEY       AS kafka_key
INCLUDE TIMESTAMP AS kafka_timestamp
INCLUDE PARTITION AS kafka_partition
INCLUDE OFFSET    AS kafka_offset
WITH (
    connector                    = 'kafka',
    topic                        = 'cronus.casino.out.br',
    properties.bootstrap.server  = 'prd2-kafka-bootstrap.kaizengaming.net:443',
    properties.security.protocol = 'SSL',
    group.id.prefix              = 'rw-readonly-casino-landing',
    scan.startup.mode            = 'latest',
    source_rate_limit            = 1
)
FORMAT PLAIN ENCODE BYTES
```

### Sportsbook (`dbt/models/casino_prd/src_bets_br_landing.sql`)

Same pattern on topic `bets-out-br`, bootstrap `prd4-kafka-bootstrap.kaizengaming.net:443`, group `rw-readonly-bets-landing`.

**Key constraint:** `APPEND ONLY` must appear immediately after the column block, before any `INCLUDE` clauses. Placing it after `INCLUDE` causes a SQL parser error.

**Schema constraint:** `ENCODE BYTES` requires exactly one column of type `BYTEA`. All Kafka metadata (key, timestamp, partition, offset) is added via `INCLUDE` clauses, not as schema columns.

---

## 2. Materialized Views

MVs add a `year_month` computed column for partitioning and observability.

### `dbt/models/casino_prd/mv_casino_landing.sql`

```sql
SELECT
    payload,
    kafka_key,
    kafka_timestamp,
    kafka_partition,
    kafka_offset,
    TO_CHAR(kafka_timestamp AT TIME ZONE 'UTC', 'YYYY-MM') AS year_month
FROM {{ ref('src_casino_prd_landing') }}
```

`mv_sportsbook_landing.sql` is identical, referencing `src_bets_br_landing`.

---

## 3. Iceberg Sinks

Each MV has two sinks: one to Lakekeeper (internal REST catalog) and one to Databricks Unity Catalog.

### Databricks sinks

`dbt/models/casino_prd/sink_casino_landing_databricks.sql` and `sink_sportsbook_landing_databricks.sql` follow the same pattern as existing Databricks sinks in the project:

```sql
WITH (
    connector                   = 'iceberg',
    type                        = 'append-only',
    force_append_only           = 'true',
    catalog.type                = 'rest',
    catalog.uri                 = '{{ env_var("DBT_DATABRICKS_HOST") }}/api/2.1/unity-catalog/iceberg-rest',
    catalog.oauth2_server_uri   = 'https://login.microsoftonline.com/{{ env_var("DATABRICKS_AZURE_TENANT_ID") }}/oauth2/v2.0/token',
    catalog.credential          = '{{ env_var("DATABRICKS_AZURE_CLIENT_ID") }}:{{ env_var("DATABRICKS_AZURE_CLIENT_SECRET") }}',
    catalog.scope               = '2ff814a6-3304-4ab8-85cb-cd0e6f879c1d/.default',
    warehouse.path              = '{{ env_var("DATABRICKS_CATALOG") }}',
    database.name               = 'rw_poc',
    table.name                  = 'rw_casino_landing',   -- or rw_sportsbook_landing
    adlsgen2.account_name       = '{{ env_var("ADLS_ACCOUNT_NAME") }}',
    adlsgen2.account_key        = '{{ env_var("ADLS_ACCOUNT_KEY") }}',
    commit_checkpoint_interval  = 5
)
```

**Important:** the Databricks tables must be created **without** `PARTITIONED BY`. The `year_month` column exists in the data but must not be a partition spec at the table level — RisingWave's Iceberg sink cannot write to tables that have an Iceberg partition spec referencing a column by index beyond the base schema columns. Attempting this produces `invalid extra partition column index 7`.

---

## 4. Databricks Tables

### Landing tables

Created in `de_dev.rw_poc` without partitioning:

```sql
CREATE TABLE IF NOT EXISTS de_dev.rw_poc.rw_casino_landing
USING ICEBERG
AS SELECT
    CAST(NULL AS BINARY)    AS payload,
    CAST(NULL AS BINARY)    AS kafka_key,
    CAST(NULL AS TIMESTAMP) AS kafka_timestamp,
    CAST(NULL AS INT)       AS kafka_partition,
    CAST(NULL AS BIGINT)    AS kafka_offset,
    CAST(NULL AS STRING)    AS year_month
WHERE 1 = 0;
```

`rw_sportsbook_landing` follows the same schema.

### Bronze table

```sql
CREATE TABLE IF NOT EXISTS de_dev.rw_poc.rw_casino_landing_bronze
USING ICEBERG
AS SELECT
    CAST(NULL AS INT)       AS customer_id,
    CAST(NULL AS INT)       AS message_type_id,
    CAST(NULL AS INT)       AS account_id,
    CAST(NULL AS INT)       AS currency_id,
    CAST(NULL AS TIMESTAMP) AS transaction_created_at,
    CAST(NULL AS DOUBLE)    AS amount_abs,
    CAST(NULL AS STRING)    AS properties
WHERE 1 = 0;
```

The schema mirrors `rw_casino_transactions` (the live PROTOBUF-decoded bronze) so the two tables are directly comparable.

---

## 5. Protobuf Descriptor

The compiled binary descriptor for `CasinoRoundInfoDto` is stored in a Unity Catalog Volume so the notebook can read it at runtime without embedding bytes in code. Swapping the descriptor file (e.g. after a schema change) is sufficient to update the decode — no notebook changes required.

- **Volume:** `de_dev.rw_poc.proto`
- **Volume path:** `/Volumes/de_dev/rw_poc/proto/casinoroundinfodto.pb`
- **Local copy:** `proto/casinoroundinfodto.pb`

To re-upload the descriptor after a schema change:

```python
# Via Databricks MCP or CLI
manage_volume_files(action="upload",
                    volume_path="/Volumes/de_dev/rw_poc/proto/casinoroundinfodto.pb",
                    local_path="proto/casinoroundinfodto.pb")
```

### Proto message structure

```
CasinoRoundInfoDto
  ├── CustomerId (int32)
  ├── CompanyId, CasinoProviderId, ExternalProviderId (int32)
  ├── GameInfo
  │     ├── GameId, GameType (int32)
  │     ├── IsLive (bool)
  │     └── ProviderGameCode (string)
  └── RoundInfo
        ├── GameRoundRef (string)
        ├── RoundCreated (google.protobuf.Timestamp)
        └── Messages[] → CasinoMessageInformation
              ├── MessageTypeId, MessageId, SessionId, TokenTypeId (int32)
              ├── JackpotWinAmount, JackpotContributionAmount (string)
              ├── IsRoundClosed (bool)
              └── Transactions[] → TransactionInformation
                    ├── TransactionId (int64)
                    ├── Created (google.protobuf.Timestamp)
                    ├── CurrencyId, AccountId, SourceId (int32)
                    ├── Amount, CurrencyRateToEuro, BonusAction (string)
                    └── PandoraJourneyId (int64), CustomerCampaignId, CampaignId,
                        CampaignTypeId, TransactionTypeId (int32)
```

Decoding notes:
- `google.protobuf.Timestamp` fields decode natively as `TimestampType` on Databricks Runtime 13.3+. No `.seconds` / `.nanos` extraction needed.
- `Amount` and similar monetary fields are encoded as strings. Cast to `DOUBLE` and handle empty string as `NULL`.
- Two `explode()` calls are required: first on `Messages[]`, then on `Transactions[]` within each message.

---

## 6. Re-processing Notebook

**Workspace path:** `/Workspace/Shared/rw_poc/landing_to_bronze_casino`  
**Source file:** `notebooks/landing_to_bronze_casino.py`

The notebook reads all current rows from the landing table, decodes them, and overwrites the bronze table. It is a **full re-process** — safe to re-run at any time.

### Cell-by-cell summary

**Cell 1 — Setup**
```python
from pyspark.sql.functions import col, explode, abs as spark_abs, when, to_json, struct, lit
from pyspark.sql.protobuf.functions import from_protobuf

DESCRIPTOR_PATH = "/Volumes/de_dev/rw_poc/proto/casinoroundinfodto.pb"
MESSAGE_NAME    = "Cronus.CasinoService.RoundInfo.Abstractions.CasinoRoundInfoDto"
LANDING_TABLE   = "de_dev.rw_poc.rw_casino_landing"
BRONZE_TABLE    = "de_dev.rw_poc.rw_casino_landing_bronze"

with open(DESCRIPTOR_PATH, "rb") as f:
    descriptor_bytes = f.read()
```

**Cell 2 — Read landing**
```python
df_raw = spark.table(LANDING_TABLE)
```

**Cell 3 — Decode Protobuf**
```python
df_decoded = df_raw.select(
    from_protobuf(col("payload"), MESSAGE_NAME,
                  binaryDescriptorSet=descriptor_bytes).alias("msg"))
```

**Cell 4 — Explode Messages → Transactions**
```python
df_messages = df_decoded.select(
    col("msg.CustomerId").alias("customer_id"), ...,
    explode("msg.RoundInfo.Messages").alias("message"))

df_transactions = df_messages.select(
    ..., explode("message.Transactions").alias("txn"))
```

**Cell 5 — Build bronze columns**
```python
df_bronze = df_transactions.select(
    col("customer_id"),
    col("message_type_id"),
    col("txn.AccountId").alias("account_id"),
    col("txn.CurrencyId").alias("currency_id"),
    col("txn.Created").alias("transaction_created_at"),
    spark_abs(when(col("txn.Amount") != "", col("txn.Amount").cast("double"))
              .otherwise(lit(None).cast("double"))).alias("amount_abs"),
    to_json(struct(col("company_id"), col("casino_provider_id"), ...)).alias("properties"))
```

**Cell 6 — Write and exit**
```python
df_bronze.write.format("iceberg").mode("overwrite").saveAsTable(BRONZE_TABLE)

bronze_count = spark.table(BRONZE_TABLE).count()
print(f"✓ Written {bronze_count:,} rows to {BRONZE_TABLE}")

dbutils.notebook.exit(str(bronze_count))  # consumed by Dagster for metadata
```

**Cell 7 — Verification query**

Compares row counts, unique customers, and timestamp ranges between `rw_casino_landing_bronze` (re-processed) and `rw_casino_transactions` (live PROTOBUF decode).

---

## 7. Dagster Orchestration

**Asset:** `landing_to_bronze_casino`  
**Job:** `landing_to_bronze_job`  
**Source file:** `orchestration/assets/landing_to_bronze.py`

The asset submits the notebook as a one-off run via the Databricks Runs Submit API, polls until it completes, then reads the notebook's exit value to capture the bronze row count as Dagster metadata.

### Key functions

| Function | Purpose |
|---|---|
| `_get_token()` | Obtains an Azure AD token for the service principal via client credentials flow |
| `_submit_run(token)` | Posts to `/api/2.1/jobs/runs/submit` with `existing_cluster_id` |
| `_poll_run(token, run_id)` | Polls `/api/2.1/jobs/runs/get` until `TERMINATED` / `INTERNAL_ERROR` |
| `_get_notebook_row_count(token, run_id)` | Reads `notebook_output.result` from `/api/2.1/jobs/runs/get-output`; parses the integer returned by `dbutils.notebook.exit()` |

### Asset metadata surfaced in Dagster UI

| Key | Type | Description |
|---|---|---|
| `run_id` | int | Databricks run ID |
| `result` | text | `SUCCESS` / `FAILED` |
| `run_page_url` | url | Direct link to the run in Databricks UI |
| `row_count` | int | Row count of `rw_casino_landing_bronze` after the write |

### Dependency

The asset declares `AssetDep` on `sink_casino_landing_databricks`, so Dagster knows the landing sink must be materialized before the bronze reprocess runs.

### Cluster

The run uses `existing_cluster_id: 1216-211611-qcydcvqf` ("Swansea All-Purpose Dev"). Databricks auto-starts the cluster if it is TERMINATED when the job is submitted.

> **Note:** Serverless compute is not enabled on this workspace. Enabling it requires account-level admin access at `accounts.azuredatabricks.net`.

---

## 8. Design Decisions and Gotchas

### Why ENCODE BYTES instead of decoding everything in RisingWave?

RisingWave decodes Protobuf at ingestion time. If the schema changes, any field added or renamed after the source was created is silently dropped — there is no way to re-decode old messages from within RisingWave. The BYTES landing layer preserves the original wire-format bytes, so a schema update + notebook re-run reconstructs the full history with the new schema.

### Why a parallel source instead of replacing the PROTOBUF source?

The PROTOBUF source feeds the live `rw_casino_transactions` table, which is used for real-time queries. The BYTES source runs on a separate consumer group and separate MVs/sinks so the two paths are fully independent. A failure or rebuild of the landing path has no impact on the live decode path.

### Why overwrite mode in the notebook?

The landing table is append-only and grows continuously. Merging or upserting into the bronze table would require a stable unique key across the nested Protobuf structure. Overwrite is simpler, always consistent, and the landing table is the single source of truth — the bronze table is derived and fully reproducible.

### Why read the row count from `dbutils.notebook.exit()` instead of querying the SQL warehouse?

The service principal (`spn-dtbr-users-dev-crf`) does not have `CAN_USE` permission on the SQL warehouse. The notebook exit value avoids any SQL warehouse dependency — the count comes directly from the Spark job that wrote the data, read back via the Runs Output API.

### Why store the notebook in `/Workspace/Shared/`?

The service principal's home folder (`/Workspace/Users/3b7f531f-db93-4186-af75-6566c12c076b/`) is not accessible when the same service principal submits a run via the Jobs API — Databricks treats job runner permissions differently from workspace file ownership. `/Workspace/Shared/` is accessible to all workspace identities.

---

## 9. Row Count Relationship: Landing vs Bronze

The bronze table has **more rows** than the landing table. This is expected.

Each row in `rw_casino_landing` is one Kafka message — one `CasinoRoundInfoDto` on the wire. The notebook applies two `explode()` calls to unpack the nested arrays:

```
rw_casino_landing        (1 row per Kafka message / CasinoRoundInfoDto)
  └── explode(RoundInfo.Messages[])
        └── explode(message.Transactions[])
              = rw_casino_landing_bronze  (1 row per transaction)
```

A single Protobuf message can contain multiple `Messages`, each with multiple `Transactions`. The ratio reflects the average number of transactions per round event — for example, 3,376 bronze rows from 2,715 landing rows equals ~1.24 transactions per message on average.

This is consistent with `rw_casino_transactions` (the live PROTOBUF-decoded path), which is also at the transaction grain and uses the same two-level explosion in RisingWave.

---

## 10. Automatic Optimization (Databricks Predictive Optimization)

Predictive Optimization is **enabled at the `de_dev.rw_poc` schema level**, so all three landing/bronze tables inherit it automatically — no per-table configuration is required.

```
DESCRIBE SCHEMA EXTENDED de_dev.rw_poc;
-- Predictive Optimization | ENABLE
```

What this means in practice:

- **OPTIMIZE (compaction)** — Databricks automatically compacts small Parquet files in the background. This is especially important for the landing tables: RisingWave commits on a 5-second `commit_checkpoint_interval`, producing one small file per checkpoint. Without compaction, read performance degrades quickly.
- **VACUUM** — Databricks automatically removes orphaned files and expired snapshots, keeping storage costs under control without manual maintenance.

The `gc.enabled = false` property visible in `SHOW TBLPROPERTIES` is the Iceberg-level garbage-collection flag. Predictive Optimization operates at the Databricks/UC layer above this and is not affected by it.

---

## 11. Re-processing Workflow (Schema Change)

1. Update the `.proto` file and recompile the descriptor:
   ```bash
   protoc --descriptor_set_out=proto/casinoroundinfodto.pb \
          --include_imports casinoroundinfodto.proto
   ```
2. Upload the new descriptor to the UC Volume (replaces the old file):
   ```bash
   # via Databricks CLI or MCP manage_volume_files
   ```
3. If columns were added to the bronze table schema, run `ALTER TABLE de_dev.rw_poc.rw_casino_landing_bronze ADD COLUMN ...` in Databricks.
4. Trigger `landing_to_bronze_job` in Dagster. The notebook overwrites the bronze table using the new descriptor against all bytes accumulated in the landing table.
5. Verify the `row_count` metadata in the Dagster run and cross-check against `rw_casino_transactions`.
