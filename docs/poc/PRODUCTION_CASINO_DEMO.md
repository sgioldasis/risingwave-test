# Production Casino Demo — RisingWave Streaming Pipeline

Real-time streaming pipeline that reads live casino and sportsbook data from Kaizen production Kafka clusters into RisingWave, computes two customer-level metrics, and lands results into Lakekeeper-managed Iceberg tables.

> **Status:** working as of 2026-06-13. RisingWave **v3.0.0** (upgraded from 2.7.4 → 2.8.0 → 3.0.0 — see [BRAZIL_WORKLOAD_TUNING.md](BRAZIL_WORKLOAD_TUNING.md) §19, §20), Lakekeeper v0.12.3, MinIO local, Trino 481. No producer required — the pipeline consumes live production topics directly.
> _Known limitations re-verified on v3.0.0 (2026-06-13): UNNEST/watermark **FIXED** (since 2.8.0); snapshot expiration **FIXED** (v3.0.0 — see §20); `CREATE VIEW` over Iceberg sources **broken by default** (DataFusion plan error — workaround: `SET enable_datafusion_engine = false`, auto-applied via dbt `pre_hook`); `enable_pk_index = 'true'` causes compute node crash (executor not yet shipped in v3.0.0)._
>
> **Architecture notes (read first):**
> - Sources use `scan.startup.mode = 'latest'` — fast, history-free startup; the 5-minute rolling windows fill over time as live events arrive. `'earliest'` (full-history backfill) is available but slow/unpredictable on this cluster — run it off-demo, not interactively.
> - Iceberg sinks use `connector = 'iceberg'` (write directly to Lakekeeper), **not** `ENGINE = iceberg` managed tables. The sink auto-creates the Iceberg table.
> - The `rw_managed_*` Iceberg tables are queried via **Trino** (`datalake` catalog), not RisingWave — there are no RisingWave Iceberg read-sources.
> - RisingWave-native compaction works (`enable_compaction` + `compaction.trigger_snapshot_count`); snapshot expiration also works as of v3.0.0 (was broken through v2.8.4 — see §7 and BRAZIL_WORKLOAD_TUNING.md §20).
>
> **High-volume tuning:** for running this demo against the high-volume **Brazil** topics
> (`cronus.casino.out.br`, `bets-out-br`) on a single-node stack — including the
> `source_rate_limit`, rolling-window, MinIO/Hummock, Iceberg-commit, Dagster, and Trino
> changes (with reasoning for each) — see
> [BRAZIL_WORKLOAD_TUNING.md](BRAZIL_WORKLOAD_TUNING.md).
> - **Window sizes** are now **5 minutes** for both UC1 and UC2 (tuned down for the high-volume
>   single-node demo — see [BRAZIL_WORKLOAD_TUNING.md](BRAZIL_WORKLOAD_TUNING.md) §13). Object/column
>   names keep their historical labels (`mv_casino_turnover_90d`, `mv_sportsbook_turnover_90d`,
>   `rolling_1d_real_bet_amount`) — only the `RANGE` interval changed.
---

## 1. Architecture

```
┌──────────────────────────┐  one-shot fetch+compile  ┌────────────────────────┐
│ Kaizen Apicurio Registry │ ────────────────────────▶│  MinIO (hummock001)     │
│ (staging-schema-registry)│                          │  proto/casino…dto.pb   │
│   bigdata/casinoroundinfo│                          │  proto/betinfo.desc    │
└──────────────────────────┘                          └──────────┬─────────────┘
                                                                 │ s3:// fetch at
                                                                 ▼ CREATE TABLE time
┌──────────────────────────┐    SSL one-way   ┌──────────────────────────────────────────────┐
│ Kaizen prd2 Kafka        │ ────────────────▶│  RisingWave (compute-node-0)                 │
│ cronus.casino.out.br     │                  │                                              │
└──────────────────────────┘                  │  UC1: mv_casino_transactions_full            │
                                              │       → mv_casino_real_bet (rolling 5m) ──────┼──▶ casino_real_bet_output
┌──────────────────────────┐    SSL one-way   │                                              │     (Redpanda, JSON)
│ Kaizen prd4 Kafka        │ ────────────────▶│  UC2: mv_casino_turnover_90d (5m)            │
│ bets-out-br              │                  │       mv_sportsbook_turnover_90d (5m)        │
└──────────────────────────┘                  │       → mv_turnover_percentage ──────────────┼──▶ casino_turnover_percentage_output
                                              └────────────┬─────────────────────────────────┘     (Redpanda, JSON)
                                                           │ connector='iceberg' upsert sinks (~30-40s)
                                                           ▼
                                              ┌─────────────────────────────────┐
                                              │ Lakekeeper REST catalog          │
                                              │ + MinIO S3 (hummock001)          │
                                              │  rw_managed_casino_real_bet      │◀── queried via Trino
                                              │  rw_managed_turnover_percentage  │    (datalake catalog)
                                              └─────────────────────────────────┘
```

---

## 2. Production Environment

| | |
|---|---|
| Casino Kafka bootstrap | `prd2-kafka-bootstrap.kaizengaming.net:443` |
| Casino topic | `cronus.casino.out.br` (20 partitions; high-volume Brazil tenant — bursts of ~95 k msgs/min observed) |
| Bets Kafka bootstrap | `prd4-kafka-bootstrap.kaizengaming.net:443` |
| Bets topic | `bets-out-br` (10 partitions) |
| Security | SSL one-way TLS, DigiCert `*.kaizengaming.net`. No SASL, no client cert. |
| Wire format | Raw protobuf — no Confluent 5-byte framing prefix |
| Schema registry | `http://staging-schema-registry.kaizengaming.net` (Apicurio v2 native) |
| Casino message FQN | `Cronus.CasinoService.RoundInfo.Abstractions.CasinoRoundInfoDto` |
| Bets message FQN | `PandoraBetInfoVm` |

---

## 3. Preparation

All steps in this section are prerequisites for both UC1 and UC2. Run them once; they are idempotent.

### 3.1 Proto schema compilation and upload

RisingWave's `schema.location` requires a compiled binary `FileDescriptorSet` (`.pb`/`.desc`), not `.proto` source text. Apicurio's native v2 endpoint returns the raw `.proto` text; the ccompat endpoint returns binary FDS in a format RisingWave cannot use. The correct flow is: fetch → compile → upload to MinIO → RisingWave fetches at `CREATE TABLE` time.

**Casino schema:**

```bash
# Fetch .proto source from Apicurio native v2
curl -fsSL -H 'Accept: text/plain' \
  http://staging-schema-registry.kaizengaming.net/apis/registry/v2/groups/bigdata/artifacts/casinoroundinfo \
  > proto/casinoroundinfodto.proto

# Compile to binary FileDescriptorSet (--include_imports bakes in google.protobuf.Timestamp)
protoc --include_imports \
  --descriptor_set_out=proto/casinoroundinfodto.pb \
  proto/casinoroundinfodto.proto

# Upload to MinIO — RisingWave fetches from here at CREATE TABLE time
AWS_ACCESS_KEY_ID=hummockadmin AWS_SECRET_ACCESS_KEY=hummockadmin \
  aws --endpoint-url http://localhost:9301 s3 cp \
    proto/casinoroundinfodto.pb s3://hummock001/proto/casinoroundinfodto.pb
```

**Bets schema (UC2):**

```bash
curl -fsSL -H 'Accept: text/plain' \
  http://staging-schema-registry.kaizengaming.net/apis/registry/v2/groups/bigdata/artifacts/betinfo \
  > proto/betinfo.proto

# Note: --proto_path includes homebrew well-known types (google/protobuf/*.proto)
protoc --include_imports \
  --descriptor_set_out=proto/betinfo.desc \
  --proto_path=/opt/homebrew/include \
  --proto_path=proto \
  proto/betinfo.proto

AWS_ACCESS_KEY_ID=hummockadmin AWS_SECRET_ACCESS_KEY=hummockadmin \
  aws --endpoint-url http://localhost:9301 s3 cp \
    proto/betinfo.desc s3://hummock001/proto/betinfo.desc
```

### 3.2 Start services

```bash
docker compose up -d \
  minio-0 meta-node-0 compute-node-0 compactor-0 frontend-node-0 \
  lakekeeper-db lakekeeper-migrate lakekeeper lakekeeper-bootstrap \
  prometheus-0 grafana-0
```

Wait for RisingWave to accept connections:

```bash
psql postgresql://root@localhost:4566/dev -tAc 'SELECT 1'
```

### 3.3 Create the casino source table (`src_casino_prd`)

```sql
SET client_min_messages = WARNING;
DROP TABLE IF EXISTS src_casino_prd CASCADE;

CREATE TABLE src_casino_prd (*)
APPEND ONLY
WITH (
    connector                     = 'kafka',
    topic                         = 'cronus.casino.out.br',
    properties.bootstrap.server   = 'prd2-kafka-bootstrap.kaizengaming.net:443',
    properties.security.protocol  = 'SSL',
    group.id.prefix               = 'rw-readonly-casino-demo',
    scan.startup.mode             = 'latest'
)
FORMAT PLAIN ENCODE PROTOBUF (
    schema.location  = 's3://hummock001/proto/casinoroundinfodto.pb',
    message          = 'Cronus.CasinoService.RoundInfo.Abstractions.CasinoRoundInfoDto',
    s3.region        = 'us-east-1',
    s3.endpoint      = 'http://minio-0:9301',
    s3.access.key    = 'hummockadmin',
    s3.secret.key    = 'hummockadmin'
);
```

**Key clauses explained:**

| Clause | Why |
|--------|-----|
| `CREATE TABLE` (not `SOURCE`) | Persists the Kafka topic into RisingWave's internal state store. A `SOURCE` re-scans Kafka at read time, making batch `SELECT COUNT(*)` disagree with streaming MV counts; a `TABLE` materializes once so both paths agree. |
| `(*)` | Auto-discovers all columns from the protobuf FileDescriptorSet. No manual schema declaration needed. |
| `APPEND ONLY` | Casino rounds are never updated or deleted — telling RisingWave this eliminates delete-tracking overhead and enables downstream MVs to skip retraction handling. |
| `scan.startup.mode = 'latest'` | Fast, history-free startup — MVs materialize in seconds and the rolling windows fill over time. Use `'earliest'` for full-history backfill, but on this cluster that grinds under backpressure (UC2's rolling-window MVs) and can take hours — run it off-demo, not interactively. |
| `source_rate_limit = 1` | Sources are created rate-limited to ~0 rows/s so almost no data accumulates while downstream MVs are built (fast, backfill-free creation). The Dagster pipeline ramps this to `200` after the build. See [BRAZIL_WORKLOAD_TUNING.md](BRAZIL_WORKLOAD_TUNING.md) §1. |
| `schema.location = 's3://...'` | RisingWave fetches the compiled `.pb` at DDL time. No container volume mount required. |

> **dbt note:** the dbt models add a `pre_hook` that runs `DROP TABLE IF EXISTS … CASCADE` before recreating the source, so the `scan.startup.mode` change takes effect cleanly on every build.

Top-level columns decoded from protobuf (PascalCase — must be double-quoted in SQL):

```
"UniqueId"            VARCHAR
"CustomerId"          INT
"CompanyId"           INT
"CasinoProviderId"    INT
"ExternalProviderId"  INT
"GameInfo"            STRUCT<"GameId" INT, "ProviderGameCode" VARCHAR, "IsLive" BOOLEAN,
                              "ProviderTableCode" VARCHAR, "GameType" INT,
                              "IsJackpotContributionsFromOperator" BOOLEAN>
"RoundInfo"           STRUCT<"GameRoundRef" VARCHAR,
                              "RoundCreated" STRUCT<seconds BIGINT, nanos INT>,
                              "RoundEnded"   STRUCT<seconds BIGINT, nanos INT>,
                              "Messages"     STRUCT<"MessageTypeId" INT, "AccountId" INT,
                                                    "Created" STRUCT<seconds BIGINT, nanos INT>,
                                                    "Transactions" STRUCT<...>[], ...>[]>
"IsBonusLockedOnFatMessageCreation"  BOOLEAN
"IsBonusCampaignWagering"            BOOLEAN
```

Verify the table exists and is ingesting:

```sql
SELECT COUNT(*) FROM src_casino_prd;
-- Expected: starts near 0 and grows as new Kafka messages arrive (from 'latest')
```

### 3.4 Create the bets source table (`src_bets_br`)

Required for UC2 only. Can be created after UC1 is running.

```sql
SET client_min_messages = WARNING;
DROP TABLE IF EXISTS src_bets_br CASCADE;

CREATE TABLE src_bets_br (*)
APPEND ONLY
WITH (
    connector                         = 'kafka',
    topic                             = 'bets-out-br',
    properties.bootstrap.server       = 'prd4-kafka-bootstrap.kaizengaming.net:443',
    properties.security.protocol      = 'SSL',
    group.id.prefix                   = 'rw-readonly-bets-demo',
    scan.startup.mode                 = 'latest'
)
FORMAT PLAIN ENCODE PROTOBUF (
    schema.location   = 's3://hummock001/proto/betinfo.desc',
    message           = 'PandoraBetInfoVm',
    messages_as_jsonb = 'PlayerSubstitutionInfoVm',
    s3.region         = 'us-east-1',
    s3.endpoint       = 'http://minio-0:9301',
    s3.access.key     = 'hummockadmin',
    s3.secret.key     = 'hummockadmin'
);
```

**`messages_as_jsonb = 'PlayerSubstitutionInfoVm'`:** `PlayerSubstitutionInfoVm` is self-referential (contains a field of its own type), creating a recursive schema RisingWave cannot represent as a native `STRUCT`. This option decodes that specific message type as `JSONB` instead. UC2 only reads `TotalStake.Euro` and `PlacedAt`, so these JSONB columns are never accessed — zero cost.

### 3.5 Iceberg connection and session settings

The `connector='iceberg'` sinks reference a named `CONNECTION` object that holds the Lakekeeper REST catalog + MinIO S3 credentials. Create it once:

```sql
CREATE CONNECTION IF NOT EXISTS lakekeeper_catalog_conn WITH (
    type                  = 'iceberg',
    catalog.type          = 'rest',
    catalog.uri           = 'http://lakekeeper:8181/catalog/',
    warehouse.path        = 'risingwave-warehouse',
    s3.access.key         = 'hummockadmin',
    s3.secret.key         = 'hummockadmin',
    s3.path.style.access  = 'true',
    s3.endpoint           = 'http://minio-0:9301',
    s3.region             = 'us-east-1'
);

-- Required before creating MVs that share the same source —
-- prevents RisingWave from spawning one Kafka consumer per MV chain.
SET streaming_use_shared_source = true;
```

> The sinks reference this connection via `connection = lakekeeper_catalog_conn` in their `WITH (...)` clause (see §4.3). We no longer use `ENGINE = iceberg` managed tables or `SET iceberg_engine_connection`.

---

## 4. UC1 — Casino Real Bet Amount

### Business Definition

> *Total amount of real money bets placed by the customer in the casino over the past two weeks. Used to assess recent customer activity, identify trends, and design targeted engagement or retention strategies.*
>
> — PoC Document §4.1

### Pipeline

```
src_casino_prd
  → mv_casino_transactions_full     (flatten nested protobuf: one row per transaction + properties JSON blob)
  → mv_casino_real_bet              (filter UC1 bets + rolling 5-min real-bet SUM)
     ├── sink_casino_real_bet    →  rw_managed_casino_real_bet  (Iceberg, upsert)
     └── sink_casino_real_bet_kafka → casino_real_bet_output    (Redpanda, JSON, append)
```

### 4.1 `mv_casino_transactions_full` — Transaction-level flat view

This is the shared foundation for UC1, UC2, and the Databricks/Lakekeeper sinks. The casino protobuf message has a deeply nested structure: each `CasinoRoundInfoDto` contains a `RoundInfo.Messages[]` array, and each message contains a `Transactions[]` array. This MV unnests both levels to produce one flat row per transaction. It exposes 6 core typed columns plus a `properties` JSONB blob that carries all other proto fields.

```sql
SET streaming_use_shared_source = true;

CREATE MATERIALIZED VIEW mv_casino_transactions_full AS
SELECT
    s."CustomerId"                                        AS customer_id,
    msg."MessageTypeId"                                   AS message_type_id,
    txn."AccountId"                                       AS account_id,
    txn."CurrencyId"                                      AS currency_id,
    TO_TIMESTAMP((txn."Created").seconds)                 AS transaction_created_at,
    ABS(NULLIF(txn."Amount", '')::numeric)                AS amount_abs,
    JSONB_BUILD_OBJECT(
        'message_created_at', TO_TIMESTAMP((msg."Created").seconds),
        'transaction_id',     txn."TransactionId",
        'amount_raw',         txn."Amount",
        'bonus_action',       txn."BonusAction",
        'game_id',            (s."GameInfo")."GameId",
        'game_type',          (s."GameInfo")."GameType",
        'is_live',            (s."GameInfo")."IsLive",
        'company_id',         s."CompanyId"
    )                                                     AS properties
FROM
    src_casino_prd                             AS s,
    UNNEST((s."RoundInfo")."Messages")         AS msg,
    UNNEST(msg."Transactions")                 AS txn;
```

**Key implementation notes:**

- **Struct field access needs parentheses.** `s."RoundInfo"."Messages"` is parsed as `schema.table.column` and fails. The correct form is `(s."RoundInfo")."Messages"`.
- **`UNNEST(struct[])` flattens to rows.** The alias (`msg`, `txn`) names the row, not a struct. Inner fields are accessed directly by name, e.g. `msg."MessageTypeId"` — not `msg.MessageTypeId` (unquoted folds to lowercase).
- **Double UNNEST with name collision.** Both `CasinoMessageInformation` and `TransactionInformation` have a `Created` field. Using row aliases (`AS msg`, `AS txn`) disambiguates: `msg."Created"` vs `txn."Created"`.
- **`Amount` is a string field.** Proto3 strings decode as `''` when absent. `NULLIF(txn."Amount", '')::numeric` safely handles empty strings; `ABS(...)` normalises sign (debits are negative in some message types).
- **`google.protobuf.Timestamp` → `TIMESTAMPTZ`.** The proto type lands as `STRUCT<seconds BIGINT, nanos INT>`. Convert with `TO_TIMESTAMP((ts).seconds)`.

Verify:

```sql
SELECT COUNT(*) FROM mv_casino_transactions_full;
-- Expected: higher than src_casino_prd because each round has multiple transactions

SELECT message_type_id, COUNT(*) FROM mv_casino_transactions_full GROUP BY 1 ORDER BY 1;
-- MessageTypeId=1 → bet placed
-- MessageTypeId=2 → payout/withdrawal
```

### 4.2 `mv_casino_real_bet` — Rolling 5-min real bet total

```sql
CREATE MATERIALIZED VIEW mv_casino_real_bet AS
SELECT
    customer_id,
    currency_id,
    transaction_created_at                                AS event_ts,
    SUM(amount_abs) OVER (
        PARTITION BY customer_id, currency_id
        ORDER BY transaction_created_at
        RANGE BETWEEN INTERVAL '300 SECONDS' PRECEDING AND CURRENT ROW
    )                                                     AS rolling_1d_real_bet_amount
FROM mv_casino_transactions_full
WHERE message_type_id = 1
  AND account_id      = 1
  AND amount_abs IS NOT NULL;
```

**Filters:** `message_type_id = 1` (bet placed), `account_id = 1` (real money, not bonus), `amount_abs IS NOT NULL` (excludes empty/null amounts).

**Window function:** `RANGE BETWEEN INTERVAL '300 SECONDS' PRECEDING AND CURRENT ROW` = 5 minutes. A sliding window — for every new bet event RisingWave emits an updated row with that customer's real-bet sum over the preceding 5 minutes. One row per event, not per customer; consumers key on `(customer_id, currency_id)` and keep the latest `event_ts`. (Window reduced 14d → 1d → 5min for the high-volume single-node demo — see [BRAZIL_WORKLOAD_TUNING.md](BRAZIL_WORKLOAD_TUNING.md) §2/§13; the `rolling_1d_real_bet_amount` column name is a kept misnomer.)

> **Currency note:** `currency_id` is an opaque integer from the Kaizen system. All observed rows in the GH dataset have `currency_id = 16`. The mapping to a named currency has not been confirmed — amounts are treated as unitless until Kaizen provides the reference table.

Verify:

```sql
SELECT COUNT(*) FROM mv_casino_real_bet;
-- Same count as the filtered bets in mv_casino_transactions_full (one row per bet event)

-- Top customers by current rolling real bet (deduped to latest per customer/currency)
SELECT customer_id, currency_id, rolling_1d_real_bet_amount
FROM (
    SELECT customer_id, currency_id, rolling_1d_real_bet_amount,
           ROW_NUMBER() OVER (PARTITION BY customer_id, currency_id ORDER BY event_ts DESC) AS rn
    FROM mv_casino_real_bet
) t
WHERE rn = 1
ORDER BY rolling_1d_real_bet_amount DESC NULLS LAST LIMIT 20;
```

### 4.4 `sink_casino_real_bet` — Iceberg upsert sink

The sink uses `connector = 'iceberg'` and **auto-creates** the `rw_managed_casino_real_bet` Iceberg table in Lakekeeper (`create_table_if_not_exists = 'true'`). There is no separate `CREATE TABLE … ENGINE = iceberg` — that approach was abandoned because `enable_compaction` only works on `connector='iceberg'` sinks (see §7).

```sql
SET background_ddl = true;  -- don't block while the initial snapshot commits

CREATE SINK sink_casino_real_bet
FROM mv_casino_real_bet
WITH (
    connector                            = 'iceberg',
    type                                 = 'upsert',
    primary_key                          = 'customer_id,currency_id,event_ts',
    enable_compaction                    = 'true',
    compaction_interval_sec              = '60',
    compaction.trigger_snapshot_count    = '5',
    enable_snapshot_expiration           = 'true',
    connection                           = lakekeeper_catalog_conn,
    database.name                        = 'public',
    table.name                           = 'rw_managed_casino_real_bet',
    create_table_if_not_exists           = 'true',
    commit_checkpoint_interval           = 20,
    compaction.write_parquet_compression = 'zstd'
);
```

**Key options:**

| Option | Effect |
|--------|--------|
| `connector = 'iceberg'` | Writes directly to Lakekeeper. Required for compaction to work (the `ENGINE = iceberg` managed-table path silently ignores `enable_compaction` in 2.7.4). |
| `create_table_if_not_exists = 'true'` | Sink creates the Iceberg table on first run — no manual `CREATE TABLE` needed. |
| `commit_checkpoint_interval = 20` | Commit every 20 checkpoints. With `barrier_interval_ms = 2000`, that's roughly **one Iceberg commit every ~30–40 s**. This is checkpoints, **not seconds** — see §7. (Reduced from 40 to roughly halve first-commit latency for Brazil — see [BRAZIL_WORKLOAD_TUNING.md](BRAZIL_WORKLOAD_TUNING.md) §8.) |
| `enable_compaction` + `compaction.trigger_snapshot_count = '5'` | Merges small Parquet files. The `trigger_snapshot_count` is essential — without it compaction triggers unreliably (see §7). |
| `enable_snapshot_expiration = 'true'` | Intended to prune old snapshots — **does not actually prune in 2.7.4 or 2.8.0** (see §7). |
| `compaction.write_parquet_compression = 'zstd'` | Compacted files use zstd (~2-3× smaller than snappy). |

**`background_ddl = true`:** Applied only here (after all MVs exist) so the sink returns immediately while the initial snapshot commits asynchronously.

Monitor sink creation:

```sql
SELECT ddl_id, ddl_statement, progress FROM rw_catalog.rw_ddl_progress;
SELECT name, connector, status FROM rw_catalog.rw_sinks WHERE name = 'sink_casino_real_bet';
```

### 4.5 Querying the Iceberg output (via Trino)

The `rw_managed_casino_real_bet` table lives in Lakekeeper and is **not** queryable from RisingWave directly (no read-source is created). Query it through Trino's `datalake` catalog:

```bash
docker exec trino trino --execute "SELECT COUNT(*) FROM datalake.public.rw_managed_casino_real_bet"

docker exec trino trino --execute "
SELECT customer_id, currency_id, rolling_1d_real_bet_amount
FROM datalake.public.rw_managed_casino_real_bet
ORDER BY rolling_1d_real_bet_amount DESC NULLS LAST LIMIT 10"
```

The Grafana dashboard's Iceberg row-count panels use the Trino datasource for exactly this.

### 4.6 `sink_casino_real_bet_kafka` — Kafka output sink (PoC R4)

> **Requires Redpanda** (`docker compose up -d redpanda`).

Required by the PoC spec: "Emit updates to destination Kafka topic in near-real-time." Enables the R4 latency benchmark (Kafka source → Kafka sink p95 < 1 s).

```sql
CREATE SINK sink_casino_real_bet_kafka
FROM mv_casino_real_bet
WITH (
    connector                   = 'kafka',
    properties.bootstrap.server = 'redpanda:9092',
    topic                       = 'casino_real_bet_output'
)
FORMAT PLAIN ENCODE JSON (
    force_append_only = 'true'
);
```

Each update to `mv_casino_real_bet` is published as a JSON message. `force_append_only = 'true'` emits every row as an insert — consumers receive the latest rolling window value and key on `(customer_id, currency_id)` to keep the most recent.

Verify — consume messages from Redpanda:

```bash
docker exec redpanda rpk topic consume casino_real_bet_output -n 5
```

Expected JSON output:
```json
{"customer_id":12345,"currency_id":1,"event_ts":"2026-05-31T10:23:45+00:00","rolling_1d_real_bet_amount":"245.50"}
```

---

## 5. UC2 — Casino Turnover Percentage

### Business Definition

> *Ratio of casino betting turnover to total betting turnover. Used to assess a customer's preference for casino activity relative to their overall betting behavior.*
>
> — PoC Document §4.2

Total turnover = casino turnover + sportsbook turnover. Both sides use 5-minute rolling windows (tuned down from 7 days — §13) and are Euro-normalised so they are directly comparable across currencies.

### Pipeline

```
mv_casino_transactions_full         src_bets_br
  → mv_casino_turnover_90d            → mv_sportsbook_turnover_90d
  → mv_casino_turnover_latest         → mv_sportsbook_turnover_latest
         ↘                                   ↗
           mv_turnover_percentage
              ├── sink_turnover_percentage      → rw_managed_turnover_percentage  (Iceberg, upsert)
              └── sink_turnover_percentage_kafka → casino_turnover_percentage_output (Redpanda, JSON)
```

> **Note:** `mv_casino_transactions_full` is shared with UC1. UC2 reads it for casino payout events — a different filter (`message_type_id = 2`) than UC1's bet events (`message_type_id = 1`).

### 5.1 `mv_casino_turnover_90d` — Rolling 5-min casino turnover

```sql
CREATE MATERIALIZED VIEW mv_casino_turnover_90d AS
SELECT
    customer_id,
    transaction_created_at                                AS event_ts,
    SUM(amount_abs) OVER (
        PARTITION BY customer_id
        ORDER BY transaction_created_at
        RANGE BETWEEN INTERVAL '300 SECONDS' PRECEDING AND CURRENT ROW
    )                                                     AS rolling_7d_turnover
FROM mv_casino_transactions_full
WHERE message_type_id = 2         -- payout/withdrawal events
  AND account_id      IN (1, 4)   -- real money (1) and bonus (4) accounts
  AND amount_abs IS NOT NULL;
```

**Filters:**

| Filter | Meaning |
|--------|---------|
| `message_type_id = 2` | Payout/withdrawal events (casino turnover) |
| `account_id IN (1, 4)` | Real money account (1) and bonus account (4) — both count toward turnover |

**Window:** `300 SECONDS` = 5 minutes. One row emitted per payout event with the updated 5-min rolling sum for that customer. (The MV keeps its `_90d` name though the window is now 5 min — see the note at the top of this doc; reduced 90d → 7d → 5min, §2/§13.)

Verify:

```sql
SELECT COUNT(*) FROM mv_casino_turnover_90d;

SELECT customer_id, rolling_7d_turnover
FROM mv_casino_turnover_90d
ORDER BY event_ts DESC LIMIT 10;
```

### 5.2 `mv_sportsbook_turnover_90d` — Rolling 5-min sportsbook turnover

```sql
CREATE MATERIALIZED VIEW mv_sportsbook_turnover_90d AS
SELECT
    ("CustomerInfo")."Id"                                                        AS customer_id,
    TO_TIMESTAMP(("PlacedAt").seconds)                                           AS event_ts,
    SUM((("TotalStake")."Euro")."units"::NUMERIC
        + (("TotalStake")."Euro")."nanos"::NUMERIC / 1000000000) OVER (
        PARTITION BY ("CustomerInfo")."Id"
        ORDER BY TO_TIMESTAMP(("PlacedAt").seconds)
        RANGE BETWEEN INTERVAL '300 SECONDS' PRECEDING AND CURRENT ROW
    )                                                                            AS rolling_7d_turnover
FROM src_bets_br
WHERE ("CustomerInfo")."Id" IS NOT NULL
  AND ("TotalStake")."Euro" IS NOT NULL;
```

**Key points:**
- Reads directly from `src_bets_br` (not from `mv_casino_transactions_full`).
- `TotalStake.Euro` uses the `DecimalValue` encoding: `units` (integer part) + `nanos / 1e9` (fractional part). This reconstructs the Euro amount as a single `NUMERIC`.
- Euro-normalised amounts make casino and sportsbook directly comparable regardless of the player's operating currency.
- The `PlacedAt` proto timestamp is `STRUCT<seconds BIGINT, nanos INT>` — converted with `TO_TIMESTAMP(...)`.

Verify:

```sql
SELECT COUNT(*) FROM mv_sportsbook_turnover_90d;

SELECT customer_id, rolling_7d_turnover
FROM mv_sportsbook_turnover_90d
ORDER BY event_ts DESC LIMIT 10;
```

### 5.3 `mv_casino_turnover_latest` — Latest casino turnover per customer

The 7d sliding-window MVs emit one row per incoming event — many rows per customer. The ratio computation needs exactly one (the most recent) row per customer.

```sql
CREATE MATERIALIZED VIEW mv_casino_turnover_latest AS
SELECT customer_id, casino_turnover, event_ts
FROM (
    SELECT
        customer_id,
        rolling_7d_turnover                                                     AS casino_turnover,
        event_ts,
        ROW_NUMBER() OVER (PARTITION BY customer_id ORDER BY event_ts DESC)      AS rn
    FROM mv_casino_turnover_90d
) t
WHERE rn = 1;
```

**Why `ROW_NUMBER()` and not `DISTINCT ON … ORDER BY`:**
In RisingWave's streaming executor, a trailing `ORDER BY` on a `CREATE MATERIALIZED VIEW` applies only at DDL time — it does not affect how results are maintained as new events arrive. `DISTINCT ON` therefore picks an arbitrary row per `customer_id`, not the latest one. The `ROW_NUMBER() OVER (PARTITION BY … ORDER BY event_ts DESC)` pattern compiles to RisingWave's stateful `TopN` operator, which correctly maintains the top-1 row per key as new events arrive and retracts stale rows.

Verify:

```sql
SELECT COUNT(*) FROM mv_casino_turnover_latest;
-- One row per distinct customer_id

SELECT customer_id, casino_turnover FROM mv_casino_turnover_latest
ORDER BY casino_turnover DESC NULLS LAST LIMIT 10;
```

### 5.4 `mv_sportsbook_turnover_latest` — Latest sportsbook turnover per customer

Same Top-1 pattern applied to the sportsbook side:

```sql
CREATE MATERIALIZED VIEW mv_sportsbook_turnover_latest AS
SELECT customer_id, sportsbook_turnover, event_ts
FROM (
    SELECT
        customer_id,
        rolling_7d_turnover                                                     AS sportsbook_turnover,
        event_ts,
        ROW_NUMBER() OVER (PARTITION BY customer_id ORDER BY event_ts DESC)      AS rn
    FROM mv_sportsbook_turnover_90d
) t
WHERE rn = 1;
```

Verify:

```sql
SELECT COUNT(*) FROM mv_sportsbook_turnover_latest;
-- One row per distinct customer_id with sportsbook activity
```

### 5.5 `mv_turnover_percentage` — Casino vs sportsbook ratio

```sql
CREATE MATERIALIZED VIEW mv_turnover_percentage AS
SELECT
    customer_id,
    SUM(casino_turnover)                             AS casino_turnover,
    SUM(sportsbook_turnover)                         AS sportsbook_turnover,
    SUM(casino_turnover) + SUM(sportsbook_turnover)  AS total_turnover,
    CASE
        WHEN SUM(casino_turnover) + SUM(sportsbook_turnover) = 0 THEN 0
        ELSE SUM(casino_turnover)
             / (SUM(casino_turnover) + SUM(sportsbook_turnover))
    END                                              AS casino_ratio,
    CASE
        WHEN SUM(casino_turnover) + SUM(sportsbook_turnover) = 0 THEN 0
        ELSE SUM(sportsbook_turnover)
             / (SUM(casino_turnover) + SUM(sportsbook_turnover))
    END                                              AS sportsbook_ratio
FROM (
    SELECT customer_id, casino_turnover, 0::NUMERIC AS sportsbook_turnover
    FROM mv_casino_turnover_latest
    UNION ALL
    SELECT customer_id, 0::NUMERIC AS casino_turnover, sportsbook_turnover
    FROM mv_sportsbook_turnover_latest
) u
GROUP BY customer_id;
```

**Why `UNION ALL + GROUP BY` instead of `FULL OUTER JOIN`:**
The obvious implementation is a `FULL OUTER JOIN` of the two `*_latest` MVs on `customer_id`. This was tried and **reset the entire database under live ingestion**:

```
Executor error: join key: [428491138], pk: [], row: [428491138, 3924.70],
state_table_id: 48: double inserting a join state entry
```

Root cause: both `*_latest` MVs use `ROW_NUMBER()` Top-1, so their stream key IS `customer_id`. A hash join keyed on `customer_id` stores state with no extra primary key, assuming at most one row per customer per side. But the Top-1 operator emits retract+insert pairs as new events arrive. Under live ingestion, the insert can reach the join state before the retract, momentarily presenting two rows for the same `customer_id` — the join panics.

The `UNION ALL + GROUP BY` pivot avoids this: each branch contributes its own metric and `0` for the other side, and a hash aggregation applies `+`/`-` deltas as updates arrive. A customer present on only one side simply gets `0` from the other branch — identical semantics to `COALESCE(…, 0)` in a full outer join.

Verify:

```sql
SELECT COUNT(*) FROM mv_turnover_percentage;
-- One row per customer who has casino OR sportsbook activity

-- Top customers by total turnover
SELECT customer_id, casino_ratio, sportsbook_ratio, total_turnover
FROM mv_turnover_percentage
ORDER BY total_turnover DESC NULLS LAST LIMIT 20;

-- Verify ratios sum to 1
SELECT customer_id, casino_ratio + sportsbook_ratio AS sum_should_be_1
FROM mv_turnover_percentage
WHERE casino_ratio + sportsbook_ratio <> 1
LIMIT 5;
-- Expected: 0 rows
```

### 5.6 `sink_turnover_percentage` — Iceberg upsert sink

Same `connector='iceberg'` pattern as UC1 — the sink auto-creates `rw_managed_turnover_percentage` in Lakekeeper.

```sql
SET background_ddl = true;

CREATE SINK sink_turnover_percentage
FROM mv_turnover_percentage
WITH (
    connector                            = 'iceberg',
    type                                 = 'upsert',
    primary_key                          = 'customer_id',
    enable_compaction                    = 'true',
    compaction_interval_sec              = '60',
    compaction.trigger_snapshot_count    = '5',
    enable_snapshot_expiration           = 'true',
    connection                           = lakekeeper_catalog_conn,
    database.name                        = 'public',
    table.name                           = 'rw_managed_turnover_percentage',
    create_table_if_not_exists           = 'true',
    commit_checkpoint_interval           = 20,
    compaction.write_parquet_compression = 'zstd'
);
```

### 5.7 Querying the Iceberg output (via Trino)

```bash
docker exec trino trino --execute "SELECT COUNT(*) FROM datalake.public.rw_managed_turnover_percentage"

docker exec trino trino --execute "
SELECT customer_id, casino_ratio, sportsbook_ratio, total_turnover
FROM datalake.public.rw_managed_turnover_percentage
ORDER BY total_turnover DESC NULLS LAST LIMIT 10"
```

### 5.8 `sink_turnover_percentage_kafka` — Kafka output sink (PoC R4)

```sql
CREATE SINK sink_turnover_percentage_kafka
FROM mv_turnover_percentage
WITH (
    connector                   = 'kafka',
    properties.bootstrap.server = 'redpanda:9092',
    topic                       = 'casino_turnover_percentage_output'
)
FORMAT PLAIN ENCODE JSON (
    force_append_only = 'true'
);
```

Each update to `mv_turnover_percentage` (triggered by a new casino or sportsbook event for any customer) is published as a JSON message. Consumers key on `customer_id`.

Verify:

```bash
docker exec redpanda rpk topic consume casino_turnover_percentage_output -n 5
```

Expected JSON output:
```json
{"customer_id":12345,"casino_turnover":"3924.70","sportsbook_turnover":"1200.00","total_turnover":"5124.70","casino_ratio":"0.7659","sportsbook_ratio":"0.2341"}
```

---

## 6. End-to-end verification

**MV counts (RisingWave)** — with `scan.startup.mode='latest'` these start near 0 and grow as live events arrive:

```sql
-- UC1
SELECT 'mv_casino_transactions_full' AS object, COUNT(*) FROM mv_casino_transactions_full
UNION ALL SELECT 'mv_casino_real_bet',           COUNT(*) FROM mv_casino_real_bet;

-- UC2
SELECT 'mv_casino_turnover_90d'        AS object, COUNT(*) FROM mv_casino_turnover_90d
UNION ALL SELECT 'mv_sportsbook_turnover_90d',  COUNT(*) FROM mv_sportsbook_turnover_90d
UNION ALL SELECT 'mv_casino_turnover_latest',   COUNT(*) FROM mv_casino_turnover_latest
UNION ALL SELECT 'mv_sportsbook_turnover_latest', COUNT(*) FROM mv_sportsbook_turnover_latest
UNION ALL SELECT 'mv_turnover_percentage',      COUNT(*) FROM mv_turnover_percentage;
```

**Iceberg output counts (Trino)** — the `rw_managed_*` tables aren't in RisingWave; query via Trino, lagging the MVs by up to one commit interval (~30-40s):

```bash
docker exec trino trino --execute "
SELECT 'casino_real_bet' AS t, COUNT(*) FROM datalake.public.rw_managed_casino_real_bet
UNION ALL SELECT 'turnover_pct', COUNT(*) FROM datalake.public.rw_managed_turnover_percentage"
```

Open the **Casino PoC — UC1 & UC2 Metrics** dashboard in Grafana at `http://localhost:3001` → Dashboards → RisingWave → **Casino PoC — UC1 & UC2 Metrics**. It auto-refreshes every 1 minute.

The dashboard has three sections:
- **UC1** — customers tracked, MV row count, Iceberg rows, total real bet volume, top 20 customers table
- **UC2** — customers tracked, avg casino ratio, Iceberg rows, total turnover, top 20 customers table with casino/sportsbook breakdown
- **Pipeline Health** — sink throughput (rows/s), Iceberg commit rate, source ingestion rate from both production Kafka topics

Check all sink health (Iceberg + Kafka):

```sql
SELECT name, connector, status
FROM rw_catalog.rw_sinks
WHERE name LIKE '%casino%' OR name LIKE '%turnover%'
ORDER BY name;
```

Expected — 4 sinks running:
```
 sink_casino_real_bet              | iceberg | RUNNING
 sink_casino_real_bet_kafka        | kafka   | RUNNING
 sink_turnover_percentage          | iceberg | RUNNING
 sink_turnover_percentage_kafka    | kafka   | RUNNING
```

Verify Kafka topics are receiving messages:

```bash
docker exec redpanda rpk topic consume casino_real_bet_output -n 3
docker exec redpanda rpk topic consume casino_turnover_percentage_output -n 3
```

---

## 7. RisingWave optimizations applied

### `APPEND ONLY` on source tables

Both `src_casino_prd` and `src_bets_br` carry immutable event records. `APPEND ONLY` tells RisingWave to skip delete-tracking bookkeeping and retraction handling in downstream MVs — reducing storage and CPU overhead.

### Shared Kafka source (`streaming_use_shared_source`)

`src_casino_prd` is consumed by three independent MV chains: `mv_casino_transactions_full` (and its descendants), `mv_casino_raw` (raw archive), and potentially others. Without `SET streaming_use_shared_source = true`, RisingWave spawns a separate Kafka consumer per chain — multiplying broker connections and network bandwidth. With it, a single consumer fans out internally.

### Selective `background_ddl`

`SET background_ddl = true` causes DDL statements to return before the object is catalog-visible. This is safe only for terminal objects (sinks, indexes) with no downstream dependents in the same session. It must NOT be set during the MV chain creation — each step must see the previous MV in the catalog before it can reference it. The pipeline sets it immediately before the first `CREATE SINK`.

### `ROW_NUMBER()` Top-1 instead of `DISTINCT ON`

`DISTINCT ON (customer_id) … ORDER BY customer_id, event_ts DESC` does not work in RisingWave streaming mode — the `ORDER BY` only applies at DDL time, not to ongoing updates. The `ROW_NUMBER() OVER (PARTITION BY customer_id ORDER BY event_ts DESC)` pattern compiles to the stateful `TopN` operator, which correctly maintains the top-1 row per key as new events arrive.

### `commit_checkpoint_interval` is checkpoints, not seconds

`commit_checkpoint_interval = 20` does **not** mean "commit every 20 seconds." It means "commit every 20 **checkpoints**." The checkpoint cadence derives from `barrier_interval_ms` (2000 ms in `risingwave.toml` for this cluster, with `checkpoint_frequency = 2`):

```
commit cadence ≈ commit_checkpoint_interval × barrier_interval_ms
               ≈ 20 × 2000 ms ≈ ~30–40 seconds
```

So lowering this value shortens the time to the first Iceberg commit but increases snapshot/small-file churn. `20` was chosen as the Brazil balance (down from `40` ≈ ~60–80 s) — see [BRAZIL_WORKLOAD_TUNING.md](BRAZIL_WORKLOAD_TUNING.md) §8. Note the Grafana "commits/min" metric is per-actor, so it reads higher than the true table-level commit rate.

### Iceberg compaction — works via `connector='iceberg'` + `trigger_snapshot_count`

`enable_compaction = 'true'` activates the dedicated `compactor-1` service to merge small Parquet files. Two conditions matter:

1. **Sink must be `connector = 'iceberg'`.** The `ENGINE = iceberg` managed-table path silently ignores `enable_compaction` in 2.7.4 — the DDL is accepted but no compaction ever runs. This is why the pipeline uses `connector='iceberg'` sinks.
2. **`compaction.trigger_snapshot_count = '5'` is required.** Compaction uses a dual-trigger (time interval AND snapshot count). Without an explicit `trigger_snapshot_count`, the default (16) combined with the interval triggers unreliably. Setting it to `5` makes compaction fire predictably.

Verify compaction is running (look for `replace` operations in the snapshot log via Trino):

```bash
docker exec trino trino --execute "
SELECT operation, COUNT(*) FROM datalake.public.\"rw_managed_casino_real_bet\$snapshots\" GROUP BY operation"
# append  = checkpoint commits ;  replace = compaction merges (should be > 0)
```

| Option | Value | Effect |
|--------|-------|--------|
| `enable_compaction` | `'true'` | Activates `compactor-1` for Parquet file merging |
| `compaction_interval_sec` | `'60'` | Minimum time between compaction runs |
| `compaction.trigger_snapshot_count` | `'5'` | Minimum snapshots before compaction fires (both conditions must be met) |
| `compaction.write_parquet_compression` | `'zstd'` | Compacted output uses zstd compression |

### Snapshot expiration — ✅ FIXED in v3.0.0

`enable_snapshot_expiration = 'true'` now actually prunes snapshots with the Lakekeeper REST catalog
as of RisingWave v3.0.0 (PRs #25700/#25723/#25749/#25902). It is **on by default** (1 h max age,
retain last 12 snapshots) and compaction snapshots are protected from GC. The sinks already carry
`enable_snapshot_expiration='true'` — no config change needed.

**Historical note (2.7.4–2.8.4):** `enable_snapshot_expiration` and related options were accepted
in the DDL but had no effect with the Lakekeeper REST catalog. Definitively tested on 2.8.0
(2026-06-01): with `max_age='60000'` (1 min), snapshot count grew 17→23 and oldest aged past 14.5
min — nothing pruned. See BRAZIL_WORKLOAD_TUNING.md §19 for the sampled data. The Trino
`expire_snapshots` workaround is no longer needed.

> **Note:** To change options on an existing sink, DROP and recreate it — Iceberg table data in Lakekeeper/MinIO is preserved.

### Indexes on serving MVs

```sql
CREATE INDEX IF NOT EXISTS idx_casino_real_bet_customer
    ON mv_casino_real_bet (customer_id, currency_id);

CREATE INDEX IF NOT EXISTS idx_turnover_percentage_customer
    ON mv_turnover_percentage (customer_id);
```

Without these, queries filtering by `customer_id` perform full heap scans. RisingWave maintains indexes incrementally — small write overhead, instant point lookups.

---

## 8. Gotchas

- **`schema.location` requires binary FDS, not `.proto` text.** Apicurio's native v2 endpoint returns `.proto` text; passing that URL directly to `schema.location` fails. Must compile with `protoc --include_imports` first.

- **`CREATE TABLE` vs `CREATE SOURCE`.** A `SOURCE` re-scans Kafka at read time; a `TABLE` persists the topic into RisingWave state so batch and streaming counts agree. With `scan.startup.mode='earliest'` a TABLE also backfills full history.

- **`scan.startup.mode = 'latest'` vs `'earliest'`.** The pipeline ships with `'latest'` (fast startup, tracks live events; rolling windows fill over time). `'earliest'` does a full-history backfill, but on this cluster it grinds under backpressure from UC2's rolling-window MVs and can take hours — use it off-demo only. The dbt source models use a `pre_hook` DROP so the mode change applies on rebuild.

- **`CREATE VIEW` over Iceberg source fails in v3.0.0 with DataFusion enabled.** `enable_datafusion_engine` defaulted to `true` in v3.0.0. `CREATE VIEW ... AS SELECT * FROM <iceberg_source>` fails with `Expected RisingWave plan in BatchPlanChoice, but got DataFusion plan`. Workaround: `SET enable_datafusion_engine = false` before the `CREATE VIEW`. Applied automatically in dbt via `pre_hook`. Bug filed: https://github.com/risingwavelabs/risingwave/issues/25968.

- **`enable_pk_index = 'true'` crashes compute node in v3.0.0.** The frontend planning for deletion-vector upserts (PR #25346) shipped in v3.0.0 but the streaming executor did not. The sink DDL is accepted, then the compute node crashes seconds later. Do not use `enable_pk_index` until a future v3.x patch.

- **`connector='iceberg'` sinks, not `ENGINE = iceberg`.** `enable_compaction` is silently ignored on `ENGINE = iceberg` managed tables in 2.7.4. Use `connector='iceberg'` sinks with `create_table_if_not_exists='true'`; query the resulting tables via Trino, not RisingWave.

- **`commit_checkpoint_interval` counts checkpoints, not seconds.** Multiply by `barrier_interval_ms` (2000 ms here) to estimate the real cadence: `20 × 2000 ms ≈ ~30–40 s`. See §7.

- **Snapshot expiration works as of v3.0.0.** Was broken through v2.8.4 (accepted in DDL, no effect with REST catalog). Fixed in v3.0.0 — see §7.

- **Grafana needs the `trino-datasource` plugin + dollar-free views.** Trino's metadata tables (`table$snapshots`, `table$partitions`) contain a `$`, which Grafana interprets as a variable. The `casino_trino_views` Dagster asset creates dollar-free views: `casino_real_bet_snapshots` / `turnover_pct_snapshots` (feed the snapshot-count and operations panels) and `casino_real_bet_rowcount` / `turnover_pct_rowcount` (feed the "Iceberg Rows" panels via `SUM(record_count)` over `$partitions` — instant metadata reads instead of a `COUNT(*)` full scan; see [BRAZIL_WORKLOAD_TUNING.md](BRAZIL_WORKLOAD_TUNING.md) §11). The plugin auto-installs via `GF_INSTALL_PLUGINS=trino-datasource`, and Trino needs `http-server.process-forwarded=true` to accept Grafana's proxied requests.

- **PascalCase identifiers must be double-quoted.** RisingWave preserves protobuf field names verbatim. Unquoted `CustomerId` folds to `customerid` and fails to resolve.

- **Struct field access requires parentheses.** `s."RoundInfo"."Messages"` is parsed as `schema.table.column`. Use `(s."RoundInfo")."Messages"`.

- **Decimal-as-string proto fields.** `Amount` and similar fields are `string` typed for arbitrary precision. Proto3 encodes absent fields as `''`. Always guard with `NULLIF(field, '')::numeric`.

- **`connector='iceberg'` auto-creates the table from the MV schema.** With `create_table_if_not_exists='true'`, the Iceberg table's columns are derived from the upstream MV's `SELECT` — no manual `CREATE TABLE` to keep in sync. (The old `ENGINE = iceberg` approach required a hand-written table with positionally-matched columns; that's no longer used.)

- **Iceberg sink `primary_key` is lowercased.** `primary_key = 'UniqueId'` looks for a column named `uniqueid`. Always use snake_case column names in the upstream MV and match them in `primary_key`.

- **`background_ddl = true` breaks chained MV creation.** Set it only after all MVs are fully created and visible in the catalog.

- **`DISTINCT ON` is not a reliable Top-1 in streaming mode.** Use `ROW_NUMBER()` Top-1 instead. See §5.3.

- **`DISTINCT ON` inputs to `FULL OUTER JOIN` can panic the database.** See §5.5 for the full post-mortem. Use `UNION ALL + GROUP BY` instead.

- **License cap disables `DatabaseFailureIsolation`.** The cluster has more CPU cores than the license allows, so any single streaming job failure resets the entire database. The DDL files are idempotent (`DROP … IF EXISTS` / `CREATE … IF NOT EXISTS`), and `bin/3_run_casino_prd_demo.sh` wraps pipeline steps in `run_sql_with_retry` (3 attempts, 15 s wait), so a JVM cold-start reset is handled automatically.

- **OrbStack HTTP proxy.** `bin/bootstrap_lakekeeper.sh` unsets `http_proxy/HTTPS_PROXY` because the OrbStack proxy can't resolve docker-network hostnames like `lakekeeper`.

---

## 9. dbt + Dagster Integration

The same casino pipeline can be run and orchestrated through dbt and Dagster instead of (or alongside) the raw SQL scripts. Both paths create identical RisingWave objects — they share the same source table names (`src_casino_prd`, `src_bets_br`) and MV names so they are fully interchangeable.

### 9.1 Why dbt

dbt adds:
- **Dependency graph** — `{{ ref(...) }}` wires models together; dbt resolves the correct execution order automatically
- **Idempotent DDL** — each model uses `CREATE … IF NOT EXISTS`, safe to re-run
- **Observability** — dbt generates a lineage graph showing every object and its upstream dependencies
- **Tagging** — models are tagged `casino_uc1` or `casino_uc2` so each use case can be built, tested, or selected independently

### 9.2 dbt project structure

The casino models live under `dbt/models/casino_prd/` inside the existing `realtime_funnel` dbt project (`dbt/dbt_project.yml`). The subfolder inherits all project-level settings and adds a `pre-hook` to set the required session variable:

```yaml
# dbt/dbt_project.yml
models:
  realtime_funnel:
    +materialized: materialized_view
    +schema: public
    casino_prd:
      +pre-hook:
        - "SET streaming_use_shared_source = true"
```

The `pre-hook` runs before every model in the subfolder. Because each dbt model runs in its own database connection, session variables must be set per-connection — a pre-hook is the correct place.

**Model files:**

16 model files (no separate Iceberg-table models — the `connector='iceberg'` sinks auto-create the tables):

| File | Materialization | Tag(s) | RisingWave object |
|------|----------------|--------|-------------------|
| `src_casino_prd.sql` | `kafka_table` | `casino_uc1` | `src_casino_prd` (Kafka TABLE, `scan.startup.mode='latest'`) |
| `src_bets_br.sql` | `kafka_table` | `casino_uc2` | `src_bets_br` (Kafka TABLE) |
| `mv_casino_transactions_full.sql` | `materialized_view` | `casino_uc1` | `mv_casino_transactions_full` (shared foundation: 6 core columns + `properties` JSON blob) |
| `mv_casino_real_bet.sql` | `materialized_view` | `casino_uc1` | `mv_casino_real_bet` (rolling 5-min on `mv_casino_transactions_full`) |
| `sink_casino_real_bet.sql` | `sink` | `casino_uc1` | `sink_casino_real_bet` (`connector='iceberg'`, auto-creates `rw_managed_casino_real_bet`) |
| `sink_casino_real_bet_kafka.sql` | `sink` | `casino_uc1` | `sink_casino_real_bet_kafka` → `casino_real_bet_output` (Redpanda) |
| `mv_casino_turnover_90d.sql` | `materialized_view` | `casino_uc2` | `mv_casino_turnover_90d` |
| `mv_sportsbook_turnover_90d.sql` | `materialized_view` | `casino_uc2` | `mv_sportsbook_turnover_90d` |
| `mv_casino_turnover_latest.sql` | `materialized_view` | `casino_uc2` | `mv_casino_turnover_latest` |
| `mv_sportsbook_turnover_latest.sql` | `materialized_view` | `casino_uc2` | `mv_sportsbook_turnover_latest` |
| `mv_turnover_percentage.sql` | `materialized_view` | `casino_uc2` | `mv_turnover_percentage` |
| `sink_turnover_percentage.sql` | `sink` | `casino_uc2` | `sink_turnover_percentage` (`connector='iceberg'`, auto-creates `rw_managed_turnover_percentage`) |
| `sink_turnover_percentage_kafka.sql` | `sink` | `casino_uc2` | `sink_turnover_percentage_kafka` → `casino_turnover_percentage_output` (Redpanda) |

### 9.3 Custom dbt materializations

The standard dbt materializations (`table`, `view`, `incremental`) don't map to RisingWave's streaming objects. Four custom materializations are defined in `dbt/macros/materializations/`:

| Materialization | Used for | Key behaviour |
|----------------|----------|---------------|
| `kafka_table` | Kafka-backed source tables | Passes the model SQL verbatim; requires `topic` config. Uses `{{ this }}` as the table name so `{{ ref(...) }}` resolves correctly. Source models add a `pre_hook` DROP so `scan.startup.mode` changes apply on rebuild. |
| `materialized_view` | Streaming MVs | Standard dbt MV materialization extended for RisingWave. |
| `sink` | RisingWave sinks (Iceberg + Kafka) | Calls `create_iceberg_connection()`, then sets `background_ddl = true` before `CREATE SINK` to avoid blocking while the initial Iceberg snapshot commits. The Iceberg sinks use `connector='iceberg'` + `create_table_if_not_exists` (no separate table model). |

> The `iceberg_table` materialization still exists in the repo for other pipelines but is **not used** by casino — the `connector='iceberg'` sinks create their own tables.

**Important:** `kafka_table` models use `{{ this }}` (the dbt relation name) as the table name in the DDL — not a hardcoded string. This ensures `{{ ref('src_casino_prd') }}` in downstream models resolves to the same name that was actually created in RisingWave.

### 9.4 Running dbt manually

```bash
# Compile to validate the dependency graph (no SQL executed)
cd dbt
dbt compile --select casino_prd

# Build UC1 only
dbt build --select tag:casino_uc1

# Build UC2 only (UC1 must already be materialised — mv_casino_transactions_full is a UC1 model)
dbt build --select tag:casino_uc2

# Build everything in the casino_prd folder
dbt build --select casino_prd

# Drop and rebuild a single model
dbt run --select mv_casino_real_bet --full-refresh
```

Before running, set the `DBT_HOST` environment variable to point at the RisingWave frontend:

```bash
export DBT_HOST=localhost   # or risingwave-frontend inside Docker
dbt build --select casino_prd
```

---

### 9.5 Dagster integration

Dagster orchestrates the full pipeline — from proto schema upload through dbt model materialisation — and provides a UI for triggering, monitoring, and observing every step.

#### Asset groups

Dagster represents each dbt model as an **asset**. Assets are grouped visually in the UI:

| Dagster group | Contents |
|--------------|----------|
| `casino_prd_setup` | Python assets: `casino_prd_proto_fetch`, `casino_prd_proto_compile`, `casino_prd_proto_upload`, and `casino_trino_views` (creates the dollar-free Trino views the Grafana snapshot/operations and "Iceberg Rows" panels query — runs after the sinks exist) |
| `casino_uc1` | All dbt models tagged `casino_uc1`: source, `mv_casino_transactions_full`, `mv_casino_real_bet`, Iceberg sink, Kafka sink |
| `casino_uc2` | All dbt models tagged `casino_uc2`: `mv_casino_turnover_90d`, `mv_sportsbook_turnover_90d`, `*_latest` MVs, `mv_turnover_percentage`, Iceberg sink, Kafka sink |

UC2 assets show UC1 assets (`mv_casino_transactions_full`) as upstream dependencies in the asset graph — cross-group dependency tracking works automatically.

#### Setup assets (`casino_prd_setup` group)

Three plain Python `@asset` functions in `orchestration/assets/casino_prd_setup.py` handle the prerequisites that must run before any dbt model:

**`casino_prd_proto_fetch`**
Fetches `.proto` source files from the Apicurio schema registry native v2 endpoint using `httpx`. Writes to `proto/casinoroundinfodto.proto` and `proto/betinfo.proto`. Falls back gracefully if the registry is unreachable (e.g. no VPN) and the `.proto` files already exist on disk — in that case it logs a warning and continues. Fails explicitly only if the registry is unreachable AND no local file exists.

```
Apicurio native v2 → proto/casinoroundinfodto.proto
                   → proto/betinfo.proto
```

**`casino_prd_proto_compile`** (depends on `casino_prd_proto_fetch`)
Runs `protoc --include_imports` as a subprocess to compile `.proto` files to binary `FileDescriptorSet`. Falls back gracefully if `protoc` is not on `PATH` and pre-built `.pb`/`.desc` files already exist on disk (useful when running inside Docker where the host toolchain may not be available).

```
proto/casinoroundinfodto.proto → proto/casinoroundinfodto.pb
proto/betinfo.proto            → proto/betinfo.desc
```

**`casino_prd_proto_upload`** (depends on `casino_prd_proto_compile`)
Uploads the compiled descriptors to MinIO using `boto3`, at `s3://hummock001/proto/`. These S3 paths are the same ones referenced in the `schema.location` clause of the `CREATE TABLE` DDL — RisingWave fetches them at DDL execution time.

```
proto/casinoroundinfodto.pb → s3://hummock001/proto/casinoroundinfodto.pb
proto/betinfo.desc          → s3://hummock001/proto/betinfo.desc
```

#### dbt assets (`casino_uc1` and `casino_uc2` groups)

Two `@dbt_assets`-decorated functions in `orchestration/definitions.py` wrap the dbt models. The `select=` parameter on the decorator tells Dagster which dbt models belong to this asset function, and also scopes the dbt CLI invocation:

```python
@dbt_assets(manifest=dbt_project.manifest_path,
            select="tag:casino_uc1",
            dagster_dbt_translator=custom_translator)
def casino_uc1_dbt_assets(context, dbt):
    yield from dbt.cli(["build"], context=context).stream()

@dbt_assets(manifest=dbt_project.manifest_path,
            select="tag:casino_uc2",
            dagster_dbt_translator=custom_translator)
def casino_uc2_dbt_assets(context, dbt):
    yield from dbt.cli(["build"], context=context).stream()
```

The `CustomDagsterDbtTranslator` assigns assets to groups based on their dbt tags:
- Models tagged `casino_uc1` → group `casino_uc1`
- Models tagged `casino_uc2` only → group `casino_uc2`

The existing `realtime_funnel_dbt_assets` function is scoped with `exclude="casino_prd"` so casino models don't appear in the funnel asset graph.

#### Jobs

A single end-to-end job is defined for the casino pipeline:

| Job | What it runs | When to use |
|-----|-------------|-------------|
| `casino_prd_full_job` | proto setup → `casino_prd_dbt_assets` (UC1 + UC2) → `casino_trino_views` | **Full demo run — single click** |

`casino_prd_full_job` is the entry point for demos. Dagster enforces the correct execution
order automatically: the proto assets complete before the dbt build starts, UC1 models
(including `mv_casino_transactions_full`) are created before the UC2 models that depend on them,
and the Trino metadata views are created last once the Iceberg sinks have committed.

To run just part of the pipeline ad-hoc (e.g. re-upload proto descriptors after a schema
change), materialize the relevant assets directly from the Dagster asset graph — no
dedicated job is needed. UC1 and UC2 are built together in the single `casino_prd_dbt_assets`
step (they were previously split into separate jobs/steps; merging them removed a ~100 s
inter-step scheduling gap — see [BRAZIL_WORKLOAD_TUNING.md](BRAZIL_WORKLOAD_TUNING.md) §9a).

#### Running via Dagster UI — step by step

**Step 1: Start the full stack**
```bash
./bin/1_up.sh
```
This starts RisingWave, MinIO, Lakekeeper, Redpanda, Grafana, and Dagster. Open the Dagster UI at `http://localhost:3000`.

**Step 2: Run the full casino pipeline**
- Navigate to **Jobs** → `casino_prd_full_job`
- Click **Materialize all**
- Dagster executes in dependency order: `casino_prd_setup` → `casino_uc1` → `casino_uc2`

**Step 3: Monitor progress**
- Go to **Runs** → click the active run
- Each asset tile turns green on success; click any tile to view logs
- The setup group (proto fetch/compile/upload) runs first — if it fails due to network (VPN required for Apicurio), the pre-built `.pb`/`.desc` files in `proto/` are used as fallback

**Step 4: Verify in Dagster UI**
- Go to **Asset catalog** — filter by group `casino_uc1` or `casino_uc2`
- All assets should show **Materialized** with a green tick and a timestamp

**Step 5: Verify in RisingWave**

After the run completes, connect to RisingWave and check:
```bash
psql postgresql://root@localhost:4566/dev
```

```sql
-- All 4 sinks should be RUNNING
SELECT name, connector, status FROM rw_catalog.rw_sinks
WHERE name LIKE '%casino%' OR name LIKE '%turnover%'
ORDER BY name;

-- UC1 / UC2 MV row counts (RisingWave)
SELECT COUNT(*) FROM mv_casino_real_bet;
SELECT COUNT(*) FROM mv_turnover_percentage;
```

The `rw_managed_*` Iceberg tables are queried via Trino, not RisingWave:
```bash
docker exec trino trino --execute "SELECT COUNT(*) FROM datalake.public.rw_managed_casino_real_bet"
docker exec trino trino --execute "SELECT COUNT(*) FROM datalake.public.rw_managed_turnover_percentage"
```

**Step 6: Verify Kafka output**
```bash
docker exec redpanda rpk topic consume casino_real_bet_output -n 3
docker exec redpanda rpk topic consume casino_turnover_percentage_output -n 3
```

**Step 7: Verify in Grafana**

Open `http://localhost:3001` → Dashboards → RisingWave → **Casino PoC — UC1 & UC2 Metrics**.

| Panel | What to look for |
|-------|-----------------|
| Customers Tracked (UC1) | Non-zero, growing during backfill |
| MV Row Count / Iceberg Rows (UC1) | MV count from RisingWave; Iceberg count from the `$partitions` metadata (instant — see §11 of the tuning guide) |
| Total Real Bet Volume (1d rolling) | Non-zero total (amounts shown without currency unit — see note below) |
| Most Recently Active Customers — 1-Day Real Bet Amount (UC1) | Table with Latest Bet, 1-Day Real Bet, Last Event columns; sorted by most recent event |
| Avg Casino Ratio | Between 0 and 1 |
| Top 20 Customers (UC2) | casino_ratio + sportsbook_ratio columns sum to ~100% |
| Iceberg Operations / min | `appends` (checkpoint commits) and `compactions` (replace ops) per table — a non-zero `compactions` line confirms compaction is running |
| Iceberg Commits / min (true cadence) | Per-table commit rate (divided by actor count) — ~1.5–2/min at `commit_checkpoint_interval=20` |
| Source Throughput | Lines for `src_casino_prd` and `src_bets_br` |

> **Currency note:** UC1 amounts (`rolling_1d_real_bet_amount`, `latest_single_bet`) are in the player's account currency (`currency_id`), and the mapping from `currency_id` to a named currency has not been confirmed by Kaizen. UC1 amounts are therefore displayed as raw numbers without a currency symbol until this is confirmed. UC2 amounts are correctly in EUR because `mv_sportsbook_turnover_90d` explicitly uses `TotalStake.Euro` from the bets protobuf.

---

**Partial runs (ad-hoc, via the asset graph):**

UC1 and UC2 build together in one `casino_prd_dbt_assets` step, so there are no separate
per-UC jobs. For partial runs, materialize assets directly from the Assets page rather than
running a job.

| What | How |
|------|-----|
| Proto schemas only | Assets → select `casino_prd_proto_fetch`/`compile`/`upload` → Materialize |
| Casino models only | Assets → filter group `casino_uc1` → Materialize selected |
| Turnover models only | Assets → filter group `casino_uc2` → Materialize selected |
| Full pipeline | Jobs → `casino_prd_full_job` → Materialize all |

#### Relationship between dbt and raw SQL

Both approaches create the same RisingWave objects with the same names:

| Object | Raw SQL script | dbt model |
|--------|---------------|-----------|
| `src_casino_prd` | `sql/casino_prd_source.sql` | `dbt/models/casino_prd/src_casino_prd.sql` |
| `mv_casino_transactions_full` | `sql/casino_prd_funnel_iceberg.sql` | `dbt/models/casino_prd/mv_casino_transactions_full.sql` |
| `mv_casino_real_bet` | `sql/casino_prd_funnel_iceberg.sql` | `dbt/models/casino_prd/mv_casino_real_bet.sql` |
| … | … | … |

They are not designed to run simultaneously on the same RisingWave instance — the DROP statements in the SQL script will remove dbt-created objects and vice versa. Choose one path per session.

---

## 10. Files of interest  

| Path | Purpose |
|---|---|
| [proto/casinoroundinfodto.proto](../proto/casinoroundinfodto.proto) | Casino schema, fetched from Apicurio v2 native |
| [proto/casinoroundinfodto.pb](../proto/casinoroundinfodto.pb) | Compiled FileDescriptorSet for casino; uploaded to `s3://hummock001/proto/` |
| [proto/betinfo.proto](../proto/betinfo.proto) | Sportsbook bets schema (UC2) |
| [proto/betinfo.desc](../proto/betinfo.desc) | Compiled FileDescriptorSet for bets; uploaded to `s3://hummock001/proto/` |
| [sql/casino_prd_source.sql](../sql/casino_prd_source.sql) | Creates `src_casino_prd` |
| [sql/casino_prd_bets_source.sql](../sql/casino_prd_bets_source.sql) | Creates `src_bets_br` |
| [sql/casino_prd_funnel_iceberg.sql](../sql/casino_prd_funnel_iceberg.sql) | UC1 + UC2 MVs + `connector='iceberg'` sinks + Kafka sinks |
| [sql/casino_prd_raw_iceberg.sql](../sql/casino_prd_raw_iceberg.sql) | Faithful raw nested archive: `mv_casino_raw` + `rw_managed_casino_raw` |
| [dbt/models/casino_prd/](../dbt/models/casino_prd/) | dbt models for the casino pipeline (sources, MVs, Iceberg sinks, Kafka sinks) |
| [dbt/dbt_project.yml](../dbt/dbt_project.yml) | dbt project config — casino_prd subfolder config + pre-hook |
| [dbt/macros/materializations/](../dbt/macros/materializations/) | Custom RisingWave materializations: `kafka_table`, `materialized_view`, `sink` (casino uses these; `iceberg_table` exists for other pipelines) |
| [monitoring/grafana/dashboards/casino-uc-metrics.json](../monitoring/grafana/dashboards/casino-uc-metrics.json) | Grafana dashboard: UC1/UC2 business metrics + Iceberg/Kafka sink health (uses Trino + Prometheus datasources) |
| [orchestration/assets/casino_prd_setup.py](../orchestration/assets/casino_prd_setup.py) | Dagster prereq assets: proto fetch/compile/upload + `casino_trino_views` |
| [orchestration/definitions.py](../orchestration/definitions.py) | Dagster definitions: casino asset functions + jobs |
| [bin/3_run_casino_prd_demo.sh](../bin/3_run_casino_prd_demo.sh) | End-to-end setup script (proto → sources → MVs → sinks → Trino views → verify) |
| [docs/RisingWave_PoC_Document.txt](RisingWave_PoC_Document.txt) | Original PoC scope document with UC1/UC2 business definitions |

---

## 11. Reproduce from scratch

```bash
# 1. Start services (redpanda is needed for the Kafka output sinks)
docker compose up -d \
  minio-0 meta-node-0 compute-node-0 compactor-0 frontend-node-0 \
  lakekeeper-db lakekeeper-migrate lakekeeper lakekeeper-bootstrap trino redpanda

# 2. Compile and upload proto schemas
protoc --include_imports \
  --descriptor_set_out=proto/casinoroundinfodto.pb \
  proto/casinoroundinfodto.proto
AWS_ACCESS_KEY_ID=hummockadmin AWS_SECRET_ACCESS_KEY=hummockadmin \
  aws --endpoint-url http://localhost:9301 s3 cp \
    proto/casinoroundinfodto.pb s3://hummock001/proto/casinoroundinfodto.pb

protoc --include_imports \
  --descriptor_set_out=proto/betinfo.desc \
  --proto_path=/opt/homebrew/include --proto_path=proto proto/betinfo.proto
AWS_ACCESS_KEY_ID=hummockadmin AWS_SECRET_ACCESS_KEY=hummockadmin \
  aws --endpoint-url http://localhost:9301 s3 cp \
    proto/betinfo.desc s3://hummock001/proto/betinfo.desc

# 3. Create sources + MVs + Iceberg sinks (idempotent)
psql postgresql://root@localhost:4566/dev -f sql/casino_prd_source.sql
psql postgresql://root@localhost:4566/dev -f sql/casino_prd_bets_source.sql
psql postgresql://root@localhost:4566/dev -f sql/casino_prd_funnel_iceberg.sql
# If 'database 1 reset' occurs on the first iceberg CREATE TABLE (JVM cold start),
# just rerun — the DDL is idempotent and succeeds once the JVM is warm.

# 4. Verify MVs (RisingWave)
psql postgresql://root@localhost:4566/dev <<'SQL'
SELECT 'mv_casino_transactions_full' AS object, COUNT(*) FROM mv_casino_transactions_full
UNION ALL SELECT 'mv_casino_real_bet',           COUNT(*) FROM mv_casino_real_bet
UNION ALL SELECT 'mv_turnover_percentage',       COUNT(*) FROM mv_turnover_percentage;
SQL

# 5. Verify Iceberg output (Trino)
docker exec trino trino --execute "
SELECT 'casino_real_bet' AS t, COUNT(*) FROM datalake.public.rw_managed_casino_real_bet
UNION ALL SELECT 'turnover_pct', COUNT(*) FROM datalake.public.rw_managed_turnover_percentage"
```

> The script also creates the dollar-free Trino views — `casino_real_bet_snapshots`, `turnover_pct_snapshots` (snapshot/operations panels) and `casino_real_bet_rowcount`, `turnover_pct_rowcount` (the "Iceberg Rows" panels) — that the Grafana dashboard depends on.
> Kafka output sinks require Redpanda (`docker compose up -d redpanda`) — see §4.5.

Or use the all-in-one script:

```bash
./bin/3_run_casino_prd_demo.sh
```

**Alternative: via Dagster (full pipeline in one click)**

Start the full stack including Dagster:

```bash
./bin/1_up.sh
```

Then open the Dagster UI at `http://localhost:3000`, navigate to **Jobs** → `casino_prd_full_job`, and click **Materialize all**. This runs the proto setup, UC1, and UC2 in the correct dependency order with full observability in the UI.
