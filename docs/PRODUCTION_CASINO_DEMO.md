# Production Casino Stream → RisingWave → Iceberg Demo

End-to-end walkthrough of reading the live `cronus.casino.out.gh` topic from
the Kaizen production Kafka cluster into RisingWave, building real-time
materialized views, and landing the results into Lakekeeper-managed Iceberg
tables.

> **Status:** working as of 2026-05-30. RisingWave 2.8.4, Lakekeeper
> `latest-main`, MinIO local. No producer required — we consume the live
> production topic directly.

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
┌──────────────────────────┐    SSL one-way   ┌─────────────────────────────────┐
│ Kaizen prd2 Kafka        │ ────────────────▶│  RisingWave (compute-node-0)     │
│ prd2-kafka-bootstrap     │  topic           │   src_casino_prd  (TABLE)        │
│ .kaizengaming.net:443    │  cronus.casino   │   ↓                              │
│ 5 brokers, 290 topics    │  .out.gh         │   mv_casino_raw  (nested 1:1)    │
└──────────────────────────┘                  │   mv_casino_transactions         │
                                              │   mv_casino_real_bet             │
                                              │   mv_turnover_percentage         │
                                              └────────────┬────────────────────┘
                                                           │ upsert sinks
                                                           ▼  (commit ≈5s)
                                              ┌─────────────────────────────────┐
                                              │ Lakekeeper REST catalog          │
                                              │ + MinIO S3 (hummock001)          │
                                              │ rw_managed_casino_*              │
                                              └─────────────────────────────────┘
```

Key decisions:

- **Schema sourcing: compile once, store in MinIO, fetch via `s3://` at DDL
  time.** Apicurio's `ccompat/v7` layer normalises PROTOBUF artifacts to binary
  `FileDescriptorSet`, which RisingWave's `schema.registry` fetcher cannot
  parse. The native v2 endpoint returns `.proto` text, but `schema.location`
  expects a compiled binary FDS — not source text. Resolution: fetch `.proto`
  from Apicurio native v2, compile with `protoc --include_imports`, upload the
  `.pb`/`.desc` to MinIO, and reference via
  `schema.location = 's3://hummock001/proto/…'`. No container volume mount.
- **Wire format on `cronus.casino.out.*` is raw protobuf**, no Confluent
  5/6-byte magic prefix. `schema.location` reads byte 0 as a protobuf field
  tag — that matches what's on the wire, so no framing strip is needed.
  (`schema.registry` requires the Confluent magic byte and is therefore
  incompatible with these topics.)
- **`scan.startup.mode = 'earliest'`** combined with `CREATE TABLE` (not
  `CREATE SOURCE`) persists the topic into RW state, so MVs see full
  history and batch counts agree with streaming counts.
- **Source lands in Iceberg with full nested fidelity.** `mv_casino_raw`
  is a thin pass-through MV that only renames top-level columns to
  snake_case; the nested `STRUCT<…>[]` (incl. `Messages[].Transactions[]`)
  flows into Iceberg unchanged. The other MVs are analytical projections
  over the same source.

---

## 2. Production environment facts

| | |
|---|---|
| Kafka bootstrap | `prd2-kafka-bootstrap.kaizengaming.net:443` |
| Brokers | `prd2-kafka-0..4.kaizengaming.net:443` |
| Security | `SSL` (one-way TLS, DigiCert `*.kaizengaming.net`, valid through Feb 2027). No SASL, no client cert. |
| Cluster id | `xKuCz2JyRb-SF1j13WFEIg` |
| Topic | `cronus.casino.out.gh` (10 partitions, ~386 254 msgs at probe time) |
| Wire format | Raw protobuf — no Confluent framing |
| Schema id transport | Kafka **headers**: `MessageType`, `OperatorId`, `PublishId` |
| Apicurio | `http://staging-schema-registry.kaizengaming.net` (v2 native + `/apis/ccompat/v7/`) |
| Apicurio artifact | group=`bigdata`, id=`casinoroundinfo`, contentId=`299` |
| Message FQN | `Cronus.CasinoService.RoundInfo.Abstractions.CasinoRoundInfoDto` |

---

## 3. Steps performed

### 3.1 Fetch the `.proto` from Apicurio (native v2)

The ccompat endpoint hands back binary FDS for protobuf artifacts — useless
for RisingWave. The **native v2 endpoint** returns the original `.proto`
text that was uploaded, which is what we want.

```bash
curl -fsSL \
  -H 'Accept: text/plain' \
  http://staging-schema-registry.kaizengaming.net/apis/registry/v2/groups/bigdata/artifacts/casinoroundinfo \
  > proto/casinoroundinfodto.proto
```

Result: a clean `.proto` file declaring
`package Cronus.CasinoService.RoundInfo.Abstractions;` with message
`CasinoRoundInfoDto` plus nested `GameInformation`, `RoundInformation`,
`CasinoMessageInformation`, `TransactionInformation` (proto3, many `optional`
fields → oneofs under the hood).

### 3.2 Compile to a FileDescriptorSet and upload to MinIO

RisingWave's `schema.location` reads a serialised
`google.protobuf.FileDescriptorSet`, **not** `.proto` source text
(confirmed: passing the Apicurio native v2 URL directly as `schema.location`
fails with "failed to decode file descriptor set"). Compile with
`--include_imports` so well-known types like `google.protobuf.Timestamp` are
baked in, then upload to MinIO:

```bash
protoc \
  --include_imports \
  --descriptor_set_out=proto/casinoroundinfodto.pb \
  proto/casinoroundinfodto.proto

# Upload to MinIO (runs once; no container restart needed)
AWS_ACCESS_KEY_ID=hummockadmin AWS_SECRET_ACCESS_KEY=hummockadmin \
  aws --endpoint-url http://localhost:9301 s3 cp \
    proto/casinoroundinfodto.pb \
    s3://hummock001/proto/casinoroundinfodto.pb
```

RisingWave fetches the `.pb` from MinIO at `CREATE TABLE` time via
`schema.location = 's3://hummock001/proto/casinoroundinfodto.pb'`. No
container volume mount required. Schema updates are: re-run protoc →
re-upload to MinIO → re-run the source DDL.

### 3.3 Verify wire format on the topic

```bash
docker exec redpanda rpk topic consume cronus.casino.out.gh \
  -X brokers=prd2-kafka-bootstrap.kaizengaming.net:443 \
  -X tls.enabled=true \
  -n 3
```

Leading bytes were `0a …` (protobuf field tag 1, wire type 2 = length-prefixed
string). Confirmed: **no Confluent 5-byte prefix** to strip.

### 3.4 Create the ingest TABLE in RisingWave

We use a `TABLE` (not a `SOURCE`) so RisingWave persists ingested rows in its
internal state store. This lets `scan.startup.mode = 'earliest'` replay the
full topic history into downstream MVs, and makes batch `SELECT COUNT(*)`
against the table consistent with what MVs have actually consumed.

```sql
DROP TABLE IF EXISTS src_casino_prd CASCADE;

CREATE TABLE src_casino_prd
WITH (
    connector                     = 'kafka',
    topic                         = 'cronus.casino.out.gh',
    properties.bootstrap.server   = 'prd2-kafka-bootstrap.kaizengaming.net:443',
    properties.security.protocol  = 'SSL',
    group.id.prefix               = 'rw-readonly-casino-demo',
    scan.startup.mode             = 'earliest'
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

> **Why TABLE instead of SOURCE?** A `SOURCE` is a thin streaming subscription —
> batch queries re-scan Kafka at read time, while streaming MVs only see
> messages arriving **after** the source was created (controlled by
> `scan.startup.mode`). That makes `SELECT COUNT(*) FROM src` (batch path,
> reads from earliest) disagree with MV row counts (streaming path, reads from
> `latest`). A `TABLE` materializes the stream once, so both paths agree and
> historical data can backfill MVs.

Sanity check — describe column shape:

```sql
\d src_casino_prd
```

Top-level columns (PascalCase, must be double-quoted in SQL):

```
"UniqueId"            STRING
"CustomerId"          INT
"CompanyId"           INT
"CasinoProviderId"    INT
"ExternalProviderId"  INT
"GameInfo"            STRUCT<"GameId" INT, "ProviderGameCode" STRING,
                              "IsLive" BOOL, "ProviderTableCode" STRING,
                              "GameType" INT,
                              "IsJackpotContributionsFromOperator" BOOL>
"RoundInfo"           STRUCT<"GameRoundRef" STRING,
                              "RoundCreated" STRUCT<seconds BIGINT, nanos INT>,
                              "RoundEnded"   STRUCT<seconds BIGINT, nanos INT>,
                              "Messages"     STRUCT<…, "Transactions" STRUCT<…>[], …>[]>
"IsBonusLockedOnFatMessageCreation"  BOOL
"IsBonusCampaignWagering"            BOOL
```

### 3.5 Bring up Lakekeeper + MinIO

```bash
docker compose up -d lakekeeper-db lakekeeper-migrate lakekeeper lakekeeper-bootstrap
```

`bin/bootstrap_lakekeeper.sh` (executed inside the `lakekeeper-bootstrap`
container) waits for `/health`, posts the EULA accept, then creates the
`risingwave-warehouse` warehouse pointing at MinIO bucket `hummock001` with
key prefix `risingwave-lakekeeper` and a public namespace. Idempotent — safe
to re-run.

### 3.5b Create the sportsbook bets source (UC2)

UC2's `mv_sportsbook_turnover_90d` reads from a second Kafka table, `src_bets_gh`,
which decodes `PandoraBetInfoVm` protobuf from the `bets-out-gh` topic on the prd4 cluster.

```bash
# Fetch the betinfo proto from Apicurio (same native-v2 pattern as casino)
curl -fsSL -H 'Accept: text/plain' \
  http://staging-schema-registry.kaizengaming.net/apis/registry/v2/groups/bigdata/artifacts/betinfo \
  > proto/betinfo.proto

# Compile (note: proto_path includes well-known types from homebrew or your system protoc)
protoc --descriptor_set_out=proto/betinfo.desc --include_imports \
  --proto_path=/opt/homebrew/include --proto_path=proto proto/betinfo.proto

# Upload to MinIO
AWS_ACCESS_KEY_ID=hummockadmin AWS_SECRET_ACCESS_KEY=hummockadmin \
  aws --endpoint-url http://localhost:9301 s3 cp \
    proto/betinfo.desc s3://hummock001/proto/betinfo.desc
```

```sql
CREATE TABLE src_bets_gh
WITH (
    connector                     = 'kafka',
    topic                         = 'bets-out-gh',
    properties.bootstrap.server   = 'prd4-kafka-bootstrap.kaizengaming.net:443',
    properties.security.protocol  = 'SSL',
    group.id.prefix               = 'rw-readonly-bets-demo',
    scan.startup.mode             = 'earliest'
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

> **`messages_as_jsonb` for self-referential types.** `PlayerSubstitutionInfoVm`
> contains a field of its own type, creating a recursive schema that RisingWave
> cannot represent as a native `STRUCT`. The `messages_as_jsonb` option tells
> RisingWave to decode that message type as `JSONB` rather than attempting a
> native struct mapping. `mv_sportsbook_turnover_90d` only reads
> `TotalStake.Euro` and `PlacedAt`, so `PlayerSubstitutionInfoVm` columns are
> never accessed and the JSONB fallback has no cost.

### 3.6 Real-time materialized views

See [`sql/casino_prd_funnel_iceberg.sql`](../sql/casino_prd_funnel_iceberg.sql)
for the full file. The pipeline covers two use cases:

**UC1 — Real Bet Amount:** `mv_casino_transactions` → `mv_casino_real_bet`
(rolling 14-day real bet total per customer/currency; filter for MessageTypeId=1, AccountId=1 applied inline).

**UC2 — Casino Turnover Percentage:** `mv_casino_turnover_90d` +
`mv_sportsbook_turnover_90d` → `mv_turnover_percentage` (90-day casino vs
sportsbook ratio per customer). UC2 requires a second source `src_bets_gh`
(sportsbook bets — `prd4-kafka-bootstrap.kaizengaming.net:443`, topic
`bets-out-gh`, 10 partitions); see step 3.5b above for setup.

#### UC1 — Real Bet Amount

```sql
-- UC1: Rolling 14-day real bet amount per customer/currency.
-- Filter: MessageTypeId=1 (bet placed), AccountId=1 (real money, not bonus).
-- One row per event with the updated 14-day sum; consumers key on
-- (customer_id, currency_id) and keep the latest row.
CREATE MATERIALIZED VIEW mv_casino_real_bet AS
SELECT
    customer_id,
    currency_id,
    transaction_created_at                                AS event_ts,
    SUM(amount_abs) OVER (
        PARTITION BY customer_id, currency_id
        ORDER BY transaction_created_at
        RANGE BETWEEN INTERVAL '1209600 SECONDS' PRECEDING AND CURRENT ROW
    ) AS rolling_14d_real_bet_amount
FROM mv_casino_transactions
WHERE message_type_id = 1
  AND account_id      = 1
  AND amount_raw IS NOT NULL
  AND amount_raw <> '';
```

#### UC2 — Casino Turnover Percentage

```sql
-- UC2: 90-day rolling casino turnover per customer.
-- Filter: MessageTypeId=2 (withdraw/payout), AccountId IN (1,4) (real+bonus).
CREATE MATERIALIZED VIEW mv_casino_turnover_90d AS
SELECT
    customer_id,
    transaction_created_at                                AS event_ts,
    SUM(amount_abs) OVER (
        PARTITION BY customer_id
        ORDER BY transaction_created_at
        RANGE BETWEEN INTERVAL '7776000 SECONDS' PRECEDING AND CURRENT ROW
    ) AS rolling_90d_turnover
FROM mv_casino_transactions
WHERE message_type_id = 2
  AND account_id      IN (1, 4)
  AND amount_raw IS NOT NULL
  AND amount_raw <> '';

-- UC2: 90-day rolling sportsbook turnover per customer (from src_bets_gh).
-- TotalStake.Euro uses DecimalValue encoding (units + nanos/1e9).
-- Euro-normalised so casino and sportsbook turnover are comparable across currencies.
CREATE MATERIALIZED VIEW mv_sportsbook_turnover_90d AS
SELECT
    ("CustomerInfo")."Id"                                                       AS customer_id,
    TO_TIMESTAMP(("PlacedAt").seconds)                                          AS event_ts,
    SUM((("TotalStake")."Euro")."units"::NUMERIC
        + (("TotalStake")."Euro")."nanos"::NUMERIC / 1000000000) OVER (
        PARTITION BY ("CustomerInfo")."Id"
        ORDER BY TO_TIMESTAMP(("PlacedAt").seconds)
        RANGE BETWEEN INTERVAL '7776000 SECONDS' PRECEDING AND CURRENT ROW
    ) AS rolling_90d_turnover
FROM src_bets_gh
WHERE ("CustomerInfo")."Id" IS NOT NULL
  AND ("TotalStake")."Euro" IS NOT NULL;

CREATE MATERIALIZED VIEW mv_casino_turnover_latest AS
SELECT DISTINCT ON (customer_id) customer_id,
    rolling_90d_turnover AS casino_turnover, event_ts
FROM mv_casino_turnover_90d
ORDER BY customer_id, event_ts DESC;

CREATE MATERIALIZED VIEW mv_sportsbook_turnover_latest AS
SELECT DISTINCT ON (customer_id) customer_id,
    rolling_90d_turnover AS sportsbook_turnover, event_ts
FROM mv_sportsbook_turnover_90d
ORDER BY customer_id, event_ts DESC;

-- Pivot via UNION ALL + GROUP BY, NOT a FULL OUTER JOIN. The _latest MVs are
-- keyed on customer_id (DISTINCT ON), so a FULL OUTER JOIN on that key holds
-- join state with an empty extra pk and can panic with `double inserting a
-- join state entry` while the DISTINCT ON sides emit retract+insert pairs. A
-- hash aggregation consumes that same update stream safely; a customer on only
-- one side gets 0 from the other branch (same as COALESCE(...,0)).
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

### 3.7 Managed-Iceberg sinks (Lakekeeper + MinIO)

Three MVs are landed into Lakekeeper-managed Iceberg tables:
- `mv_casino_raw` → `rw_managed_casino_raw` (faithful nested archive, raw_iceberg.sql)
- `mv_casino_real_bet` → `rw_managed_casino_real_bet` (rolling 14-day real bet per customer, funnel_iceberg.sql)
- `mv_turnover_percentage` → `rw_managed_turnover_percentage` (UC2 casino vs sportsbook ratio, funnel_iceberg.sql)

`sql/casino_prd_raw_iceberg.sql` produces a **faithful raw nested archive**
of every Kafka message:

- `mv_casino_raw` — thin pass-through MV that only renames the top-level
  columns of `src_casino_prd` to snake_case (`unique_id`, `customer_id`,
  `game_info`, `round_info`, …). All nested `STRUCT<…>` and
  `STRUCT<…>[]` types flow through unchanged.
- `rw_managed_casino_raw` — managed Iceberg table mirroring `src_casino_prd`
  1:1, including the array-of-struct `RoundInfo.Messages[]` and the
  twice-nested `Messages[].Transactions[]`. No flattening.

The raw sink is the event-sourced archive you can replay or re-derive from
at any time; the analytical MVs (`mv_casino_transactions` etc.) give fast
flat projections off the same source.

#### `src_casino_prd` vs `rw_managed_casino_raw`

Both hold the same logical data but live in completely different layers:

| | `src_casino_prd` | `rw_managed_casino_raw` |
|---|---|---|
| **Type** | Kafka-backed `TABLE` (RisingWave source) | Managed Iceberg `TABLE` (`ENGINE = iceberg`) |
| **Storage** | RisingWave's internal Hummock state (Kafka topic persisted into RW) | Parquet files in MinIO under Lakekeeper's warehouse |
| **Catalog** | RW catalog only | Lakekeeper REST catalog — discoverable by Spark, DuckDB, Trino, PyIceberg, … |
| **Format / wire** | Protobuf decoded from `cronus.casino.out.gh`, schema from `/proto/casinoroundinfodto.pb` | Iceberg v2 table with Parquet data files + snapshot metadata |
| **Freshness** | Real-time — every Kafka message lands here first | ~5 s behind (`commit_checkpoint_interval = 5`) |
| **Identifier casing** | PascalCase verbatim from proto (`"UniqueId"`, `"CustomerId"`, `"GameInfo"`, …); needs double-quoting | Top-level columns snake_case (`unique_id`, `customer_id`, `game_info`, …); nested struct fields still PascalCase quoted |
| **Schema** | Full nested protobuf reflection | Same nested shape — `STRUCT<…>` and `STRUCT<…>[]` (`RoundInfo.Messages[].Transactions[]`) preserved 1:1 |
| **Primary key** | None enforced at the table level | `PRIMARY KEY (unique_id)`, upserted via the sink |
| **Write path** | Streaming consumer of Kafka | Sink fed by `mv_casino_raw` (thin pass-through MV that only renames top-level cols to snake_case for the `primary_key` config) |
| **Use case** | Hot streaming source — drive MVs, real-time queries, RW dashboards | Cold/warm archival lake — historical analytics, replay, external engine access, time-travel via Iceberg snapshots |
| **Drop blast radius** | All MVs break; re-ingest from Kafka via `scan.startup.mode='earliest'` | Iceberg files stay in MinIO; can be re-attached as a foreign table or read directly by Spark/DuckDB |

In short, `rw_managed_casino_raw` is `src_casino_prd` projected into
open-format lakehouse storage with a stable PK and snake_case top-level
columns, kept ~5 s behind in near-real-time.

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
SET iceberg_engine_connection = 'public.lakekeeper_catalog_conn';
```

Each sink follows the same pattern — explicit `CREATE TABLE … ENGINE =
iceberg` declaring the column order to match the upstream MV's `SELECT`,
then an upsert sink keyed on the table's PK. Example (UC1 rolling real bet):

```sql
CREATE TABLE rw_managed_casino_real_bet (
    customer_id                  INT,
    currency_id                  INT,
    event_ts                     TIMESTAMPTZ,
    rolling_14d_real_bet_amount  NUMERIC,
    PRIMARY KEY (customer_id, currency_id, event_ts)
) ENGINE = iceberg;

CREATE SINK rw_managed_casino_real_bet_sink
INTO rw_managed_casino_real_bet
FROM mv_casino_real_bet
WITH (
    type                        = 'upsert',
    primary_key                 = 'customer_id,currency_id,event_ts',
    commit_checkpoint_interval  = 5         -- ~5 s freshness in Iceberg
);
```

Sinks created: `rw_managed_casino_raw_sink`,
`rw_managed_casino_real_bet_sink`,
`rw_managed_turnover_percentage_sink`.

Iceberg snapshots land under
`s3://hummock001/risingwave-lakekeeper/<warehouse-uuid>/<table-uuid>/`.

### 3.8 Verify end-to-end

After ~30 s (counts shown from a live tap — lag of a few rows between MV
and iceberg is expected with `commit_checkpoint_interval = 5`):

```text
--- Materialized views ---
 mv_casino_raw             | 424395
 mv_casino_transactions    | 539895
 mv_casino_real_bet        | 389541   ← one row per event (rolling window, not aggregate)

--- Iceberg sink tables (managed) ---
 rw_managed_casino_raw             | 424368
 rw_managed_casino_real_bet        | 389490
 rw_managed_turnover_percentage    |   8120
```

The raw iceberg table preserves the full nested shape — you can still drill
into messages and transactions after the fact:

```sql
SELECT unique_id,
       (round_info)."GameRoundRef"               AS round_ref,
       array_length((round_info)."Messages")     AS n_messages
FROM rw_managed_casino_raw
WHERE (round_info)."Messages" IS NOT NULL
LIMIT 5;
```

---

## 4. RisingWave optimizations applied

The SQL files incorporate several RisingWave-specific optimizations beyond the
minimum required to get the pipeline working. This section explains each one,
why it matters, and what would go wrong without it.

### 4.1 `APPEND ONLY` on source tables

```sql
CREATE TABLE src_casino_prd
APPEND ONLY
WITH ( connector = 'kafka', … )
FORMAT PLAIN ENCODE PROTOBUF (…);
```

Both `src_casino_prd` and `src_bets_gh` are Kafka topics carrying immutable
event records — a casino round is never updated or deleted, and neither is a
bet placement. Without `APPEND ONLY`, RisingWave allocates internal
delete-tracking structures and row-versioning overhead for data that will never
change. `APPEND ONLY` tells the engine to skip that bookkeeping entirely, which
reduces storage amplification and enables downstream MVs to skip retraction
handling.

**What breaks without it:** nothing functionally, but every row carries hidden
overhead that scales with cardinality.

### 4.2 Shared Kafka source (`streaming_use_shared_source`)

```sql
SET streaming_use_shared_source = true;
```

`src_casino_prd` is read by at least three independent MV chains:
`mv_casino_transactions` (and its descendants), `mv_casino_raw`, and the raw
Iceberg sink. Without this setting, RisingWave spawns a separate
`SourceExecutor` — an independent Kafka consumer — for each of those chains.
With N downstream MVs, you get N consumer group members, N× broker connections,
and N× network bandwidth consumed from the Kafka cluster.

With `streaming_use_shared_source = true`, a single consumer fans out to all
MV operators internally. The Kafka cluster only sees one consumer per topic per
RisingWave cluster.

**What breaks without it:** nothing functionally, but Kafka broker load and
network egress scale with the number of MVs rather than being constant.

### 4.3 Non-blocking DDL (`background_ddl`) — applied selectively to terminal statements

`background_ddl = true` causes `CREATE MATERIALIZED VIEW`, `CREATE SINK`, and
`CREATE INDEX` to return before the object is fully built. This prevents the
psql session from blocking while a large backfill or index build runs.

**It is applied selectively in this pipeline, not at the top of the session.**
The reason: when `background_ddl` is on, the created object is not immediately
visible in the catalog. A subsequent `CREATE MATERIALIZED VIEW` that references
a just-created MV would fail with `table or source not found`. Because the MV
chain (`mv_casino_transactions` → `mv_casino_real_bet`, `mv_casino_turnover_90d`,
etc.) requires each step to be fully registered before the next can be created,
`background_ddl` must be off during MV creation.

Sinks and indexes have no downstream dependents in the same session — they are
terminal. `SET background_ddl = true` is therefore added immediately before the
first `CREATE SINK` / `CREATE INDEX` block in each file:

```sql
-- MVs are all created synchronously above (background_ddl = false / default).
-- From here, only sinks and indexes remain — nothing depends on them below.
SET background_ddl = true;

CREATE SINK rw_managed_casino_raw_sink INTO … FROM mv_casino_raw WITH (…);
```

Without this, `CREATE SINK rw_managed_casino_raw_sink` blocks until the full
initial Iceberg snapshot of the backfilled `mv_casino_raw` (424k+ rows) is
committed — which can take minutes and makes the demo script appear to hang.

Monitor ongoing background jobs with:

```sql
SHOW JOBS;
-- or
SELECT ddl_id, ddl_statement, progress FROM rw_catalog.rw_ddl_progress;
```

**What breaks with it applied globally (at session top):** every MV after the
first fails with "table or source not found" — the catalog registration is
asynchronous and the next statement runs before the previous MV is visible.

### 4.4 `ROW_NUMBER()` Top-1 instead of `DISTINCT ON … ORDER BY`

```sql
-- Before (broken in streaming mode):
SELECT DISTINCT ON (customer_id) customer_id, rolling_90d_turnover, event_ts
FROM mv_casino_turnover_90d
ORDER BY customer_id, event_ts DESC;

-- After (correct):
SELECT customer_id, casino_turnover, event_ts
FROM (
    SELECT customer_id,
           rolling_90d_turnover AS casino_turnover,
           event_ts,
           ROW_NUMBER() OVER (PARTITION BY customer_id ORDER BY event_ts DESC) AS rn
    FROM mv_casino_turnover_90d
) t
WHERE rn = 1;
```

This is a correctness fix, not just an optimization.

`DISTINCT ON (customer_id) … ORDER BY customer_id, event_ts DESC` works in
PostgreSQL because the `ORDER BY` determines which row within each group the
`DISTINCT ON` keeps (the one with the largest `event_ts`). In RisingWave's
streaming executor, however, a trailing `ORDER BY` on a `CREATE MATERIALIZED
VIEW` applies only at DDL-creation time — it does not affect how the streaming
result is maintained as new rows arrive. The result is that `DISTINCT ON` picks
an *arbitrary* row per `customer_id`, not necessarily the one with the latest
`event_ts`.

For `mv_casino_turnover_latest` and `mv_sportsbook_turnover_latest`, this means
the `rolling_90d_turnover` value fed into `mv_turnover_percentage` may reflect
a stale event rather than the most recent 90-day sum — silently producing wrong
casino/sportsbook ratios.

The `ROW_NUMBER() OVER (PARTITION BY customer_id ORDER BY event_ts DESC)`
pattern compiles to RisingWave's stateful `TopN` operator, which is explicitly
designed to maintain the top-1 row per key in a streaming context. It correctly
handles the retract+insert pairs emitted by the upstream sliding-window MVs
when a newer event arrives for a customer.

**What breaks without it:** `mv_turnover_percentage` computes ratios from
stale/arbitrary per-customer sums, producing incorrect Iceberg sink output.

### 4.5 `force_compaction = true` on upsert sinks

```sql
CREATE SINK rw_managed_casino_real_bet_sink
INTO rw_managed_casino_real_bet FROM mv_casino_real_bet
WITH (
    type = 'upsert', primary_key = '…',
    commit_checkpoint_interval = 5,
    force_compaction = true        -- added
);
```

Applied to all three Iceberg upsert sinks (`rw_managed_casino_real_bet`,
`rw_managed_turnover_percentage`, `rw_managed_casino_raw`).

Sliding-window MVs emit a recalculated row for every incoming event within the
window's scope. Without `force_compaction`, every intermediate state — even
those that are immediately superseded by the next event in the same checkpoint
— is written as a separate row to the Iceberg sink. For a high-frequency casino
stream, a single checkpoint can contain thousands of intermediate states per
customer, each landing as a small Iceberg data file.

With `force_compaction = true`, RisingWave deduplicates by primary key within
each checkpoint before writing, emitting only the final state per key. This
dramatically reduces small-file count in the Iceberg warehouse and the I/O
written per checkpoint.

**What breaks without it:** Iceberg small-file proliferation, slower scan
performance on the lake side, and excessive write I/O — nothing breaks
functionally but the Iceberg tables degrade over time.

### 4.6 Indexes on serving MVs

```sql
CREATE INDEX IF NOT EXISTS idx_casino_real_bet_customer
    ON mv_casino_real_bet (customer_id, currency_id);

CREATE INDEX IF NOT EXISTS idx_turnover_percentage_customer
    ON mv_turnover_percentage (customer_id);
```

`mv_casino_real_bet` and `mv_turnover_percentage` are the terminal serving MVs
queried by Grafana and application code. Without indexes, every query that
filters by `customer_id` performs a full heap scan over potentially millions of
rows. RisingWave indexes are maintained incrementally by the streaming engine
as new events arrive — they add a small write overhead but make point-lookup
queries instant regardless of total cardinality.

**What breaks without it:** Grafana dashboards and customer-level queries
become progressively slower as the cardinality grows.

### 4.7 Watermarks — known limitation

The pipeline does not yet define watermarks on the source tables. Watermarks
are required for:

- Bounded sliding-window state: without one, the 14-day and 90-day
  `RANGE BETWEEN … PRECEDING` windows accumulate state for every customer
  forever — RisingWave has no signal for when old event-time rows can be
  expired.
- `EMIT ON WINDOW CLOSE` semantics: without watermarks, window MVs emit
  partial results on every incoming event rather than final results once the
  window is closed.

Adding watermarks to these sources is blocked by the protobuf nested-struct
layout: the relevant timestamps (`txn."Created"` for casino transactions,
`"PlacedAt"` for bets) are either deeply nested inside array-of-struct fields
(requiring UNNEST before they are accessible as scalar columns) or are
`STRUCT<seconds BIGINT, nanos INT>` rather than native `TIMESTAMPTZ`, which
RisingWave's watermark clause requires.

The practical resolution is to promote a derived timestamp to a top-level
generated column on each source table and define the watermark there. This
requires schema changes not yet implemented. Until then, window state is
unbounded; plan for memory growth proportional to the number of distinct
customers in the stream.

---

## 5. Querying the source

> **Rule of thumb:** never do ad-hoc `SELECT … FROM src_casino_prd` directly.
> A bare select on a Kafka source is a **batch scan** that respects
> `scan.startup.mode='latest'` and usually returns 0 rows. Always go through
> an MV.

### 5.1 The `mv_casino_transactions` definition

One row per transaction, flattening the nested `Messages[].Transactions[]`
structure via two chained `UNNEST` calls with explicit row aliases:

```sql
CREATE MATERIALIZED VIEW mv_casino_transactions AS
SELECT
    s."CustomerId"                                        AS customer_id,
    msg."MessageTypeId"                                   AS message_type_id,
    TO_TIMESTAMP((msg."Created").seconds)                 AS message_created_at,
    txn."TransactionId"                                   AS transaction_id,
    txn."AccountId"                                       AS account_id,
    txn."CurrencyId"                                      AS currency_id,
    TO_TIMESTAMP((txn."Created").seconds)                 AS transaction_created_at,
    ABS(NULLIF(txn."Amount", '')::numeric)                AS amount_abs,
    txn."Amount"                                          AS amount_raw,
    txn."BonusAction"                                     AS bonus_action,
    (s."GameInfo")."GameId"                               AS game_id,
    (s."GameInfo")."GameType"                             AS game_type,
    (s."GameInfo")."IsLive"                               AS is_live,
    s."CompanyId"                                         AS company_id
FROM
    src_casino_prd                              AS s,
    UNNEST((s."RoundInfo")."Messages")          AS msg,
    UNNEST(msg."Transactions")                  AS txn;
```

### 5.2 Why it's written that way — the four gotchas

1. **Struct field access needs parens.**
   `s."RoundInfo"."Messages"` is parsed as `schema.table.column`. Wrap the
   struct reference: `(s."RoundInfo")."Messages"`. Same for nested:
   `((r."RoundInfo")."RoundCreated")."seconds"`.

2. **`UNNEST(struct[])` flattens.** The optional alias names the **row**, not
   a struct value:

   ```sql
   FROM src_casino_prd, UNNEST((... )."Messages") AS m
   --                                                ^^^^^
   -- m.MessageId is invalid. Just reference "MessageId" directly.
   ```

   Inner struct fields are exposed as top-level (quoted, PascalCase) columns
   of the resulting row.

3. **Double UNNEST with field-name collisions.** Both
   `CasinoMessageInformation` and `TransactionInformation` have a `Created`
   field. Use explicit row aliases (`UNNEST(...) AS msg, UNNEST(msg."Transactions") AS txn`)
   and reference them as `msg."Created"` / `txn."Created"` to disambiguate.
   Chaining the second UNNEST off the alias (`UNNEST(msg."Transactions")`) is
   the idiomatic RisingWave pattern — no CTE needed.

4. **Decimal-as-string proto fields can be empty.** `Amount`, `TokenAmount`,
   `CurrencyRateToEuro`, `CommissionAmount`, etc. are `string`-typed in the
   protobuf for arbitrary precision. Some rows hold `''`, which
   `::numeric` cannot parse. Use `NULLIF("Amount", '')::numeric` or
   `try_cast("Amount" AS numeric)`.

### 5.3 Converting `google.protobuf.Timestamp`

Lands as `STRUCT<seconds BIGINT, nanos INT>`. Convert with:

```sql
to_timestamp((ts)."seconds" + (ts)."nanos" / 1e9)
```

Returns `TIMESTAMP WITH TIME ZONE` (UTC). For local: `… AT TIME ZONE
'Europe/Athens'`. To drop the zone: `…::timestamp`.

### 5.4 Useful one-liners

```sql
-- live tail of transaction count
SELECT count(*) FROM mv_casino_transactions;

-- most recent real-money bets (latest rolling sum per customer/currency)
SELECT customer_id, currency_id, rolling_14d_real_bet_amount, event_ts
FROM mv_casino_real_bet
ORDER BY event_ts DESC LIMIT 10;

-- top customers by 14-day rolling real bet amount (latest row per customer)
SELECT customer_id, currency_id, rolling_14d_real_bet_amount
FROM (
    SELECT customer_id, currency_id, rolling_14d_real_bet_amount,
           ROW_NUMBER() OVER (PARTITION BY customer_id, currency_id ORDER BY event_ts DESC) AS rn
    FROM mv_casino_real_bet
) t
WHERE rn = 1
ORDER BY rolling_14d_real_bet_amount DESC NULLS LAST LIMIT 20;

-- UC2: current casino turnover percentage by customer
SELECT customer_id, casino_ratio, sportsbook_ratio, total_turnover
FROM mv_turnover_percentage
ORDER BY total_turnover DESC NULLS LAST LIMIT 20;
```

Bash watch:

```bash
watch -n 2 'psql -h localhost -p 4566 -d dev -U root -c \
  "SELECT count(*) FROM mv_casino_transactions"'
```

### 5.5 Why `mv_turnover_percentage` is a pivot, not a `FULL OUTER JOIN`

UC2 needs one row per customer combining their casino and sportsbook turnover.
The obvious way to write that is a `FULL OUTER JOIN` of the two
`*_turnover_latest` MVs on `customer_id`. That version **worked at rest but
reset the whole database under live ingestion** — worth understanding before
anyone "simplifies" it back.

**The failure.** During a full pipeline build against the live topic, creating
the join MV panicked:

```
Executor error: join key: [428491138], pk: [], row: [428491138, 3924.70],
state_table_id: 48: double inserting a join state entry
```

`gRPC … CreateMaterializedView failed … failed over: database 1 reset` — and
because the license disables `DatabaseFailureIsolation` (cluster has 50 CPU
cores, license cap is 4), that single job's panic reset the entire database.

**Root cause.** Both inputs are `SELECT DISTINCT ON (customer_id) …`, so each
input's **stream key is `customer_id`** — the same column the join keys on. A
hash join keyed on `customer_id` therefore keeps its state with **no extra pk**
(`pk: []`) and assumes at most one row per `customer_id` per side. But
`DISTINCT ON` is a Top-1 that emits a **retract + insert pair** whenever the
"latest" row for a customer changes. Under live ingestion (and during
backfill) the insert can momentarily reach the join state before the matching
retract, presenting two rows for the same `customer_id` → "double inserting a
join state entry". At rest, with no updates flowing, the race never fires —
which is why it looked fine in isolation.

**The fix.** Replace the join with a `UNION ALL` + `GROUP BY customer_id`
pivot: each branch contributes its own metric and `0` for the other side, and
a hash aggregation sums them per customer.

```sql
FROM (
    SELECT customer_id, casino_turnover, 0::NUMERIC AS sportsbook_turnover
    FROM mv_casino_turnover_latest
    UNION ALL
    SELECT customer_id, 0::NUMERIC AS casino_turnover, sportsbook_turnover
    FROM mv_sportsbook_turnover_latest
) u
GROUP BY customer_id
```

A hash aggregation maintains **one state row per group** and applies `+`/`-`
deltas as updates arrive — it's built to consume exactly the retract+insert
stream that `DISTINCT ON` produces, and it never uses the join-state tables
where the empty-pk assumption lives. The full-outer semantics are preserved:
a customer present on only one side simply gets `0` from the other branch,
identical to the old `COALESCE(…, 0)`.

**Verified equivalent and deterministic.** The pivot produces the same 2212
rows (= distinct-customer union of both sides), ratios sum to 1, and
`total = casino + sportsbook` exactly. Rebuilt under live ingestion, and then
rebuilt the entire funnel from scratch, both with **zero database resets** —
the operation that previously failed every time.

> **Takeaway:** when combining two streams whose join key is also their unique
> stream key, prefer `UNION ALL` + `GROUP BY` over `FULL OUTER JOIN`. The
> aggregation path is robust to the retract+insert updates that `DISTINCT ON`,
> Top-N, and other stateful upstreams emit.

---

## 6. Gotchas worth remembering

- **Apicurio ↔ RisingWave protobuf quadrilemma**
  ccompat returns binary FDS; native v2 returns `.proto` text.
  `schema.registry` requires the Confluent magic-byte wire format (our topics
  don't have it) AND expects `.proto` text from the registry.
  `schema.location` with `http://` fetches from any URL but expects binary
  FDS — so pointing it at the native v2 text endpoint also fails.
  **Resolution:** fetch `.proto` via native v2, compile to binary FDS, upload
  to MinIO, and reference via
  `schema.location = 's3://hummock001/proto/…'`. RisingWave fetches the FDS
  from MinIO at `CREATE TABLE` time. No container volume mount needed.

- **`scan.startup.mode = 'earliest'` + `CREATE TABLE` replays history.**
  Earlier iterations used `CREATE SOURCE` + `latest`; that made batch
  `SELECT COUNT(*) FROM src` (which re-scans Kafka from earliest) disagree
  with MV counts (streaming, latest). Switching to `TABLE` persists the
  topic into RW state once, so both paths agree.

- **Don't ad-hoc `SELECT` from the source.** Wrap in an MV; otherwise the
  query is a transient batch consumer.

- **PascalCase identifiers must be double-quoted.** RW preserves protobuf
  field names verbatim; unquoted `CompanyId` folds to `companyid` and
  doesn't resolve.

- **Managed Iceberg sinks bind columns *positionally*, not by name.**
  `CREATE TABLE … ENGINE = iceberg` followed by `CREATE SINK … FROM <mv>`
  matches columns by ordinal — the target table's column order MUST mirror
  the upstream MV's `SELECT` projection. A mismatch surfaces as a confusing
  error of the form `column type mismatch: <target_type> vs <mv_type>,
  column name: "<mv>.<wrong_name>"`. The reported column name is the MV's
  column at the offending ordinal — not the target column actually being
  bound — so the error often points at a totally unrelated field. When you
  see it, line up the two column lists side-by-side and reorder either the
  `CREATE TABLE` or the MV's `SELECT` to match.

- **Iceberg sink `primary_key` config lowercases the lookup string.**
  `CREATE SINK … WITH (primary_key = 'UniqueId')` against an upstream that
  exposes the column as the quoted PascalCase `"UniqueId"` fails with
  `Primary key column uniqueid not found in sink schema`. Quoting in the
  option value (`primary_key = '"UniqueId"'`) doesn't help either.
  *Workaround:* put a thin pass-through MV between source and sink that
  aliases the PK column to snake_case (e.g. `mv_casino_raw` does
  `s."UniqueId" AS unique_id`). Nested struct types pass through
  unchanged; only the top-level PK column needs renaming.

- **License caps streaming-failure isolation at 4 CPU cores.**
  The cluster has far more (50 observed). Result: `DatabaseFailureIsolation`
  is disabled and **any single streaming-job failure resets the whole
  database** (error `database 1 reset`). Known trigger that remains:
  - The first `CREATE TABLE … ENGINE = iceberg` after a fresh start, on the
    JVM/JNI catalog cold start.

  A second trigger — `mv_turnover_percentage` panicking with `double inserting
  a join state entry` — has been **structurally eliminated**: that MV was a
  `FULL OUTER JOIN` of two `DISTINCT ON (customer_id)` streams (join key ==
  stream key ⇒ empty extra pk in the join state, which can't tolerate the
  transient duplicate a retract+insert pair produces under live ingestion). It
  is now a `UNION ALL` + `GROUP BY customer_id` pivot (a hash aggregation that
  consumes update streams safely). Rebuilt against the live topic with zero
  resets.

  *Workaround for the remaining JVM cold-start trigger:* the DDL files are
  idempotent (`DROP … IF EXISTS` / `CREATE … IF NOT EXISTS`), and
  `bin/3_run_casino_prd_demo.sh` wraps the pipeline steps in
  `run_sql_with_retry` (3 attempts, 15 s recovery wait), so a transient reset
  no longer aborts startup — it converges once the JVM catalog is warm.

- **`background_ddl = true` breaks chained MV creation — apply it only to terminal statements.**
  When `background_ddl` is on, `CREATE MATERIALIZED VIEW` (and `CREATE SINK`,
  `CREATE INDEX`) return before the object is registered in the catalog. The
  next statement that references the just-created object fails with
  `table or source not found`. Safe pattern: keep `background_ddl = false`
  (the default) for all chained MV creation; then `SET background_ddl = true`
  immediately before the first terminal sink or index, which have no downstream
  dependents in the same session. This prevents the demo from appearing to hang
  while the initial Iceberg snapshot is committed. See §4.3 for detail.

- **`DISTINCT ON … ORDER BY` is not a reliable Top-1 in streaming mode.**
  In a streaming MV, `ORDER BY` at the SELECT level applies only at DDL-creation
  time; it has no effect on how ongoing results are maintained. `DISTINCT ON`
  therefore picks an arbitrary row per key, not the row with the largest or
  smallest sort column. Use `ROW_NUMBER() OVER (PARTITION BY key ORDER BY …)`
  with `WHERE rn = 1` instead — this compiles to a stateful TopN operator that
  correctly handles retract+insert update pairs. See §4.4 for detail.

- **`DISTINCT ON` inputs to a `FULL OUTER JOIN` can panic the database.**
  See §5.5 for a full post-mortem. The short version: `DISTINCT ON (k)` emits
  retract+insert pairs; a hash join keyed on `k` allocates state with an empty
  extra pk and cannot tolerate seeing two rows for the same `k` simultaneously —
  a transient state that a retract+insert pair creates. The fix is `UNION ALL` +
  `GROUP BY` (a hash aggregation, not a join). This was structurally eliminated;
  the additional safeguard added is replacing the `DISTINCT ON` inputs with
  `ROW_NUMBER()` Top-1 (§4.4) so there is no retract+insert on the TopN output
  as long as the upstream sliding-window MV emits monotonically increasing
  `event_ts` per customer.

- **`s3.*` options in the ENCODE clause produce "unknown format_encode_options" warnings.**
  The S3 credentials and endpoint options (`s3.region`, `s3.endpoint`,
  `s3.access.key`, `s3.secret.key`) live conceptually at the connector level,
  so RisingWave warns when they appear inside the `FORMAT … ENCODE PROTOBUF (…)`
  block. The warning is harmless — the options are still applied and MinIO is
  reached correctly. `CREATE TABLE` succeeds.

- **OrbStack HTTP proxy is hostile to container DNS.**
  `bin/bootstrap_lakekeeper.sh` unsets `http_proxy/HTTPS_PROXY` because the
  injected `proxyproxy.orb.internal:8305` can't resolve docker-network
  hostnames like `lakekeeper`.

---

## 7. Files of interest

| Path | Purpose |
|---|---|
| [proto/casinoroundinfodto.proto](../proto/casinoroundinfodto.proto) | Casino schema, fetched from Apicurio v2 native |
| [proto/casinoroundinfodto.pb](../proto/casinoroundinfodto.pb) | Compiled FileDescriptorSet for casino; uploaded to `s3://hummock001/proto/` |
| [proto/betinfo.proto](../proto/betinfo.proto) | Sportsbook bets schema (UC2), fetched from Apicurio v2 native |
| [proto/betinfo.desc](../proto/betinfo.desc) | Compiled FileDescriptorSet for bets; uploaded to `s3://hummock001/proto/` |
| [sql/casino_prd_source.sql](../sql/casino_prd_source.sql) | Creates `src_casino_prd` TABLE (prd2 Kafka, protobuf) |
| [sql/casino_prd_bets_source.sql](../sql/casino_prd_bets_source.sql) | Creates `src_bets_gh` TABLE (prd4 Kafka, protobuf + JSONB fallback for self-referential type) |
| [sql/casino_prd_funnel_iceberg.sql](../sql/casino_prd_funnel_iceberg.sql) | UC1 + UC2 analytical MVs + 2 managed Iceberg sinks (`rw_managed_casino_real_bet`, `rw_managed_turnover_percentage`) |
| [sql/casino_prd_raw_iceberg.sql](../sql/casino_prd_raw_iceberg.sql) | Faithful nested raw archive: `mv_casino_raw` + `rw_managed_casino_raw` |
| [bin/bootstrap_lakekeeper.sh](../bin/bootstrap_lakekeeper.sh) | Idempotent Lakekeeper warehouse bootstrap |
| [docker-compose.yml](../docker-compose.yml) | Service definitions (RW, MinIO, Lakekeeper, Redpanda for tooling) |

---

## 8. Reproduce from scratch

```bash
# 1. Casino schema — fetch, compile, upload to MinIO
curl -fsSL -H 'Accept: text/plain' \
  http://staging-schema-registry.kaizengaming.net/apis/registry/v2/groups/bigdata/artifacts/casinoroundinfo \
  > proto/casinoroundinfodto.proto
protoc --include_imports \
  --descriptor_set_out=proto/casinoroundinfodto.pb \
  proto/casinoroundinfodto.proto
AWS_ACCESS_KEY_ID=hummockadmin AWS_SECRET_ACCESS_KEY=hummockadmin \
  aws --endpoint-url http://localhost:9301 s3 cp \
    proto/casinoroundinfodto.pb s3://hummock001/proto/casinoroundinfodto.pb

# 1b. Bets schema (UC2)
curl -fsSL -H 'Accept: text/plain' \
  http://staging-schema-registry.kaizengaming.net/apis/registry/v2/groups/bigdata/artifacts/betinfo \
  > proto/betinfo.proto
protoc --descriptor_set_out=proto/betinfo.desc --include_imports \
  --proto_path=/opt/homebrew/include --proto_path=proto proto/betinfo.proto
AWS_ACCESS_KEY_ID=hummockadmin AWS_SECRET_ACCESS_KEY=hummockadmin \
  aws --endpoint-url http://localhost:9301 s3 cp \
    proto/betinfo.desc s3://hummock001/proto/betinfo.desc

# 2. Stack (MinIO must be up before step 1 uploads — adjust ordering if needed)
docker compose up -d \
  minio-0 frontend-node-0 meta-node-0 compute-node-0 compactor-0 \
  lakekeeper-db lakekeeper-migrate lakekeeper lakekeeper-bootstrap

# 3. Sources + MVs + Iceberg sinks (idempotent)
psql -h localhost -p 4566 -d dev -U root -f sql/casino_prd_source.sql
psql -h localhost -p 4566 -d dev -U root -f sql/casino_prd_bets_source.sql   # UC2
psql -h localhost -p 4566 -d dev -U root -f sql/casino_prd_funnel_iceberg.sql
psql -h localhost -p 4566 -d dev -U root -f sql/casino_prd_raw_iceberg.sql
# If the first run hits `database 1 reset` on the iceberg CREATE TABLE,
# just rerun the same command — JVM is warm, it'll complete.

# 4. Verify
psql -h localhost -p 4566 -d dev -U root <<'SQL'
SELECT 'mv_raw'            AS view, count(*) FROM mv_casino_raw
UNION ALL SELECT 'mv_transactions',  count(*) FROM mv_casino_transactions
UNION ALL SELECT 'mv_real_bet',      count(*) FROM mv_casino_real_bet
UNION ALL SELECT 'mv_turnover_pct',  count(*) FROM mv_turnover_percentage
UNION ALL SELECT 'ice_raw',          count(*) FROM rw_managed_casino_raw
UNION ALL SELECT 'ice_real_bet',     count(*) FROM rw_managed_casino_real_bet
UNION ALL SELECT 'ice_turnover_pct', count(*) FROM rw_managed_turnover_percentage;
SQL
```
