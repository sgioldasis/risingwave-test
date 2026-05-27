# Production Casino Stream → RisingWave → Iceberg Demo

End-to-end walkthrough of reading the live `cronus.casino.out.gh` topic from
the Kaizen production Kafka cluster into RisingWave, building real-time
materialized views, and landing the results into Lakekeeper-managed Iceberg
tables.

> **Status:** working as of 2026-05-27. RisingWave 2.8.2, Lakekeeper
> `latest-main`, MinIO local. No producer required — we consume the live
> production topic directly.

---

## 1. Architecture

```
┌──────────────────────────┐                      ┌───────────────────────────┐
│ Kaizen Apicurio Registry │  one-shot fetch      │  Local repo                │
│ (staging-schema-registry)│ ───────────────────▶ │  proto/casinoroundinfodto  │
│   bigdata/casinoroundinfo│                      │       .proto  + .pb        │
└──────────────────────────┘                      └─────────────┬─────────────┘
                                                                │ mounted in
                                                                ▼  RW container
┌──────────────────────────┐    SSL one-way   ┌─────────────────────────────┐
│ Kaizen prd2 Kafka        │ ────────────────▶│  RisingWave (compute-node-0)│
│ prd2-kafka-bootstrap     │  topic           │   src_casino_prd  (SOURCE)  │
│ .kaizengaming.net:443    │  cronus.casino   │   ↓                          │
│ 5 brokers, 290 topics    │  .out.gh         │   mv_casino_prd_rounds_flat  │
└──────────────────────────┘                  │   mv_casino_prd_volume_..    │
                                              │   mv_casino_prd_funnel       │
                                              └────────────┬────────────────┘
                                                           │ upsert sink
                                                           ▼  (commit ≈5s)
                                              ┌─────────────────────────────┐
                                              │ Lakekeeper REST catalog     │
                                              │ + MinIO S3 (hummock001)     │
                                              │ rw_managed_casino_prd_*     │
                                              └─────────────────────────────┘
```

Key decisions:

- **Schema sourcing is one-shot at deploy time**, not at RW runtime.
  Apicurio's `ccompat/v7` layer normalises PROTOBUF artifacts to binary
  `FileDescriptorSet`, which RisingWave's `schema.registry` fetcher cannot
  parse. We bypass it.
- **Wire format on `cronus.casino.out.*` is raw protobuf**, no Confluent
  5/6-byte magic prefix. RisingWave's `schema.location` (file-based) reads
  byte 0 as a protobuf field tag — that matches what's on the wire, so no
  framing strip is needed.
- **`scan.startup.mode = 'latest'`** so we don't drag the full 386 K+ history
  through the dev cluster; the demo proves itself on live traffic.

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

### 3.2 Compile to a FileDescriptorSet

RisingWave's `schema.location` reads a serialised
`google.protobuf.FileDescriptorSet`, **not** a `.proto` text. We compile
with `protoc --include_imports` so well-known types like
`google.protobuf.Timestamp` are baked in:

```bash
protoc \
  --include_imports \
  --descriptor_set_out=proto/casinoroundinfodto.pb \
  proto/casinoroundinfodto.proto
```

`proto/` is mounted into the RisingWave container at `/proto/`
(see [docker-compose.yml](../docker-compose.yml)).

### 3.3 Verify wire format on the topic

```bash
docker exec redpanda rpk topic consume cronus.casino.out.gh \
  -X brokers=prd2-kafka-bootstrap.kaizengaming.net:443 \
  -X tls.enabled=true \
  -n 3
```

Leading bytes were `0a …` (protobuf field tag 1, wire type 2 = length-prefixed
string). Confirmed: **no Confluent 5-byte prefix** to strip.

### 3.4 Create the source in RisingWave

```sql
DROP SOURCE IF EXISTS src_casino_prd CASCADE;

CREATE SOURCE src_casino_prd
WITH (
    connector                     = 'kafka',
    topic                         = 'cronus.casino.out.gh',
    properties.bootstrap.server   = 'prd2-kafka-bootstrap.kaizengaming.net:443',
    properties.security.protocol  = 'SSL',
    group.id.prefix               = 'rw-readonly-casino-demo',
    scan.startup.mode             = 'latest'
)
FORMAT PLAIN ENCODE PROTOBUF (
    schema.location = 'file:///proto/casinoroundinfodto.pb',
    message         = 'Cronus.CasinoService.RoundInfo.Abstractions.CasinoRoundInfoDto'
);
```

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

### 3.6 Real-time materialized views

See [`sql/casino_prd_funnel_iceberg.sql`](../sql/casino_prd_funnel_iceberg.sql)
for the full file. Highlights:

```sql
-- One row per round, flattened
CREATE MATERIALIZED VIEW mv_casino_prd_rounds_flat AS
SELECT
    r."UniqueId"                                    AS unique_id,
    r."CustomerId"                                  AS customer_id,
    r."CompanyId"                                   AS company_id,
    (r."GameInfo")."GameId"                         AS game_id,
    (r."GameInfo")."ProviderGameCode"               AS provider_game_code,
    (r."RoundInfo")."GameRoundRef"                  AS game_round_ref,
    to_timestamp(((r."RoundInfo")."RoundCreated")."seconds"
               + ((r."RoundInfo")."RoundCreated")."nanos" / 1e9)
                                                    AS round_created,
    array_length((r."RoundInfo")."Messages")        AS num_messages
FROM src_casino_prd AS r;

-- Bet-volume rollup per (company, game)
CREATE MATERIALIZED VIEW mv_casino_prd_volume_by_company_game AS
WITH exploded_msgs AS (
    SELECT
        r."CompanyId"              AS company_id,
        (r."GameInfo")."GameId"    AS game_id,
        (r."GameInfo")."GameType"  AS game_type,
        r."UniqueId"               AS unique_id,
        "Transactions"             AS txs
    FROM src_casino_prd AS r,
         UNNEST((r."RoundInfo")."Messages")
)
SELECT
    company_id, game_id, game_type,
    COUNT(DISTINCT unique_id)                          AS rounds,
    COUNT(*)                                           AS transactions,
    ROUND(SUM(NULLIF("Amount", '')::numeric), 4)       AS total_amount,
    ROUND(SUM(NULLIF("TokenAmount", '')::numeric), 4)  AS total_token_amount,
    ROUND(AVG(NULLIF("CurrencyRateToEuro", '')::numeric), 6) AS avg_fx_to_eur
FROM exploded_msgs, UNNEST(txs)
GROUP BY 1, 2, 3;

-- Per-(company, game, message-type) funnel
CREATE MATERIALIZED VIEW mv_casino_prd_funnel AS
WITH stages AS (
    SELECT
        r."CompanyId"                       AS company_id,
        (r."GameInfo")."GameId"             AS game_id,
        r."UniqueId"                        AS unique_id,
        "MessageTypeId"                     AS msg_type,
        "IsRoundClosed"                     AS round_closed
    FROM src_casino_prd AS r,
         UNNEST((r."RoundInfo")."Messages")
)
SELECT
    company_id, game_id, msg_type,
    COUNT(*)                              AS messages,
    COUNT(DISTINCT unique_id)             AS rounds_with_stage,
    COUNT(*) FILTER (WHERE round_closed)  AS round_closed_count
FROM stages
GROUP BY 1, 2, 3;
```

### 3.7 Managed-Iceberg sinks (Lakekeeper + MinIO)

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

Each sink follows the pattern:

```sql
CREATE TABLE rw_managed_casino_prd_rounds ( … PRIMARY KEY (unique_id) )
  ENGINE = iceberg;

CREATE SINK rw_managed_casino_prd_rounds_sink
INTO rw_managed_casino_prd_rounds
FROM mv_casino_prd_rounds_flat
WITH (
    type                        = 'upsert',
    primary_key                 = 'unique_id',
    enable_compaction           = 'true',
    compaction_interval_sec     = '60',
    enable_snapshot_expiration  = 'true',
    commit_checkpoint_interval  = 5         -- ~5 s freshness in Iceberg
);
```

Sinks created: `rw_managed_casino_prd_rounds_sink`,
`rw_managed_casino_prd_volume_sink`, `rw_managed_casino_prd_funnel_sink`.

### 3.8 Verify end-to-end

After ~30 s:

```text
=== MV row counts ===
 rounds_flat |  573
 volume      |   11
 funnel      |   22

=== Iceberg row counts ===
 iceberg_rounds | 566
 iceberg_volume |  11
 iceberg_funnel |  22

=== Top 5 (company, game) by total bet amount ===
 company_id | game_id | game_type | rounds | transactions | total_amount
       30 |  35697 |        2 |    103 |          308 |      145.50
       30 |  36556 |        2 |      1 |            7 |       58.5
       30 |  35937 |        2 |      1 |            2 |       42.7
       30 |   8780 |        2 |      1 |            7 |       10.9
       30 |  30410 |        2 |     15 |           24 |       7.00
```

Iceberg snapshots land under
`s3://hummock001/risingwave-lakekeeper/<warehouse-uuid>/<table-uuid>/`.

---

## 4. Querying the source

> **Rule of thumb:** never do ad-hoc `SELECT … FROM src_casino_prd` directly.
> A bare select on a Kafka source is a **batch scan** that respects
> `scan.startup.mode='latest'` and usually returns 0 rows. Always go through
> an MV.

### 4.1 The query that drove the schema discovery

The end goal was per-transaction rows with real timestamps. Final working
version:

```sql
WITH msgs AS (
    SELECT
        s."CustomerId"          AS customer_id,
        "MessageTypeId"         AS message_type_id,
        "Created"               AS message_created_struct,
        "Transactions"          AS txs
    FROM src_casino_prd AS s,
         UNNEST((s."RoundInfo")."Messages")
)
SELECT
    customer_id,
    message_type_id,
    to_timestamp(
        (message_created_struct)."seconds"
      + (message_created_struct)."nanos" / 1e9
    )                                                     AS message_created_at,
    "TransactionId"                                       AS transaction_id,
    "AccountId"                                           AS account_id,
    "CurrencyId"                                          AS currency_id,
    to_timestamp(
        (("Created"))."seconds"
      + (("Created"))."nanos" / 1e9
    )                                                     AS transaction_created_at,
    ABS(NULLIF("Amount", '')::numeric)                    AS amount_abs,
    "Amount"                                              AS amount_raw
FROM msgs, UNNEST(txs)
LIMIT 20;
```

### 4.2 Why it's written that way — the four gotchas

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
   field. Use a CTE to rename the outer level (or pass the struct forward
   with a non-colliding alias) and then `UNNEST(txs)` in the outer SELECT.

4. **Decimal-as-string proto fields can be empty.** `Amount`, `TokenAmount`,
   `CurrencyRateToEuro`, `CommissionAmount`, etc. are `string`-typed in the
   protobuf for arbitrary precision. Some rows hold `''`, which
   `::numeric` cannot parse. Use `NULLIF("Amount", '')::numeric` or
   `try_cast("Amount" AS numeric)`.

### 4.3 Converting `google.protobuf.Timestamp`

Lands as `STRUCT<seconds BIGINT, nanos INT>`. Convert with:

```sql
to_timestamp((ts)."seconds" + (ts)."nanos" / 1e9)
```

Returns `TIMESTAMP WITH TIME ZONE` (UTC). For local: `… AT TIME ZONE
'Europe/Athens'`. To drop the zone: `…::timestamp`.

### 4.4 Useful one-liners

```sql
-- live tail of round count
SELECT count(*) FROM mv_casino_prd_rounds_flat;

-- most recent rounds
SELECT unique_id, company_id, game_id, round_created
FROM mv_casino_prd_rounds_flat
ORDER BY round_created DESC LIMIT 10;

-- highest-volume games
SELECT * FROM mv_casino_prd_volume_by_company_game
ORDER BY total_amount DESC NULLS LAST LIMIT 10;

-- Iceberg-side counts (read from RW's iceberg table reader)
SELECT count(*) FROM rw_managed_casino_prd_rounds;
```

Bash watch:

```bash
watch -n 2 'psql -h localhost -p 4566 -d dev -U root -c \
  "SELECT count(*) FROM mv_casino_prd_rounds_flat"'
```

---

## 5. Gotchas worth remembering

- **Apicurio ↔ RisingWave protobuf trilemma**
  ccompat returns binary FDS for protobuf artifacts; native v2 returns the
  original `.proto`. Confluent libs require binary FDS; RisingWave's
  `schema.registry` fetcher requires the original `.proto`. These are
  mutually exclusive over the same ccompat URL. **Resolution:** source
  schemas via native v2 at deploy time, mount a compiled `.pb` into the RW
  container, use `schema.location`.

- **License caps streaming-failure isolation at 4 CPU cores.**
  Our compute node has 10. Result: `DatabaseFailureIsolation` is disabled
  and **any DDL failure resets the whole database** (error
  `database 1 reset`). The first `CREATE TABLE … ENGINE = iceberg` after a
  fresh start often trips this on the JVM/JNI catalog cold start.
  *Workaround:* keep DDL scripts idempotent (`DROP … IF EXISTS` /
  `CREATE … IF NOT EXISTS`) and just rerun once. The Java catalog is warm
  on the second pass and the rest of the script succeeds.

- **`scan.startup.mode = 'latest'` means cold-start sees zero rows.**
  This is intentional for the demo. To replay history use `earliest` (will
  pull the full 386 K+ messages; expect minutes of backfill).

- **Don't ad-hoc `SELECT` from the source.** Wrap in an MV; otherwise the
  query is a transient batch consumer.

- **PascalCase identifiers must be double-quoted.** RW preserves protobuf
  field names verbatim; unquoted `CompanyId` folds to `companyid` and
  doesn't resolve.

- **OrbStack HTTP proxy is hostile to container DNS.**
  `bin/bootstrap_lakekeeper.sh` unsets `http_proxy/HTTPS_PROXY` because the
  injected `proxyproxy.orb.internal:8305` can't resolve docker-network
  hostnames like `lakekeeper`.

---

## 6. Files of interest

| Path | Purpose |
|---|---|
| [proto/casinoroundinfodto.proto](../proto/casinoroundinfodto.proto) | Schema, fetched from Apicurio v2 native |
| [proto/casinoroundinfodto.pb](../proto/casinoroundinfodto.pb) | Compiled FileDescriptorSet, mounted at `/proto/` in RW |
| [sql/casino_prd_funnel_iceberg.sql](../sql/casino_prd_funnel_iceberg.sql) | Full DDL: source, MVs, Iceberg sinks |
| [bin/bootstrap_lakekeeper.sh](../bin/bootstrap_lakekeeper.sh) | Idempotent Lakekeeper warehouse bootstrap |
| [docker-compose.yml](../docker-compose.yml) | Service definitions (RW, MinIO, Lakekeeper, Redpanda for tooling) |

---

## 7. Reproduce from scratch

```bash
# 1. Schema
curl -fsSL -H 'Accept: text/plain' \
  http://staging-schema-registry.kaizengaming.net/apis/registry/v2/groups/bigdata/artifacts/casinoroundinfo \
  > proto/casinoroundinfodto.proto
protoc --include_imports \
  --descriptor_set_out=proto/casinoroundinfodto.pb \
  proto/casinoroundinfodto.proto

# 2. Stack
docker compose up -d \
  minio-0 frontend-node-0 meta-node-0 compute-node-0 compactor-0 \
  lakekeeper-db lakekeeper-migrate lakekeeper lakekeeper-bootstrap

# 3. Source + MVs + Iceberg sinks (idempotent)
psql -h localhost -p 4566 -d dev -U root -f sql/casino_prd_funnel_iceberg.sql
# If the first run hits `database 1 reset` on the iceberg CREATE TABLE,
# just rerun the same command — JVM is warm, it'll complete.

# 4. Verify
psql -h localhost -p 4566 -d dev -U root <<'SQL'
SELECT 'rounds_flat' AS mv, count(*) FROM mv_casino_prd_rounds_flat
UNION ALL SELECT 'volume',  count(*) FROM mv_casino_prd_volume_by_company_game
UNION ALL SELECT 'funnel',  count(*) FROM mv_casino_prd_funnel
UNION ALL SELECT 'iceberg_rounds', count(*) FROM rw_managed_casino_prd_rounds;
SQL
```
