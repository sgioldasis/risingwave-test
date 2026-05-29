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
┌──────────────────────────┐    SSL one-way   ┌─────────────────────────────────┐
│ Kaizen prd2 Kafka        │ ────────────────▶│  RisingWave (compute-node-0)     │
│ prd2-kafka-bootstrap     │  topic           │   src_casino_prd  (TABLE)        │
│ .kaizengaming.net:443    │  cronus.casino   │   ↓                              │
│ 5 brokers, 290 topics    │  .out.gh         │   mv_casino_raw  (nested 1:1)    │
└──────────────────────────┘                  │   mv_casino_transactions         │
                                              │   mv_casino_real_bet_events      │
                                              │   mv_casino_real_bet_hourly_..   │
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

- **Schema sourcing is one-shot at deploy time**, not at RW runtime.
  Apicurio's `ccompat/v7` layer normalises PROTOBUF artifacts to binary
  `FileDescriptorSet`, which RisingWave's `schema.registry` fetcher cannot
  parse. We bypass it.
- **Wire format on `cronus.casino.out.*` is raw protobuf**, no Confluent
  5/6-byte magic prefix. RisingWave's `schema.location` (file-based) reads
  byte 0 as a protobuf field tag — that matches what's on the wire, so no
  framing strip is needed.
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
    schema.location = 'file:///proto/casinoroundinfodto.pb',
    message         = 'Cronus.CasinoService.RoundInfo.Abstractions.CasinoRoundInfoDto'
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

### 3.6 Real-time materialized views

See [`sql/casino_prd_funnel_iceberg.sql`](../sql/casino_prd_funnel_iceberg.sql)
for the full file. The pipeline covers two use cases:

**UC1 — Real Bet Amount:** `mv_casino_transactions` → `mv_casino_real_bet_events`
→ `mv_casino_real_bet` (rolling 14-day real bet total per customer/currency).

**UC2 — Casino Turnover Percentage:** `mv_casino_turnover_events` +
`mv_sportsbook_turnover_events` → `mv_turnover_percentage` (90-day casino vs
sportsbook ratio per customer). UC2 requires a second source `bets_out_gh`
(sportsbook bets — `prd4-kafka-bootstrap.kaizengaming.net:443`, topic
`bets-out-gh`, 10 partitions).

#### UC1 — Real Bet Amount

```sql
-- UC1: Real bet events — filter on top of mv_casino_transactions.
-- Mirrors getCasinoRealBetAmountEvents from the Spark PoC:
--   MessageTypeId = 1  (bet placed)
--   AccountId     = 1  (real money account, not bonus)
--   Amount        IS NOT NULL AND <> '' (proto3 string fields decode as '' when absent)
CREATE MATERIALIZED VIEW mv_casino_real_bet_events AS
SELECT
    customer_id,
    transaction_id,
    currency_id,
    game_id,
    game_type,
    is_live,
    company_id,
    amount_abs                                    AS real_bet_amount,
    transaction_created_at                        AS event_ts
FROM mv_casino_transactions
WHERE message_type_id = 1
  AND account_id      = 1
  AND amount_raw IS NOT NULL
  AND amount_raw <> '';

-- UC1: Rolling 14-day real bet amount per customer/currency.
-- Mirrors the Spark mapGroupsWithState logic: on each incoming event, emit the
-- updated sum of all real bets placed by that customer in the last 14 days.
-- One row per event; consumers key on (customer_id, currency_id) and keep the latest.
CREATE MATERIALIZED VIEW mv_casino_real_bet AS
SELECT
    customer_id,
    currency_id,
    event_ts,
    SUM(real_bet_amount) OVER (
        PARTITION BY customer_id, currency_id
        ORDER BY event_ts
        RANGE BETWEEN INTERVAL '1209600 SECONDS' PRECEDING AND CURRENT ROW
    ) AS rolling_14d_real_bet_amount
FROM mv_casino_real_bet_events;
```

#### UC2 — Casino Turnover Percentage

```sql
-- UC2: Casino turnover events.
-- MessageTypeId = 2 (withdraw/payout), AccountId IN (1, 4) (real money + bonus).
CREATE MATERIALIZED VIEW mv_casino_turnover_events AS
SELECT
    customer_id,
    currency_id,
    amount_abs               AS turnover,
    transaction_created_at   AS event_ts
FROM mv_casino_transactions
WHERE message_type_id = 2
  AND account_id      IN (1, 4)
  AND amount_raw IS NOT NULL
  AND amount_raw <> '';

-- UC2: Sportsbook turnover events (from bets_out_gh source).
-- TotalStake.Euro uses DecimalValue encoding (units + nanos/1e9).
-- Euro-normalised so casino and sportsbook turnover are comparable across currencies.
CREATE MATERIALIZED VIEW mv_sportsbook_turnover_events AS
SELECT
    ("CustomerInfo")."Id"                                                       AS customer_id,
    TO_TIMESTAMP(("PlacedAt").seconds)                                          AS event_ts,
    (("TotalStake")."Euro")."units"::NUMERIC
        + (("TotalStake")."Euro")."nanos"::NUMERIC / 1000000000                AS turnover
FROM src_bets_gh
WHERE ("CustomerInfo")."Id" IS NOT NULL
  AND ("TotalStake")."Euro" IS NOT NULL;

-- UC2: 90-day rolling totals, deduplicated to latest per customer, then ratio.
CREATE MATERIALIZED VIEW mv_casino_turnover_90d AS
SELECT customer_id, event_ts,
    SUM(turnover) OVER (
        PARTITION BY customer_id
        ORDER BY event_ts
        RANGE BETWEEN INTERVAL '7776000 SECONDS' PRECEDING AND CURRENT ROW
    ) AS rolling_90d_turnover
FROM mv_casino_turnover_events;

CREATE MATERIALIZED VIEW mv_sportsbook_turnover_90d AS
SELECT customer_id, event_ts,
    SUM(turnover) OVER (
        PARTITION BY customer_id
        ORDER BY event_ts
        RANGE BETWEEN INTERVAL '7776000 SECONDS' PRECEDING AND CURRENT ROW
    ) AS rolling_90d_turnover
FROM mv_sportsbook_turnover_events;

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

CREATE MATERIALIZED VIEW mv_turnover_percentage AS
SELECT
    COALESCE(c.customer_id, s.customer_id)      AS customer_id,
    COALESCE(c.casino_turnover, 0)              AS casino_turnover,
    COALESCE(s.sportsbook_turnover, 0)          AS sportsbook_turnover,
    COALESCE(c.casino_turnover, 0)
        + COALESCE(s.sportsbook_turnover, 0)    AS total_turnover,
    CASE
        WHEN COALESCE(c.casino_turnover, 0) + COALESCE(s.sportsbook_turnover, 0) = 0 THEN 0
        ELSE COALESCE(c.casino_turnover, 0)
             / (COALESCE(c.casino_turnover, 0) + COALESCE(s.sportsbook_turnover, 0))
    END                                         AS casino_ratio,
    CASE
        WHEN COALESCE(c.casino_turnover, 0) + COALESCE(s.sportsbook_turnover, 0) = 0 THEN 0
        ELSE COALESCE(s.sportsbook_turnover, 0)
             / (COALESCE(c.casino_turnover, 0) + COALESCE(s.sportsbook_turnover, 0))
    END                                         AS sportsbook_ratio
FROM mv_casino_turnover_latest      c
FULL OUTER JOIN mv_sportsbook_turnover_latest s USING (customer_id);
```

### 3.7 Managed-Iceberg sinks (Lakekeeper + MinIO)

Four MVs (`mv_casino_raw`, `mv_casino_transactions`,
`mv_casino_real_bet_events`, `mv_casino_real_bet_hourly_per_customer`) are
landed into Lakekeeper-managed Iceberg tables.

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
then an upsert sink keyed on the table's PK. Example:

```sql
CREATE TABLE rw_managed_casino_transactions (
    customer_id              INT,
    message_type_id          INT,
    message_created_at       TIMESTAMPTZ,
    transaction_id           BIGINT,
    account_id               INT,
    currency_id              INT,
    transaction_created_at   TIMESTAMPTZ,
    amount_abs               NUMERIC,
    amount_raw               VARCHAR,
    PRIMARY KEY (transaction_id)
) ENGINE = iceberg;

CREATE SINK rw_managed_casino_transactions_sink
INTO rw_managed_casino_transactions
FROM mv_casino_transactions
WITH (
    type                        = 'upsert',
    primary_key                 = 'transaction_id',
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
 mv_casino_real_bet_events | 389541
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

## 4. Querying the source

> **Rule of thumb:** never do ad-hoc `SELECT … FROM src_casino_prd` directly.
> A bare select on a Kafka source is a **batch scan** that respects
> `scan.startup.mode='latest'` and usually returns 0 rows. Always go through
> an MV.

### 4.1 The `mv_casino_transactions` definition

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
   field. Use explicit row aliases (`UNNEST(...) AS msg, UNNEST(msg."Transactions") AS txn`)
   and reference them as `msg."Created"` / `txn."Created"` to disambiguate.
   Chaining the second UNNEST off the alias (`UNNEST(msg."Transactions")`) is
   the idiomatic RisingWave pattern — no CTE needed.

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
-- live tail of transaction count
SELECT count(*) FROM mv_casino_transactions;

-- most recent real-money bets
SELECT customer_id, real_bet_amount, event_ts
FROM mv_casino_real_bet_events
ORDER BY event_ts DESC LIMIT 10;

-- top customers by 14-day rolling real bet amount
SELECT DISTINCT ON (customer_id)
    customer_id, currency_id, rolling_14d_real_bet_amount
FROM mv_casino_real_bet
ORDER BY customer_id, event_ts DESC;

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

---

## 5. Gotchas worth remembering

- **Apicurio ↔ RisingWave protobuf trilemma**
  ccompat returns binary FDS for protobuf artifacts; native v2 returns the
  original `.proto`. Confluent libs require binary FDS; RisingWave's
  `schema.registry` fetcher requires the original `.proto`. These are
  mutually exclusive over the same ccompat URL. **Resolution:** source
  schemas via native v2 at deploy time, mount a compiled `.pb` into the RW
  container, use `schema.location`.

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
  Our compute node has 10. Result: `DatabaseFailureIsolation` is disabled
  and **any DDL failure resets the whole database** (error
  `database 1 reset`). The first `CREATE TABLE … ENGINE = iceberg` after a
  fresh start often trips this on the JVM/JNI catalog cold start.
  *Workaround:* keep DDL scripts idempotent (`DROP … IF EXISTS` /
  `CREATE … IF NOT EXISTS`) and just rerun once. The Java catalog is warm
  on the second pass and the rest of the script succeeds.

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
| [sql/casino_prd_funnel_iceberg.sql](../sql/casino_prd_funnel_iceberg.sql) | Analytical MVs + 3 managed Iceberg sinks (transactions, real bets, hourly aggregates) |
| [sql/casino_prd_raw_iceberg.sql](../sql/casino_prd_raw_iceberg.sql) | Faithful nested raw archive: `mv_casino_raw` + `rw_managed_casino_raw` |
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
psql -h localhost -p 4566 -d dev -U root -f sql/casino_prd_source.sql
psql -h localhost -p 4566 -d dev -U root -f sql/casino_prd_funnel_iceberg.sql
psql -h localhost -p 4566 -d dev -U root -f sql/casino_prd_raw_iceberg.sql
# If the first run hits `database 1 reset` on the iceberg CREATE TABLE,
# just rerun the same command — JVM is warm, it'll complete.

# 4. Verify
psql -h localhost -p 4566 -d dev -U root <<'SQL'
SELECT 'mv_raw'                AS view, count(*) FROM mv_casino_raw
UNION ALL SELECT 'mv_transactions',     count(*) FROM mv_casino_transactions
UNION ALL SELECT 'mv_real_bet_events',  count(*) FROM mv_casino_real_bet_events
UNION ALL SELECT 'mv_real_bet_hourly',  count(*) FROM mv_casino_real_bet_hourly_per_customer
UNION ALL SELECT 'ice_raw',             count(*) FROM rw_managed_casino_raw
UNION ALL SELECT 'ice_transactions',    count(*) FROM rw_managed_casino_transactions
UNION ALL SELECT 'ice_real_bet_events', count(*) FROM rw_managed_casino_real_bet_events
UNION ALL SELECT 'ice_real_bet_hourly', count(*) FROM rw_managed_casino_real_bet_hourly;
SQL
```
