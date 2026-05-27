# RisingWave Protobuf Demo — Cronus `CasinoRoundInfoDto`

End-to-end demo showing how RisingWave ingests **deeply-nested protobuf
casino round events** from Kafka (Redpanda) via Schema Registry, exposes
the nested fields as first-class SQL, builds streaming aggregates, and
sinks the results into managed Apache Iceberg tables in Lakekeeper.

Companion to the orders-shaped [PROTOBUF_NESTED_DEMO.md](./PROTOBUF_NESTED_DEMO.md).
Where that demo focuses on a *flat-ish* `Order` message with one level of
`repeated` items, this one stresses **double-nested repetition** —
rounds contain messages, messages contain transactions — using a real
production-shaped schema from the Cronus casino platform.

## TL;DR

```bash
./bin/1_up.sh                          # bring up the stack (once)
./bin/3_run_protobuf_casino_demo.sh    # produce → ingest → MV → iceberg sinks
```

Or click **🎰 Casino Protobuf Demo** in the script runner UI at
<http://localhost:4001> (started by `./bin/0_script_runner.sh`).

## What gets exercised

The schema in [proto/casinoroundinfodto.proto](../proto/casinoroundinfodto.proto)
is a `Cronus.CasinoService.RoundInfo.Abstractions.CasinoRoundInfoDto`
message that intentionally uses several non-trivial protobuf features:

| Protobuf construct                  | Where in `CasinoRoundInfoDto`                            | RisingWave SQL access                          |
| ----------------------------------- | -------------------------------------------------------- | ---------------------------------------------- |
| Nested message                      | `GameInfo`, `RoundInfo`                                  | `("GameInfo")."GameId"`                        |
| Doubly nested message               | `RoundInfo` → `Messages[]` → `Transactions[]`            | `UNNEST(("RoundInfo")."Messages")`, then `UNNEST((m)."Transactions")` |
| `repeated` nested message           | `RoundInfo.Messages`, `CasinoMessageInformation.Transactions` | array of struct — flatten with `UNNEST`   |
| `optional` scalar (proto3 explicit) | `CommissionAmount`, `IsBonusCampaignWagering`, …         | nullable column                                |
| `google.protobuf.Timestamp`         | `RoundInfo.RoundCreated`, `…RoundEnded`, `Messages.Created`, `Transactions.Created` | `struct<seconds bigint, nanos integer>` |
| Decimal-as-string                   | `Amount`, `TokenAmount`, `CurrencyRateToEuro`, balances  | `VARCHAR`, cast to `NUMERIC` for math          |
| **PascalCase field names**          | every field                                              | preserved verbatim — double-quote in SQL       |

The PascalCase point matters: the existing orders demo uses snake_case
proto fields (`order_id`, `customer`), so unquoted SQL identifiers work
fine. Cronus's `.proto` uses C#-style PascalCase (`CompanyId`,
`GameInfo`). RisingWave preserves the proto's exact casing, but the
Postgres dialect folds unquoted identifiers to lowercase — so this demo
**double-quotes every column reference** to bypass folding.

## Architecture

```
producer (python)                Redpanda                       RisingWave                              Lakekeeper / MinIO
─────────────────────            ──────────                      ──────────────                          ──────────────────
build_round()                                                                                            (iceberg REST catalog
   │  CasinoRoundInfoDto                                                                                  + S3-compatible store)
   │                                                                                                            ▲
   ▼                                                                                                            │ commit ~5s
 ProtobufSerializer ── registers ─►  subject                                                                    │
   │                                 casino_rounds-value                                                        │
   │                                  (PROTOBUF)                                                                │
   ▼                                                                                                            │
 produces ──► topic ─────────────────────► CREATE SOURCE src_casino_rounds_proto                                │
              casino_rounds               (FORMAT PLAIN ENCODE PROTOBUF + schema.registry)                      │
                                                  │                                                             │
                                                  ├─► MV mv_casino_rounds_flat ─── SINK ──► rw_managed_casino_rounds  ── iceberg
                                                  │                                                             │
                                                  └─► MV mv_casino_volume_by_company_game ── SINK ──► rw_managed_casino_volume ── iceberg
```

Three new files, all mirroring the conventions of the orders demo:

- [scripts/produce_protobuf_casino_rounds.py](../scripts/produce_protobuf_casino_rounds.py) — producer + auto-protoc + Schema Registry registration.
- [sql/protobuf_casino_demo.sql](../sql/protobuf_casino_demo.sql) — source, sanity SELECTs, MVs, iceberg sinks.
- [bin/3_run_protobuf_casino_demo.sh](../bin/3_run_protobuf_casino_demo.sh) — orchestrator.

## Prereqs

```bash
./bin/1_up.sh
```

Brings up:

- Redpanda broker — `localhost:19092`
- Schema Registry — `localhost:8081`
- Redpanda Console — <http://localhost:9090>
- RisingWave — `psql -h localhost -p 4566 -d dev -U root`
- Lakekeeper REST catalog — `http://localhost:8181`
- MinIO — `http://localhost:9301` (S3 endpoint)

## Run it

```bash
./bin/3_run_protobuf_casino_demo.sh
```

Knobs (env vars):

| Variable        | Default          | Meaning                                                       |
| --------------- | ---------------- | ------------------------------------------------------------- |
| `COUNT`         | `200`            | Number of rounds to produce (`0` = run forever)               |
| `TPS`           | `0` (unlimited)  | Producer throughput cap, rounds/sec                           |
| `TOPIC`         | `casino_rounds`  | Kafka topic name (also drives SR subject `<topic>-value`)     |
| `ICEBERG_WAIT`  | `10`             | Seconds to wait before checking iceberg row counts (`0` skips) |
| `PSQL_HOST/PORT/DB/USER` | `localhost / 4566 / dev / root` | RisingWave connection                       |

Example:

```bash
COUNT=2000 TPS=200 ./bin/3_run_protobuf_casino_demo.sh
```

What the runner does, step by step:

1. **Topic** — `docker exec redpanda rpk topic create casino_rounds --partitions 3` (no-op if it already exists).
2. **Produce** — runs `scripts/produce_protobuf_casino_rounds.py`, which:
   - auto-compiles `proto/casinoroundinfodto.proto` to `scripts/_pb/casinoroundinfodto_pb2.py` on first run (and whenever the `.proto` is newer);
   - registers the schema under subject `casino_rounds-value` via Confluent's `ProtobufSerializer`;
   - emits `COUNT` nested `CasinoRoundInfoDto` rounds with 1–4 messages each, 1–3 transactions per message.
3. **SQL** — applies [sql/protobuf_casino_demo.sql](../sql/protobuf_casino_demo.sql) against RisingWave:
   - creates `src_casino_rounds_proto` from Kafka + SR;
   - runs three sanity `SELECT`s (top-level + nested, single UNNEST, double UNNEST);
   - builds a `(company_id, game_id, game_type)` volume MV;
   - creates the iceberg connection, the flattening MV, and two managed iceberg tables with their upsert sinks.
4. **Verify** — waits `ICEBERG_WAIT` seconds and prints row counts for `rw_managed_casino_rounds` and `rw_managed_casino_volume`.

## The protobuf schema, anatomized

```protobuf
message CasinoRoundInfoDto {
  string UniqueId = 1;
  int32  CustomerId = 2;
  int32  CompanyId = 3;
  int32  CasinoProviderId = 4;
  int32  ExternalProviderId = 5;
  GameInformation  GameInfo  = 6;     // nested
  RoundInformation RoundInfo = 7;     // nested, contains repeated Messages
  bool   IsBonusLockedOnFatMessageCreation = 8;
  optional bool IsBonusCampaignWagering    = 9;
}

message RoundInformation {
  string GameRoundRef = 1;
  google.protobuf.Timestamp RoundCreated = 2;
  google.protobuf.Timestamp RoundEnded   = 3;
  repeated CasinoMessageInformation Messages = 4;   // first level of repetition
}

message CasinoMessageInformation {
  int64  MessageId = 1;
  ...
  repeated TransactionInformation Transactions = 12;  // second level of repetition
  ...
}

message TransactionInformation {
  int64  TransactionId = 1;
  google.protobuf.Timestamp Created = 2;
  ...
  string Amount = 7;             // decimal-as-string — cast to NUMERIC in SQL
  string CurrencyRateToEuro = 8;
  ...
}
```

The "fact" you'd typically want to aggregate (an individual bet/win
transaction) lives **two `repeated` levels deep**. That's the main
twist relative to the orders demo.

## The RisingWave bit

The key DDL is just one statement — no column list, no type mapping by
hand:

```sql
CREATE SOURCE src_casino_rounds_proto
WITH (
    connector = 'kafka',
    topic = 'casino_rounds',
    properties.bootstrap.server = 'redpanda:9092',
    scan.startup.mode = 'earliest'
) FORMAT PLAIN ENCODE PROTOBUF (
    schema.registry = 'http://redpanda:8081',
    message = 'Cronus.CasinoService.RoundInfo.Abstractions.CasinoRoundInfoDto'
);
```

RisingWave fetches the `.proto` from the Schema Registry, walks the
descriptor (including the imported `google.protobuf.Timestamp`), and
reflects the entire nested tree into the catalog. Verify with:

```sql
SELECT column_name, data_type
FROM information_schema.columns
WHERE table_name = 'src_casino_rounds_proto'
ORDER BY ordinal_position;
```

You'll see columns like `UniqueId VARCHAR`, `GameInfo STRUCT<...>`,
`RoundInfo STRUCT<..., Messages STRUCT<...>[]>`, etc.

## Validate it yourself

```bash
psql -h localhost -p 4566 -d dev -U root
```

> All identifiers below are **double-quoted** because the proto fields
> are PascalCase. If you drop the quotes, Postgres folds to lowercase
> and the column won't resolve.

### 1. Source row count (grows while producer runs)

```sql
SELECT COUNT(*) AS rounds_ingested FROM src_casino_rounds_proto;
```

### 2. Top-level + nested struct access

```sql
SELECT
    "UniqueId",
    "CompanyId",
    "CasinoProviderId",
    ("GameInfo")."GameId"             AS game_id,
    ("GameInfo")."GameType"           AS game_type,
    ("GameInfo")."IsLive"             AS is_live,
    ("RoundInfo")."GameRoundRef"      AS round_ref,
    array_length(("RoundInfo")."Messages") AS num_messages
FROM src_casino_rounds_proto
LIMIT 10;
```

Note the parenthesization rule — `GameInfo.GameId` would be parsed by
the Postgres dialect as `<table>.<column>`; you have to wrap the struct
column: `(GameInfo).GameId` (and quote when it's PascalCase).

### 3. Timestamp conversion

`google.protobuf.Timestamp` lands as `struct<seconds bigint, nanos
integer>`. Convert it:

```sql
SELECT
    "UniqueId",
    to_timestamp(
        (("RoundInfo")."RoundCreated")."seconds"
      + (("RoundInfo")."RoundCreated")."nanos" / 1e9
    ) AS round_created,
    to_timestamp(
        (("RoundInfo")."RoundEnded")."seconds"
      + (("RoundInfo")."RoundEnded")."nanos" / 1e9
    ) AS round_ended
FROM src_casino_rounds_proto
LIMIT 5;
```

### 4. Single UNNEST — round → messages

```sql
SELECT
    "UniqueId",
    "MessageId"      AS message_id,
    "MessageTypeId"  AS msg_type,
    "IsRoundClosed"  AS round_closed,
    array_length("Transactions") AS num_tx,
    to_timestamp(
        ("Created")."seconds" + ("Created")."nanos" / 1e9
    ) AS msg_created
FROM src_casino_rounds_proto,
     UNNEST(("RoundInfo")."Messages")
LIMIT 10;
```

Key rule: **`UNNEST(struct[])` in RisingWave flattens the element struct's
fields into top-level columns of the resulting row.** An optional
`AS alias` after `UNNEST` names the *row*, not a struct value, so
`alias.field` does **not** work — reference the fields by their bare
(quoted) names: `"MessageId"`, `"Created"`, etc.

### 5. Double UNNEST — round → message → transaction

This is the headline pattern. One row per *transaction*, with full context
from the parent message and round joined in. Because both
`CasinoMessageInformation` and `TransactionInformation` have a `Created`
field, the message-level columns are renamed in a CTE before the second
`UNNEST` exposes the transaction-level fields:

```sql
WITH exploded_msgs AS (
    SELECT
        r."UniqueId"               AS unique_id,
        r."CompanyId"              AS company_id,
        (r."GameInfo")."GameId"    AS game_id,
        "MessageTypeId"            AS msg_type,
        "Transactions"             AS txs
    FROM src_casino_rounds_proto AS r,
         UNNEST((r."RoundInfo")."Messages")
)
SELECT
    unique_id,
    company_id,
    game_id,
    msg_type,
    "TransactionId"   AS tx_id,
    "CurrencyId"      AS currency_id,
    "Amount"          AS amount,           -- string!
    "BonusAction"     AS bonus_action,
    to_timestamp(("Created")."seconds" + ("Created")."nanos" / 1e9) AS tx_created
FROM exploded_msgs, UNNEST(txs)
LIMIT 20;
```

Cardinality check — average transactions per round:

```sql
WITH tx AS (
  SELECT r."UniqueId" AS unique_id, "Transactions" AS txs
  FROM src_casino_rounds_proto AS r,
       UNNEST((r."RoundInfo")."Messages")
)
SELECT
    MIN(c)           AS min_tx,
    round(AVG(c), 2) AS avg_tx,
    MAX(c)           AS max_tx
FROM (
  SELECT unique_id, COUNT(*) AS c
  FROM tx, UNNEST(txs)
  GROUP BY unique_id
);
```

### 6. Decimal-as-string aggregation

The schema stores monetary values as `string`. Cast in flight:

```sql
WITH exploded AS (
    SELECT
        r."CompanyId" AS company_id,
        "Transactions" AS txs
    FROM src_casino_rounds_proto AS r,
         UNNEST((r."RoundInfo")."Messages")
)
SELECT
    company_id,
    COUNT(*)                                          AS transactions,
    ROUND(SUM(("Amount")::numeric), 4)                AS total_amount,
    ROUND(AVG(("CurrencyRateToEuro")::numeric), 6)    AS avg_fx,
    ROUND(SUM(("Amount")::numeric *
              ("CurrencyRateToEuro")::numeric), 4)    AS total_amount_eur
FROM exploded, UNNEST(txs)
GROUP BY 1
ORDER BY total_amount DESC;
```

If any row carries a non-numeric `Amount` (it shouldn't, in this demo),
the cast would error out — wrap with `try_cast` to be robust in
production.

### 7. Streaming materialized view — confirm live update

The demo SQL creates `mv_casino_volume_by_company_game`:

```sql
SELECT * FROM mv_casino_volume_by_company_game
ORDER BY total_amount DESC NULLS LAST
LIMIT 15;
```

In another terminal:

```bash
uv run python scripts/produce_protobuf_casino_rounds.py --count 500 --tps 100
```

Re-run the `SELECT` — `rounds`, `transactions`, `total_amount` increment
without re-running the MV definition.

### 8. Optional fields are nullable

```sql
SELECT
    SUM(("IsBonusCampaignWagering" IS NULL)::int)  AS unset_rows,
    SUM(("IsBonusCampaignWagering" IS NOT NULL)::int) AS set_rows,
    SUM(("IsBonusCampaignWagering" = true)::int)    AS true_rows
FROM src_casino_rounds_proto;
```

Same pattern for optionals deeper in the tree (e.g.
`(m)."CommissionAmount"`).

### 9. The flattening MV — preview before sinking

```sql
SELECT * FROM mv_casino_rounds_flat ORDER BY round_created DESC LIMIT 5;
```

This MV is **the thing being sunk** into iceberg. Two reasons it exists
rather than sinking from the source directly:

- Upsert sinks need a primary key in the upstream relation — the source
  doesn't expose `UniqueId` as one. The MV materializes a clean
  `unique_id` column.
- The flattening + timestamp conversion lives once in SQL, not in three
  downstream consumers.

### 10. Managed Iceberg sinks

After `ICEBERG_WAIT` seconds, both managed tables are populated:

```sql
SELECT COUNT(*) FROM rw_managed_casino_rounds;    -- ≈ COUNT
SELECT COUNT(*) FROM rw_managed_casino_volume;    -- distinct (company, game, type)

SELECT name, connector, sink_type
FROM rw_catalog.rw_sinks
WHERE name IN ('rw_managed_casino_rounds_sink',
               'rw_managed_casino_volume_sink');
```

Both sinks use `commit_checkpoint_interval = 5`, so new RisingWave
rows appear in iceberg snapshots within ~5 seconds.

The same iceberg tables are readable through the other tiles in the
script runner:

- 🦆 **DuckDB Iceberg** — [bin/5_duckdb_iceberg.sh](../bin/5_duckdb_iceberg.sh)
- 🔥 **Spark Iceberg** — [bin/5_spark_iceberg.sh](../bin/5_spark_iceberg.sh)
- 🧊 **Trino / Marimo** — [bin/5_marimo_risingwave.sh](../bin/5_marimo_risingwave.sh)

Look for `public.rw_managed_casino_rounds` and
`public.rw_managed_casino_volume` under the Lakekeeper namespace.

### 11. Schema registry peek

Open <http://localhost:9090> (Redpanda Console) → **Schema Registry** →
subject `casino_rounds-value`. You should see the `.proto` definition,
with the full schema id (referenced internally by the magic-byte prefix
on each Kafka message).

Programmatic check:

```bash
curl -s http://localhost:8081/subjects | jq
curl -s http://localhost:8081/subjects/casino_rounds-value/versions/latest | jq
```

### 12. Internal catalog peek (RisingWave-specific)

```sql
SELECT name, owner FROM rw_catalog.rw_sources
WHERE name = 'src_casino_rounds_proto';

SELECT name FROM rw_catalog.rw_materialized_views
WHERE name LIKE 'mv_casino_%';
```

### 13. Throughput in Grafana

`rw_catalog.rw_source_throughput` / `rw_sink_throughput` are **not** implemented
in RisingWave yet ([issue #1695](https://github.com/risingwavelabs/risingwave/issues/1695)).
This repo ships Prometheus + Grafana dashboards instead.

1. Open Grafana at **<http://localhost:3001>** (admin / admin).
2. Navigate to **Dashboards → RisingWave → RisingWave Pipeline Health**.
3. Scroll to the bottom — two panels appear after running the demo:
   - **Source Throughput (rows/s)** — one line per source. You'll see
     `src_casino_rounds_proto` plus the two auto-created
     `__iceberg_source_rw_managed_casino_*` readers.
   - **Sink Throughput (rows/s)** — one line per sink. Look for
     `__iceberg_sink_rw_managed_casino_rounds` and
     `__iceberg_sink_rw_managed_casino_volume` (`connector_type=iceberg`).

Drive some traffic so the lines move:

```bash
COUNT=2000 TPS=20 uv run python scripts/produce_protobuf_casino_rounds.py
```

> Iceberg sinks commit on `commit_checkpoint_interval = 5` (seconds), so expect
> **spiky** rates on the sink panel rather than a steady line — that's normal.

Prefer the terminal? The same metric, straight from Prometheus:

```bash
curl -sG 'http://localhost:9500/api/v1/query' \
  --data-urlencode 'query=sum by (source_name) (rate(stream_source_output_rows_counts[1m]))' \
  | python3 -m json.tool
```

The RisingWave dashboard at <http://localhost:5691> only shows **topology**
(fragments, parallelism, dependencies) — not per-actor row counters.

## Why this demo, vs the orders demo

| Concern                            | Orders demo                    | Casino demo                                              |
| ---------------------------------- | ------------------------------ | -------------------------------------------------------- |
| Top-level message                  | `demo.Order`                   | `Cronus.CasinoService.RoundInfo.Abstractions.CasinoRoundInfoDto` |
| Field naming convention            | snake_case (`order_id`)        | PascalCase (`UniqueId`) — must double-quote in SQL       |
| Levels of `repeated`               | 1 (`items[]`)                  | **2** (`Messages[].Transactions[]`)                      |
| `map<K,V>` exercised               | yes (`attributes`)             | no                                                       |
| `oneof` exercised                  | yes (`payment`)                | no                                                       |
| `enum` exercised                   | yes (`status`)                 | no (`MessageTypeId` is plain `int32`)                    |
| Decimal handling                   | `double total`                 | `string` decimals — explicit `::numeric` cast            |
| Schema namespace                   | `demo.Order`                   | dotted C# namespace                                      |
| Iceberg fact-table grain           | one row per order              | one row per round (transaction-grain available via MV)   |
| Iceberg rollup grain               | `(country, category)`          | `(company_id, game_id, game_type)`                       |

Run **both** if you want a thorough picture of RisingWave's protobuf
support — they exercise complementary surface area.

## Common gotchas

- **Lowercase identifiers won't resolve.** `SELECT companyid FROM
  src_casino_rounds_proto` returns *column does not exist*. Quote it:
  `SELECT "CompanyId" FROM …`.
- **Struct field requires parenthesization.** `GameInfo.GameId` parses
  as `<table>.<column>`; write `("GameInfo")."GameId"`.
- **`UNNEST(struct[])` flattens — no alias dot-access.**
  `UNNEST(items) AS x` does **not** give you a struct `x`; the inner
  fields land as top-level columns. Reference them by their bare
  (quoted) names. The orders demo gets away with `(product).id` because
  `product` is itself a struct *field* of the unnested `LineItem`, not
  an alias for the row.
- **Field-name collisions on nested UNNEST.** `Messages.Created` and
  `Transactions.Created` collide. Use a CTE to rename the outer level
  before unnesting the inner array.
- **Decimal strings.** `SUM(t.Amount)` errors — the type is `varchar`.
  Cast: `SUM(("Amount")::numeric)`.
- **Iceberg sinks are eventually consistent.** With
  `commit_checkpoint_interval = 5`, new data shows up in iceberg ~5 s
  after the source ingests it. Use `ICEBERG_WAIT` (or just wait) before
  asserting row counts.
- **PK requirement for upsert sinks.** Upstream MV must expose the same
  columns named in `primary_key`. `mv_casino_rounds_flat` projects
  `unique_id` precisely for that reason.
- **No `connector`/`connection` in managed-iceberg sink `WITH`.**
  RisingWave infers all of that from the `ENGINE = iceberg` target
  table — the sink `WITH` clause only takes `type`, `primary_key`, and
  maintenance options.

## Schema evolution (optional)

Add an optional field to `proto/casinoroundinfodto.proto`, e.g.:

```protobuf
message CasinoRoundInfoDto {
  // …existing fields…
  optional string OperatorTag = 100;  // NEW
}
```

Then re-run the producer and refresh the source:

```bash
COUNT=50 ./bin/3_run_protobuf_casino_demo.sh
```

```sql
DROP SOURCE src_casino_rounds_proto CASCADE;
\i sql/protobuf_casino_demo.sql
SELECT column_name FROM information_schema.columns
WHERE table_name = 'src_casino_rounds_proto';
```

RisingWave re-fetches the descriptor from SR; the new field shows up.
Existing rows return `NULL` for it (proto3 optional default).

## Cleanup

```bash
psql -h localhost -p 4566 -d dev -U root <<SQL
DROP SINK IF EXISTS rw_managed_casino_rounds_sink;
DROP SINK IF EXISTS rw_managed_casino_volume_sink;
DROP TABLE IF EXISTS rw_managed_casino_rounds;
DROP TABLE IF EXISTS rw_managed_casino_volume;
DROP MATERIALIZED VIEW IF EXISTS mv_casino_rounds_flat;
DROP MATERIALIZED VIEW IF EXISTS mv_casino_volume_by_company_game;
DROP SOURCE IF EXISTS src_casino_rounds_proto CASCADE;
SQL

docker exec redpanda rpk topic delete casino_rounds
curl -X DELETE http://localhost:8081/subjects/casino_rounds-value
curl -X DELETE 'http://localhost:8081/subjects/casino_rounds-value?permanent=true'
```

The iceberg snapshots stay in MinIO until you drop the Lakekeeper
warehouse (or `./bin/6_down.sh`).

## File reference

- [proto/casinoroundinfodto.proto](../proto/casinoroundinfodto.proto) — the schema
- [scripts/produce_protobuf_casino_rounds.py](../scripts/produce_protobuf_casino_rounds.py) — producer
- [sql/protobuf_casino_demo.sql](../sql/protobuf_casino_demo.sql) — RisingWave pipeline
- [bin/3_run_protobuf_casino_demo.sh](../bin/3_run_protobuf_casino_demo.sh) — runner
- [docs/PROTOBUF_NESTED_DEMO.md](./PROTOBUF_NESTED_DEMO.md) — companion orders demo
- [docs/PROTOBUF_FILEDESC_WALKTHROUGH.md](./PROTOBUF_FILEDESC_WALKTHROUGH.md) — FileDescriptorSet variant (no Schema Registry)

---

## Registering this `.proto` in the Kaizen Apicurio Registry

The Kaizen "schema registry" is **Apicurio Registry**, not Confluent's. Apicurio
exposes two REST APIs on the same host:

- **Native Apicurio API** — `/apis/registry/v2/...` (or `v3/...` on newer
  deployments). Richer feature set (artifact types, groups, content rules,
  labels, branches…). The Apicurio web UI uses this.
- **Confluent-compatible API** — `/apis/ccompat/v6/` (or `/v7/`). A 1:1 facade
  over the Confluent SR REST API. **This is what RisingWave, kcat, and the
  Confluent Java/Python serializers talk to.**

Both endpoints write to the same backing store — a schema registered through
ccompat is visible in the Apicurio UI and vice versa.

### Registry URLs

| Environment | Base URL                                           |
| ----------- | -------------------------------------------------- |
| Staging     | `http://staging-schema-registry.kaizengaming.net/` |
| Production  | `http://schema-registry.kaizengaming.net/`         |

Concrete endpoints we'll use:

```bash
export REG="http://staging-schema-registry.kaizengaming.net"   # or prod
export CCOMPAT="$REG/apis/ccompat/v7"     # use this from RisingWave / Kafka clients
export APICURIO="$REG/apis/registry/v3"   # use this for Apicurio-native ops
```

> **You already uploaded the schema via the Apicurio UI** — so steps 1–4 below
> are the equivalent REST calls (for CI/CD or re-registration in prod).
> Skip to step 6 (verify) and step 7 (wire RisingWave) if you just need to
> consume the existing artifact.

### 1. (Apicurio-native) — what the UI does

Apicurio organizes artifacts by `groupId/artifactId`. The UI defaults to
`default` for the group; `artifactId` is whatever you typed.

```bash
GROUP="default"
ARTIFACT="CasinoRoundInfoDto"

curl -sS -X POST "$APICURIO/groups/$GROUP/artifacts" \
  -H "Content-Type: application/json" \
  -H "X-Registry-ArtifactId: $ARTIFACT" \
  -H "X-Registry-ArtifactType: PROTOBUF" \
  --data-binary @proto/casinoroundinfodto.proto | jq
```

That endpoint returns the full artifact metadata. The Apicurio UI was doing
exactly this when you uploaded.

### 2. (ccompat) — what Kafka clients & RisingWave need

For RisingWave to consume the topic, the schema must be reachable under a
**Confluent-style subject name** — typically `<topic>-value` — via the ccompat
facade. If you registered through the Apicurio UI under a different
`artifactId`, the subject seen by ccompat will be that `artifactId`, not
`<topic>-value`. Most likely you need to register it under the subject name
your consumers expect:

```bash
TOPIC="casino_rounds"
SUBJECT="${TOPIC}-value"          # TopicNameStrategy (Confluent default)

PROTO_FILE="proto/casinoroundinfodto.proto"
SCHEMA_JSON=$(jq -Rs . < "$PROTO_FILE")

cat > /tmp/sr-payload.json <<JSON
{
  "schemaType": "PROTOBUF",
  "schema": $SCHEMA_JSON
}
JSON

curl -sS -X POST "$CCOMPAT/subjects/$SUBJECT/versions" \
  -H "Content-Type: application/vnd.schemaregistry.v1+json" \
  --data @/tmp/sr-payload.json | jq
# => { "id": 12345 }
```

The call is **idempotent** — identical schema returns the same id.

> Confirm with whoever owns the topic whether your platform uses
> `TopicNameStrategy` (`<topic>-value`), `RecordNameStrategy` (`<protobuf-FQN>`)
> or `TopicRecordNameStrategy` (`<topic>-<FQN>`). The wrong subject is the #1
> reason producers succeed but consumers/RisingWave can't find the schema.

### 3. Schema references (only if your `.proto` imports other custom protos)

Not needed for this demo — `casinoroundinfodto.proto` only imports
`google/protobuf/timestamp.proto` which is built into both Apicurio and
Confluent. If you ever need them, register the imported subjects first, then:

```jsonc
{
  "schemaType": "PROTOBUF",
  "schema": "...",
  "references": [
    { "name": "common.proto", "subject": "common-value", "version": 1 }
  ]
}
```

### 4. (Optional) Compatibility-check before registering

```bash
curl -sS -X POST "$CCOMPAT/compatibility/subjects/$SUBJECT/versions/latest" \
  -H "Content-Type: application/vnd.schemaregistry.v1+json" \
  --data @/tmp/sr-payload.json | jq
# => { "is_compatible": true }
```

### 5. (Optional) Pin compatibility per subject

```bash
curl -sS -X PUT "$CCOMPAT/config/$SUBJECT" \
  -H "Content-Type: application/vnd.schemaregistry.v1+json" \
  -d '{"compatibility":"BACKWARD"}' | jq
```

### 6. Verify (via either API)

```bash
# Confluent-compat view
curl -sS "$CCOMPAT/subjects" | jq
curl -sS "$CCOMPAT/subjects/$SUBJECT/versions" | jq
curl -sS "$CCOMPAT/subjects/$SUBJECT/versions/latest" | jq '.id, .version, .schemaType'
curl -sS "$CCOMPAT/subjects/$SUBJECT/versions/latest" | jq -r .schema

# Apicurio-native view
curl -sS "$APICURIO/search/artifacts?name=$ARTIFACT" | jq
curl -sS "$APICURIO/groups/$GROUP/artifacts/$ARTIFACT" | jq
curl -sS "$APICURIO/groups/$GROUP/artifacts/$ARTIFACT/versions" | jq
```

### 7. Wire RisingWave to the Apicurio ccompat endpoint

```sql
CREATE SOURCE src_casino_rounds_proto
WITH (
    connector = 'kafka',
    topic = 'casino_rounds',
    properties.bootstrap.server = '<kaizen-bootstrap>:9092',
    scan.startup.mode = 'earliest'
)
FORMAT PLAIN ENCODE PROTOBUF (
    schema.registry = 'http://staging-schema-registry.kaizengaming.net/apis/ccompat/v7',
    message = 'Cronus.CasinoService.RoundInfo.Abstractions.CasinoRoundInfoDto'
);
```

> ⚠ **Crucial:** `schema.registry` must point at `/apis/ccompat/v7` (or `v6`),
> **not** the bare host. RisingWave speaks the Confluent SR REST dialect; the
> bare Apicurio root will 404 on `/subjects/...`.

### 8. ⚠ Wire-format pitfall: Apicurio default vs Confluent

This trips up every Apicurio + RisingWave setup. Producers can publish records
with **two different on-wire framings** depending on which Apicurio Java/.NET
serializer mode is used:

| Mode                                                 | Per-record framing                                                 | RisingWave compatible? |
| ---------------------------------------------------- | ------------------------------------------------------------------ | ---------------------- |
| Apicurio default (header-id encoding)                | Schema id in Kafka **record headers** (`apicurio.value.globalId`)  | ❌ No                  |
| Apicurio with `apicurio.registry.as-confluent=true`  | Confluent magic-byte + 4-byte id prefix in **payload**             | ✅ Yes                 |
| Confluent serializer pointed at ccompat              | Confluent magic-byte + 4-byte id prefix in payload                 | ✅ Yes                 |

Tell the producing team **explicitly** to use Confluent-style framing —
either via the Confluent serializer against `/apis/ccompat/v7`, or with the
Apicurio serializer configured with:

```properties
apicurio.registry.url=http://staging-schema-registry.kaizengaming.net/apis/ccompat/v7
apicurio.registry.as-confluent=true
apicurio.registry.use-id=contentId
```

(.NET: `UseIdStrategy = IdStrategy.ContentId`, `EnableConfluentIdHandler = true`.)

Quick smoke test from your laptop — peek at the first byte of a record:

```bash
docker run --rm --network host edenhill/kcat:1.7.1 \
  -C -b <kaizen-bootstrap>:9092 -t casino_rounds -c 1 -e -q | xxd | head -1
# Confluent framing → first byte is 00 (magic byte), bytes 1..4 are big-endian schema id.
# If you see protobuf field tags right away (e.g. 0a, 12, 1a...), producers are still on Apicurio header-id mode → RisingWave will fail to decode.
```

### 9. Deregistering

```bash
# Soft delete (keeps history; same id can be re-registered)
curl -sS -X DELETE "$CCOMPAT/subjects/$SUBJECT" | jq

# Permanent delete (only after soft delete)
curl -sS -X DELETE "$CCOMPAT/subjects/$SUBJECT?permanent=true" | jq

# Apicurio-native delete
curl -sS -X DELETE "$APICURIO/groups/$GROUP/artifacts/$ARTIFACT" | jq
```

> Don't `permanent=true` against a subject with live consumers — they'll fail
> their next deserialize with `Schema not found`.

### Pitfalls / FAQ

- **`HTTP 422 incompatible`** — new schema violates the subject's compatibility
  rule. Tag-number reuse for a different type is the usual culprit; never
  reuse a field number.
- **`HTTP 409 reference does not exist`** — register imported `.proto` subjects
  first, in dependency order.
- **`subject not found` from RisingWave / Kafka clients** but the schema is
  visible in the Apicurio UI — the UI created an artifact under
  `groupId=default, artifactId=<whatever>`, not the ccompat subject
  `<topic>-value`. Use step 2 above to register under the expected subject
  name.
- **Records decode partially or RisingWave logs `Failed to deserialize`** —
  producers are publishing with Apicurio header-id framing instead of
  Confluent magic-byte framing. See step 8.
- **`schema.registry` 404 on RisingWave source creation** — you pointed at the
  bare Apicurio host instead of `/apis/ccompat/v7`.
