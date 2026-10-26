# Jobotron contract parity: from the RFC's sketch to the team's real envelope

Status: **implemented** (2026-10-26) — all seven steps built and tested (56 contract/publish/consume/setup/reset/defs tests, 8 mutation anchors, full suite green); uncommitted, awaiting review. This is the design that brought `JobotronProducer`
(`orchestration/components/reverse_etl_jobotron.py`, builder in
`orchestration/assets/reverse_etl_jobotron.py`) to full wire compatibility with the
Jobotron team's real producer. The team's actual notebooks
(`~/Projects/jobotron_task/`: `kafka_message_publisher.ipynb` plus the CY/GR/RO
unverified-customers jobs) were validated against our component on 2026-10-26; this doc
transcribes the contract they revealed and designs the change. The notebooks live outside
this repo, so the contract transcription below is the in-repo record.

## What the real producer emits

One JSON message per selected row, keyed by `CustomerId`. The value's exact shape
(`kafka_message_publisher.ipynb`, `toKafkaMessages`; Spark renders it with
`ignoreNullFields=false`, so nulls survive):

```
{
  "JobId":           "<JobName>_<CompanyId>_<yyyyMMddHHmmss UTC>",   // per row's CompanyId
  "JobName":         "Send_Email_to_NonVerified_CY_Customers",       // the job's human name
  "JobCreated":      "2026-10-26T13:00:00Z",                         // per-run, second-truncated UTC
  "RunId":           "<uuid>",                                       // per run
  "CompanyId":       7,                                              // from the row
  "CustomerId":      "12345",                                        // the key, cast to string
  "ItemsCount":      1500,                                           // rows in this run (batch total)
  "ItemNumber":      42,                                             // 1-based position, ordered by CustomerId
  "ItemId":          "<ItemPayloadHash>-<ItemNumber>",
  "ItemPayload":     { ...every column of the row... },              // incl. CustomerId/CompanyId again
  "ItemPayloadHash": "<sha256 hex of the ItemPayload JSON>",
  "IsDryRun":        true,                                           // the job widget DEFAULTS to true
  "SchemaVersion":   1
}
```

How each field is produced there: `ItemPayloadHash` is `sha2(to_json(ItemPayload), 256)`;
`ItemNumber` is `row_number() over Window.orderBy("CustomerId")`; `ItemsCount` is the
run's `rows.count()`; `JobCreated` is `Instant.now()` truncated to seconds; `JobId` embeds
**the row's** `CompanyId` (constant per run in practice — each notebook selects one
company). Publishing is `messages.write.format("kafka")` from Databricks — warehouse
compute per market, which is exactly the cost our component removes.

## The validated gaps

Field-for-field against our current envelope (`run_id`, `job_type`, payload inlined;
`_jobotron_schemas` at reverse_etl_jobotron.py:145):

| Real field | Ours today | Gap |
|---|---|---|
| `ItemPayload` nesting | payload inlined at top level | structural — a worker parsing `ItemPayload` sees nothing |
| `ItemPayloadHash`, `ItemId` | absent | the worker's dedup/idempotency handles |
| `ItemNumber`, `ItemsCount` | absent | batch position and total |
| `JobId`, `JobName`, `JobCreated` | `job_type` only | run identity is partial |
| `IsDryRun` | absent | no blast-radius gate; theirs defaults **true** |
| `SchemaVersion` | absent (Avro registry versioning instead) | different mechanism |
| wire format | Avro + Apicurio | theirs is JSON — compatibility question |
| `RunId` | Dagster run id | ✅ equivalent (a per-run id) |
| key | `key_columns`, as-typed | ✅ identical — `CustomerId` is the key, fixed by the contract; the config field retires (D4) |

What matches and stays: one message per selected row; the **full eligible set republished
every run** with the worker owning dedup and re-validation; run identity in the envelope;
null-tolerant payloads; unknown-operator refusal; the bounded produce/flush backpressure.

The validation also settled a deployment question: the three real jobs are **not one
query with an operator column** — GR additionally excludes `cusTypeID 15`, and each market
has its own group id (1424/1425/1321). The component's one-query shape stays, but the
real-world adoption shape is **one instance per market** (each with its own query and
topic) — the shape D9 makes structural — and it needs the sources generalized from the
current hardcoded two tables to N named tables (D8), since the real jobs join three.

## The decisions

**D1 — JSON is the parity wire format; Avro moves behind `encoding`.** The real producer
publishes JSON; a worker that reads `ItemPayload` from JSON must not need a registry to
parse us. `encoding: json` (new default) serializes the envelope exactly as the template
above; `encoding: avro` keeps the registry path for consumers who want schemas — deferred
(see Deferred). This mirrors the sync component's `encoding` field: same family, same
pattern. Consequence: JSON mode needs no registry call; the schema-ensure step runs only
for Avro.

**D2 — the envelope becomes the real one, PascalCase, no extras.** `run_id`/`job_type` are
retired from the message (the RFC's sketch — `REVERSE_ETL_JOBOTRON.md` §172 — is
superseded by the validated contract). `RunId` keeps the Dagster run id: the field means
"an id for this run", and the Dagster id is exactly that. `JobName` comes from a new
`job_name` config field (the demo instances take the notebooks' own names —
`Send_Email_to_NonVerified_CY_Customers` and its GR/RO siblings); the envelope field name
is `JobName` regardless. Top-level `CustomerId`/`CompanyId` are read from the query result
under those literal names — every validated notebook selects exactly those two aliases,
and the contract test requires both (D4).

**D3 — the hash is sha256 over a canonical JSON we define once.** Spark's `to_json` is
their canon; ours is: compact separators (`,`/`:`), fields in the query-result's column
order, nulls explicit, `ensure_ascii=False`, UTF-8. The same canonical rendering is what
lands in the message's `ItemPayload`, so a worker can recompute and verify. The canonical
form is pinned by a golden-value test — the hash only needs to be *consistent* between
producer and worker, but pinning it makes drift a test failure, not a silent one.

**D4 — the key is `CustomerId`, contract-fixed; `key_columns` retires from config.** The
envelope fixes the identity in two places — the Kafka record key *is* `CustomerId` and
`ItemNumber` is `row_number() over orderBy("CustomerId")` — and `CustomerId` is a required
envelope field, so a config override could only ever produce a message claiming one
identity while keyed by another. A different identity is a SchemaVersion extension, not
instance config. What the old `key_columns` generalization conflated with the key —
tie-breaking — moves into code: ordering is `CustomerId`, then the remaining payload
columns in result order — a deterministic total order, identical to theirs whenever a
customer has one row (every validated case), stable if a query type ever returns several.
Validation replaces declaration: a contract test asserts the result carries `CustomerId`
and `CompanyId`, failing before any produce with the missing column named. `ItemsCount` =
`result.num_rows`, computed once. An empty result publishes nothing but still reports
`ItemsCount: 0` in the metadata.

**D5 — `dry_run` defaults to `true` and gates at the producer, not in the message.** The
team's widget defaults to true — adopt the same default (the safe side). In dry-run the
asset runs the full path — scan, query, envelope building, hashing, counting — then
**sends nothing** (no `produce()` call) and reports what it would have sent in the
metadata (`items_count`, a preview). `dry_run: false` publishes with
`IsDryRun: false` in every message. Rationale: the real contract's `IsDryRun` flag exists
because their sink is a Spark writer they toggle; our gate-at-producer is strictly safer
for the demo (a misconfigured instance cannot pollute an operator topic at all), and the
flag is still carried for workers that check it. Sending dry-run-marked messages is the
deferred alternative if the team says their worker expects them. And like their widget,
the mode is flippable **per run**: the launch config carries an optional `dry_run` —
`None` (a blank launch or a schedule) means the YAML decides, an override applies to that
run only, is logged when it differs, and lands in the metadata. The dangerous direction
still needs an explicit act, but it is a launch-dialog act, not a code edit; the YAML
value remains what schedules and unattended runs see.

**D6 — `SchemaVersion` is a config field, default 1.** Bumped by the author when the
payload shape changes; the worker can route on it. In JSON mode this is the only
versioning (the registry stays Avro-only).

**D7 — the envelope is built by one pure function.** The notebooks' `toKafkaMessages` is
pure and that is why it is testable; ours currently inlines envelope construction in the
asset loop. New: `build_messages(rows: pa.Table, cfg, *, job_created, run_id, dry_run) ->
list[dict]` in `reverse_etl_jobotron.py` — no Kafka, no registry, no Dagster context.
The asset loop becomes: scan → query → `build_messages` → (gate) → serialize → produce.
This is the same move as the sync's chunked envelope builder, and it is what makes the
contract tests below possible without services.

**D8 — the sources are N named tables, not two hardcoded ones.** Today the component's
source shape is fixed at `customers` + `kyc_cases` (`customers_table`, `kyc_table`,
`scan_columns`); the real jobs join three tables (`cCustomer`,
`mVerificationStatus`, `cCustomer_Group`). New YAML attribute `sources` — a map of
DuckDB-registration alias to `{table?, columns}` — replaces all three:

```yaml
sources:
  customers:
    columns: [customer_id, email, locale, operator, is_active]
  kyc_cases:
    columns: [customer_id, status]
  cCustomer_Group:                 # a real-market instance's third join
    table: cCustomer_Group         # table defaults to the alias; override for a rename
    columns: [ccgCustomerID, ccgGroupID, ccgValue0]
```

The alias is the name the query registers (and the setup/reset assets create or tear
down); `table` defaults to the alias, columns is the required pruning allowlist (the
query may touch nothing else). `customers_table`/`kyc_table`/
`scan_columns` are retired (PoC: no back-compat shim). The setup asset and the reset job
generalize to loop over the source map; `customers_fqn`/
`kyc_fqn` become derived per-alias fqns. The demo instances carry no `sources` at all —
the default engine resolves the tables in the warehouse — so the map exists for the
zero-DBU engines' bronze-table transcriptions.

**D9 — one instance is one query type × one operator; the routing layer dissolves.** The
validated estate is per-market: each real job's query hardcodes its market (`cusCompanyID`,
group id, per-market `cusTypeID` exclusions), so a shared query plus operator routing would
silently change eligibility. Instance identity becomes `(query_type, operator)` — both
typed, both load-bearing:

- the folder is `<query_type>_<operator>/defs.yaml`, transcribing the team's own notebook
  names 1:1 (`unverified_customers_cy.ipynb` → `unverified_customers_cy/defs.yaml`); a new
  job family is new folders, no component change.
- inside the YAML: `query_type` (the job family), `operator` (the market), `job_name` (the
  envelope's `JobName`), `topic` (typed when the platform owns it; default derives as
  `jobotron_<query_type>_<operator>`).
- the Dagster object names derive `jobotron_<query_type>_<operator>[_<role>]`; two
  instances must not share `(query_type, operator)` — the sync's "must not share a name"
  rule, pinned by a load test.
- retired: `operators`, `operator_column`, `topic_template`, and the unknown-operator
  refusal — with a per-market `WHERE` clause, cross-market leakage is structurally
  impossible instead of runtime-refused.
- `query_type` never touches the wire: the envelope stays the real contract (D2);
  `job_name` is the only wire identity.
- the demo becomes the three real instances (`unverified_customers_cy`, `_gr`, `_ro`) —
  read-only scans of `de.voperdb` through the default engine; nothing provisioned,
  nothing seeded, no shared demo tables at all.

**D10 — `scan_engine: databricks_sql`: run the real query on the real views.** Validated
against the catalog 2026-10-26 (personal profile): the notebook queries run on
`de.voperdb.cCustomer` — a sharded UNION view over ~30 per-market `voperdb_<mkt>_bronze`
Delta tables (daily shallow-clone refreshes, `partitionColumns: []` and
`clusteringColumns: []` everywhere, sub-GB per market). delta-rs cannot scan views, so the
zero-DBU engines depend on a faithful transcription of that layering into `sources`. The
third engine removes the transcription: the YAML's `query` goes **verbatim** — tokens,
not bytes: whitespace is whatever the paste produces, since the notebooks' two-space
indentation doesn't survive a YAML block scalar — in the Databricks
dialect — the notebooks' SQL, no porting — to a SQL warehouse via the Statement Execution
API, polls to a terminal state, fetches the rows as Arrow, and feeds the same
`build_messages()` (D7) — the envelope and Kafka path are untouched; the engine choice
sits strictly upstream.

**2026-10-26 amendment — the demo qualifies the queries to the market shards.** Warehouse
congestion (SP statements PENDING >10 min on the shared Serverless Starter warehouse) made
the 29-way UNION view the demo's bottleneck, so the three demo YAMLs qualify their tables
to the market shard: `de.voperdb_<mkt>.cCustomer` / `de.voperdb_<mkt>.cCustomer_Group`
(pass-through views over the `<mkt>_bronze` Delta tables, sub-GB each) and the never-sharded
dimension `de.metadatadb.mVerificationStatus`. Equivalence evidence, recorded live:
`SELECT DISTINCT cusCompanyID FROM de.voperdb_<mkt>.cCustomer` returns exactly the
instance's company — `[7]` cy (2 s), `[1]` gr (5 s), `[4]` ro — so the union's other legs
contribute nothing to these queries; the `cusCompanyID`/`ccgGroupID` filters stay as
guards. The notebooks still run the views; the qualification is the demo's, deliberate and
dated. Single-market statements run in seconds-class even while the warehouse queues the
union forms. (The zero-DBU engines stay blocked for these tables: the service principal
authenticates to the storage account but is denied direct reads on UC-managed
`__unitystorage` paths — "the operation lacked the necessary privileges" — so
`delta_scan`/delta-rs need storage RBAC the SP doesn't have; `sources` now accepts
absolute fqns for the day that changes.)

- the warehouse id is an environment value: `DATABRICKS_WAREHOUSE_ID` (required for this
  engine, never in the YAML — one defs.yaml still deploys to stg and prd). Auth reuses the
  service-principal trio (`reverse_etl_connections.databricks()`).
- `sources` is optional for this engine — the warehouse resolves the views; the pruning
  allowlist governs only our own scans.
- run metadata gains the statement's duration; a statement failure fails the run before
  any produce.
- the cost is stated, not hidden: this re-introduces warehouse DBUs — the thing the RFC's
  zero-DBU headline removes. At the validated data scale (sub-GB scans, candidate sets of
  thousands) a serverless run is seconds. And it is the **default**: defaults follow the
  validated adoption path — the team's own SQL on the team's own warehouse — and the
  zero-DBU engines become the opt-in optimization (`delta_extension` for large scans,
  `delta_rs` where maintaining the bronze-table transcription is worth it). The RFC's
  headline reframes accordingly: zero-DBU where it matters, opt in.
- result streaming (pre-count for `ItemsCount`, then stream batches to the producer
  instead of materializing) is one shared seam for all three engines; built when real
  candidate volumes require it.

## The config surface (delta)

| Field | Type | Default | Meaning |
|---|---|---|---|
| `job_name` | str | — (replaces `job_type`) | the envelope's `JobName` |
| `dry_run` | bool | `true` | D5 — the blast-radius gate; the launch config's optional `dry_run` overrides it per run (the notebooks' widget), the YAML still governs schedules |
| `schema_version` | int | `1` | D6 |
| `encoding` | `"json"` \| `"avro"` | `"json"` | D1 (avro deferred) |
| `sources` | map alias → `{table?, columns}` | — (replaces `customers_table`/`kyc_table`/`scan_columns`) | D8 — the N named tables the query joins |
| `query_type` | str | — | D9 — the job family; names the folder and the Dagster objects |
| `operator` | str | — | D9 — the market this instance serves |
| `topic` | str | `jobotron_<query_type>_<operator>` | D9 — typed when the platform owns the topic |
| `scan_engine` | `"databricks_sql"` \| `"delta_rs"` \| `"delta_extension"` | `databricks_sql` | D10 — the default is the team's SQL on the team's warehouse; the zero-DBU engines opt in |

Unchanged: `query`, the registry URL (Avro mode only).
Retired: `job_type` (→ `job_name`), `operators`/`operator_column`/`topic_template` (D9),
`customers_table`/`kyc_table`/`scan_columns` (D8), `key_columns` (D4 — the contract fixes
the key at `CustomerId`; validation replaces declaration).

**The minimal YAML** — six typed facts and nothing else:

```yaml
type: orchestration.components.reverse_etl_jobotron.JobotronProducer
attributes:
  query_type: unverified_customers
  operator: cy
  job_name: Send_Email_to_NonVerified_CY_Customers   # the wire identity — typed, never derived
  catalog: de
  schema_name: voperdb
  query: |
    SELECT cusCustomerID AS CustomerId, cusCompanyID AS CompanyId
    FROM cCustomer ...                                # the notebook's SQL, verbatim
```

Everything else derives or defaults: `topic` → `jobotron_unverified_customers_cy`,
`scan_engine` → `databricks_sql`, `encoding` → `json`, `dry_run` → `true`,
`schema_version` → `1`. The environment supplies the rest — `DATABRICKS_WAREHOUSE_ID`
(**required** for the default engine: a run without it fails naming the variable) plus the
service-principal trio the env contract already carries. The split is principled: the YAML
carries what the SQL *means* (`catalog`/`schema_name` included — they resolve the
unqualified table names); the env carries where it *runs* — warehouse and credentials are
per-workspace coordinates, so no YAML field and no override-with-fallback: one source, no
precedence. Run metadata records the warehouse id used — audits ask the run, not the repo.
A team that wants zero DBUs opts
into `scan_engine: delta_extension` or `delta_rs` — and takes the bronze-table
transcription (`sources`) that comes with them. And the query carries the one obligation
the config no longer states: it must emit `CustomerId` and `CompanyId` — the envelope
reads both from the row — and a contract test fails the run before any produce, naming
the missing column.

## Implementation steps

1. **The envelope builder + the contract tests.** `build_messages` per D7 with the exact
   field set of the transcription; order by `CustomerId` tie-broken by the remaining
   payload columns in result order (D4); hash per D3. Tests
   (`tests/test_jobotron_contract.py`): all 13 fields present, PascalCase, nothing extra;
   golden hash over a known payload (computed by hand in the test, not by the
   implementation under test); `ItemId` = `hash-number`; `ItemNumber` ordering stable
   across runs, including multiple rows per customer; the result carries `CustomerId` and
   `CompanyId` (a missing column fails before any produce, named); `ItemsCount` = batch
   total; `JobId` format `<job_name>_<the row's CompanyId>_<yyyyMMddHHmmss>` (the transcription
   — per-row CompanyId, constant per run in practice); `JobCreated`
   second-truncated UTC; nulls survive. 2 mutation anchors: the hash canonicalization
   (`separators`/`ensure_ascii`) and the field set (a dropped `ItemPayloadHash`).
2. **Wire the builder into the publish asset.** Replace the inline envelope; JSON
   serialization (compact, same canonical rules); dry-run gate before `produce()` (D5) —
   in dry-run, log + metadata only; metadata gains `dry_run`, `items_count`,
   `schema_version`. Existing behavior kept: unknown-operator refusal, bounded
   flush/retry, preview cap. Tests: dry-run default gates the producer (`produce` asserted
   not called via the fake); `dry_run: false` sets `IsDryRun: false` and produces; the
   message key is `CustomerId`.
3. **Config surface + the demo's YAMLs.** The component and `JobotronConfig` gain the four
   envelope fields (D2, D5–D6: `job_name`, `dry_run`, `schema_version`, `encoding`) and
   D9's `query_type`/`operator`/`topic`; `job_type`, `operators`, `operator_column`,
   `topic_template` and `key_columns` retire (D4 — validation replaces declaration). The
   demo becomes the three real instances (`unverified_customers_cy`, `_gr`, `_ro` —
   separate defs folders, the notebooks' SQL token-verbatim, the minimal YAML): read-only
   scans of `de.voperdb` through the default `scan_engine: databricks_sql` — nothing
   provisioned, nothing seeded. The demo runs end-to-end once step 6's engine exists. The
   dry-run default *is* the demo's first act: a run sends nothing and reports
   `items_count` per instance in the metadata; the reveal launches one instance with
   run-config `dry_run: false` and reads the envelopes off local Redpanda. The trade is stated: the demo
   now requires the warehouse environment (`DATABRICKS_WAREHOUSE_ID` + the
   service-principal trio) and spends seconds of DBUs per run (validated: the CY query
   returned 13 candidates through the serverless warehouse in ~38 s wall); the zero-DBU
   engines keep their coverage in the fake tests only. The
   consume asset decodes JSON and
   the new field names (reads `ItemPayload`, dedups on `ItemId`). Tests: retired fields
   rejected; the three demo defs load without Dagster name collisions; the consumer decodes
   a real envelope fixture.
4. **N named sources (D8).** The `sources` map replaces `customers_table`/`kyc_table`/
   `scan_columns`; the setup asset and reset job loop over it; the
   publish asset registers every alias; `customers_fqn`/`kyc_fqn` become per-alias
   fqns. Tests: a three-source config scans and registers all three (the fake tables);
   a query touching an unlisted column fails with the alias named; the demo defs load
   with `sources` absent (the default engine needs none).
5. **Idempotent topic setup.** The demo instances are read-only against `de.voperdb` —
   nothing owns tables — so the only state to manage is the topics: each instance's setup
   creates only its own derived topic, create-if-not-exists — and under dry-run it is a
   no-op (zero side effects: not even a Kafka connection); a topic exists only once a
   setup run carries `dry_run: false` (run config or YAML). Tests: running setup twice
   changes nothing; the
   dry-run setup never touches Kafka; the three setups run in any order without collision.
6. **`scan_engine: databricks_sql` (D10).** A statement-execution client behind the same
   seam as the other engines: submit the query verbatim in the YAML's `catalog.schema`
   context (the notebook's `use de.voperdb` — without it the API resolves unqualified
   names against the warehouse default and fails TABLE_OR_VIEW_NOT_FOUND; verified live
   2026-10-26) → poll to a terminal state →
   fetch chunks → Arrow → `build_messages()`; warehouse id from `DATABRICKS_WAREHOUSE_ID`
   (a run without it fails naming the variable — env values stay call-time, per the env
   contract's rule); `sources` optional for this engine; statement duration and the
   warehouse id in the run metadata. Tests with a fake statement client:
   the query is submitted unmodified, chunks assemble in order, a poll timeout fails the
   run, and a statement failure produces nothing.
7. **Docs.** `REVERSE_ETL_JOBOTRON.md`: the message-contract section replaced by the
   transcription above marked "validated against the team's notebooks 2026-10-26"; the
   per-market finding, the validated table layout (sharded UNION views over per-market
   `_bronze` Delta tables — so `sources` points at bronze, never at the views) and D10's
   cost trade-off against the zero-DBU headline recorded; the RFC sketch marked
   superseded. The component guide row for `JobotronProducer` gains the new fields.

Gates per step: pytest, ruff on touched files, the mutation anchors, GraphQL
reloadWorkspace smoke.

## What we deliberately do not adopt

- **Spark / warehouse execution** — the notebooks burn DBUs per market per run; ours scans
  with the service principal and runs DuckDB. Parity is the message contract, not the
  engine.
- **One notebook per market as a code pattern** — three copies of the publisher is their
  dedup-by-copy; ours stays one component instantiated per market.
- **`publish()` as a TODO** — their notebook does not actually sink; ours has the real
  producer with bounded flush.

## Deferred

- **`encoding: avro`** — same envelope as a nested Avro record (`ItemPayload` record +
  hash fields), registry-managed; only if a consumer wants schemas. The JSON path is the
  validated contract.
- **Sending dry-run-marked messages** — if the team's worker expects `IsDryRun: true`
  messages on the topic rather than nothing, flip D5's gate to "produce with the flag".
  One-line change once `build_messages` owns the envelope.
- **Per-market query templates in one YAML** — the three real queries differ in business
  rules, not just constants; a templating layer would invite exactly the silent-eligibility
  change the validation warned about. One instance per market stays the adoption shape.
