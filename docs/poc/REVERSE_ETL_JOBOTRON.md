# REVERSE_ETL_JOBOTRON.md — the Jobotron producer (Dagster, Spark-free, zero-DBU)

**Status: v3 — the wire contract is now the team's real one, validated against their notebooks on 2026-10-26 (§4; the field-by-field transcription and every decision behind it live in [`JOBOTRON_CONTRACT_PARITY.md`](JOBOTRON_CONTRACT_PARITY.md)). The RFC-era sketch below (the `run_id`/`job_type` envelope, Avro, per-operator routing) is superseded where it contradicts the validated contract. The zero-DBU engine story (§3) is now the opt-in path — the default engine runs the team's own SQL on their warehouse (D10).**

---

## 1. Where this component sits (RFC context)

The RFC proposes **Jobotron**: a worker service (per-operator Kafka consumers), a Job Status WebAPI, and a state database — owned by Accounts & Payments (KYC solutions). **None of that is this component.** The RFC is explicit that scheduling and candidate selection are externalized:

> "Scheduling and candidate selection remain externalized to upstream producers. Such producers can be scheduled Databricks jobs that query Data Lake to identify eligible customers and publish individual messages to the relevant operator topic."

**This component is that upstream producer**, realized the Big Data way — per the RFC review comment (Big Data, on the draft):

> "Dagster is now our standard orchestrator for new scheduled data jobs and can already reach the operator Kafka topics. Could the RFC name Dagster as the scheduler and publisher? It would run the candidate query on Databricks and publish the per-customer messages itself."

Two other review comments are addressed by this plan:

- **"Maybe the Databricks part should also be mentioned as a milestone?"** — this doc is the design for that milestone.
- **"We should probably decide and mention which team will own/develop the Dagster code"** — Big Data owns the producer (this component); Accounts & Payments owns the worker. The §8 schema-contract question is where the two meet.

**Why this strengthens the RFC, not just satisfies it.** The RFC lists "Databricks as a datasource" as a con (~$10/month/task, exact cost TBD) and "E2E testing difficulty" as another. The producer described here answers both:

- It runs the candidate query **directly against the lake with a service principal** — Delta reads (delta-rs snapshots day one; the `delta` extension's streaming `delta_scan` as the verified-live scale option, §3.1), no warehouse, no Spark, **0.0 DBUs** (the same thesis as [`REVERSE_ETL_DBU_COST_NOTES.md`](REVERSE_ETL_DBU_COST_NOTES.md)).
- It runs on the **existing Dagster stack** and the **existing local demo stack** (Kafka + Apicurio), so the producer→Kafka path is E2E-testable on a laptop — the exact pain the RFC's review thread flagged.

## 2. What the producer does

On a per-instance schedule, run the instance's **candidate-selection SQL** and publish **one Kafka message per selected customer** to the **instance's topic**, as **JSON in the team's real 13-field envelope** (§4 — validated against their notebooks; no registry, no Avro). That message is the RFC's *unit-of-work*: the Jobotron worker consumes it, verifies it hasn't been processed before, re-validates eligibility, executes the actions (Iris notifications, customer-group updates), and records the outcome.

Design consequences that fall out of the RFC's model:

- **Publish the full eligible set every run.** The worker owns deduplication (Redis markers) and re-validation against live data, so a stateless producer that republishes the whole segment is correct by construction — and stale batch input "does not produce wrong actions" (RFC, Safe Execution). No watermark, no state table, no CDF.
- **`RunId` on every message** (the Dagster run id) so the worker can group items into its "job runs" concept (the RFC's state model counts ~57 runs/day: 9 daily jobs + 2 hourly × 24).
- **One topic per instance** (D9: one query type × one operator — the validated estate is per-market; each real job's query hardcodes its market, so routing by an operator column would silently change eligibility). The RFC's shared-per-operator topic is what the worker still sees.
- **Volume is tiny on our side**: ~20,000 messages/day, peak ~100 msg/s, 400 B–1 KB each. The demo's 1M-row produce tuning transfers as headroom, not as a requirement.

## 3. Architecture

```
jobotron.yaml (per job)
   │  schedule (hourly/daily cron)
   ▼
resolve each source table location (UC REST API — reused)
   │
   ▼
scan per the job's scan_engine (§4) ── column-pruned either way
   │  delta_rs (day one):  delta-rs snapshot scans (abfss://, service principal)
   │                       → pyarrow, zero-copy registration
   │  delta_extension:     DuckDB views over the delta extension's streaming
   │                       delta_scan (no Arrow materialization, verified live)
   ▼
DuckDB over the aliases ── runs the YAML candidate query (joins included)
   │  result batch (pyarrow): customer_id, operator, payload fields
   ▼
route by operator → topic_template; stamp run_id
   │
   ├─► derive Avro schema ──► register/verify subject (Apicurio)
   ▼
produce one message per customer (key = customer_id) — lz4, chunked flush, backpressure
   │
   ▼
run metadata: customers published, per-operator counts, duration, warehouse_seconds = 0.0
```

### 3.1 The engine decision

**Update 2026-10-26 (D10): the default engine is now `databricks_sql`** — the team's own SQL on
their warehouse via the Statement Execution API, seconds of DBUs at the validated scale (sub-GB
scans; the CY query returned 13 candidates in ~38 s wall). The zero-DBU paths below become the
opt-in optimization — and they are the only ones that need `sources`, because delta-rs cannot scan
the estate the real queries run on (`de.voperdb.cCustomer` is a sharded UNION view over per-market
`_bronze` Delta tables; when a job transcribes that layering, **`sources` points at bronze, never
at the views**).

| Option | SQL power | Delta access | JVM | Verdict |
|---|---|---|---|---|
| **delta-rs scan → DuckDB over the snapshots** (recommended) | full SQL incl. joins | delta-rs — **proven on our tables** | no | recommended |
| DuckDB + its `delta` extension | full SQL + pushdown | delta-kernel-rs streaming scan — no Arrow materialization | no | Phase-4 lever (§8.6); **verified live on our tables (2026-10-21)** — see below |
| Daft / polars | partial SQL | delta-rs based | no | dialect limits on the joins we need |
| Databricks job (the RFC's literal wording) | full SQL | native | n/a | meters DBUs — the RFC's own cost con; this plan exists to avoid it |

Same reasoning as v1, with one update: **multi-table joins are in scope from day one** — real selection queries join customer attributes with KYC/domain tables (the RFC's whole point is moving *complex* selection queries off VOperDB). delta-rs scans each source into pyarrow; DuckDB registers each under its YAML alias and joins freely. The known trade-off stays: no predicate pushdown into the scans on day one, mitigated by column pruning (§4) and a sizing spike (§9 Phase 0).

**The `delta` extension path is verified live (2026-10-21), with one environment caveat.** The exact defs.yaml candidate query ran unchanged over both demo tables' abfss locations via `delta_scan` — `INSTALL azure` + one secret in the `PROVIDER service_principal` / `TENANT_ID` parameter form, using the same SP credentials the producer already holds (the `OAUTH2_ENDPOINT` parameter form is rejected on this version) — and returned the publish run's exact 4-row selection in ~6 s cold. The tables are column-mapped, so the extension's old "unverified column-mapping behavior" caveat is retired by direct test, not just by the kernel's design (delta-kernel-rs handles column mapping and deletion vectors at the kernel level; pushdown features — constant-filter pushdown since v0.1.0, file skipping + partition-info pushdown since v0.3.0 — are shipped, not future work). Day one still recommends the delta-rs path: zero new moving parts and the volumes don't need it yet — the extension is the scale lever (§8.6), not a correction. The caveat: the path works from the **host venv** but fails inside the **Dagster container** — the image's `duckdb-azure` build cannot fetch Azure tokens (`GetToken(): … Problem with the SSL CA cert`), even though system TLS to login.microsoftonline.com is fine there; a container-image issue, not a credentials or design one. The demo therefore stays on `delta_rs` (§12), and a job's default flips to the extension only when the image is fixed **and** the sizing spike says so (§8.6).

## 4. The YAML contract (v3 — the validated one)

One instance is **one query type × one operator** (D9); the folder is
`orchestration/defs/<query_type>_<operator>/defs.yaml`. The minimal YAML is six typed facts —
everything else derives or defaults, and the environment carries the coordinates
(`DATABRICKS_WAREHOUSE_ID` + the service-principal trio; the YAML never carries them — one
defs.yaml still deploys to stg and prd):

```yaml
# orchestration/defs/unverified_customers_cy/defs.yaml — the team's CY notebook, as an instance
type: orchestration.components.reverse_etl_jobotron.JobotronProducer
attributes:
  query_type: unverified_customers     # the job family; names the folder and the Dagster objects
  operator: cy                         # the market this instance serves
  job_name: Send_Email_to_NonVerified_CY_Customers   # the envelope's JobName — the wire identity
  catalog: de
  schema_name: voperdb
  query: |                             # the notebook's SQL, token-verbatim (whitespace is the paste's)
    select cusCustomerID as CustomerId, cusCompanyID as CompanyId
    from cCustomer ...
```

Derived/defaults: `topic` → `jobotron_<query_type>_<operator>` (typed when the platform owns it),
`scan_engine` → `databricks_sql`, `encoding` → `json`, `dry_run` → `true` (D5: the whole path runs
and nothing is sent — and the launch config's optional `dry_run` flips the mode per run, the
notebooks' own widget; the YAML value still governs schedules and blank launches),
`schema_version` → `1`. Optional: `sources` — the zero-DBU engines' alias →
`{table?, columns}` map (the pruning allowlist; `table` defaults to the alias); `databricks_sql`
needs none, the warehouse resolves the views. Retired (rejected at load): `job_type`, `operators`,
`operator_column`, `topic_template`, `key_columns`, `customers_table`/`kyc_table`/`scan_columns`.

**The message contract** is the team's real envelope, transcribed from
`kafka_message_publisher.ipynb` and **validated against the team's notebooks 2026-10-26** — the
authoritative field-by-field record (and every decision behind it: D1–D10) lives in
[`JOBOTRON_CONTRACT_PARITY.md`](JOBOTRON_CONTRACT_PARITY.md). One JSON message per selected row,
keyed by `CustomerId`:

```
JobId <JobName>_<CompanyId>_<yyyyMMddHHmmss UTC> · JobName · JobCreated (second-truncated UTC) ·
RunId (the Dagster run id) · CompanyId (from the row) · CustomerId (the key, cast to string) ·
ItemsCount (the batch total) · ItemNumber (row_number over CustomerId, tie-broken by the remaining
payload columns in result order) · ItemId (<ItemPayloadHash>-<ItemNumber>) · ItemPayload (every
result column, nested) · ItemPayloadHash (sha256 over the pinned canonical JSON: compact
separators, result-column order, nulls explicit, ensure_ascii=False) · IsDryRun · SchemaVersion
```

Notes:

- **JSON on the wire** (D1): no Apicurio call anywhere; `encoding: avro` is deferred behind the
  config knob. Nulls survive (`ignoreNullFields=false` parity).
- The query must emit `CustomerId` and `CompanyId` — the envelope reads both from the row, and a
  result missing them fails the run before any produce, naming the column.
- `query_type` never touches the wire; `job_name` is the only wire identity.

## 5. Pipeline stages

v3: stages 4–5 are replaced by the pure `build_messages` envelope builder and the dry-run gate (JSON, no registry — §4); the list below is the v2 record.

1. **Resolve** — table locations via the existing cached UC helper; per-source storage options.
2. **Scan** — per the job's `scan_engine` knob (§4): `delta_rs` (day one) reads column-pruned snapshots via delta-rs `to_pyarrow_table(columns=…)`; `delta_extension` registers each alias as a DuckDB view over the extension's streaming `delta_scan()`, projected to the same column allowlist (verified live 2026-10-21, §3.1/§8.6).
3. **Query** — execute the YAML query over the aliases (registered snapshots or `delta_scan` views — identical text either way); fetch as pyarrow.
4. **Route & stamp** — derive each row's topic from `operator_column` + `topic_template`; stamp `run_id` and `job_type`.
5. **Schema** — derive the Avro schema from the result shape + envelope fields; register/verify key + value subjects in Apicurio (reused logic, known-check, `ZonedTimestamp` microsecond-UTC convention).
6. **Produce** — one message per customer, key = `key_columns` tuple; the reused producer loop (lz4, linger, chunked produce with final flush, `BufferError` backpressure, delivery-callback accounting). Routing by topic is a per-partition produce detail, not a new mechanism.
7. **Record** — Dagster metadata: rows published, per-operator counts, bytes, duration, and `warehouse_seconds: 0.0` asserted in the cost meter.

## 6. Reuse map

| Piece | From | Reuse |
|---|---|---|
| Location resolution + cache, ADLS options | sync (`reverse_etl_cdf_setup.py`) | as-is |
| Avro type mapping + serializers, Apicurio registration | sync | adapt input: pyarrow schema instead of Delta schema |
| Kafka producer + produce loop + backpressure | sync | as-is (plus per-topic routing) |
| Cost meter, health-check pattern, defs loading | sync + daemon | as-is (Jobotron health check) |
| Local E2E stack (Kafka + Apicurio + helper/page) | demo | the producer's testbed — answers an RFC review concern |
| Query engine, YAML contract, operator routing, run stamping | — | **new** |

New dependency: `duckdb` only (pure wheel, no JVM).

## 7. Constraints (inherited from the demo + imposed by the RFC)

**From the demo's hard-won lessons:**

- Any producer-owned Delta table (if a later phase needs one) is **delta-rs-native only** — never Spark- or SQL-written (v7 writer protocol). delta-rs 1.6.6 creates tables at protocol (reader 1, writer 2) — minimal, no features — and a Databricks write conforms to the table's existing protocol rather than upgrading it; the lock-out happens at *birth* (Databricks-created tables are v7 from the first commit, verified live: delta-rs answers `CommitFailedError: Unsupported table features required`) or by Databricks-side DDL that enables a newer writer feature. Note also (verified live 2026-10-19): the demo's service principal cannot register a delta-rs-born table in Unity Catalog — it holds neither `CREATE EXTERNAL TABLE` nor `MANAGE` on the storage's external location — so producer-owned tables stay warehouse-invisible unless that privilege is granted.
- **No CDF anywhere** — snapshot queries only; that's what makes the component Spark-free *today* (see v1 rationale). Deletion vectors are an unambiguous win here (smaller scans, no CDF bloat possible).
- Column-mapped tables scan fine via delta-rs (verified live) — and via the `delta` extension's `delta_scan` (verified live 2026-10-21, §3.1). Timestamps travel as microsecond-UTC strings per the `ZonedTimestamp` convention.

**From the RFC:**

- **GDPR — personal data in the payload.** `customer_id`, `email`, `phone_number` are exactly the payload's business. Mitigations, aligned with the RFC: TLS on the prod Kafka path (the RFC's in-transit answer — our connection config must support SSL), payload minimization by construction (§4), 10-day topic retention, and the RFC's anonymization-service integration for anything the worker persists. Review comment [o] raises at-rest encryption for certain job types (e.g. HR-like data) — treat as a per-job YAML concern if such a job ever lands here (§8).
- **Operator isolation** — routing must never mix operators; a bad `operator` value should fail the row loudly (unknown topic), not publish cross-market.
- **The worker is the safety net** — dedup and eligibility re-validation are theirs. The producer guarantees *at-least-once, current-as-of-run*; nothing more.

## 8. Semantics & open questions (to refine)

1. **Full-set vs delta-of-set.** Full eligible set every run (recommended, stateless); the worker dedups. A delta-of-set producer (publish only newly-eligible customers) would cut volume ~10× but needs producer-side state and shifts correctness onto us. Decide with A&P; default full-set.
2. **Segment exit.** If a customer leaves the segment, no message is published — fine for "act once" jobs (KYC PMs), potentially wrong for "maintain membership" jobs. Confirm every migrated job's action semantics with A&P.
3. **Operator routing** — `topic_template` + loud failure on unknown operator (§7), vs a static allowlist per job. Leaning allowlist in YAML + template for naming.
4. **Avro schema contract ownership.** The value subject is a cross-team contract (Big Data publishes, A&P consumes). Apicurio compatibility = BACKWARD; a shape change requires A&P's worker to be compatible first. Who approves? (Ties to the RFC's open ownership comment.)
5. **Payload field set per job type** — A&P states what the actions need; the YAML selects exactly that. Email/Phone in payload is the RFC's stated GDPR concern — keep them out of any job that doesn't need them.
6. **Scan sizing — the scale ladder.** Customer tables are large; the day-one path materializes each column-pruned snapshot in RAM (`to_pyarrow_table()`), so at real scale the binding constraint is **memory before time**: five pruned columns at 5 B rows ≈ 40 GB (`customer_id`) + ~150 GB (`email`) + ~75 GB (`phone_number`) + ~25 GB (`locale`) + 5 GB (`is_active`) ≈ **~295 GB** — an OOM on any single worker long before the scan finishes; ~1 B rows is ~60 GB. The rungs, in the order to reach for them:
   1. **Column discipline** — the YAML's `scan_columns` hints already make GDPR minimization ≈ memory minimization; a job that never touches a heavy string never scans it.
   2. **Two-pass** — pass 1 scans only ids + filter columns and joins to the eligible-id set; pass 2 fetches payload fields only for the selected ids (the small segment at full-set semantics). Halves the heavy-string bytes; pass 2's cost depends on file skipping engaging on `customer_id` (rung 3's layout caveat).
   3. **The `delta` extension** (Phase 4, verified live — §3.1): the scan streams through DuckDB's morsel-driven operators and joins spill to disk, so the memory wall disappears and the wall becomes I/O time. Pushdown only pays when the layout cooperates: `status = 'PENDING'` skips files only if status is clustered or the table is partitioned on it — on an interleaved table every file's min/max contains 'PENDING' and I/O stays full. At billions of rows, warehouse-side clustering/partitioning (Databricks OPTIMIZE/liquid clustering) becomes part of the producer's data contract, agreed per job.
   4. **Shard by operator** — routing is already per-operator; per-operator scans bound each worker's memory and parallelize. Buys parallelism, not fewer bytes.
   5. **Per-job fallback** — a job whose table is billions of rows with low selectivity and no layout control gets the RFC's literal Databricks job (~$10/month/task is small money against straining the path); the YAML can carry a per-job engine choice, so the producer is not all-or-nothing.
   The spike (§9 Phase 0) therefore measures **peak memory alongside time**, at realistic scale and real selectivity (the actual PENDING fraction), on real file layout — that decides which rung a job lands on. The criterion extends from "hour-safe" to "hour-safe *and* fits the worker, or streams via the extension."
7. **Dagster code ownership** — the RFC's open comment; this plan assumes Big Data owns the producer, A&P owns the worker and the message-contract approval (§8.4).
8. **Prod Kafka TLS** — the demo stack is plaintext; prod needs SSL config in the producer's Kafka settings (RFC's GDPR in-transit answer).
9. **Run metadata vs RFC run concept** — is Dagster's run metadata sufficient for Big Data's visibility, or does A&P want a producer-side heartbeat (e.g., a control message per run) their WebAPI could surface? Ask; default: Dagster metadata only.
10. **Customer identity across operators** — the demo assigns globally-unique ids (one sequence); real per-operator customer bases can collide on ids. Per-operator topics/tables/workers tolerate collisions (uniqueness within a market is all the worker's dedup needs), but the Kafka record key carries `customer_id` alone — if anything ever converges markets downstream (a shared dedup store across worker deployments, merged audit storage), the key must become composite `(operator, customer_id)` or ids must be globally assigned. Confirm how customer ids are assigned with A&P.

## 9. Phased plan

**Phase 0 — spikes (before component code):**
1. *Selection round-trip*: delta-rs scans (column-pruned) of two realistic lake tables → DuckDB join + filter → correct rows; timing **and peak memory** on a realistic-scale customer table (§8.6).
2. *Message contract*: pyarrow result → Avro envelope (`run_id`, `job_type`, payload) → register in Apicurio → consume and decode; timestamps byte-identical to the sync's convention.
3. *Operator routing*: one result set → per-operator topics, loud failure on unknown operator.

**Phase 1 — MVP (one real KYC job):** manual Dagster run → staging Kafka/Apicurio; `warehouse_seconds: 0.0` asserted; correctness = one message per query row, keyed by `customer_id`, landed on the right operator topic. This is the artifact to attach to the RFC thread (comment [t]'s milestone).

**Phase 2 — schedules & ops:** hourly/daily crons, Jobotron producer health check, alerting to the owning team, cost-meter line in the RFC's terms ($0/month/task measured).

**Phase 3 — more job types:** onboard the remaining KYC jobs; per-job payload contracts with A&P; encryption-at-rest variant if a job type requires it (§8).

**Phase 4 — pushdown (only if spike says so):** switch the scan source to the DuckDB `delta` extension. **Implemented 2026-10-21** as the per-job `scan_engine` knob (§4) and verified live on the demo tables from the host venv. The demo's `defs.yaml` stays on the day-one `delta_rs` default for now — the Dagster container image's `duckdb-azure` build cannot fetch Azure tokens (TLS/CA failure inside the image, §3.1) — so a job's default flips to the extension only when the image is fixed **and** the spike says so (§8.6).

## 10. Verification criteria

- **Zero DBU**: cost meter records `0.0` warehouse seconds — the producer's answer to the RFC's "~$10/month/task" con.
- **No JVM**: zero `java` processes; no pyspark import.
- **Contract-correct messages**: one message per selected customer, key = `CustomerId`, the validated 13-field envelope in exact JSON canon (a worker can recompute `ItemPayloadHash`), dry-run sends nothing.
- **E2E on a laptop**: the full path (YAML → lake → Kafka → Apicurio) runs against the local demo stack — the RFC review thread's testing concern, answered.
- **Hour-safe**: every hourly job completes in minutes, well within its cadence.
- Ruff-clean, like the sync files.

## 11. Non-goals

- Everything the RFC assigns to Jobotron itself: the worker, the WebAPI, the state database, Redis idempotency, eligibility re-validation, Iris/CustomerService/VOperDB actions.
- CDF/change replication (the existing sync's job) — no CDF read here ever, and since 2026-10-19 the sync does not read CDF either: it reconstructs changes by diffing snapshots per commit version (`reverse_etl_delta_reader.py`), so the delta-rs CDF gap is irrelevant to both components.
- The Hangfire-vs-Jobotron debate (RFC comments [h]–[m]) — settled there; this doc only designs the data-platform producer.
- Producer-side tombstones / segment-exit messages (§8.2 unless a job type demands it).

## 12. The demo (built)

Three Dagster instances — the team's three real jobs as components, read-only against their own
warehouse:

| | |
|---|---|
| Component | `orchestration/components/reverse_etl_jobotron.py` — `JobotronProducer`; one folder per instance: `orchestration/defs/unverified_customers_{cy,gr,ro}/defs.yaml` (the six-fact minimal YAML, the notebooks' SQL token-verbatim) |
| Assets per instance | `jobotron_unverified_customers_<mkt>` (publish), `_setup` (the topic, create-if-not-exists), `_consume` (demo worker stand-in: decode the JSON envelope, dedup on ItemId, mirror into Postgres); jobs `<instance>_setup_job` and `<instance>_reset_job` |
| Engines | `databricks_sql` (the default): the query verbatim on the warehouse from `DATABRICKS_WAREHOUSE_ID` via the Statement Execution API; `delta_rs`/`delta_extension` opt in with `sources` |
| Demo data | none of ours — the instances read `de.voperdb` (the sharded UNION views over the per-market `_bronze` Delta tables); nothing provisioned, nothing seeded |
| Topics | `jobotron_unverified_customers_<mkt>`; setup is a no-op under dry-run (zero side effects, not even a Kafka connection) — the topic is created by a setup run carrying `dry_run: false` (run config or YAML) |

The dry-run default is the demo's first act: a run submits the real query, builds and hashes every
envelope, and sends nothing — `items_count`, the candidate query, the statement's seconds and the
warehouse id land in the run metadata (validated live 2026-10-26: the CY query returned 13
candidates through the serverless warehouse in ~38 s wall). The reveal flips the mode with the
per-run switch (`dry_run: false` in the launch config) and reads the envelopes off the topic. The
old animation-page demo (demo tables, change ops, Apicurio subjects) is retired with the routing
layer it demonstrated; the helper server carries a minimal replacement panel at
`http://localhost:4002/jobotron` — instance picker, the dry-run checkbox, run buttons for
setup/publish/consume, and read-only topic/mirror checks.

## 13. Live-demo script

1. Open the Dagster UI: three asset groups, `jobotron_unverified_customers_{cy,gr,ro}`.
2. Run one instance's publish (blank launch): green, `items_count` in the metadata, nothing on
   the topic — the blast-radius story.
3. The mode is a per-run switch, like the notebook's `IsDryRun` widget: launch setup and then
   publish with run config `{"ops": {"<asset>": {"config": {"dry_run": false}}}}` (in the UI:
   Launch run → add configuration). Read the JSON envelopes off Redpanda (console or
   `rpk topic read`): the 13-field contract, `JobId` embedding the row's CompanyId,
   `ItemPayload` nested, `ItemPayloadHash` recomputable.
4. Run consume: the mirror table fills one row per ItemId; re-run publish + consume — same items,
   no duplicates (the worker's dedup token demonstrated).
