# REVERSE_ETL_JOBOTRON.md — the Jobotron producer (Dagster, Spark-free, zero-DBU)

**Status: DRAFT v2 — refined against the Jobotron RFC (08.10.2026, STATUS: Review). The Phase-1 demo is built and verified live (see §12); §3, §8 and §9 remain the parts to argue.**

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

- It runs the candidate query **directly against the lake with a service principal** — delta-rs reads, no warehouse, no Spark, **0.0 DBUs** (the same thesis as `REVERSE_ETL_DBU_COST_NOTES.md`).
- It runs on the **existing Dagster stack** and the **existing local demo stack** (Kafka + Apicurio), so the producer→Kafka path is E2E-testable on a laptop — the exact pain the RFC's review thread flagged.

## 2. What the producer does

On a per-job schedule (hourly or daily), run the job's **candidate-selection SQL against the lake's Delta tables**, and publish **one Kafka message per selected customer** to that customer's **operator topic**, serialized as **Avro with the schema registered in Apicurio**. That message is the RFC's *unit-of-work*: the Jobotron worker consumes it, verifies it hasn't been processed before, re-validates eligibility, executes the actions (Iris notifications, customer-group updates), and records the outcome.

Design consequences that fall out of the RFC's model:

- **Publish the full eligible set every run.** The worker owns deduplication (Redis markers) and re-validation against live data, so a stateless producer that republishes the whole segment is correct by construction — and stale batch input "does not produce wrong actions" (RFC, Safe Execution). No watermark, no state table, no CDF.
- **`run_id` stamped on every message** so the worker can group items into its "job runs" concept (the RFC's state model counts ~57 runs/day: 9 daily jobs + 2 hourly × 24).
- **One shared topic per operator** (RFC: all job types share it; Brazil has 10 partitions, others 3), so messages are **routed by an operator column** in the query output.
- **Volume is tiny on our side**: ~20,000 messages/day, peak ~100 msg/s, 400 B–1 KB each. The demo's 1M-row produce tuning transfers as headroom, not as a requirement.

## 3. Architecture

```
jobotron.yaml (per job)
   │  schedule (hourly/daily cron)
   ▼
resolve each source table location (UC REST API — reused)
   │
   ▼
delta-rs snapshot scans (abfss://, service principal) ── column-pruned
   │  pyarrow, zero-copy registration
   ▼
DuckDB over the snapshots ── runs the YAML candidate query (joins included)
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

| Option | SQL power | Delta access | JVM | Verdict |
|---|---|---|---|---|
| **delta-rs scan → DuckDB over the snapshots** (recommended) | full SQL incl. joins | delta-rs — **proven on our tables** | no | recommended |
| DuckDB + its `delta` extension | full SQL + pushdown | bundles its own kernel reader | no | later optimization (§8.6); unverified column-mapping behavior |
| Daft / polars | partial SQL | delta-rs based | no | dialect limits on the joins we need |
| Databricks job (the RFC's literal wording) | full SQL | native | n/a | meters DBUs — the RFC's own cost con; this plan exists to avoid it |

Same reasoning as v1, with one update: **multi-table joins are in scope from day one** — real selection queries join customer attributes with KYC/domain tables (the RFC's whole point is moving *complex* selection queries off VOperDB). delta-rs scans each source into pyarrow; DuckDB registers each under its YAML alias and joins freely. The known trade-off stays: no predicate pushdown into the scans on day one, mitigated by column pruning (§4) and a sizing spike (§9 Phase 0).

## 4. The YAML contract (v2)

```yaml
# orchestration/assets/reverse_etl_jobotron/<job>/jobotron.yaml
name: kyc_unverified_pm
job_type: kyc.unverified_pm            # the worker's job-type identifier (in every message)
schedule: "0 * * * *"                  # hourly; daily jobs use their daily cron
sources:                               # alias -> lake table (each registered in DuckDB under its alias)
  customers:
    fqn: de_dev.sr_poc_external.customers
    columns: [customer_id, email, phone_number, locale, is_active]   # optional scan-pruning hint
  kyc_cases:
    fqn: de_dev.sr_poc_external.kyc_cases
    columns: [customer_id, status, operator]
query: |
  SELECT c.customer_id, c.email, c.phone_number, c.locale, k.operator
  FROM customers AS c
  JOIN kyc_cases AS k ON k.customer_id = c.customer_id
  WHERE k.status = 'PENDING' AND c.is_active
key_columns: [customer_id]             # Kafka record key (worker's idempotency identity)
operator_column: operator              # routing column (consumed at produce time)
topic_template: "jobotron-{operator}"  # per-operator shared topic per the RFC
```

Notes:

- The **Avro value schema is exactly the query's output shape plus `run_id` and `job_type`** — the published contract is what the YAML author selects, which makes payload minimization (GDPR, §7) true by construction.
- `run_id` is stamped automatically per execution (the scheduled run's timestamp), matching the RFC's job-run grouping.
- Day one: one row per customer per run. If a job ever needs one-message-per-(customer, item) semantics, that's a per-job YAML variant, not a redesign.

## 5. Pipeline stages

1. **Resolve** — table locations via the existing cached UC helper; per-source storage options.
2. **Scan** — delta-rs `to_pyarrow_table(columns=…)` per source, honoring the pruning hints.
3. **Query** — register all sources in DuckDB under their aliases; execute the YAML query; fetch as pyarrow.
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

- Any producer-owned Delta table (if a later phase needs one) is **delta-rs-native only** — never Spark- or SQL-written (v7 writer protocol).
- **No CDF anywhere** — snapshot queries only; that's what makes the component Spark-free *today* (see v1 rationale). Deletion vectors are an unambiguous win here (smaller scans, no CDF bloat possible).
- Column-mapped tables scan fine via delta-rs (verified live); timestamps travel as microsecond-UTC strings per the `ZonedTimestamp` convention.

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
6. **Scan sizing.** Customer tables are large; the day-one no-pushdown scan needs a spike (§9) measuring a realistic multi-million-row, column-pruned scan + join. If it's slow, the DuckDB `delta` extension (pushdown) is the Phase-4 lever.
7. **Dagster code ownership** — the RFC's open comment; this plan assumes Big Data owns the producer, A&P owns the worker and the message-contract approval (§8.4).
8. **Prod Kafka TLS** — the demo stack is plaintext; prod needs SSL config in the producer's Kafka settings (RFC's GDPR in-transit answer).
9. **Run metadata vs RFC run concept** — is Dagster's run metadata sufficient for Big Data's visibility, or does A&P want a producer-side heartbeat (e.g., a control message per run) their WebAPI could surface? Ask; default: Dagster metadata only.
10. **Customer identity across operators** — the demo assigns globally-unique ids (one sequence); real per-operator customer bases can collide on ids. Per-operator topics/tables/workers tolerate collisions (uniqueness within a market is all the worker's dedup needs), but the Kafka record key carries `customer_id` alone — if anything ever converges markets downstream (a shared dedup store across worker deployments, merged audit storage), the key must become composite `(operator, customer_id)` or ids must be globally assigned. Confirm how customer ids are assigned with A&P.

## 9. Phased plan

**Phase 0 — spikes (before component code):**
1. *Selection round-trip*: delta-rs scans (column-pruned) of two realistic lake tables → DuckDB join + filter → correct rows, timing on a multi-million-row customer table.
2. *Message contract*: pyarrow result → Avro envelope (`run_id`, `job_type`, payload) → register in Apicurio → consume and decode; timestamps byte-identical to the sync's convention.
3. *Operator routing*: one result set → per-operator topics, loud failure on unknown operator.

**Phase 1 — MVP (one real KYC job):** manual Dagster run → staging Kafka/Apicurio; `warehouse_seconds: 0.0` asserted; correctness = one message per query row, keyed by `customer_id`, landed on the right operator topic. This is the artifact to attach to the RFC thread (comment [t]'s milestone).

**Phase 2 — schedules & ops:** hourly/daily crons, Jobotron producer health check, alerting to the owning team, cost-meter line in the RFC's terms ($0/month/task measured).

**Phase 3 — more job types:** onboard the remaining KYC jobs; per-job payload contracts with A&P; encryption-at-rest variant if a job type requires it (§8).

**Phase 4 — pushdown (only if spike says so):** evaluate the DuckDB `delta` extension.

## 10. Verification criteria

- **Zero DBU**: cost meter records `0.0` warehouse seconds — the producer's answer to the RFC's "~$10/month/task" con.
- **No JVM**: zero `java` processes; no pyspark import.
- **Contract-correct messages**: one message per selected customer, key = `customer_id`, `run_id`/`job_type` stamped, subject registered in Apicurio, operator routing exact.
- **E2E on a laptop**: the full path (YAML → lake → Kafka → Apicurio) runs against the local demo stack — the RFC review thread's testing concern, answered.
- **Hour-safe**: every hourly job completes in minutes, well within its cadence.
- Ruff-clean, like the sync files.

## 11. Non-goals

- Everything the RFC assigns to Jobotron itself: the worker, the WebAPI, the state database, Redis idempotency, eligibility re-validation, Iris/CustomerService/VOperDB actions.
- CDF/change replication (the existing sync's job) — and no CDF read here ever, so the delta-rs CDF gap is irrelevant to this component.
- The Hangfire-vs-Jobotron debate (RFC comments [h]–[m]) — settled there; this doc only designs the data-platform producer.
- Producer-side tombstones / segment-exit messages (§8.2 unless a job type demands it).

## 12. The live demo (Phase 1, built)

The producer exists as one Dagster component and is driven by an animation page, exactly like the CDF sync's demo:

| | |
|---|---|
| Component | `orchestration/components/reverse_etl_jobotron.py` — `JobotronProducer`, YAML at `orchestration/defs/reverse_etl_jobotron/defs.yaml` |
| Assets | `orchestration/assets/reverse_etl_jobotron.py`: `reverse_etl_jobotron_setup` (tables + topics + seed), `reverse_etl_jobotron_publish` (the whole scan→join→route→produce path), `reverse_etl_jobotron_consume` (demo consumer: operator topics → per-operator Postgres tables), `reverse_etl_jobotron_make_change` (add/verify/deactivate ops) |
| Jobs | `reverse_etl_jobotron_setup_job`, `reverse_etl_jobotron_reset_job`, `reverse_etl_jobotron_make_change_job`; publish runs via `__ASSET_JOB` selecting `reverse_etl_jobotron_publish` |
| Page | `docs/poc/jobotron_data_flow.html` at <http://localhost:4002/jobotron> (same helper server; `bin/3_run_reverse_etl_demo.sh` sources `.env` for the topics check) |
| Demo tables | `de_dev.sr_poc_external.reverse_etl_jobotron_customers` + `reverse_etl_jobotron_kyc_cases` — plain Delta, no CDF, no state table |
| Topics / subjects | `jobotron_demo_gr` / `jobotron_demo_br`; Apicurio subjects `<topic>-key` / `<topic>-value` (identical schemas share ids across topics) |
| Reset chain | page's "Reset and set up" runs reset → setup → first publish → first consume (4 steps): after it, topics AND the per-operator Postgres tables hold exactly the fresh segment |

What one publish run does (verified 2026-10-18): UC location lookups (cached, control plane only), column-pruned delta-rs scans of both tables, the join in DuckDB, Avro key/value schemas derived from the result shape and ensured in Apicurio, one message per selected customer routed by `operator`, bounded flush — **~5s end to end, `0.0` warehouse seconds**, segment 4 = gr 2 (1001, 1002) + br 2 (1004, 1005). A change op (add/verify/deactivate) followed by a publish moves the segment with no tombstones — the customer simply stops (or starts) appearing, which is the full-snapshot semantics of §8.1 in action.

The **demo consumer** (`reverse_etl_jobotron_consume`, the page's step 4) closes the loop observably: it re-reads each operator topic from the beginning and rebuilds `jobotron_demo_<operator>` in the demo Postgres (the same database the Debezium sink writes) from **the messages of the latest publish run only** — topics append, so grouping by the last message's `run_id` is what makes the table the *current* segment; a customer that left it disappears at the next consume. The RFC's real worker would instead act per message and keep its own item state.

## 13. Live-demo script

**Pre-flight:** VPN + stack up + helper running; open `http://localhost:4002/jobotron`; click **Reset and set up** and wait for the four green runs (known state: 6 customers, segment 4 = gr 1001+1002 + br 1004+1005 — one excluded customer per operator: 1003 inactive, 1006 verified; topics and Postgres tables hold exactly that).

**Narrative:** (1) Frame — the RFC's upstream producer on Dagster, no Spark/JVM/DBUs, vs the RFC's ~$10/month/task con. (2) Step 1 — the two plain-Delta tables, no CDF, no state table; the CDF punchline (snapshots are why it is Spark-free today). (3) Step 2.1 — **Run producer**, narrate scan → DuckDB join (the SQL in the notes) → Avro/Apicurio → per-operator routing; on green, point at the **"Databricks cost: none"** chip; optionally 2.3 (schemas) and 3 (topics). (4) Step 4 — **Run demo consumer**, then **Show Postgres tables**: the segment mirrored per operator. (5) The money moment, twice: **add an unverified customer** → publish → consume → it appears in its operator's table only; **verify a customer** → publish → consume → it disappears, no tombstone. (6) Optional: open a run in Dagster (metadata + `0.0` warehouse seconds), close on §8's open questions.

**Craft:** one click at a time (each ≈ 25–40s — talk during the spinners); every step is idempotent, so re-click on any failure; never restart the helper mid-demo (a stale helper says "unknown action" — restart before the audience).
