# Ververica Proof-of-Concept Plan

A narrow, evidence-gathering POC to validate or challenge the recommendation in [RISINGWAVE_VS_VERVERICA_ANALYSIS.md](RISINGWAVE_VS_VERVERICA_ANALYSIS.md). The POC is scoped to produce a side-by-side comparison without disturbing the existing RisingWave-based pipelines.

Note: Ververica is commercial software. This POC assumes either an active Ververica Cloud subscription / trial, or a Ververica Platform license for self-hosted use. If neither is available, the equivalent POC can be run on bare Apache Flink, but the operational comparison will not reflect what production on Ververica actually looks like.

## Goal

Reproduce one representative derived dataset (`funnel_summary` in this repo) from the same Kafka topics using Ververica, publish results to a parallel output, and collect evidence on:

- correctness vs RisingWave
- end-to-end latency
- recovery behavior (savepoints, job restart from the Ververica UI)
- developer and operational effort, including how the missing live-SQL surface is replaced
- licensing and run-cost shape at expected production scale

## Scope

### In scope

- Consume `page_views`, `cart_events`, and `purchases` from Kafka.
- Compute 1-minute funnel summary windows (viewers, carters, purchasers, conversion rates) as a Flink SQL job deployed via Ververica.
- Emit results to one parallel artifact:
  - first choice: a parallel Kafka topic (e.g., `funnel_ververica`)
  - second choice: a parallel PostgreSQL table (e.g., `funnel_summary_ververica`)
- Stand up the serving-layer story explicitly — pick how live consumers would query the Ververica-produced data (Postgres, Iceberg, or KV) and operate it for the duration of the POC.
- Measure latency, recovery, and operational effort.
- Capture licensing / run-cost data for the expected production scale.

### Out of scope

- Replacing ML polling paths in production code.
- Replacing `funnel_training` or other derived datasets.
- Replacing RisingWave UDF workflows.
- Replacing dbt materializations.
- Removing RisingWave from the local stack.

### Why this scope

The scope isolates the comparison to the area where the platforms overlap most directly. It also forces an explicit answer to the question that Ververica raises and RisingWave does not: *where does the live serving layer live, and what does it cost to operate?*

## Implementation Steps

### Step 1. Stand up Ververica alongside the existing stack

Provision a Ververica Cloud workspace (or a local Ververica Platform deployment) that can consume the same Kafka topics as RisingWave. RisingWave continues to run untouched.

### Step 2. Define the Ververica deployment

Implement one Flink SQL job, deployed via Ververica, with:

- Kafka source ingestion
- event-time watermarks
- 1-minute tumbling-window funnel computation
- output to a separate sink

Prefer Flink SQL for the closest conceptual comparison with the current RisingWave SQL. Use DataStream / Table API only if the POC is also meant as a foundation for production code.

### Step 3. Decide and operate the serving layer

Because Ververica does not expose live derived state for query, pick one serving path and run it for the POC:

- write the funnel output to a parallel Postgres table consumed by ad-hoc SQL, or
- write to an Iceberg table read by analytics consumers, or
- write to Kafka and have consumers maintain their own materialized view.

Document the components introduced and who would own them in production.

### Step 4. Create parallel output artifacts

Do not overwrite existing outputs. Create one of:

- Kafka topic `funnel_ververica`
- PostgreSQL table `funnel_summary_ververica`

The dashboard and existing consumers continue to work unchanged. Comparison happens without cutover risk.

### Step 5. Add validation queries and parity checks

Compare RisingWave and Ververica output by window:

- row counts by `window_start`
- viewers, carters, purchasers deltas
- rate differences per window
- late-event behavior under the same replay

The point is measurable comparison data, not anecdotal impressions.

### Step 6. Measure latency, recovery, and operational ergonomics

Run the same producer workload through both systems and capture:

- time from event arrival to output availability
- stability under short bursts
- savepoint / restart behavior using the Ververica UI
- developer ergonomics for editing, redeploying, and observing a job

### Step 7. Capture cost data

Record:

- Ververica licensing / subscription cost at the configuration used
- compute cost on both sides for equivalent throughput
- estimated cost of operating the serving layer chosen in Step 3 in production

This is the part that is easy to skip and expensive to skip.

### Step 8. Review and decide

Pick one of three follow-up paths:

- stop and retain RisingWave only
- adopt a hybrid model where Ververica owns selected sink-oriented paths
- expand Ververica into more complex artifacts (Iceberg outputs, correction flows)

## Repo-Oriented Change Plan

### New files likely needed

- a Ververica deployment manifest or workspace configuration
- one Flink SQL job definition (and / or DataStream source if exercising that path)
- a short run script for local startup and submission
- configuration for the chosen serving layer (Postgres schema, Iceberg table, etc.)
- a validation document describing how to compare outputs

### Existing files likely to be touched

- `docker-compose.yml` only if running Ververica Platform locally
- `README.md` or a new doc under `docs/` for POC usage
- possibly `bin/` scripts for job submission and comparison

### Files that should remain unchanged

- current RisingWave dbt models
- current ML code paths
- current dashboard code
- current sink definitions

## Acceptance Criteria

The POC is successful if all of the following hold:

1. A Ververica deployment consumes the same Kafka topics successfully.
2. It produces a parallel artifact with the same core business metrics as `funnel_summary`.
3. Output can be compared side by side with RisingWave without disrupting the current demo.
4. The serving-layer choice (Step 3) has been operated long enough to estimate its production cost and operational burden.
5. Licensing and run-cost data have been captured at the expected production scale.
6. Stakeholders can review parity, latency, operational observations, and cost in a short report.
7. The team can clearly state whether Ververica should remain a narrow experiment, become a hybrid addition, or justify further adoption work.

## Decision Framework After the POC

- If Ververica shows no material latency, cost, or flexibility advantage *and* the serving layer cost is non-trivial, retain RisingWave.
- If Ververica matches correctness and offers better control for future stream jobs that will need DataStream-level features, keep it for selected sink-oriented paths.
- If Ververica proves significantly better for long-term architecture goals (portability across Flink runtimes, custom-operator workloads on the roadmap), plan a phased adoption rather than expanding ad hoc.
