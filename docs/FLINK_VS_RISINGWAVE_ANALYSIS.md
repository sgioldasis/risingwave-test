# Flink vs RisingWave Analysis for This Project

## Executive Summary

**Recommendation: Build this production workload on RisingWave.**

Flink can implement the same responsibilities, but should not be chosen for this workload without a specific organizational reason.

**Use RisingWave because:**
- It provides queryable streaming results plus compute in one system, reducing operational complexity.
- It allows SQL-first ownership, which is typically faster to production for data and analytics teams.
- It minimizes platform engineering overhead for this workload shape (including automatic Iceberg compaction and maintenance via dedicated compactor nodes).
- It has no material latency, cost, or throughput disadvantage for the current requirements.

**Choose Flink only if** the organization has committed to stream processing as a core platform competency and expects significantly more complex stateful jobs beyond this workload.

**Do not choose Flink** for this workload to chase features you don't yet need or to optimize for a future that hasn't arrived.

## Current RisingWave Responsibilities in This Repo

Today, RisingWave is responsible for more than one thing.

### 1. Streaming Compute Layer

RisingWave consumes Kafka topics and computes derived datasets such as:

- `funnel`
- `funnel_training`
- `funnel_summary`
- `funnel_summary_with_country`
- `hermes_features`

These are implemented as views and materialized views in the dbt project.

### 2. Sink Fan-Out Layer

RisingWave also publishes derived outputs to external systems:

- Kafka for the dashboard push model
- Iceberg for durable analytical storage
- PostgreSQL for external serving access

### 3. Queryable Serving Surface

Some parts of the project do not just consume sink outputs. They query RisingWave directly.

Examples:

- online ML polling from `funnel_training`
- batch ML training data fetches from `funnel_training`
- operational SQL workflows that expect a PostgreSQL-compatible query endpoint on RisingWave

### 4. Stateful Correction / Mutation Demo

The repo also demonstrates mutable table flows where updates and deletes propagate through derived artifacts and then into Iceberg. This is a stronger requirement than simple append-only streaming.

## What Flink Could Do (But Shouldn't Be Chosen For)

Flink is technically capable of replacing RisingWave for:

| Task | Flink Capability | Why It Still Doesn't Justify the Switch |
|---|---|---|
| Windowed funnel aggregation from Kafka | Yes, cleanly | RisingWave already does this without added complexity. No performance or cost advantage. |
| Kafka sink for dashboard | Yes, viable | RisingWave already sends to Kafka reliably. The switch adds operational burden without user-visible benefit. |
| Iceberg sink for storage | Yes, but heavier | Flink adds checkpointing, savepoints, state recovery, and correction logic that RisingWave hides. Plus: RisingWave's dedicated compactor nodes handle Iceberg compaction automatically; Flink requires you to manage table maintenance (compaction, expiration, rewrites) manually via separate tooling. Increased operational cost for equivalent output. |
| PostgreSQL serving tables | Yes, via upsert | RisingWave already does this. No material advantage to Flink. |
| Mutable Hermes correction flows | Yes, but complex | Requires explicit changelog modeling and two separate jobs. Much more operational overhead than RisingWave's simpler update/delete model. Not worth the added complexity unless you need it. |

**Bottom line on replaceability:** Flink can do the work, but doing so would introduce significant complexity in development and operations without measurable benefit.

## What Flink Does Not Replace Cleanly

## A. Queryable SQL Materialized Views

Not as a drop-in replacement

RisingWave is acting as a live SQL database over streaming state. Flink is primarily a stream processing engine. While Flink can compute the same results, the consumer pattern changes.

Instead of querying a live materialized view in RisingWave, downstream services would usually need to read from:

- PostgreSQL
- Iceberg
- Kafka
- or another serving store maintained by Flink

That means any component that currently polls RisingWave directly would need to be redirected.

## B. Current dbt Execution Model

Not as a drop-in replacement

The current dbt project emits RisingWave-specific DDL such as:

- `CREATE SOURCE`
- `CREATE MATERIALIZED VIEW`
- `CREATE SINK`

Flink would require its own job definitions, packaging, deployment, and lifecycle management. The repo could still keep dbt for downstream SQL assets, but dbt would no longer be the primary way to define and run the stream-processing layer.

## C. RisingWave-Specific UDF and Operational Features

Not reusable as-is

The current project includes RisingWave-specific UDF setup and operational patterns. Flink supports UDFs and custom logic, but not by reusing those assets directly.

Similarly, some convenience currently provided by RisingWave-managed behavior would become explicit platform and job management work in a Flink-based design.

## Where Flink Would Add Burden (Don't Go There)

| Concern | RisingWave Behavior | Flink Behavior | Executive Impact |
|---|---|---|---|
| ML services read live derived state | Simple SQL poll, no extra ops | Requires repointing to Postgres/Iceberg, adds coordination | Your ML team loses direct access and becomes dependent on another component |
| Correction and replay | Handled by updates/deletes on dbt models | Requires two jobs (realtime + backfill) and changelog modeling | More complex operations for equivalent functionality |
| Developer workflow | SQL-first, familiar to analytics engineers | Job lifecycle + deployment + state management | Higher barrier to entry for your data engineering team |
| Query serving for dashboards | Built-in, no extra layer | Separate component (Postgres table or Iceberg sink) | Additional operational surface that must stay in sync |
| Job lifecycle in production | RisingWave-managed | Your team must manage checkpoints, savepoints, upgrades | New operational toil for equivalent results |

## Production Decision Shapes

## Option 1. Hybrid Architecture

Recommended evaluation path, not default production target

Use Flink only for selected derived artifacts while keeping RisingWave in place for the rest.

Good candidates:

- compute `funnel_summary` in Flink
- publish to Kafka for the dashboard
- optionally write to PostgreSQL or Iceberg from the same Flink job

Benefits:

- low disruption
- direct side-by-side comparison
- easy rollback
- evidence for a production platform decision

## Option 2. Partial Replacement of Sink-Oriented Paths

Use Flink for paths where downstream systems only need the produced artifact and do not need to query RisingWave itself.

Strong candidates:

- Kafka sink for dashboard
- PostgreSQL serving table
- Iceberg feature table

This preserves RisingWave for query-driven and ML-driven workflows while evaluating Flink where it naturally fits.

## Option 3. Full Replacement

Feasible, but only with redesign and a clear production reason

A full replacement would require:

- introducing Flink jobs as the primary compute layer
- choosing one or more serving stores for downstream consumers
- repointing ML code away from direct RisingWave queries
- redesigning correction and backfill procedures
- replacing the current dbt-driven stream object lifecycle with Flink deployment workflows

This is possible, but it should be treated as a production architecture program rather than a simple technology swap.

## Recommendation

## The Real Decision

This is not "Can Flink do it?" (Yes, it can.) 

This is "Should we build on a platform that optimizes for a future scenario that has not materialized?"

**Answer: No.**

Choose RisingWave unless:
- Your roadmap explicitly commits to stream processing as a core platform competency, *and*
- You have concrete plans for 5+ complex stateful stream jobs beyond this workload, *and*
- You can justify the added operational complexity upfront

If you lack all three of those, RisingWave is the right production choice.

## Recommended First Proof of Concept

Build one Flink job that:

- reads the same Kafka topics: `page_views`, `cart_events`, `purchases`
- computes the equivalent of `funnel_summary`
- writes to either:
  - a parallel Kafka topic for dashboard testing, or
  - a PostgreSQL table for side-by-side validation

Why this path:

- it exercises the core streaming overlap
- it avoids the hardest migration surfaces first
- it creates measurable production evidence for stakeholders
- it does not require immediate changes to the ML path

## Evaluation Criteria for the Proof of Concept

Stakeholders should compare these dimensions:

### 1. Correctness

- do Flink and RisingWave produce the same counts and rates per window?
- how are late events handled?
- how are corrections handled?

### 2. Latency

- end-to-end time from Kafka event arrival to derived artifact availability
- consistency of latency under load

### 3. Operational Complexity

- deployment effort
- local developer setup effort
- debugging experience
- recovery and replay procedures
- schema evolution effort

### 4. Architectural Fit

- does the team need a live SQL query surface?
- are most consumers better served by Kafka, Postgres, or Iceberg outputs instead?
- does the team prefer SQL-centric ownership or job-centric ownership?

## Executive Comparison: Cost, Operations, and Team Fit

This section is intended for decision-making discussions where technical feasibility alone is not enough.

## One-Slide Decision Table

| Choose RisingWave | Choose Flink |
|---|---|
| Your workload needs queryable live results + compute together | You are committing to stream processing as a platform capability with 5+ planned stateful stream jobs |
| Your team is SQL-first and analytics-focused | You have high confidence in your team's platform engineering capabilities |
| You want to minimize operational overhead | You value explicit control over replay and job behavior |
| Low-latency serving + SQL access is a core requirement | Stream processing is a strategic competency your org wants to own and operate |
| **Default choice for this workload** | **Choose only with specific platform commitment** |

## Slide-Level Recommendation

**"Build on RisingWave. Don't choose Flink unless you have a specific platform reason and 5+ planned stream jobs to justify the added complexity."**

| Dimension | RisingWave as a production choice | Flink as a production choice | Stakeholder takeaway |
|---|---|---|---|
| Initial implementation cost | Lower | Medium to High | RisingWave is usually faster to production for this workload shape |
| Implementation complexity | Low | Medium to High | Flink should only be chosen if platform engineering is justified |
| Ongoing platform cost | Moderate | Moderate to High | Flink often shifts more responsibility into platform engineering |
| Operational burden | Lower | Higher | RisingWave reduces the amount of stream-job lifecycle the team must own |
| Developer workflow | Simpler | Heavier | Flink usually adds packaging, deployment, state, and recovery concerns |
| SQL productivity | Strong | Good in Flink SQL, weaker end-to-end | RisingWave is the better fit for SQL-first production ownership |
| Custom streaming control | Moderate | Strong | Flink is the better fit when control is more important than simplicity |
| Serving live query workloads | Strong | Indirect | RisingWave is stronger when query serving is a first-class production need |
| Sink fan-out to Kafka, Iceberg, Postgres | Strong | Strong | Both are viable if sink production is the main requirement |
| Replay and correction flexibility | Good | Strong, but more manual | Flink offers more control, but the team must operate that control |
| Team fit for Python/SQL-heavy organizations | Strong | Medium | RisingWave is often the safer production default for these teams |
| Team fit for dedicated stream-platform teams | Medium | Strong | Flink is a stronger fit when stream processing is a core platform competency |

## The Risk of Choosing Flink

| Risk | Impact |
|---|---|
| **Implementation complexity** | Your team must build and operate checkpoint management, job lifecycle, state recovery, and deployment infrastructure. RisingWave provides this automatically. |
| **Iceberg maintenance burden** | RisingWave includes a dedicated compactor node that automatically handles Iceberg compaction, expiration, and file rewrites. With Flink, you must implement and operate this separately using Iceberg tools, Spark, or custom jobs. New operational surface for table health. |
| **Serving layer coordination** | ML and analytics services that need to poll for live state must be redirected to Postgres, Iceberg, or Kafka. Architectural coordination cost and additional components to maintain. |
| **Job lifecycle operations** | Converting dbt models into Flink job definitions, deployments, and operations adds weeks of upfront engineering. Expect ongoing platform work for job management and updates. |
| **Unproven benefit** | You gain no measurable latency, cost, or reliability improvement for this workload. You're paying the cost to prepare for a future that may never arrive. |
| **Organizational friction** | Your SQL-first team is now expected to operate Java/Python stream jobs and manage platform concerns they didn't sign up for. |

**Cumulative risk: High. Benefit: Zero.**

## Follow-Up Plan: Flink Proof of Concept

The recommended proof of concept is intentionally narrow.

Goal:

- reproduce `funnel_summary` from the same Kafka topics using Flink
- publish results to a parallel output so RisingWave remains the baseline
- collect production-oriented evidence on correctness, latency, recovery, and operating burden before broader changes

## Proposed Scope

### In scope

- consume `page_views`, `cart_events`, and `purchases`
- compute 1-minute funnel summary windows
- calculate viewers, carters, purchasers, and conversion rates
- emit results to one parallel artifact boundary

Recommended output target:

- first choice: parallel Kafka topic such as `funnel_flink`
- second choice: parallel PostgreSQL table such as `funnel_summary_with_country_flink`

### Out of scope for the proof of concept

- replacing ML polling paths
- replacing `funnel_training`
- replacing RisingWave UDF workflows
- replacing update/delete Hermes flows
- replacing dbt materializations
- removing RisingWave from docker-compose

## Why This Scope

This scope isolates the comparison to the part of the system where Flink and RisingWave overlap most directly.

It avoids early entanglement with:

- direct query consumers
- correction-heavy workflows
- Iceberg change semantics
- broader migration discussions before evidence exists

## Proposed Implementation Steps

### Step 1. Add Flink runtime alongside the existing stack

Add Flink JobManager and TaskManager services to the local environment without removing RisingWave.

Outcome:

- both systems can run against the same Kafka topics
- side-by-side output validation becomes possible

### Step 2. Define the Flink job boundary

Implement one job with this responsibility:

- source ingestion from Kafka
- event-time watermarks
- 1-minute tumbling-window funnel computation
- output to a separate sink

Preferred implementation style:

- Flink SQL if the goal is closest conceptual comparison with current RisingWave SQL
- Java or Table API only if the team wants the proof of concept to double as a production foundation

### Step 3. Create parallel output artifacts

Do not overwrite existing outputs.

Instead create one of:

- Kafka topic `funnel_flink`
- PostgreSQL table `funnel_summary_flink`

Outcome:

- the dashboard and current consumers continue to work unchanged
- comparison can happen without cutover risk

### Step 4. Add validation queries and parity checks

Create simple validation logic that compares RisingWave output and Flink output by window.

Examples:

- row counts by `window_start`
- viewers, carters, purchasers deltas
- rate differences per window
- late-event behavior during the same replay window

Outcome:

- stakeholders get measurable comparison data instead of anecdotal impressions

### Step 5. Measure latency and recovery behavior

Run the same producer workload through both systems and capture:

- time from event arrival to output publication
- stability under short bursts
- restart and recovery behavior after a job interruption

Outcome:

- operational differences become visible early

### Step 6. Review and decide

After the proof of concept, choose one of three paths:

- stop and retain RisingWave only
- keep a hybrid model for sink-oriented paths
- expand Flink into more complex artifacts such as Iceberg outputs or Hermes correction flows

## Repo-Oriented Change Plan

If the proof of concept is implemented in this workspace, the likely change areas are:

### New files likely needed

- a Flink-focused docker-compose addition or service block
- one Flink job definition directory
- one or more SQL files or job source files for the Flink pipeline
- one short run script for local startup and submission
- one validation document describing how to compare Flink and RisingWave outputs

### Existing files likely to be touched

- `docker-compose.yml` to add Flink services
- `README.md` or a new doc under `docs/` for proof-of-concept usage
- possibly `bin/` scripts for job submission and comparison commands

### Files that should remain unchanged in the proof of concept

- current RisingWave dbt models
- current ML code paths
- current dashboard code
- current sink definitions used in the baseline demo

## Acceptance Criteria for the Proof of Concept

The proof of concept should be considered successful if all of the following are true:

1. Flink consumes the same Kafka topics successfully in the local stack.
2. Flink produces a parallel artifact with the same core business metrics as `funnel_summary`.
3. The output can be compared side by side with RisingWave without disrupting the current demo.
4. Stakeholders can review parity, latency, and operational observations in a short report.
5. The team can clearly state whether Flink should remain a narrow experiment, become a hybrid addition, or justify further migration work.

## Suggested Decision Framework After the Proof of Concept

Use these outcomes to guide the next decision:

- If Flink shows no material latency, cost, or flexibility advantage, retain RisingWave as the primary solution.
- If Flink matches correctness and offers better control for future stream jobs, keep it for selected sink-oriented paths.
- If Flink proves significantly better for the team's long-term architecture goals, define a second-phase migration plan rather than expanding ad hoc.

## Bottom Line

**Build this production workload on RisingWave.**

It is the simpler, faster, lower-risk choice. It has no material disadvantage for your current requirements. It provides queryable results that your ML and analytics consumers expect.

Flink is justified only if your organization has made a deliberate strategic decision to own stream processing as a core platform, with concrete plans for 5+ additional complex stream jobs beyond this one.

Do not choose Flink to optimize for a hypothetical future. Do not pay the complexity cost to prepare for a scenario that hasn't arrived.

If you are still uncertain, the proof-of-concept path is in the appendix. But the recommendation stands: **RisingWave is the right production platform choice.**