---
title: OSS Flink vs Ververica for Hermes
description: Comparison of open source Flink and Ververica for the Hermes streaming architecture, including job topology, serving and storage layers, correction workflows, deployment, and operational tradeoffs.
author: Copilot
ms.date: 2026-04-27
ms.topic: concept
keywords:
  - flink
  - ververica
  - kafka
  - iceberg
  - postgres
  - streaming
estimated_reading_time: 10
---

## Purpose

This document compares open source Flink and Ververica specifically for the Hermes setup described in this repository.

Assumptions:

* Kafka topics remain the sources
* Iceberg remains the analytical system of record
* Postgres remains the fast serving layer
* Hermes still uses one realtime job and one bounded backfill job

## Executive Summary

For Hermes, the business architecture does not fundamentally change between open source Flink and Ververica.

The major difference is operational:

* open source Flink gives you the engine and full control
* Ververica gives you a managed control plane and a much smoother production operations story

If you are only asking whether Hermes should be designed differently, the answer is mostly no.
If you are asking whether Hermes becomes easier to run in production, the answer is yes.

## What Stays The Same

These parts of the Hermes design stay effectively the same on both platforms.

1. Job topology

* one continuous realtime job
* one bounded backfill and recompute job

2. Source model

* Kafka page_views
* Kafka cart_events
* Kafka purchases
* optional hermes_corrections topic

3. Storage model

* Iceberg as durable analytical history
* Postgres as fast serving layer
* Flink state backend for windows, joins, and corrections

4. Correction strategy

* small fixes as correction or changelog events
* full backfills as bounded replay and promotion

5. Core implementation choices

* Flink SQL remains good for relational Hermes logic
* Java remains the strongest choice for maximum control
* PyFlink remains viable for Python-heavy teams

## What Changes With Ververica

The architecture stays similar, but the runtime experience changes substantially.

## Deployment And Lifecycle

### Open source Flink

You manage:

* cluster provisioning
* job submission tooling
* savepoint workflows
* scaling and upgrades
* operational guardrails

### Ververica

You get a managed deployment control plane for:

* job deployment
* versioned rollouts
* savepoint-driven upgrades
* restart and lifecycle management
* operational visibility

For Hermes, this matters most for the long-running realtime job.

## Operations And Reliability

### Open source Flink

Open source Flink gives you full control, but you have to build more of the operating model yourself.

Typical responsibilities:

* checkpoint tuning
* autoscaling strategy
* alerting and monitoring integration
* rollout safety procedures
* multi-environment deployment conventions

### Ververica

Ververica reduces that burden.

Typical advantages:

* better job lifecycle controls
* better upgrade ergonomics
* stronger built-in observability
* easier environment separation
* easier production scaling workflows

For a state-heavy Hermes pipeline, those are meaningful advantages.

## Comparison For Hermes Concerns

| Concern | OSS Flink | Ververica |
|---|---|---|
| Core Hermes logic | Same | Same |
| Kafka sources | Same | Same |
| Iceberg sink | Same | Same |
| Postgres serving sink | Same | Same |
| Correction event handling | Same | Same |
| Backfill job design | Same | Same |
| Cluster operations | You own it | Mostly managed |
| Savepoint upgrades | You script and manage them | Better managed workflow |
| Scaling | You design and operate it | Easier managed experience |
| Monitoring | You assemble it | Better integrated experience |
| Governance and tenancy | You build conventions | Typically stronger platform support |

## Practical Impact On Hermes

## Realtime Job

For the Hermes realtime job, Ververica changes how you run it, not what it computes.

Still required:

* event-time watermarks
* multi-window state
* correction handling
* dual sinks to Iceberg and Postgres

Improved with Ververica:

* deployment safety
* savepoint upgrades
* observability
* resource tuning workflows

## Backfill Job

For the Hermes backfill job, Ververica again changes how you operate it more than how you implement it.

Still required:

* bounded input range
* recompute of affected windows
* dual shadow tables in Iceberg and Postgres
* validation before promotion

Improved with Ververica:

* cleaner job submission workflow
* better environment separation
* easier management of one-off or scheduled replay jobs

## Correction Workflows

Ververica does not remove the need to explicitly model corrections.

You still need two lanes.

### Small fixes

* emit correction events
* update state in realtime job
* write corrected rows to Iceberg and Postgres
* reconcile both stores

### Full backfill

* define affected interval
* run bounded replay job
* write to shadow tables
* validate parity
* promote both stores

That logic does not disappear just because the runtime is managed.

## Where Ververica Helps The Most

Ververica helps most when Hermes becomes expensive to operate.

That tends to happen when you have:

* large state from distinct counts and joins
* frequent upgrades
* multiple environments
* strict reliability requirements
* team fatigue around self-managing Flink lifecycle

## Where Ververica Does Not Change The Hard Parts

These are still your problem even on Ververica.

* designing correct event identity for updates and deletes
* keeping Iceberg and Postgres consistent enough for your needs
* defining promotion rules for backfills
* choosing SQL versus Java versus PyFlink
* validating parity against existing Hermes logic

## dbt, Dagster, And Ververica

The recommended tool split stays mostly the same.

### dbt

Use dbt for:

* modeling support
* SQL artifact management
* documentation of relational logic

Do not use dbt as the primary runtime lifecycle tool for Hermes Flink jobs.

### Dagster

Use Dagster for:

* orchestrating submissions
* triggering backfills
* running validation checks
* coordinating promotion of Iceberg and Postgres shadow outputs

### Ververica

Use Ververica for:

* job deployment
* lifecycle management
* savepoint upgrades
* monitoring and runtime operations

## Recommendation For Hermes

If Hermes is a serious production pipeline, Ververica is attractive primarily because it lowers operational friction.

Choose open source Flink if:

* you want full control
* you already have strong internal Flink operations capability
* you prefer self-managed infrastructure

Choose Ververica if:

* you want the same Hermes design with a better operational experience
* you expect frequent upgrades or multiple environments
* you want a more managed deployment and observability model

## Bottom Line

For Hermes, Ververica is not a redesign decision.
It is a runtime and operations decision.

Your jobs, sinks, correction model, and storage layers stay mostly the same.
What improves is the safety and manageability of running them at scale.
