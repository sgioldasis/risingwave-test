---
title: StarRocks AGGREGATE KEY Table Demo (Funnel)
description: Applying the AGGREGATE KEY table model to funnel viewers/carters/purchasers, after an earlier attempt on the wallet demo produced numbers that could never match
---

<!-- markdownlint-disable-file -->

## Why this exists

Suggestion #5 from the StarRocks-skill-driven review of this project
(see [SR_POC_WALLET_UPSERT_DEMO.md](SR_POC_WALLET_UPSERT_DEMO.md)'s
add-on comparisons): `AGGREGATE KEY` and `UNIQUE KEY` table models were
never demonstrated anywhere in this project. A first attempt built
`wallet_type_totals_agg` on the wallet demo — a genuine, working
`AGGREGATE KEY` table, but its numbers could **never** match the existing
Primary-Key-table bar charts: a reversed `bet` still counts toward `bet`'s
gross total in a `SUM`-by-`type` table (the reversal event carries
`type: 'reversal'`, a different key, so there's no way to net it back out
at the storage-aggregation layer), while the PK table's upsert *does* net
it out (the reversal overwrites the whole row, moving it out of `bet`
entirely). Two structurally different computations — real, useful to show
the difference, but not usable as "here's an alternate implementation of
the same chart."

**Funnel `viewers`/`carters`/`purchasers` are a better fit**: pure
additive counts, no reversal/negation concept at all. This doc covers
building `AGGREGATE KEY` correctly the second time, on that data instead.

## What didn't work first: the `funnel` Kafka topic and the `funnel`/`funnel_summary` MVs

The obvious-looking shortcut was to source from the already-existing
`funnel` Kafka topic (`sink_funnel_to_kafka` publishing `funnel_summary`'s
output — one row per 1-minute window with `viewers`/`carters`/`purchasers`
already computed) rather than raw events. **Confirmed live it doesn't
work**: consuming the topic directly showed the same `window_start`
appearing repeatedly with a *growing* count each time —

```text
viewers=1   (window_start=04:53:00)
viewers=11  (window_start=04:53:00, same window)
viewers=21  (window_start=04:53:00, same window)
...
viewers=56  (window_start=04:53:00, same window)
viewers=5   (window_start=04:54:00, NEW window)
```

— not one final, immutable row per window. `funnel_summary.sql`'s own
comment claims `"EMIT ON WINDOW CLOSE finalises rows once watermarks pass
window_end so downstream sinks see immutable rows"` — that's aspirational,
not what the SQL actually does: neither `funnel.sql` nor
`funnel_summary.sql` has an actual `EMIT ON WINDOW CLOSE` clause in the
materialized view definition. Both continuously push incremental updates
for a still-open window as new events arrive, and the sink's
`force_append_only = 'true'` converts each of those updates into a plain
`INSERT` rather than suppressing/upserting them. Naively `SUM`-ing these
into an `AGGREGATE KEY` table would massively over-count (each revision
adds on top of the previous one, rather than replacing it).

## What worked: the raw event topics

`page_views`, `cart_events`, `purchases` are genuinely append-only — one
Kafka message per real occurrence (a single pageview, a single cart add, a
single purchase), confirmed via `src_page.sql`/equivalent source
definitions and by consuming the topics directly. No revision/update
semantics to fight at all.

## Built

- **`funnel_daily_totals_agg`** — `AGGREGATE KEY (day, country)` table,
  `SUM` columns for `viewers`/`carters`/`purchasers`:
  ```sql
  CREATE TABLE sr_local_db_sr_local_db.funnel_daily_totals_agg (
      day DATE,
      country VARCHAR(2),
      viewers BIGINT SUM,
      carters BIGINT SUM,
      purchasers BIGINT SUM
  )
  AGGREGATE KEY (day, country)
  DISTRIBUTED BY HASH (day, country)
  ```
- **Three independent Routine Load jobs**, one per raw topic
  (`funnel_viewers_agg_load` on `page_views`, `funnel_carters_agg_load` on
  `cart_events`, `funnel_purchasers_agg_load` on `purchases`), each
  incrementing only its own counter column into the *same* table row —
  every job explicitly sets all three counter columns (`1` for the one it
  owns, `0` literal for the other two) rather than relying on a default
  value for omitted columns:
  ```sql
  CREATE ROUTINE LOAD sr_local_db_sr_local_db.funnel_viewers_agg_load ON funnel_daily_totals_agg
  COLUMNS(event_time_raw, day = str_to_date(left(event_time_raw, 10), '%Y-%m-%d'),
          country = 'GR', viewers = 1, carters = 0, purchasers = 0)
  PROPERTIES ("format" = "json", "jsonpaths" = "[\"$.event_time\"]", ...)
  FROM KAFKA ("kafka_topic" = "page_views", ...)
  ```
  `day` is derived with `LEFT(event_time_raw, 10)` (the date portion is
  always the first 10 characters of an ISO 8601 string regardless of what
  follows) then `STR_TO_DATE`.
- `orchestration/assets/funnel_agg_key_setup.py` — idempotent Dagster
  asset creating the table and all three load jobs, wired into
  `modern_dashboard_setup_job`.

**Verified live before building anything** (per this project's
established practice): created a throwaway `AGGREGATE KEY` table plus two
Routine Load jobs on two separate test topics, each incrementing a
different `SUM` column — confirmed the merge correctly combined both
jobs' contributions into the same key's row without either clobbering the
other's column.

**Confirmed after building**: with the funnel producer running, queried
the table twice a few seconds apart —
```text
viewers=4544, carters=1333, purchasers=464
viewers=4644, carters=1367, purchasers=473   (10s later)
```
— growing correctly and continuously, no `REFRESH`, no MV, no query-time
`GROUP BY` needed at all: `SELECT *` already shows the merged running
total.

## `dbt-starrocks` cannot build any of this

Checked the actual installed adapter source (not assumed) before
concluding this:

- `table_type` in `starrocks__olap_table`
  (`dbt/include/starrocks/macros/adapters/relation_helpers.sql`) only
  accepts `DUPLICATE`, `PRIMARY`, and `UNIQUE` — anything else hits an
  explicit `raise_compiler_error`. `AGGREGATE` isn't reachable through
  dbt's table materialization at all, and couldn't be expressed as a
  plain CTAS anyway — `AGGREGATE KEY` tables need each column
  individually annotated with its aggregate function (`SUM`, `REPLACE`,
  ...) directly in the `CREATE TABLE` DDL, which a SELECT-driven table
  materialization has no schema/config surface for.
- There is no dbt materialization for Routine Load anywhere in the
  adapter (scanned every macro file: table/view/materialized_view/
  snapshot/seed — that's the complete list).

So this is a plain Dagster/SQLAlchemy asset, same pattern as every other
Routine-Load-based table built for the wallet demo
(`wallet_direct_kafka_setup.py`, `wallet_sync_mv_setup.py`,
`wallet_agg_key_setup.py`) — not a stylistic choice, a hard adapter
limitation.

## Real issue found (unrelated to this table, surfaced while testing it)

Launching `modern_dashboard_setup_job` while the wallet producer/Routine
Load jobs were actively writing failed on an unrelated step —
`starrocks_unified_dbt_assets` (the full `dbt_starrocks` project rebuild)
errored trying to drop `wallet_transactions`:

```text
1064 (HY000): Failed to drop table ...wallet_transactions. msg: There are
still some transactions in the COMMITTED state waiting to be completed.
```

`wallet_transactions` is dbt-managed with `materialized='table'`, which
drops and recreates it on every run (see
[SR_POC_WALLET_UPSERT_DEMO.md](SR_POC_WALLET_UPSERT_DEMO.md#rebuilding-the-job-wipes-the-starrocks-table-expected-not-a-bug))
— StarRocks refused the drop because active Routine Load/sink writers had
in-flight transactions against it at that exact moment. **Not
destructive**: the table survived completely unchanged (the drop failed
before touching anything, confirmed by re-querying it immediately after —
row count intact). `funnel_daily_totals_agg` itself built and ran
successfully in the same job run; only the unrelated
`starrocks_unified_dbt_assets` step failed. Just needs a retry once write
traffic settles, or run the wallet pipeline's producer stopped while
rebuilding — not something to fix in code.

## Superset

Added a table chart ("Daily Totals — AGGREGATE KEY Table [storage-level
SUM, no MV needed]") over `funnel_daily_totals_agg` to the **Funnel
Dashboard** (not the Wallet Upsert Demo — this is a funnel-side add-on),
plus a markdown explaining the mechanism and why the `funnel` topic/MVs
were ruled out.
