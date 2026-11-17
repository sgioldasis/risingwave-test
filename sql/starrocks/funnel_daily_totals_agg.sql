-- WHAT: the funnel AGGREGATE KEY table -- one merged row per (day, country)
--   holding viewers/carters/purchasers sums.
-- FEATURE: the AGGREGATE KEY table model -- per-column aggregate functions
--   (SUM) merge rows at ingest, no MV and no query-time GROUP BY.
-- WHY: the AGGREGATE KEY model applied to the funnel use case, where the
--   counts are pure additive (no reversal semantics to fight, unlike the
--   wallet). Deliberately fed from the RAW event topics, not the
--   revision-emitting funnel topic (see the gotcha below).
-- WHERE: created by the funnel_daily_totals_agg Dagster asset. Story:
--   docs/SR_POC_FUNNEL_AGGREGATE_KEY_DEMO.md.

-- AGGREGATE KEY table, one row per (day, country), kept continuously summed
-- at the storage layer by three independent Routine Load jobs (see
-- funnel_daily_totals_agg_load.sql), each incrementing only its own counter
-- column into the SAME row -- confirmed live on a throwaway table that two
-- jobs on two different topics merge correctly into the same key's row
-- without clobbering each other's column.
--
-- NOT sourced from the `funnel` Kafka topic (the pre-aggregated
-- funnel_summary output) or the `funnel` materialized view -- confirmed live
-- 2026-09-13 that both continuously re-emit a GROWING snapshot for the same
-- still-open window (viewers climbing 1 -> 11 -> 21 -> ... within one
-- window_start, not a single final row), despite funnel_summary.sql's own
-- comment claiming "EMIT ON WINDOW CLOSE finalises rows" -- that comment
-- describes aspirational behavior, not what the SQL (which has no actual
-- EMIT ON WINDOW CLOSE clause) does. Naively summing those revisions would
-- massively over-count. Sourced from the three RAW event topics
-- (page_views, cart_events, purchases) instead -- genuinely append-only,
-- one message per real occurrence, no revision concept.
--
-- dbt-starrocks CANNOT build this: the adapter's table_type only accepts
-- DUPLICATE/PRIMARY/UNIQUE (AGGREGATE hits a compiler error), and there is
-- no dbt materialization for Routine Load at all. Owned by
-- orchestration/assets/funnel_agg_key_setup.py.
CREATE TABLE IF NOT EXISTS {schema}.funnel_daily_totals_agg (
    day DATE,
    country VARCHAR(2),
    viewers BIGINT SUM,
    carters BIGINT SUM,
    purchasers BIGINT SUM
)
AGGREGATE KEY (day, country)
DISTRIBUTED BY HASH (day, country)
