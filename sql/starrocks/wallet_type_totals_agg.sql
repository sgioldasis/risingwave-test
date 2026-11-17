-- WHAT: the wallet AGGREGATE KEY table -- one merged row per wallet type.
-- FEATURE: the AGGREGATE KEY table model -- per-column aggregate functions
--   (SUM / REPLACE) merge rows at ingest, no MV and no query-time GROUP BY.
-- WHY: demonstrates the AGGREGATE KEY table model (the model the wallet demo
--   had never covered) as a third, structurally different answer to "total
--   amount and event count per type": query-time aggregation over
--   wallet_transactions, a query-time-rewritten rollup MV over
--   wallet_transactions_log, and now storage-level incremental aggregation
--   on ingest.
-- WHERE: Wallet Upsert Demo dashboard; created by the wallet_type_totals_agg
--   Dagster asset. Story: docs/SR_POC_WALLET_UPSERT_DEMO.md.

-- AGGREGATE KEY table: one row per wallet `type`, kept continuously summed
-- at the storage layer (no MV, no query-time GROUP BY). On a Primary Key
-- table a second row with the same key REPLACES the first (whole-row); here
-- a second row with the same key MERGES into it via each column's declared
-- aggregate function -- confirmed live on a throwaway table before building
-- this: two rows for the same key summed correctly (SUM columns) and
-- correctly kept only the latest value (the REPLACE column), with COUNT(*)
-- and a plain SELECT * already reflecting the merged, one-row-per-key state.
--
-- `event_count = 1` is a constant per-row contribution -- summed on merge,
-- giving a running count without a separate COUNT aggregate (StarRocks
-- Aggregate tables only support one declared aggregate function per
-- column, applied uniformly; a literal-1 SUM column is the standard way to
-- get a count). `last_event_time` uses REPLACE (keep the latest value on
-- merge) rather than SUM, deliberately mixing aggregate functions in one
-- table to show they're independent per column.
--
-- dbt cannot build this: the dbt-starrocks adapter's table_type only
-- accepts DUPLICATE/PRIMARY/UNIQUE, so AGGREGATE is reachable only via raw
-- DDL. Owned by orchestration/assets/wallet_agg_key_setup.py.
CREATE TABLE IF NOT EXISTS {schema}.wallet_type_totals_agg (
    type VARCHAR(16),
    total_amount DECIMAL(38,9) SUM,
    event_count BIGINT SUM,
    last_event_time DATETIME REPLACE
)
AGGREGATE KEY (type)
DISTRIBUTED BY HASH (type)
