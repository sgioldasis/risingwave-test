-- WHAT: a synchronous (rollup) MV over wallet_transactions_log: SUM /
--   count / latest-event per wallet type, maintained by the storage engine
--   itself.
-- FEATURE: the synchronous materialized view (rollup) -- an engine-maintained
--   single-table aggregate index with no REFRESH statement at all.
-- WHY: contrasts with every asynchronous MV in this project (which all
--   need an explicit REFRESH and drove the staleness-handling work): a
--   synchronous MV updates in lockstep with every write and has no REFRESH
--   statement at all -- zero-lag aggregation with zero refresh machinery.
-- WHERE: Wallet Upsert Demo dashboard; created by the
--   wallet_transactions_log_setup Dagster asset.

-- Synchronous (rollup) MV over wallet_transactions_log: maintained directly
-- by the storage engine on a single base table, updates in lockstep with
-- every write, and has NO REFRESH statement at all -- unlike every
-- asynchronous MV elsewhere in this project (mv_unified_funnel_summary,
-- mv_funnel_daily_country_rollup, mv_iceberg_countries_cache), which all
-- need an explicit REFRESH to pick up new data.
--
-- COUNT(*) is rejected by synchronous MV creation -- confirmed live ("The
-- materialized view currently does not support const expr in select
-- statement ... Please use Asynchronous Materialized View instead").
-- COUNT(transaction_id) (a real column) works fine.
-- Owned by orchestration/assets/wallet_sync_mv_setup.py.
CREATE MATERIALIZED VIEW {schema}.mv_wallet_type_rollup AS
SELECT type, SUM(amount) AS total_amount, COUNT(transaction_id) AS event_count, MAX(event_time) AS last_event_time
FROM {schema}.wallet_transactions_log
GROUP BY type
