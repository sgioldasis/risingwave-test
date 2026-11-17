-- WHAT: the 1-minute StarRocks task refreshing wallet_type_totals_unique
--   from wallet_transactions_direct_kafka.
-- FEATURE: SUBMIT TASK -- an FE-side scheduled job (INSERT body only on
--   4.1.7, 1-minute cadence here).
-- WHY: UNIQUE KEY upserts whole rows at refresh time (unlike AGGREGATE's
--   ingest-time merge), so it needs this periodic INSERT-SELECT; the task
--   lives in the FE and keeps running with Dagster down.
-- WHERE: created by the starrocks_feature_demo_setup Dagster asset.

-- 1-minute refresh task for wallet_type_totals_unique (INSERT body only --
-- SUBMIT TASK rejects anything else on 4.1.7). The asset DROPs and
-- re-SUBMITs this task on every run.
-- Owned by orchestration/assets/starrocks_feature_demo_setup.py.
SUBMIT TASK {schema}.wallet_type_totals_unique_refresh
SCHEDULE EVERY (INTERVAL 1 MINUTE)
AS INSERT INTO {schema}.wallet_type_totals_unique (type, total, updated_at)
SELECT type, SUM(amount), now()
FROM {schema}.wallet_transactions_direct_kafka
GROUP BY type
