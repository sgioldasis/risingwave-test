-- WHAT: the 1-minute StarRocks scheduled task refreshing funnel_window_stats
--   from funnel_summary_direct.
-- FEATURE: SUBMIT TASK -- an FE-side scheduled job (INSERT body only on
--   4.1.7, 1-minute cadence here).
-- WHY: computed columns don't maintain themselves -- the base columns must
--   be re-inserted per window, and the PK upsert on window_start makes each
--   tick idempotent. The task lives in the FE, so the table keeps updating
--   even with Dagster down (same pattern as funnel_historical_watermark).
-- WHERE: created by the starrocks_feature_demo_setup Dagster asset.

-- 1-minute refresh task for funnel_window_stats (computed columns):
-- SUBMIT TASK accepts an INSERT body only (anything else is rejected on
-- 4.1.7 -- same constraint as funnel_historical_watermark). The PK upsert
-- on window_start makes the every-window re-insert idempotent. The asset
-- DROPs and re-SUBMITs this task on every run so the body always matches
-- this file.
-- Owned by orchestration/assets/starrocks_feature_demo_setup.py.
SUBMIT TASK {schema}.funnel_window_stats_refresh
SCHEDULE EVERY (INTERVAL 1 MINUTE)
AS INSERT INTO {schema}.funnel_window_stats (window_start, viewers, carters, purchasers)
SELECT window_start, viewers, carters, purchasers
FROM {schema}.funnel_summary_direct
