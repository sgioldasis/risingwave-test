-- WHAT: first-run seed for funnel_window_stats.
-- FEATURE: Primary Key upsert -- re-inserting an existing window_start
--   replaces its row, making the seed idempotent.
-- WHY: so the dashboard has data immediately, before the 1-minute refresh
--   task's first tick (idempotent via the NOT IN anti-join).
-- WHERE: executed by the starrocks_feature_demo_setup Dagster asset on
--   every run.

-- Seeds funnel_window_stats on the FIRST run so the dashboard has data
-- before the 1-minute refresh task's first tick (PK upsert = idempotent).
-- Owned by orchestration/assets/starrocks_feature_demo_setup.py.
INSERT INTO {schema}.funnel_window_stats (window_start, viewers, carters, purchasers)
SELECT window_start, viewers, carters, purchasers
FROM {schema}.funnel_summary_direct
WHERE window_start NOT IN (SELECT window_start FROM {schema}.funnel_window_stats)
