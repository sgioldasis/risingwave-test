-- WHAT: the rg_superset_demo resource group + its user classifier.
-- FEATURE: resource groups + the query classifier -- workload isolation by
--   CPU weight, with queries pinned to a group by their connecting user.
-- WHY: the workload-isolation story of the Serving Features dashboard --
--   BI SELECTs get their own CPU slice (weight 80) next to default_wg's 100
--   and default_mv_wg's 1, so MV-refresh traffic can't starve the dashboard
--   (and vice versa).
-- WHERE: created by the starrocks_feature_demo_setup Dagster asset;
--   charted on dashboard 4 via resource_group_snapshot.

-- Workload-group isolation for the Serving Features dashboard: the
-- classifier TO (USER='superset_demo') pins the dashboard's SELECTs to
-- rg_superset_demo (cpu_weight 80 vs default_wg's 100 and default_mv_wg's
-- 1), so BI traffic gets its own CPU share no matter what the MV-refresh
-- tasks are doing.
--
-- Scope note (verified on shared-data 4.1.7): per-resource-group query
-- QUEUEING is not available here -- the v1 queue properties
-- (max_concurrency / max_queue_size / queue_timeout) no longer exist, and
-- under query queue v2 (the 4.1 default) group-level queuing via
-- concurrency_limit is NOT implemented (StarRocks PR #78425, unmerged).
-- See orchestration/assets/starrocks_feature_demo_setup.py and
-- rg_superset_demo_alter.sql for what concurrency_limit DOES do.
-- Owned by orchestration/assets/starrocks_feature_demo_setup.py.
CREATE RESOURCE GROUP IF NOT EXISTS rg_superset_demo
TO (USER='superset_demo')
WITH ('cpu_weight_percent'='80', 'mem_limit'='80%')
