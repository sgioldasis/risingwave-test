-- WHAT: the re-assertion of rg_superset_demo's properties on every run.
-- FEATURE: resource groups -- runtime property re-assert via ALTER (CREATE
--   IF NOT EXISTS doesn't update an existing group).
-- WHY: CREATE ... IF NOT EXISTS does not update an existing group; the
--   ALTER also arms the concurrency_limit=10 fail-fast cap (the
--   queueing-vs-admission truth is below).
-- WHERE: executed by the starrocks_feature_demo_setup Dagster asset on
--   every run, right after rg_superset_demo.sql.

-- CREATE ... IF NOT EXISTS does not update an existing group, so the asset
-- re-asserts the properties (incl. concurrency_limit) on every run via
-- ALTER. 10 is far above what a dashboard render issues (~5 charts), so
-- demos never trip it -- it arms fail-fast overload protection: with query
-- queue v2 (the 4.1 default) and enable_group_level_query_queue=false (the
-- default), the BE enforces the cap by FAILING the over-limit query with
-- "Exceed concurrency limit" rather than queueing it (true per-group
-- queueing is not implemented on 4.1.7 -- see the asset docstring and
-- StarRocks PR #78425).
-- Owned by orchestration/assets/starrocks_feature_demo_setup.py.
ALTER RESOURCE GROUP rg_superset_demo
WITH ('cpu_weight_percent'='80', 'mem_limit'='80%', 'concurrency_limit'='10')
