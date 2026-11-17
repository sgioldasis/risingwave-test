-- WHAT: the demo read-only user for the Serving Features dashboard.
-- FEATURE: StarRocks native user management (CREATE USER).
-- WHY: every chart on that dashboard connects as superset_demo -- an empty
--   password + SELECT-only keeps the demo credential's blast radius at
--   zero, and the user is the classifier key that pins its queries to
--   rg_superset_demo (see rg_superset_demo.sql).
-- WHERE: created by the starrocks_feature_demo_setup Dagster asset.

-- Demo reader for the Serving Features dashboard: SELECT-only, no password
-- -- the blast radius of the demo credential is zero.
-- Owned by orchestration/assets/starrocks_feature_demo_setup.py.
CREATE USER IF NOT EXISTS 'superset_demo'@'%' IDENTIFIED BY ''
