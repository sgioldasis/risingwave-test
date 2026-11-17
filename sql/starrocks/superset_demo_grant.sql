-- WHAT: the SELECT grant making superset_demo a pure reader.
-- FEATURE: GRANT-based access control (db-scoped RBAC, with the 4.1.7
--   view-grant caveat below).
-- WHY: pairs with superset_demo_user.sql; the 4.1.7 view-grant quirk below
--   is why all of the demo's datasets query base tables directly.
-- WHERE: executed by the starrocks_feature_demo_setup Dagster asset on
--   every run.

-- SELECT-only grant for the demo reader. NOTE (verified on 4.1.7): db-level
-- grants do NOT cover views (GRANT ... ON db.* passes tables only, and an
-- explicit view grant fails with "cannot find table") -- all superset_demo
-- datasets query base tables directly.
-- Owned by orchestration/assets/starrocks_feature_demo_setup.py.
GRANT SELECT ON {schema}.* TO 'superset_demo'@'%'
