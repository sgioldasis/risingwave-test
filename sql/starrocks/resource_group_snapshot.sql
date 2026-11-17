-- WHAT: the bridge table that makes resource groups chartable.
-- FEATURE: Primary Key upsert -- an idempotent snapshot table the dashboard
--   can chart (the resource-group feature itself has no information_schema
--   table on 4.1.7, which is why this bridge exists).
-- WHY: information_schema has no resource-group table on 4.1.7, so the
--   dashboard can't query SHOW RESOURCE GROUPS ALL -- the asset reads the
--   live SHOW output and upserts the ACTUAL weights here on every run, so
--   the chart always reflects real cluster config, never a hardcoded copy.
-- WHERE: charted on the Serving Features dashboard (isolation panel);
--   created by the starrocks_feature_demo_setup Dagster asset -- the
--   INSERT itself stays in the asset (values parsed from SHOW output at
--   runtime).

-- information_schema has NO resource-group table on 4.1.7 (verified
-- 2026-11-17), so the dashboard can't chart SHOW RESOURCE GROUPS ALL
-- directly. This snapshot table is the bridge: the asset reads the live
-- SHOW output and upserts the ACTUAL weights on every run (every demo
-- reset), so the chart always reflects real cluster config, not a
-- hardcoded copy. The INSERT itself stays in the asset (values parsed
-- from SHOW output at runtime). PK upsert on group_name keeps it
-- idempotent.
-- Owned by orchestration/assets/starrocks_feature_demo_setup.py.
CREATE TABLE IF NOT EXISTS {schema}.resource_group_snapshot (
    group_name VARCHAR(64) NOT NULL,
    cpu_weight INT,
    mem_limit VARCHAR(16),
    updated_at DATETIME
) PRIMARY KEY (group_name)
DISTRIBUTED BY HASH(group_name)
