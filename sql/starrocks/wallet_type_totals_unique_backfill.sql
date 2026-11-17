-- WHAT: immediate first-fill for wallet_type_totals_unique.
-- FEATURE: the UNIQUE KEY whole-row upsert -- re-inserting a key replaces
--   the row, making the backfill idempotent.
-- WHY: so the dashboard has data before the 1-minute task's first tick
--   (whole-row upsert = idempotent).
-- WHERE: executed by the starrocks_feature_demo_setup Dagster asset on
--   every run.

-- Immediate first-fill for wallet_type_totals_unique so the dashboard has
-- data before the 1-minute task's first tick (PK-style upsert = idempotent).
-- Owned by orchestration/assets/starrocks_feature_demo_setup.py.
INSERT INTO {schema}.wallet_type_totals_unique (type, total, updated_at)
SELECT type, SUM(amount), now()
FROM {schema}.wallet_transactions_direct_kafka
GROUP BY type
