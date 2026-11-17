-- WHAT: the UNIQUE KEY table-model demo -- per-type wallet totals.
-- FEATURE: the UNIQUE KEY table model -- row-level upsert with whole-row
--   REPLACE (no partial-column update).
-- WHY: the one table model the wallet trio didn't show. UNIQUE KEY has the
--   same row-level upsert semantics as PRIMARY KEY but as whole-row REPLACE
--   (no partial-column update), refreshed by the same 1-minute
--   INSERT-SELECT pattern as the AGGREGATE KEY twin -- AGGREGATE sums
--   incrementally at ingest, UNIQUE re-upserts whole rows at refresh time.
-- WHERE: StarRocks Serving Features dashboard; created by the
--   starrocks_feature_demo_setup Dagster asset.

-- The UNIQUE KEY table model -- the one model the Wallet dashboard's
-- DUPLICATE / AGGREGATE / PRIMARY KEY trio doesn't show. UNIQUE KEY gives
-- the same row-level upsert semantics as PRIMARY KEY but as whole-row
-- REPLACE: an INSERT re-writing a key replaces the entire row (no
-- partial-column update). Stores per-type wallet totals, refreshed by the
-- same 1-minute INSERT-SELECT pattern as the AGGREGATE KEY twin
-- (wallet_type_totals_agg) -- AGGREGATE maintains SUM incrementally at
-- ingest, UNIQUE does full-row upsert per key at refresh time.
-- Owned by orchestration/assets/starrocks_feature_demo_setup.py.
CREATE TABLE IF NOT EXISTS {schema}.wallet_type_totals_unique (
    type VARCHAR(32) NOT NULL,
    total DECIMAL(38,9),
    updated_at DATETIME
) UNIQUE KEY (type)
DISTRIBUTED BY HASH(type)
