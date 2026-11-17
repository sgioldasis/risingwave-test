-- WHAT: the per-country risk-score publication feed (country, ts,
--   risk_score) used as the right side of the ASOF JOIN demo.
-- FEATURE: ASOF JOIN -- matching each row to the latest prior row of another
--   table (which requires a real Primary Key table on the right side).
-- WHY: ASOF JOIN needs a real table on both sides (a subquery-derived
--   right side is rejected as a "correlated condition") -- this PRIMARY KEY
--   table is that real side: each page view gets matched to the most recent
--   score published for its country at or before the event.
-- WHERE: StarRocks Serving Features dashboard (ASOF JOIN chart); created
--   by the starrocks_feature_demo_setup Dagster asset.

-- Per-country risk-score publication feed for the ASOF JOIN demo: each
-- page view is matched to the most recent score published for its country
-- at or before the event. ASOF needs a REAL-column equi predicate alongside
-- the temporal >= (confirmed live: equi on a constant/aliased column
-- through a subquery -- or any subquery-derived right side -- is rejected
-- as a "correlated condition"; both sides must be real tables).
-- Owned by orchestration/assets/starrocks_feature_demo_setup.py.
CREATE TABLE IF NOT EXISTS {schema}.country_scores (
    country VARCHAR(32) NOT NULL,
    ts DATETIME NOT NULL,
    risk_score DOUBLE NOT NULL
) PRIMARY KEY (country, ts)
DISTRIBUTED BY HASH(country)
