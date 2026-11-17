-- WHAT: seed data for country_scores -- 30 hourly publications per country
--   (6 countries, deterministic sin-wave scores), walking back from now+1h.
-- FEATURE: TABLE(generate_series) -- StarRocks's built-in synthetic-row
--   generator, cross-joined against a real table for realistic shape.
-- WHY: the ASOF demo's page views are all "now"-ish, so the feed must
--   extend an hour past seed time for every event to find a match; the
--   WHERE guard makes re-running a no-op.
-- WHERE: executed by the starrocks_feature_demo_setup Dagster asset on
--   every run (seeds only while the table is empty).

-- Seeds country_scores: hourly publications per country; score =
-- 0.2 + 0.1 * sin(...) gives a deterministic wave. 6 countries (dim_users'
-- full set) x 30 hourly points ending at seed time; the page views it
-- matches are all "now"-ish, so the feed is seeded up to now+1h to keep
-- every event matched. dim_users' country mix: GR, RO, CY, PT, ES, IT
-- (from the % 10 CASE). ts walks back hourly from now+1h; per-country
-- phase offset via LENGTH. The WHERE guard makes re-running a no-op.
-- Owned by orchestration/assets/starrocks_feature_demo_setup.py.
INSERT INTO {schema}.country_scores
SELECT
    c.country,
    date_add(date_trunc('hour', now()), INTERVAL (t.generate_series - 30) HOUR) AS ts,
    round(0.2 + 0.1 * sin((t.generate_series + length(c.country)) / 6.0), 4) AS risk_score
FROM TABLE(generate_series(1, 31)) t
CROSS JOIN (SELECT DISTINCT country FROM {schema}.dim_users) c
WHERE (SELECT COUNT(*) FROM {schema}.country_scores) = 0
