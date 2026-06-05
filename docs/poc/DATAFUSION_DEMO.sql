-- DataFusion batch analytics demo
-- Run these queries from psql or any RisingWave client:
--   psql -h localhost -p 4566 -U root -d dev
--
-- RisingWave 2.8 routes SELECT queries on Iceberg sources through Apache DataFusion
-- (vectorized, Arrow-native columnar engine) automatically — no special syntax needed.
-- This means you get OLAP performance on your streaming Iceberg data from the same
-- connection used for streaming SQL, without Trino or Spark.
--
-- Prerequisites: run casino_prd_full_job first so the Iceberg sources exist.

-- ─────────────────────────────────────────────────────────
-- Q1  Top 10 customers by real bet amount (ranking)
-- ─────────────────────────────────────────────────────────
SELECT customer_id, currency_id,
       SUM(rolling_1d_real_bet_amount)::numeric AS total_bet
FROM src_iceberg_casino_real_bet
GROUP BY customer_id, currency_id
ORDER BY total_bet DESC
LIMIT 10;

-- ─────────────────────────────────────────────────────────
-- Q2  Turnover ratio segmentation
-- ─────────────────────────────────────────────────────────
SELECT
    CASE
        WHEN casino_ratio > 0.7     THEN 'casino-heavy'
        WHEN sportsbook_ratio > 0.7 THEN 'sports-heavy'
        ELSE 'balanced'
    END AS segment,
    COUNT(*) AS customers,
    ROUND(AVG(total_turnover)::numeric, 2) AS avg_turnover
FROM src_iceberg_turnover_percentage
GROUP BY segment
ORDER BY customers DESC;

-- ─────────────────────────────────────────────────────────
-- Q3  Cross-table join: top 20 customers — bets + turnover
-- ─────────────────────────────────────────────────────────
SELECT b.customer_id,
       SUM(b.rolling_1d_real_bet_amount)::numeric AS real_bet,
       ROUND(t.casino_ratio::numeric, 3) AS casino_ratio
FROM src_iceberg_casino_real_bet b
JOIN src_iceberg_turnover_percentage t ON b.customer_id = t.customer_id
GROUP BY b.customer_id, t.casino_ratio
ORDER BY real_bet DESC
LIMIT 20;

-- ─────────────────────────────────────────────────────────
-- Q4  Data volume and freshness
-- ─────────────────────────────────────────────────────────
SELECT COUNT(*) AS rows,
       MAX(event_ts) AS latest_event
FROM src_iceberg_casino_real_bet;

-- ─────────────────────────────────────────────────────────
-- Q5  Iceberg snapshot count (how many commits so far)
--     Note: this one goes via Trino, not DataFusion
-- ─────────────────────────────────────────────────────────
-- SELECT COUNT(*) FROM datalake.public."rw_managed_casino_real_bet$snapshots";
