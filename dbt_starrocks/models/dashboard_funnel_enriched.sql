{{
  config(
    materialized='view',
    alias='dashboard_funnel_enriched'
  )
}}

WITH country_aggregated AS (
  SELECT
    window_start,
    window_end,
    SUM(viewers) AS viewers,
    SUM(carters) AS carters,
    SUM(purchasers) AS purchasers,
    SUM(carters) / NULLIF(SUM(viewers), 0) AS view_to_cart_rate,
    SUM(purchasers) / NULLIF(SUM(carters), 0) AS cart_to_buy_rate
  FROM {{ ref('dashboard_funnel_serving') }}
  GROUP BY window_start, window_end
)

SELECT
  *,
  CASE
    WHEN view_to_cart_rate IS NULL THEN 'unknown'
    WHEN view_to_cart_rate >= 0.5 THEN 'excellent'
    WHEN view_to_cart_rate >= 0.3 THEN 'good'
    WHEN view_to_cart_rate >= 0.1 THEN 'average'
    ELSE 'needs_improvement'
  END AS view_to_cart_category,
  CASE
    WHEN cart_to_buy_rate IS NULL THEN 'unknown'
    WHEN cart_to_buy_rate >= 0.5 THEN 'excellent'
    WHEN cart_to_buy_rate >= 0.3 THEN 'good'
    WHEN cart_to_buy_rate >= 0.1 THEN 'average'
    ELSE 'needs_improvement'
  END AS cart_to_buy_category,
  ROUND(
    (carters / NULLIF(viewers, 0)) * 0.4
    + (purchasers / NULLIF(carters, 0)) * 0.6,
    2
  ) AS funnel_score,
  CASE
    WHEN view_to_cart_rate IS NULL THEN 'N/A'
    WHEN view_to_cart_rate >= 0.5 THEN CONCAT('High ', ROUND(view_to_cart_rate * 100, 1), '%')
    WHEN view_to_cart_rate >= 0.3 THEN CONCAT('Medium ', ROUND(view_to_cart_rate * 100, 1), '%')
    WHEN view_to_cart_rate >= 0.1 THEN CONCAT('Low ', ROUND(view_to_cart_rate * 100, 1), '%')
    ELSE CONCAT('Critical ', ROUND(view_to_cart_rate * 100, 1), '%')
  END AS view_to_cart_emoji,
  CASE
    WHEN cart_to_buy_rate IS NULL THEN 'N/A'
    WHEN cart_to_buy_rate >= 0.5 THEN CONCAT('High ', ROUND(cart_to_buy_rate * 100, 1), '%')
    WHEN cart_to_buy_rate >= 0.3 THEN CONCAT('Medium ', ROUND(cart_to_buy_rate * 100, 1), '%')
    WHEN cart_to_buy_rate >= 0.1 THEN CONCAT('Low ', ROUND(cart_to_buy_rate * 100, 1), '%')
    ELSE CONCAT('Critical ', ROUND(cart_to_buy_rate * 100, 1), '%')
  END AS cart_to_buy_emoji,
  CASE
    WHEN view_to_cart_rate IS NULL OR cart_to_buy_rate IS NULL THEN 'unknown'
    WHEN view_to_cart_rate >= 0.3 AND cart_to_buy_rate >= 0.3 THEN 'strong'
    WHEN view_to_cart_rate >= 0.2 OR cart_to_buy_rate >= 0.2 THEN 'moderate'
    ELSE 'weak'
  END AS funnel_health
FROM country_aggregated