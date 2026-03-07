{{ config(materialized='view') }}

-- Join funnel_summary with Iceberg countries data within RisingWave
-- This avoids cross-catalog join issues in Trino
-- Using VIEW materialization to ensure fresh data on each query

SELECT
    f.window_start,
    f.country,
    c.country_name,
    f.viewers,
    f.carters,
    f.purchasers,
    f.view_to_cart_rate,
    f.cart_to_buy_rate
FROM {{ ref('funnel_summary') }} f
LEFT JOIN {{ ref('rw_countries') }} c
    ON f.country = c.country
