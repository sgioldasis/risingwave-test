{{ config(materialized='materialized_view') }}

-- Join funnel_summary with Iceberg countries data within RisingWave
-- This avoids cross-catalog join issues in Trino

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
LEFT JOIN {{ ref('vw_iceberg_countries') }} c
    ON f.country = c.country
