{{ config(materialized='view') }}

SELECT
    f.window_start,
    f.window_end,
    f.country,
    c.country_name::varchar AS country_name,
    f.viewers,
    f.carters,
    f.purchasers,
    f.view_to_cart_rate,
    f.cart_to_buy_rate
FROM {{ ref('funnel_summary') }} f
LEFT JOIN {{ ref('iceberg_countries_ref') }} AS c
    ON f.country = c.country
