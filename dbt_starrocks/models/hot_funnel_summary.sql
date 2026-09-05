{{
  config(
    materialized='view'
  )
}}

SELECT
  CAST(window_start AS DATETIME) AS window_start,
  CAST(window_end AS DATETIME) AS window_end,
  country,
  viewers,
  carters,
  purchasers,
  CAST(view_to_cart_rate AS DOUBLE) AS view_to_cart_rate,
  CAST(cart_to_buy_rate AS DOUBLE) AS cart_to_buy_rate
FROM {{ source('risingwave', 'funnel_summary') }}

