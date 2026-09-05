{{
  config(
    materialized='table',
    unique_key='window_start, country',
    sort_keys=['window_start', 'country'],
    distributed_by='HASH(window_start)',
    properties={'replication_num': '1'}
  )
}}

SELECT
    CAST(window_start AS DATETIME) AS window_start,
    CAST(window_end AS DATETIME) AS window_end,
    country,
    viewers,
    carters,
    purchasers,
    view_to_cart_rate,
    cart_to_buy_rate
FROM {{ source('risingwave', 'funnel_summary') }}
WHERE window_start >= DATE_SUB(CURRENT_TIMESTAMP(), INTERVAL 1 DAY)
