{#
  Model: funnel_for_iceberg
  Purpose: Casts funnel data to types compatible with Iceberg sink
  Converts NUMERIC rates to DOUBLE for Iceberg compatibility
#}

{{ config(
    materialized='materialized_view',
    schema='public',
    tags=['iceberg', 'funnel']
) }}

SELECT 
    window_start,
    window_end,
    viewers,
    carters,
    purchasers,
    view_to_cart_rate::DOUBLE as view_to_cart_rate,
    cart_to_buy_rate::DOUBLE as cart_to_buy_rate
FROM {{ ref('funnel') }}
