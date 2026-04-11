{{ config(
    materialized='view',
    meta={
        'dagster': {
            'deps': ['risingwave_python_udfs']
        }
    }
) }}

-- Enriched funnel with Python UDF-enhanced metrics
-- Based on funnel_summary_with_country, aggregated across all countries
-- NOTE: Using a regular VIEW instead of MATERIALIZED_VIEW to avoid RisingWave
-- "failed to collect barrier" error with Python UDFs in streaming context.
WITH country_aggregated AS (
    SELECT
        window_start,
        window_end,
        SUM(viewers) as viewers,
        SUM(carters) as carters,
        SUM(purchasers) as purchasers,
        -- Calculate weighted average rates across countries
        SUM(viewers * view_to_cart_rate) / NULLIF(SUM(viewers), 0) as view_to_cart_rate,
        SUM(carters * cart_to_buy_rate) / NULLIF(SUM(carters), 0) as cart_to_buy_rate
    FROM {{ ref('funnel_summary_with_country') }}
    GROUP BY window_start, window_end
)
SELECT
    window_start,
    window_end,
    viewers,
    carters,
    purchasers,
    view_to_cart_rate,
    cart_to_buy_rate,
    -- Python UDF-enhanced categorization
    conversion_category(view_to_cart_rate) as view_to_cart_category,
    conversion_category(cart_to_buy_rate) as cart_to_buy_category,
    -- Python UDF weighted funnel score
    calculate_funnel_score(viewers::bigint, carters::bigint, purchasers::bigint) as funnel_score,
    -- Python UDF visual formatting with emojis
    format_rate_with_emoji(view_to_cart_rate) as view_to_cart_emoji,
    format_rate_with_emoji(cart_to_buy_rate) as cart_to_buy_emoji,
    -- Python UDF for health status
    calculate_funnel_health(view_to_cart_rate, cart_to_buy_rate) as funnel_health
FROM country_aggregated
