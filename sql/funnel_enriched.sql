-- ==========================================================
-- Enriched Funnel Materialized View with Python UDFs
-- Requires: funnel MV and Python UDFs to be created first
-- ==========================================================

-- Drop existing if recreating
DROP MATERIALIZED VIEW IF EXISTS funnel_enriched;

-- Create enriched funnel with UDF-enhanced metrics
CREATE MATERIALIZED VIEW funnel_enriched AS
SELECT 
    window_start,
    window_end,
    viewers,
    carters,
    purchasers,
    view_to_cart_rate,
    cart_to_buy_rate,
    -- UDF-enhanced categorization
    conversion_category(view_to_cart_rate) as view_to_cart_category,
    conversion_category(cart_to_buy_rate) as cart_to_buy_category,
    -- Weighted funnel score
    calculate_funnel_score(viewers, carters, purchasers) as funnel_score,
    -- Visual formatting
    format_rate_with_emoji(view_to_cart_rate) as view_to_cart_emoji,
    format_rate_with_emoji(cart_to_buy_rate) as cart_to_buy_emoji,
    -- Combined status indicator
    CASE 
        WHEN view_to_cart_rate >= 0.3 AND cart_to_buy_rate >= 0.3 THEN 'strong'
        WHEN view_to_cart_rate >= 0.2 OR cart_to_buy_rate >= 0.2 THEN 'moderate'
        ELSE 'weak'
    END as funnel_health
FROM funnel;

-- Create index for faster lookups
CREATE INDEX idx_funnel_enriched_time ON funnel_enriched(window_start);

-- Verify creation
SELECT 'funnel_enriched MV created successfully' as status;
