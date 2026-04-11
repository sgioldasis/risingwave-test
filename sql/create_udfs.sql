-- ==========================================================
-- RisingWave Python UDFs for Funnel Demo
-- ==========================================================

-- Drop existing functions (with CASCADE to handle dependencies)
DROP FUNCTION IF EXISTS conversion_category(float) CASCADE;
DROP FUNCTION IF EXISTS calculate_funnel_score(bigint, bigint, bigint) CASCADE;
DROP FUNCTION IF EXISTS calculate_funnel_score(numeric, numeric, numeric) CASCADE;
DROP FUNCTION IF EXISTS format_rate_with_emoji(float) CASCADE;
DROP FUNCTION IF EXISTS revenue_tier(numeric) CASCADE;
DROP FUNCTION IF EXISTS calculate_funnel_health(float, float) CASCADE;
DROP FUNCTION IF EXISTS calculate_funnel_health(double precision, double precision) CASCADE;
DROP FUNCTION IF EXISTS calculate_funnel_health(numeric, numeric) CASCADE;

-- Conversion rate categorization
CREATE FUNCTION conversion_category(rate float) 
RETURNS varchar 
LANGUAGE python AS $$
def conversion_category(rate):
    if rate is None:
        return 'unknown'
    if rate >= 0.5:
        return 'excellent'
    elif rate >= 0.3:
        return 'good'
    elif rate >= 0.1:
        return 'average'
    else:
        return 'needs_improvement'
$$;

-- Calculate weighted funnel score
CREATE FUNCTION calculate_funnel_score(viewers bigint, carters bigint, purchasers bigint) 
RETURNS float 
LANGUAGE python AS $$
def calculate_funnel_score(viewers, carters, purchasers):
    if viewers is None or viewers == 0:
        return 0.0
    view_to_cart = carters / float(viewers) if carters else 0.0
    cart_to_buy = purchasers / float(carters) if carters else 0.0
    return round(view_to_cart * 0.4 + cart_to_buy * 0.6, 2)
$$;

-- Format conversion rate with emoji indicator and text
CREATE FUNCTION format_rate_with_emoji(rate float)
RETURNS varchar
LANGUAGE python AS $$
def format_rate_with_emoji(rate):
    if rate is None:
        return '⚪ N/A'
    percentage = round(rate * 100, 1)
    if rate >= 0.5:
        return f'🟢 High {percentage}%'
    elif rate >= 0.3:
        return f'🟡 Medium {percentage}%'
    elif rate >= 0.1:
        return f'🟠 Low {percentage}%'
    else:
        return f'🔴 Critical {percentage}%'
$$;

-- Revenue tier classification
CREATE FUNCTION revenue_tier(amount numeric) 
RETURNS varchar 
LANGUAGE python AS $$
def revenue_tier(amount):
    if amount is None:
        return 'unknown'
    if amount >= 1000:
        return 'premium'
    elif amount >= 500:
        return 'high'
    elif amount >= 100:
        return 'medium'
    else:
        return 'low'
$$;

-- Calculate funnel health based on conversion rates
CREATE FUNCTION calculate_funnel_health(view_to_cart_rate float, cart_to_buy_rate float)
RETURNS varchar
LANGUAGE python AS $$
def calculate_funnel_health(view_to_cart_rate, cart_to_buy_rate):
    if view_to_cart_rate is None or cart_to_buy_rate is None:
        return 'unknown'
    if view_to_cart_rate >= 0.3 and cart_to_buy_rate >= 0.3:
        return 'strong'
    elif view_to_cart_rate >= 0.2 or cart_to_buy_rate >= 0.2:
        return 'moderate'
    else:
        return 'weak'
$$;
