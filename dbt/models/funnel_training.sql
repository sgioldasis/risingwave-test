{{
    config(
        materialized='materialized_view',
        on_configuration_change='apply'
    )
}}

-- =============================================================================
-- Funnel Training Data for ML Models
-- =============================================================================
-- This materialized view provides time-series data optimized for ML training.
-- It includes:
--   - Raw funnel metrics (viewers, carters, purchasers)
--   - Conversion rates
--   - Time-based features (hour, minute, day_of_week) for pattern recognition
--   - Lag features (previous minute values) for trend analysis
-- =============================================================================

WITH funnel_base AS (
    SELECT
        window_start,
        window_end,
        viewers,
        carters,
        purchasers,
        view_to_cart_rate,
        cart_to_buy_rate
    FROM {{ ref('funnel') }}
),

-- Add time-based features to help ML model detect patterns
-- (e.g., hourly patterns, daily patterns)
time_features AS (
    SELECT
        *,
        EXTRACT(HOUR FROM window_start) AS hour_of_day,
        EXTRACT(MINUTE FROM window_start) AS minute_of_hour,
        EXTRACT(DOW FROM window_start) AS day_of_week,
        EXTRACT(EPOCH FROM window_start) / 60 AS minute_sequence  -- Continuous minute counter
    FROM funnel_base
),

-- Add lag features (previous minute values) for trend context
-- These help the model understand momentum and trends
-- Note: PARTITION BY 1::int is required for RisingWave compatibility
lag_features AS (
    SELECT
        *,
        LAG(viewers, 1) OVER (PARTITION BY 1::int ORDER BY window_start) AS viewers_lag_1,
        LAG(viewers, 2) OVER (PARTITION BY 1::int ORDER BY window_start) AS viewers_lag_2,
        LAG(viewers, 3) OVER (PARTITION BY 1::int ORDER BY window_start) AS viewers_lag_3,
        LAG(carters, 1) OVER (PARTITION BY 1::int ORDER BY window_start) AS carters_lag_1,
        LAG(carters, 2) OVER (PARTITION BY 1::int ORDER BY window_start) AS carters_lag_2,
        LAG(carters, 3) OVER (PARTITION BY 1::int ORDER BY window_start) AS carters_lag_3,
        LAG(purchasers, 1) OVER (PARTITION BY 1::int ORDER BY window_start) AS purchasers_lag_1,
        LAG(purchasers, 2) OVER (PARTITION BY 1::int ORDER BY window_start) AS purchasers_lag_2,
        LAG(purchasers, 3) OVER (PARTITION BY 1::int ORDER BY window_start) AS purchasers_lag_3
    FROM time_features
),

-- Calculate rolling averages with PARTITION BY for RisingWave compatibility
-- With 20-second windows: 15 windows = 5 minutes of data
rolling_features AS (
    SELECT
        *,
        AVG(viewers) OVER (
            PARTITION BY 1::int
            ORDER BY window_start
            ROWS BETWEEN 14 PRECEDING AND CURRENT ROW
        ) AS viewers_ma_5,
        AVG(carters) OVER (
            PARTITION BY 1::int
            ORDER BY window_start
            ROWS BETWEEN 14 PRECEDING AND CURRENT ROW
        ) AS carters_ma_5
    FROM lag_features
)

SELECT
    window_start,
    window_end,
    viewers,
    carters,
    purchasers,
    view_to_cart_rate,
    cart_to_buy_rate,
    -- Time features
    hour_of_day,
    minute_of_hour,
    day_of_week,
    minute_sequence,
    -- Lag features
    viewers_lag_1,
    viewers_lag_2,
    viewers_lag_3,
    carters_lag_1,
    carters_lag_2,
    carters_lag_3,
    purchasers_lag_1,
    purchasers_lag_2,
    purchasers_lag_3,
    -- Moving averages
    viewers_ma_5,
    carters_ma_5,
    -- Trend indicators (positive/negative momentum)
    CASE
        WHEN viewers_lag_1 IS NOT NULL AND viewers_lag_1 > 0
        THEN (viewers - viewers_lag_1)::numeric / viewers_lag_1
        ELSE 0
    END AS viewers_trend,
    CASE
        WHEN carters_lag_1 IS NOT NULL AND carters_lag_1 > 0
        THEN (carters - carters_lag_1)::numeric / carters_lag_1
        ELSE 0
    END AS carters_trend,
    -- TPS (transactions per second) - calculated from viewers rate of change
    -- With 20-second windows, lag_1 is 60 seconds (3 windows) ago
    -- TPS = absolute change per second (change / 60 seconds)
    CASE
        WHEN viewers_lag_1 IS NOT NULL
        THEN ABS(viewers - viewers_lag_1)::numeric / 60.0  -- 60 seconds between lag_1 and current
        ELSE 0
    END AS tps_viewers,
    CASE
        WHEN carters_lag_1 IS NOT NULL
        THEN ABS(carters - carters_lag_1)::numeric / 60.0
        ELSE 0
    END AS tps_carters,
    -- Smoothed TPS over last 3 lag periods (3 minutes of data)
    CASE
        WHEN viewers_lag_1 IS NOT NULL AND viewers_lag_2 IS NOT NULL AND viewers_lag_3 IS NOT NULL
        THEN (ABS(viewers - viewers_lag_1) + ABS(viewers_lag_1 - viewers_lag_2) + ABS(viewers_lag_2 - viewers_lag_3))::numeric / 180.0
        ELSE NULL
    END AS tps_smoothed
FROM rolling_features
