{{ config(
    materialized='materialized_view',
    persist_docs={"relation": true, "columns": true}
) }}

-- Materialized view that reads from the Iceberg source
-- This creates a local copy in RisingWave that refreshes automatically
-- when the underlying Iceberg data changes
SELECT
    country,
    country_name
FROM {{ ref('vw_iceberg_countries') }}
