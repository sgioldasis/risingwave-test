{{ config(
    materialized='materialized_view',
    persist_docs={"relation": true, "columns": true},
    meta={
        "dagster": {
            "deps": [{"asset_key": ["public", "src_iceberg_countries"]}]
        }
    }
) }}

-- View (vw_iceberg_countries) that shows only the latest snapshot from Iceberg
-- Reads from native RisingWave SOURCE iceberg.iceberg_countries (src_iceberg_countries)
-- Filters out changelog history using ROW_NUMBER()
-- This gives you current data only, not all changes

WITH ranked_changes AS (
    SELECT
        country::varchar as country,
        country_name::varchar as country_name,
        -- Use _row_id which is monotonically increasing in RisingWave
        -- Higher _row_id = more recent change
        ROW_NUMBER() OVER (
            PARTITION BY country
            ORDER BY _row_id DESC NULLS LAST
        ) as rn
    FROM {{ source('iceberg', 'iceberg_countries') }}
)
SELECT
    country,
    country_name
FROM ranked_changes
WHERE rn = 1
