{{ config(
    materialized='view',
    persist_docs={"relation": true, "columns": true},
    meta={
        "dagster": {
            "deps": [{"asset_key": ["csv", "iceberg_countries"]}]
        }
    }
) }}

-- View (rw_countries) that shows the current snapshot from Iceberg
-- Reads from native RisingWave SOURCE src_iceberg_countries
-- Note: Using 'view' materialization so it always queries fresh data from the source
-- If the source itself is stale, recreate it with:
--   DROP SOURCE IF EXISTS src_iceberg_countries CASCADE;
--   CREATE SOURCE src_iceberg_countries WITH (...);

SELECT
    country::varchar as country,
    country_name::varchar as country_name
FROM {{ ref('src_iceberg_countries') }}
