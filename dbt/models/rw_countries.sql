{{ config(
    materialized='view',
    persist_docs={"relation": true, "columns": true},
    meta={
        "dagster": {
            "deps": [{"asset_key": ["csv", "iceberg_countries"]}]
        }
    }
) }}

SELECT
    country::varchar as country,
    country_name::varchar as country_name
FROM {{ ref('iceberg_countries_ref') }}
