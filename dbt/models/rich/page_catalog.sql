{{ config(materialized='table') }}

SELECT 
    ''::varchar as page_url,
    ''::varchar as page_category,
    ''::varchar as product_id,
    ''::varchar as product_category,
    ''::varchar as ingested_at
WHERE false
