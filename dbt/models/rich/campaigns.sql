{{ config(materialized='table') }}

SELECT 
    ''::varchar as campaign_id,
    ''::varchar as source,
    ''::varchar as medium,
    ''::varchar as campaign,
    ''::varchar as content,
    ''::varchar as term,
    ''::varchar as ingested_at
WHERE false
