{{ config(materialized='table') }}

SELECT 
    ''::varchar as device_id,
    ''::varchar as device_type,
    ''::varchar as os,
    ''::varchar as browser,
    ''::varchar as user_agent,
    ''::varchar as ingested_at
WHERE false
