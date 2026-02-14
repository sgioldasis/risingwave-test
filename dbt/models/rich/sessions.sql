{{ config(materialized='table') }}

SELECT 
    ''::varchar as session_id,
    0::bigint as user_id,
    ''::varchar as device_id,
    ''::varchar as session_start,
    ''::varchar as ip_address,
    ''::varchar as geo_city,
    ''::varchar as geo_region,
    ''::varchar as ingested_at
WHERE false
