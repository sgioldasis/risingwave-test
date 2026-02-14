{{ config(materialized='table') }}

SELECT 
    0::bigint as user_id,
    ''::varchar as full_name,
    ''::varchar as email,
    ''::varchar as country,
    ''::varchar as signup_time,
    false::boolean as marketing_opt_in,
    ''::varchar as ingested_at
WHERE false
