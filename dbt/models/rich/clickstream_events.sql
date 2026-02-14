{{ config(materialized='table') }}

SELECT 
    ''::varchar as event_id,
    0::bigint as user_id,
    ''::varchar as session_id,
    ''::varchar as event_type,
    ''::varchar as page_url,
    ''::varchar as element_id,
    ''::varchar as event_time,
    ''::varchar as referrer,
    ''::varchar as campaign_id,
    0.0::double precision as revenue_usd,
    ''::varchar as ingested_at
WHERE false
