{#
  Model: iceberg_clickstream_events
  Purpose: Creates the Iceberg table for clickstream events
  This stores rich clickstream events from the direct producer
#}

{{ config(
    materialized='iceberg_table',
    schema='public',
    tags=['iceberg']
) }}

CREATE TABLE IF NOT EXISTS iceberg_clickstream_events (
    event_id VARCHAR,
    user_id BIGINT,
    session_id VARCHAR,
    event_type VARCHAR,
    page_url VARCHAR,
    element_id VARCHAR,
    event_time VARCHAR,
    referrer VARCHAR,
    campaign_id VARCHAR,
    revenue_usd DOUBLE PRECISION,
    ingested_at VARCHAR
) ENGINE = iceberg
