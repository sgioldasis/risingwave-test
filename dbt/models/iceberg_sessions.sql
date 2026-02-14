{#
  Model: iceberg_sessions
  Purpose: Creates the Iceberg table for sessions
  This stores session data from the direct producer
#}

{{ config(
    materialized='iceberg_table',
    schema='public',
    tags=['iceberg']
) }}

CREATE TABLE IF NOT EXISTS iceberg_sessions (
    session_id VARCHAR,
    user_id BIGINT,
    device_id VARCHAR,
    session_start VARCHAR,
    ip_address VARCHAR,
    geo_city VARCHAR,
    geo_region VARCHAR,
    ingested_at VARCHAR
) ENGINE = iceberg
