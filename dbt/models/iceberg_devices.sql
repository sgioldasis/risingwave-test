{#
  Model: iceberg_devices
  Purpose: Creates the Iceberg table for devices
  This stores device data from the direct producer
#}

{{ config(
    materialized='iceberg_table',
    schema='public',
    tags=['iceberg']
) }}

CREATE TABLE IF NOT EXISTS iceberg_devices (
    device_id VARCHAR,
    device_type VARCHAR,
    os VARCHAR,
    browser VARCHAR,
    user_agent VARCHAR,
    ingested_at VARCHAR
) ENGINE = iceberg
