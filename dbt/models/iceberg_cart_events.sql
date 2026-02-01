{#
  Model: iceberg_cart_events
  Purpose: Creates the Iceberg table for raw cart events
  This stores raw cart add/remove events from the Kafka source
#}

{{ config(
    materialized='iceberg_table',
    schema='public',
    tags=['iceberg']
) }}

-- This table stores data from: {{ ref('src_cart') }}
CREATE TABLE IF NOT EXISTS iceberg_cart_events (
    user_id INT,
    item_id VARCHAR,
    event_time TIMESTAMP
) ENGINE = iceberg