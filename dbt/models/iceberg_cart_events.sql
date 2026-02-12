{#
  Model: iceberg_cart_events
  Purpose: Creates the Iceberg table for raw cart events
  This stores raw cart events from the Kafka source
#}

{{ config(
    materialized='iceberg_table',
    schema='public',
    tags=['iceberg']
) }}

CREATE TABLE IF NOT EXISTS iceberg_cart_events (
    user_id INT,
    item_id VARCHAR,
    event_time TIMESTAMP
) ENGINE = iceberg