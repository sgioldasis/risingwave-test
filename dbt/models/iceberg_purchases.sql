{#
  Model: iceberg_purchases
  Purpose: Creates the Iceberg table for raw purchase events
  This stores raw purchase events from the Kafka source
#}

{{ config(
    materialized='iceberg_table',
    schema='public',
    tags=['iceberg']
) }}

CREATE TABLE IF NOT EXISTS iceberg_purchases (
    user_id INT,
    amount NUMERIC,
    event_time TIMESTAMP
) ENGINE = iceberg