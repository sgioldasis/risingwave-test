{#
  Model: iceberg_page_views
  Purpose: Creates the Iceberg table for raw page view events
  This stores raw page view events from the Kafka source
#}

{{ config(
    materialized='iceberg_table',
    schema='public'
) }}

CREATE TABLE IF NOT EXISTS iceberg_page_views (
    user_id INT,
    page_id VARCHAR,
    event_time TIMESTAMP
) ENGINE = iceberg