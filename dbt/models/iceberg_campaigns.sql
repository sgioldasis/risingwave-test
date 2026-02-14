{#
  Model: iceberg_campaigns
  Purpose: Creates the Iceberg table for campaigns
  This stores campaign data from the direct producer
#}

{{ config(
    materialized='iceberg_table',
    schema='public',
    tags=['iceberg']
) }}

CREATE TABLE IF NOT EXISTS iceberg_campaigns (
    campaign_id VARCHAR,
    source VARCHAR,
    medium VARCHAR,
    campaign VARCHAR,
    content VARCHAR,
    term VARCHAR,
    ingested_at VARCHAR
) ENGINE = iceberg
