{#
  Model: iceberg_page_catalog
  Purpose: Creates the Iceberg table for page catalog
  This stores page metadata from the direct producer
#}

{{ config(
    materialized='iceberg_table',
    schema='public',
    tags=['iceberg']
) }}

CREATE TABLE IF NOT EXISTS iceberg_page_catalog (
    page_url VARCHAR,
    page_category VARCHAR,
    product_id VARCHAR,
    product_category VARCHAR,
    ingested_at VARCHAR
) ENGINE = iceberg
