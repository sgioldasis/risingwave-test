{#
  Model: sink_page_catalog_to_iceberg
  Purpose: Creates a sink from page_catalog to the Iceberg table
  This writes page catalog data to Iceberg for persistent storage
#}

{{ config(
    materialized='sink',
    schema='public',
    tags=['iceberg']
) }}

-- This sink depends on: {{ ref('iceberg_page_catalog') }}
CREATE SINK IF NOT EXISTS iceberg_page_catalog_sink
INTO iceberg_page_catalog
FROM {{ ref('page_catalog') }}
WITH (
    type = 'append-only',
    force_append_only = 'true',
    commit_checkpoint_interval = 1,
    sink_decouple = false
)
