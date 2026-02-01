{#
  Model: sink_page_views_to_iceberg
  Purpose: Creates a sink from page views source to the Iceberg table
  This writes raw page view events to Iceberg for persistent storage
#}

{{ config(
    materialized='sink',
    schema='public',
    tags=['iceberg']
) }}

-- This sink depends on: {{ ref('iceberg_page_views') }}
CREATE SINK IF NOT EXISTS iceberg_page_views_sink
INTO iceberg_page_views
FROM {{ ref('src_page') }}
WITH (
    type = 'append-only',
    commit_checkpoint_interval = 10,
    sink_decouple = true
)