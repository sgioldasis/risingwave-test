{#
  Model: sink_cart_events_to_iceberg
  Purpose: Creates a sink from cart events source to the Iceberg table
  This writes raw cart events to Iceberg for persistent storage
#}

{{ config(
    materialized='sink',
    schema='public',
    tags=['iceberg']
) }}

-- This sink depends on: {{ ref('iceberg_cart_events') }}
CREATE SINK IF NOT EXISTS iceberg_cart_events_sink
INTO iceberg_cart_events
FROM {{ ref('src_cart') }}
WITH (
    type = 'append-only',
    commit_checkpoint_interval = 1,
    sink_decouple = false
)