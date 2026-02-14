{#
  Model: sink_clickstream_to_iceberg
  Purpose: Creates a sink from clickstream_events to the Iceberg table
  This writes clickstream events to Iceberg for persistent storage
#}

{{ config(
    materialized='sink',
    schema='public',
    tags=['iceberg']
) }}

-- This sink depends on: {{ ref('iceberg_clickstream_events') }}
CREATE SINK IF NOT EXISTS iceberg_clickstream_events_sink
INTO iceberg_clickstream_events
FROM {{ ref('clickstream_events') }}
WITH (
    type = 'append-only',
    force_append_only = 'true',
    commit_checkpoint_interval = 1,
    sink_decouple = false
)
