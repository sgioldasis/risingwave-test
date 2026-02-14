{#
  Model: sink_sessions_to_iceberg
  Purpose: Creates a sink from sessions to the Iceberg table
  This writes session data to Iceberg for persistent storage
#}

{{ config(
    materialized='sink',
    schema='public',
    tags=['iceberg']
) }}

-- This sink depends on: {{ ref('iceberg_sessions') }}
CREATE SINK IF NOT EXISTS iceberg_sessions_sink
INTO iceberg_sessions
FROM {{ ref('sessions') }}
WITH (
    type = 'append-only',
    force_append_only = 'true',
    commit_checkpoint_interval = 1,
    sink_decouple = false
)
