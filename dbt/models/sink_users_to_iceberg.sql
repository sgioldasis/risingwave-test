{#
  Model: sink_users_to_iceberg
  Purpose: Creates a sink from users to the Iceberg table
  This writes user data to Iceberg for persistent storage
#}

{{ config(
    materialized='sink',
    schema='public',
    tags=['iceberg']
) }}

-- This sink depends on: {{ ref('iceberg_users') }}
CREATE SINK IF NOT EXISTS iceberg_users_sink
INTO iceberg_users
FROM {{ ref('users') }}
WITH (
    type = 'append-only',
    force_append_only = 'true',
    commit_checkpoint_interval = 1,
    sink_decouple = false
)
