{#
  Model: sink_devices_to_iceberg
  Purpose: Creates a sink from devices to the Iceberg table
  This writes device data to Iceberg for persistent storage
#}

{{ config(
    materialized='sink',
    schema='public',
    tags=['iceberg']
) }}

-- This sink depends on: {{ ref('iceberg_devices') }}
CREATE SINK IF NOT EXISTS iceberg_devices_sink
INTO iceberg_devices
FROM {{ ref('devices') }}
WITH (
    type = 'append-only',
    force_append_only = 'true',
    commit_checkpoint_interval = 1,
    sink_decouple = false
)
