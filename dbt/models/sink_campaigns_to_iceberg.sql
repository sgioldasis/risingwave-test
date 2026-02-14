{#
  Model: sink_campaigns_to_iceberg
  Purpose: Creates a sink from campaigns to the Iceberg table
  This writes campaign data to Iceberg for persistent storage
#}

{{ config(
    materialized='sink',
    schema='public',
    tags=['iceberg']
) }}

-- This sink depends on: {{ ref('iceberg_campaigns') }}
CREATE SINK IF NOT EXISTS iceberg_campaigns_sink
INTO iceberg_campaigns
FROM {{ ref('campaigns') }}
WITH (
    type = 'append-only',
    force_append_only = 'true',
    commit_checkpoint_interval = 1,
    sink_decouple = false
)
