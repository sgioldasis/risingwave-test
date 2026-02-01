{#
  Model: sink_purchases_to_iceberg
  Purpose: Creates a sink from purchases source to the Iceberg table
  This writes raw purchase events to Iceberg for persistent storage
#}

{{ config(
    materialized='sink',
    schema='public'
) }}

CREATE SINK IF NOT EXISTS iceberg_purchases_sink
INTO iceberg_purchases
FROM {{ ref('src_purchase') }}
WITH (
    type = 'append-only',
    commit_checkpoint_interval = 10,
    sink_decouple = true
)