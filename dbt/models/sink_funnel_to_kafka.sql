{#
  Model: sink_funnel_to_kafka
  Purpose: Creates a sink from funnel materialized view to a Kafka topic
  This publishes funnel analytics to Kafka for downstream consumption
#}

{{ config(
    materialized='sink',
    schema='public',
    tags=['kafka', 'funnel']
) }}

-- This sink publishes funnel data to Kafka topic
CREATE SINK IF NOT EXISTS funnel_kafka_sink
FROM {{ ref('funnel') }}
WITH (
    connector = 'kafka',
    properties.bootstrap.server = 'redpanda:9092',
    topic = 'funnel'
)
FORMAT PLAIN ENCODE JSON (
    force_append_only = 'true'
)
