{#
  Model: sink_funnel_to_kafka
  Purpose: Creates a sink from funnel_summary materialized view to a Kafka topic
  This publishes 1-minute aggregated funnel analytics to Kafka for dashboard consumption
#}

{{ config(
    materialized='sink',
    schema='public',
    tags=['kafka', 'funnel']
) }}

-- This sink publishes funnel_summary data (1-minute aggregates) to Kafka topic
CREATE SINK IF NOT EXISTS funnel_kafka_sink
FROM {{ ref('funnel_summary') }}
WITH (
    connector = 'kafka',
    properties.bootstrap.server = 'redpanda:9092',
    topic = 'funnel'
)
FORMAT PLAIN ENCODE JSON (
    force_append_only = 'true'
)
