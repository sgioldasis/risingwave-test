{{ config(
    materialized='kafka_table',
    tags=['casino_prd_setup', 'casino_avro'],
    topic='casino_out_avro',
    pre_hook=['DROP TABLE IF EXISTS ' ~ this ~ ' CASCADE']
) }}
-- depends_on: {{ ref('sink_casino_avro_redpanda') }}

CREATE TABLE IF NOT EXISTS {{ this }} (*)
APPEND ONLY
WITH (
    connector                   = 'kafka',
    topic                       = 'casino_out_avro',
    properties.bootstrap.server = 'redpanda:9092',
    scan.startup.mode           = 'earliest'
) FORMAT PLAIN ENCODE AVRO (
    schema.registry = 'http://redpanda:8081'
)
