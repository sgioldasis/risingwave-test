{{ config(
    materialized='kafka_table',
    tags=['casino_prd_setup', 'casino_avro'],
    topic='rw_poc_casino_out_avro',
    pre_hook=['DROP TABLE IF EXISTS ' ~ this ~ ' CASCADE']
) }}
-- depends_on: {{ ref('sink_casino_avro_redpanda') }}

CREATE TABLE IF NOT EXISTS {{ this }} (*)
APPEND ONLY
WITH (
    connector                   = 'kafka',
    topic                       = 'rw_poc_casino_out_avro',
    properties.bootstrap.server = '{{ env_var("KAFKA_OUTPUT_BOOTSTRAP", "redpanda:9092") }}'
    {%- if env_var("KAFKA_OUTPUT_SASL_USERNAME", "") != "" %},
    properties.security.protocol = 'SASL_SSL',
    properties.sasl.mechanism    = '{{ env_var("KAFKA_OUTPUT_SASL_MECHANISM", "SCRAM-SHA-512") }}',
    properties.sasl.username     = '{{ env_var("KAFKA_OUTPUT_SASL_USERNAME") }}',
    properties.sasl.password     = '{{ env_var("KAFKA_OUTPUT_SASL_PASSWORD") }}'
    {%- endif %},
    scan.startup.mode           = 'earliest'
) FORMAT PLAIN ENCODE AVRO (
    schema.registry = '{{ env_var("SCHEMA_REGISTRY_URL", "http://redpanda:8081") }}'
)
