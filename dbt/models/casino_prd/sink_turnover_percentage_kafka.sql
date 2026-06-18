{{ config(
    materialized='sink',
    tags=['casino_uc2']
) }}

CREATE SINK IF NOT EXISTS sink_turnover_percentage_kafka
FROM {{ ref('mv_turnover_percentage') }}
WITH (
    connector                     = 'kafka',
    properties.bootstrap.server   = '{{ env_var("KAFKA_OUTPUT_BOOTSTRAP", "redpanda:9092") }}',
    topic                         = 'rw_poc_casino_out_turnover_percentage'
    {%- if env_var("KAFKA_OUTPUT_SASL_USERNAME", "") != "" %},
    properties.security.protocol  = 'SASL_SSL',
    properties.sasl.mechanism     = '{{ env_var("KAFKA_OUTPUT_SASL_MECHANISM", "SCRAM-SHA-512") }}',
    properties.sasl.username      = '{{ env_var("KAFKA_OUTPUT_SASL_USERNAME") }}',
    properties.sasl.password      = '{{ env_var("KAFKA_OUTPUT_SASL_PASSWORD") }}'
    {%- endif %}
)
FORMAT PLAIN ENCODE JSON (
    force_append_only = 'true'
)
