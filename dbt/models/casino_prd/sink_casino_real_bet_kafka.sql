{{ config(
    materialized='sink',
    tags=['casino_uc1']
) }}

CREATE SINK IF NOT EXISTS sink_casino_real_bet_kafka
FROM {{ ref('mv_casino_real_bet') }}
WITH (
    connector                     = 'kafka',
    properties.bootstrap.server   = '{{ env_var("KAFKA_OUTPUT_BOOTSTRAP", "redpanda:9092") }}',
    topic                         = 'rw_poc_casino_out_real_bet'
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
