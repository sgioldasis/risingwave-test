{{ config(
    materialized='sink',
    tags=['casino_prd_setup', 'casino_avro']
) }}

-- Sink the full CasinoRoundInfoDto proto message (re-encoded as Avro) to local Redpanda.
-- Reading from src_casino_prd preserves the complete nested structure — GameInfo,
-- RoundInfo, Messages[], Transactions[], Timestamps — so src_casino_avro can apply
-- the same UNNEST transforms as the proto-native pipeline.
CREATE SINK IF NOT EXISTS {{ this }}
FROM {{ ref('src_casino_prd') }}
WITH (
    connector                   = 'kafka',
    topic                       = 'rw_poc_casino_out_avro',
    properties.bootstrap.server = '{{ env_var("KAFKA_OUTPUT_BOOTSTRAP", "redpanda:9092") }}'
    {%- if env_var("KAFKA_OUTPUT_SASL_USERNAME", "") != "" %},
    properties.security.protocol = 'SASL_SSL',
    properties.sasl.mechanism    = '{{ env_var("KAFKA_OUTPUT_SASL_MECHANISM", "SCRAM-SHA-512") }}',
    properties.sasl.username     = '{{ env_var("KAFKA_OUTPUT_SASL_USERNAME") }}',
    properties.sasl.password     = '{{ env_var("KAFKA_OUTPUT_SASL_PASSWORD") }}'
    {%- endif %}
) FORMAT PLAIN ENCODE AVRO (
    schema.registry = '{{ env_var("SCHEMA_REGISTRY_URL", "http://redpanda:8081") }}'
)
