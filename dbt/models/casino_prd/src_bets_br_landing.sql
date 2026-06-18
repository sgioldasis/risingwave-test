{{ config(
    materialized='kafka_table',
    tags=['casino_prd_setup'],
    topic='bets-out-br',
    pre_hook=['DROP TABLE IF EXISTS ' ~ this ~ ' CASCADE']
) }}

CREATE TABLE IF NOT EXISTS {{ this }} (
    payload BYTEA
)
APPEND ONLY
INCLUDE KEY       AS kafka_key
INCLUDE TIMESTAMP AS kafka_timestamp
INCLUDE PARTITION AS kafka_partition
INCLUDE OFFSET    AS kafka_offset
WITH (
    connector                     = 'kafka',
    topic                         = 'bets-out-br',
    properties.bootstrap.server   = '{{ env_var("KAFKA_BETS_BOOTSTRAP", "") }}',
    properties.security.protocol  = '{{ env_var("KAFKA_INPUT_SECURITY_PROTOCOL", "SSL") }}'
    {%- if env_var("KAFKA_SASL_USERNAME", "") != "" %},
    properties.sasl.mechanism     = '{{ env_var("KAFKA_SASL_MECHANISM", "SCRAM-SHA-512") }}',
    properties.sasl.username      = '{{ env_var("KAFKA_SASL_USERNAME") }}',
    properties.sasl.password      = '{{ env_var("KAFKA_SASL_PASSWORD") }}'
    {%- endif %},
    group.id.prefix               = 'rw-readonly-bets-landing',
    scan.startup.mode             = 'latest',
    source_rate_limit             = 1
)
FORMAT PLAIN ENCODE BYTES
