{{ config(
    materialized='kafka_table',
    tags=['casino_prd_setup', 'casino_uc1'],
    topic='cronus.casino.out.br',
    pre_hook=['DROP TABLE IF EXISTS ' ~ this ~ ' CASCADE']
) }}

CREATE TABLE IF NOT EXISTS {{ this }} (*)
APPEND ONLY
WITH (
    connector                     = 'kafka',
    topic                         = 'cronus.casino.out.br',
    properties.bootstrap.server   = '{{ env_var("KAFKA_CASINO_BOOTSTRAP", "") }}',
    properties.security.protocol  = '{{ env_var("KAFKA_INPUT_SECURITY_PROTOCOL", "SSL") }}'
    {%- if env_var("KAFKA_SASL_USERNAME", "") != "" %},
    properties.sasl.mechanism     = '{{ env_var("KAFKA_SASL_MECHANISM", "SCRAM-SHA-512") }}',
    properties.sasl.username      = '{{ env_var("KAFKA_SASL_USERNAME") }}',
    properties.sasl.password      = '{{ env_var("KAFKA_SASL_PASSWORD") }}'
    {%- endif %},
    group.id.prefix               = 'rw-readonly-casino-demo',
    scan.startup.mode = 'earliest',
    source_rate_limit             = 1
)
FORMAT PLAIN ENCODE PROTOBUF (
    schema.location  = '{{ "https://" ~ env_var("ADLS_ACCOUNT_NAME", "stkznneurwpoccdddevstd") ~ ".blob.core.windows.net/" ~ env_var("ADLS_CONTAINER", "cont1") ~ "/proto/casinoroundinfodto.pb?" ~ env_var("ADLS_PROTO_SAS_TOKEN", "") }}',
    message          = 'Cronus.CasinoService.RoundInfo.Abstractions.CasinoRoundInfoDto'
)
