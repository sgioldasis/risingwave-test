{{ config(
    materialized='kafka_table',
    tags=['casino_uc2'],
    topic='bets-out-br',
    pre_hook=['DROP TABLE IF EXISTS ' ~ this ~ ' CASCADE']
) }}

CREATE TABLE IF NOT EXISTS {{ this }} (*)
APPEND ONLY
WITH (
    connector                         = 'kafka',
    topic                             = 'bets-out-br',
    properties.bootstrap.server       = '{{ env_var("KAFKA_BETS_BOOTSTRAP", "") }}',
    properties.security.protocol      = '{{ env_var("KAFKA_INPUT_SECURITY_PROTOCOL", "SSL") }}'
    {%- if env_var("KAFKA_SASL_USERNAME", "") != "" %},
    properties.sasl.mechanism         = '{{ env_var("KAFKA_SASL_MECHANISM", "SCRAM-SHA-512") }}',
    properties.sasl.username          = '{{ env_var("KAFKA_SASL_USERNAME") }}',
    properties.sasl.password          = '{{ env_var("KAFKA_SASL_PASSWORD") }}'
    {%- endif %},
    group.id.prefix                   = 'rw-readonly-bets-demo',
    scan.startup.mode = 'earliest',
    source_rate_limit                 = 1
)
FORMAT PLAIN ENCODE PROTOBUF (
    schema.location   = '{{ "https://" ~ env_var("ADLS_ACCOUNT_NAME", "stkznneurwpoccdddevstd") ~ ".blob.core.windows.net/" ~ env_var("ADLS_CONTAINER", "cont1") ~ "/proto/betinfo.desc?" ~ env_var("ADLS_PROTO_SAS_TOKEN", "") }}',
    message           = 'PandoraBetInfoVm',
    messages_as_jsonb = 'PlayerSubstitutionInfoVm'
)
