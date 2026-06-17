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
    properties.bootstrap.server       = 'prd4-kafka-bootstrap.kaizengaming.net:443',
    properties.security.protocol      = 'SSL',
    group.id.prefix                   = 'rw-readonly-bets-demo',
    scan.startup.mode                 = 'latest',
    source_rate_limit                 = 1
)
FORMAT PLAIN ENCODE PROTOBUF (
    schema.location   = '{{ "https://" ~ env_var("ADLS_ACCOUNT_NAME", "stkznneurwpoccdddevstd") ~ ".blob.core.windows.net/" ~ env_var("ADLS_CONTAINER", "cont1") ~ "/proto/betinfo.desc?" ~ env_var("ADLS_PROTO_SAS_TOKEN", "") }}',
    message           = 'PandoraBetInfoVm',
    messages_as_jsonb = 'PlayerSubstitutionInfoVm'
)
