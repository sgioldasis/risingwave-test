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
    properties.bootstrap.server   = 'prd2-kafka-bootstrap.kaizengaming.net:443',
    properties.security.protocol  = 'SSL',
    group.id.prefix               = 'rw-readonly-casino-demo',
    scan.startup.mode             = 'latest',
    source_rate_limit             = 1
)
FORMAT PLAIN ENCODE PROTOBUF (
    schema.location  = '{{ "https://" ~ env_var("ADLS_ACCOUNT_NAME", "stkznneurwpoccdddevstd") ~ ".blob.core.windows.net/" ~ env_var("ADLS_CONTAINER", "cont1") ~ "/proto/casinoroundinfodto.pb?" ~ env_var("ADLS_PROTO_SAS_TOKEN", "") }}',
    message          = 'Cronus.CasinoService.RoundInfo.Abstractions.CasinoRoundInfoDto'
)
