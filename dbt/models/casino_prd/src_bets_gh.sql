{{ config(
    materialized='kafka_table',
    tags=['casino_uc2'],
    topic='bets-out-gh',
    pre_hook=['DROP TABLE IF EXISTS ' ~ this ~ ' CASCADE']
) }}

CREATE TABLE IF NOT EXISTS {{ this }} (*)
APPEND ONLY
WITH (
    connector                         = 'kafka',
    topic                             = 'bets-out-gh',
    properties.bootstrap.server       = 'prd4-kafka-bootstrap.kaizengaming.net:443',
    properties.security.protocol      = 'SSL',
    group.id.prefix                   = 'rw-readonly-bets-demo',
    scan.startup.mode                 = 'latest'
)
FORMAT PLAIN ENCODE PROTOBUF (
    schema.location   = 's3://hummock001/proto/betinfo.desc',
    message           = 'PandoraBetInfoVm',
    messages_as_jsonb = 'PlayerSubstitutionInfoVm',
    s3.region         = 'us-east-1',
    s3.endpoint       = 'http://minio-0:9301',
    s3.access.key     = 'hummockadmin',
    s3.secret.key     = 'hummockadmin'
)
