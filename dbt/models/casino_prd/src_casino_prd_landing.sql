{{ config(
    materialized='kafka_table',
    tags=['casino_prd_setup'],
    topic='cronus.casino.out.br',
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
    topic                         = 'cronus.casino.out.br',
    properties.bootstrap.server   = 'prd2-kafka-bootstrap.kaizengaming.net:443',
    properties.security.protocol  = 'SSL',
    group.id.prefix               = 'rw-readonly-casino-landing',
    scan.startup.mode             = 'latest',
    source_rate_limit             = 1
)
FORMAT PLAIN ENCODE BYTES
