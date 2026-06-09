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
    properties.bootstrap.server   = 'prd4-kafka-bootstrap.kaizengaming.net:443',
    properties.security.protocol  = 'SSL',
    group.id.prefix               = 'rw-readonly-bets-landing',
    scan.startup.mode             = 'latest',
    source_rate_limit             = 1
)
FORMAT PLAIN ENCODE BYTES
