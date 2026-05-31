{{ config(
    materialized='sink',
    tags=['casino_uc1']
) }}

CREATE SINK IF NOT EXISTS sink_casino_real_bet_kafka
FROM {{ ref('mv_casino_real_bet') }}
WITH (
    connector                     = 'kafka',
    properties.bootstrap.server   = 'redpanda:9092',
    topic                         = 'casino_real_bet_output'
)
FORMAT PLAIN ENCODE JSON (
    force_append_only = 'true'
)
