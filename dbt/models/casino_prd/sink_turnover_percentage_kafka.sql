{{ config(
    materialized='sink',
    tags=['casino_uc2']
) }}

CREATE SINK IF NOT EXISTS sink_turnover_percentage_kafka
FROM {{ ref('mv_turnover_percentage') }}
WITH (
    connector                     = 'kafka',
    properties.bootstrap.server   = 'redpanda:9092',
    topic                         = 'casino_turnover_percentage_output'
)
FORMAT PLAIN ENCODE JSON (
    force_append_only = 'true'
)
