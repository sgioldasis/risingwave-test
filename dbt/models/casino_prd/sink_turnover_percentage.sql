{{ config(
    materialized='sink',
    tags=['casino_uc2']
) }}

CREATE SINK IF NOT EXISTS sink_turnover_percentage
INTO {{ ref('rw_managed_turnover_percentage') }}
FROM {{ ref('mv_turnover_percentage') }}
WITH (
    type                        = 'upsert',
    primary_key                 = 'customer_id',
    commit_checkpoint_interval  = 5,
    force_compaction            = true
)
