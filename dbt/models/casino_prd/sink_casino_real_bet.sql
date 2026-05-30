{{ config(
    materialized='sink',
    tags=['casino_uc1']
) }}

CREATE SINK IF NOT EXISTS sink_casino_real_bet
INTO {{ ref('rw_managed_casino_real_bet') }}
FROM {{ ref('mv_casino_real_bet') }}
WITH (
    type                        = 'upsert',
    primary_key                 = 'customer_id,currency_id,event_ts',
    commit_checkpoint_interval  = 5,
    force_compaction            = true
)
