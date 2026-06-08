{{ config(
    materialized='sink',
    tags=['casino_prd_setup', 'casino_avro']
) }}

-- Sink the full CasinoRoundInfoDto proto message (re-encoded as Avro) to local Redpanda.
-- Reading from src_casino_prd preserves the complete nested structure — GameInfo,
-- RoundInfo, Messages[], Transactions[], Timestamps — so src_casino_avro can apply
-- the same UNNEST transforms as the proto-native pipeline.
CREATE SINK IF NOT EXISTS {{ this }}
FROM {{ ref('src_casino_prd') }}
WITH (
    connector                   = 'kafka',
    topic                       = 'casino_out_avro',
    properties.bootstrap.server = 'redpanda:9092'
) FORMAT PLAIN ENCODE AVRO (
    schema.registry = 'http://redpanda:8081'
)
