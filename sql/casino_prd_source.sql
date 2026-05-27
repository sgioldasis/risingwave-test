-- =============================================================================
-- Prod casino rounds source
--
-- Reads cronus.casino.out.gh from prd2 Kafka (SSL) and decodes the
-- CasinoRoundInfoDto protobuf via the compiled FileDescriptorSet at
-- /proto/casinoroundinfodto.pb (mounted into the RisingWave container).
--
-- Idempotent: CASCADE drops dependent MVs/sinks so the source can be
-- rebuilt cleanly. Re-run the MV/sink DDL afterwards.
-- =============================================================================

DROP SOURCE IF EXISTS src_casino_prd CASCADE;

CREATE SOURCE src_casino_prd
WITH (
    connector                     = 'kafka',
    topic                         = 'cronus.casino.out.gh',
    properties.bootstrap.server   = 'prd2-kafka-bootstrap.kaizengaming.net:443',
    properties.security.protocol  = 'SSL',
    group.id.prefix               = 'rw-readonly-casino-demo',
    scan.startup.mode             = 'latest'
)
FORMAT PLAIN ENCODE PROTOBUF (
    schema.location = 'file:///proto/casinoroundinfodto.pb',
    message         = 'Cronus.CasinoService.RoundInfo.Abstractions.CasinoRoundInfoDto'
);
