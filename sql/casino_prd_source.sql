-- =============================================================================
-- Prod casino rounds table
--
-- Reads cronus.casino.out.gh from prd2 Kafka (SSL) and decodes the
-- CasinoRoundInfoDto protobuf via the compiled FileDescriptorSet at
-- /proto/casinoroundinfodto.pb (mounted into the RisingWave container).
--
-- Using a TABLE (not SOURCE) so RisingWave persists ingested rows in its
-- internal state store. This way:
--   * scan.startup.mode = 'earliest' replays the full topic into the table
--   * downstream MVs see ALL historical rows (not just messages arriving
--     after the streaming job started), and batch SELECT COUNT(*) on
--     the table matches what MVs have consumed.
--
-- Idempotent: CASCADE drops dependent MVs/sinks so the table can be
-- rebuilt cleanly. Re-run the MV/sink DDL afterwards.
-- =============================================================================

-- Drop both possible prior shapes (legacy SOURCE or current TABLE) so this
-- script is idempotent across the SOURCE→TABLE migration. RisingWave refuses
-- DROP SOURCE on a TABLE (and vice versa) even with IF EXISTS, so we toggle
-- ON_ERROR_STOP around each attempt.
\set ON_ERROR_STOP off
DROP SOURCE IF EXISTS src_casino_prd CASCADE;
DROP TABLE  IF EXISTS src_casino_prd CASCADE;
\set ON_ERROR_STOP on

CREATE TABLE src_casino_prd
WITH (
    connector                     = 'kafka',
    topic                         = 'cronus.casino.out.gh',
    properties.bootstrap.server   = 'prd2-kafka-bootstrap.kaizengaming.net:443',
    properties.security.protocol  = 'SSL',
    group.id.prefix               = 'rw-readonly-casino-demo',
    scan.startup.mode             = 'earliest'
)
FORMAT PLAIN ENCODE PROTOBUF (
    schema.location = 'file:///proto/casinoroundinfodto.pb',
    message         = 'Cronus.CasinoService.RoundInfo.Abstractions.CasinoRoundInfoDto'
);
