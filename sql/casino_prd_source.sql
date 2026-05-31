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

SET client_min_messages = WARNING;

DROP TABLE IF EXISTS src_casino_prd CASCADE;

CREATE TABLE src_casino_prd (*)
APPEND ONLY
WITH (
    connector                     = 'kafka',
    topic                         = 'cronus.casino.out.gh',
    properties.bootstrap.server   = 'prd2-kafka-bootstrap.kaizengaming.net:443',
    properties.security.protocol  = 'SSL',
    group.id.prefix               = 'rw-readonly-casino-demo',
    scan.startup.mode             = 'latest'
)
FORMAT PLAIN ENCODE PROTOBUF (
    schema.location  = 's3://hummock001/proto/casinoroundinfodto.pb',
    message          = 'Cronus.CasinoService.RoundInfo.Abstractions.CasinoRoundInfoDto',
    s3.region        = 'us-east-1',
    s3.endpoint      = 'http://minio-0:9301',
    s3.access.key    = 'hummockadmin',
    s3.secret.key    = 'hummockadmin'
);
