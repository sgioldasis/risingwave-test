-- =============================================================================
-- Prod sportsbook bets source (UC2: Casino Turnover Percentage)
--
-- Reads bets-out-br from prd4 Kafka (SSL) and decodes PandoraBetInfoVm
-- protobuf via the compiled FileDescriptorSet at /proto/betinfo.desc
-- (mounted into the RisingWave container).
--
-- PlayerSubstitutionInfoVm is self-referential (contains itself recursively)
-- so RisingWave cannot decode it as a native struct — stored as JSONB instead
-- via messages_as_jsonb.
--
-- Fetch proto:
--   curl -fsSL -H 'Accept: text/plain' \
--     http://staging-schema-registry.kaizengaming.net/apis/registry/v2/groups/bigdata/artifacts/betinfo \
--     > proto/betinfo.proto
--
-- Recompile descriptor:
--   protoc --descriptor_set_out=proto/betinfo.desc --include_imports \
--     --proto_path=/opt/homebrew/include --proto_path=proto proto/betinfo.proto
--
-- Idempotent: CASCADE drops dependent MVs/sinks so the table can be rebuilt.
-- =============================================================================

SET client_min_messages = WARNING;

DROP TABLE IF EXISTS src_bets_br CASCADE;

CREATE TABLE src_bets_br (*)
APPEND ONLY
WITH (
    connector                         = 'kafka',
    topic                             = 'bets-out-br',
    properties.bootstrap.server       = 'prd4-kafka-bootstrap.kaizengaming.net:443',
    properties.security.protocol      = 'SSL',
    group.id.prefix                   = 'rw-readonly-bets-demo',
    scan.startup.mode                 = 'latest',
    source_rate_limit                 = 1
)
FORMAT PLAIN ENCODE PROTOBUF (
    schema.location   = 's3://hummock001/proto/betinfo.desc',
    message           = 'PandoraBetInfoVm',
    messages_as_jsonb = 'PlayerSubstitutionInfoVm',
    s3.region         = 'us-east-1',
    s3.endpoint       = 'http://minio-0:9301',
    s3.access.key     = 'hummockadmin',
    s3.secret.key     = 'hummockadmin'
);
