-- =============================================================================
-- Prod sportsbook bets source (UC2: Casino Turnover Percentage)
--
-- Reads bets-out-gh from prd4 Kafka (SSL) and decodes PandoraBetInfoVm
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

\set ON_ERROR_STOP off
DROP SOURCE IF EXISTS src_bets_gh CASCADE;
DROP TABLE  IF EXISTS src_bets_gh CASCADE;
\set ON_ERROR_STOP on

CREATE TABLE src_bets_gh
WITH (
    connector                         = 'kafka',
    topic                             = 'bets-out-gh',
    properties.bootstrap.server       = 'prd4-kafka-bootstrap.kaizengaming.net:443',
    properties.security.protocol      = 'SSL',
    group.id.prefix                   = 'rw-readonly-bets-demo',
    scan.startup.mode                 = 'earliest'
)
FORMAT PLAIN ENCODE PROTOBUF (
    message           = 'PandoraBetInfoVm',
    schema.location   = 'file:///proto/betinfo.desc',
    messages_as_jsonb = 'PlayerSubstitutionInfoVm'
);
