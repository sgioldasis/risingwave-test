-- =============================================================================
-- Prod sportsbook bets source (UC2: Casino Turnover Percentage)
--
-- Reads bets-out-br from prd4 Kafka (SSL) and decodes PandoraBetInfoVm
-- protobuf via the compiled FileDescriptorSet fetched from ADLS Gen2
-- (cont1/proto/betinfo.desc) via HTTPS + SAS token.
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
--
-- Variables (passed via psql -v):
--   KAFKA_BETS_BOOTSTRAP    Kafka bootstrap server
--   USE_SASL              true → SASL_SSL with SCRAM-SHA-512; false → SSL only
--   KAFKA_SASL_USERNAME   SASL username (only used when USE_SASL=true)
--   KAFKA_SASL_PASSWORD   SASL password (only used when USE_SASL=true)
--   PROTO_BETS_URL        HTTPS URL to betinfo.desc in ADLS (with SAS token)
-- =============================================================================

SET client_min_messages = WARNING;

DROP TABLE IF EXISTS src_bets_br CASCADE;

\if :USE_SASL
CREATE TABLE src_bets_br (*)
APPEND ONLY
WITH (
    connector                         = 'kafka',
    topic                             = 'bets-out-br',
    properties.bootstrap.server       = :'KAFKA_BETS_BOOTSTRAP',
    properties.security.protocol      = 'SASL_SSL',
    properties.sasl.mechanism         = 'SCRAM-SHA-512',
    properties.sasl.username          = :'KAFKA_SASL_USERNAME',
    properties.sasl.password          = :'KAFKA_SASL_PASSWORD',
    group.id.prefix                   = 'rw-readonly-bets-demo',
    scan.startup.mode = 'earliest',
    source_rate_limit                 = 1
)
FORMAT PLAIN ENCODE PROTOBUF (
    schema.location   = :'PROTO_BETS_URL',
    message           = 'PandoraBetInfoVm',
    messages_as_jsonb = 'PlayerSubstitutionInfoVm'
);
\else
CREATE TABLE src_bets_br (*)
APPEND ONLY
WITH (
    connector                         = 'kafka',
    topic                             = 'bets-out-br',
    properties.bootstrap.server       = :'KAFKA_BETS_BOOTSTRAP',
    properties.security.protocol      = 'SSL',
    group.id.prefix                   = 'rw-readonly-bets-demo',
    scan.startup.mode = 'earliest',
    source_rate_limit                 = 1
)
FORMAT PLAIN ENCODE PROTOBUF (
    schema.location   = :'PROTO_BETS_URL',
    message           = 'PandoraBetInfoVm',
    messages_as_jsonb = 'PlayerSubstitutionInfoVm'
);
\endif
