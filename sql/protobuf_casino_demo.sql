-- =============================================================================
-- RisingWave PROTOBUF demo — Cronus CasinoRoundInfoDto
--
-- Companion to:
--   scripts/produce_protobuf_casino_rounds.py     (producer + SR registration)
--   bin/3_run_protobuf_casino_demo.sh             (end-to-end runner)
--
-- Schema-Registry-based source (no .proto/.pb file needed inside RisingWave).
--
-- Note on identifiers:
--   The .proto uses PascalCase field names (CompanyId, GameInfo, ...).
--   RisingWave preserves protobuf field names as-is, so we double-quote
--   identifiers in this file to avoid Postgres lowercase folding.
-- =============================================================================

DROP SOURCE IF EXISTS src_casino_rounds_proto CASCADE;

CREATE SOURCE src_casino_rounds_proto
WITH (
    connector = 'kafka',
    topic = 'casino_rounds',
    properties.bootstrap.server = 'redpanda:9092',
    scan.startup.mode = 'earliest'
) FORMAT PLAIN ENCODE PROTOBUF (
    schema.registry = 'http://redpanda:8081',
    message = 'Cronus.CasinoService.RoundInfo.Abstractions.CasinoRoundInfoDto'
);

-- -----------------------------------------------------------------------------
-- Sanity #1 — top-level + nested struct field access.
-- (GameInfo).GameId notation accesses fields inside the nested message.
-- -----------------------------------------------------------------------------
\echo '=== top-level + nested GameInfo / RoundInfo ==='
SELECT
    "UniqueId",
    "CustomerId",
    "CompanyId",
    "CasinoProviderId",
    ("GameInfo")."GameId"               AS game_id,
    ("GameInfo")."ProviderGameCode"     AS provider_game_code,
    ("GameInfo")."GameType"             AS game_type,
    ("GameInfo")."IsLive"               AS is_live,
    ("RoundInfo")."GameRoundRef"        AS round_ref,
    to_timestamp(
        (("RoundInfo")."RoundCreated")."seconds"
      + (("RoundInfo")."RoundCreated")."nanos" / 1e9
    )                                   AS round_created,
    array_length(("RoundInfo")."Messages") AS num_messages
FROM src_casino_rounds_proto
LIMIT 5;

-- -----------------------------------------------------------------------------
-- Sanity #2 — UNNEST the repeated Messages.
-- Important: RisingWave's UNNEST(struct[]) FLATTENS the inner struct's fields
-- into top-level columns. The optional `AS alias` after UNNEST names the *row*,
-- not a struct value — reference the inner fields by their bare (quoted) name,
-- NOT alias.field.
-- -----------------------------------------------------------------------------
\echo ''
\echo '=== messages flattened (UNNEST RoundInfo.Messages) ==='
SELECT
    "UniqueId",
    "MessageId"                                                       AS message_id,
    "MessageTypeId"                                                   AS msg_type,
    "IsRoundClosed"                                                   AS round_closed,
    to_timestamp(("Created")."seconds" + ("Created")."nanos" / 1e9)   AS msg_created,
    array_length("Transactions")                                      AS num_tx
FROM src_casino_rounds_proto,
     UNNEST(("RoundInfo")."Messages")
LIMIT 10;

-- -----------------------------------------------------------------------------
-- Sanity #3 — Double UNNEST: rounds -> messages -> transactions.
-- Both CasinoMessageInformation and TransactionInformation have a `Created`
-- field, so the message-level fields are renamed in a CTE before the second
-- UNNEST exposes the transaction-level columns.
-- -----------------------------------------------------------------------------
\echo ''
\echo '=== transactions flattened (double UNNEST) ==='
WITH exploded_msgs AS (
    SELECT
        r."UniqueId"               AS unique_id,
        r."CompanyId"              AS company_id,
        (r."GameInfo")."GameId"    AS game_id,
        "MessageTypeId"            AS msg_type,
        "Transactions"             AS txs
    FROM src_casino_rounds_proto AS r,
         UNNEST((r."RoundInfo")."Messages")
)
SELECT
    unique_id,
    company_id,
    game_id,
    msg_type,
    "TransactionId"                                                  AS tx_id,
    "CurrencyId"                                                     AS currency_id,
    "Amount"                                                         AS amount,
    "BonusAction"                                                    AS bonus_action,
    to_timestamp(("Created")."seconds" + ("Created")."nanos" / 1e9)  AS tx_created
FROM exploded_msgs, UNNEST(txs)
LIMIT 10;

-- -----------------------------------------------------------------------------
-- Real-time aggregate — bet volume per (company, game).
-- Amount fields are decimal-as-string; cast to numeric for SUM.
-- -----------------------------------------------------------------------------
DROP MATERIALIZED VIEW IF EXISTS mv_casino_volume_by_company_game;

CREATE MATERIALIZED VIEW mv_casino_volume_by_company_game AS
WITH exploded_msgs AS (
    SELECT
        r."UniqueId"               AS unique_id,
        r."CompanyId"              AS company_id,
        (r."GameInfo")."GameId"    AS game_id,
        (r."GameInfo")."GameType"  AS game_type,
        "Transactions"             AS txs
    FROM src_casino_rounds_proto AS r,
         UNNEST((r."RoundInfo")."Messages")
)
SELECT
    company_id,
    game_id,
    game_type,
    COUNT(DISTINCT unique_id)                          AS rounds,
    COUNT(*)                                           AS transactions,
    ROUND(SUM(("Amount")::numeric), 4)                 AS total_amount,
    ROUND(SUM(("TokenAmount")::numeric), 4)            AS total_token_amount,
    ROUND(AVG(("CurrencyRateToEuro")::numeric), 6)     AS avg_fx_to_eur
FROM exploded_msgs, UNNEST(txs)
GROUP BY 1, 2, 3;

\echo ''
\echo '=== rollup: volume per (company, game) ==='
SELECT * FROM mv_casino_volume_by_company_game
ORDER BY total_amount DESC NULLS LAST
LIMIT 15;

-- =============================================================================
-- Managed-Iceberg sinks (Lakekeeper REST catalog + MinIO storage)
-- =============================================================================
CREATE CONNECTION IF NOT EXISTS lakekeeper_catalog_conn WITH (
    type = 'iceberg',
    catalog.type = 'rest',
    catalog.uri = 'http://lakekeeper:8181/catalog/',
    warehouse.path = 'risingwave-warehouse',
    s3.access.key = 'hummockadmin',
    s3.secret.key = 'hummockadmin',
    s3.path.style.access = 'true',
    s3.endpoint = 'http://minio-0:9301',
    s3.region = 'us-east-1'
);

SET iceberg_engine_connection = 'public.lakekeeper_catalog_conn';

-- --- Flattened round-level fact (one row per round) ---------------------------
DROP SINK IF EXISTS rw_managed_casino_rounds_sink;
DROP TABLE IF EXISTS rw_managed_casino_rounds;
DROP MATERIALIZED VIEW IF EXISTS mv_casino_rounds_flat;

CREATE MATERIALIZED VIEW mv_casino_rounds_flat AS
SELECT
    r."UniqueId"                          AS unique_id,
    r."CustomerId"                        AS customer_id,
    r."CompanyId"                         AS company_id,
    r."CasinoProviderId"                  AS casino_provider_id,
    r."ExternalProviderId"                AS external_provider_id,
    (r."GameInfo")."GameId"               AS game_id,
    (r."GameInfo")."ProviderGameCode"     AS provider_game_code,
    (r."GameInfo")."GameType"             AS game_type,
    (r."GameInfo")."IsLive"               AS is_live,
    (r."RoundInfo")."GameRoundRef"        AS game_round_ref,
    to_timestamp(
        ((r."RoundInfo")."RoundCreated")."seconds"
      + ((r."RoundInfo")."RoundCreated")."nanos" / 1e9
    )                                     AS round_created,
    to_timestamp(
        ((r."RoundInfo")."RoundEnded")."seconds"
      + ((r."RoundInfo")."RoundEnded")."nanos" / 1e9
    )                                     AS round_ended,
    array_length((r."RoundInfo")."Messages") AS num_messages,
    r."IsBonusLockedOnFatMessageCreation" AS is_bonus_locked
FROM src_casino_rounds_proto AS r;

CREATE TABLE rw_managed_casino_rounds (
    unique_id              VARCHAR,
    customer_id            INT,
    company_id             INT,
    casino_provider_id     INT,
    external_provider_id   INT,
    game_id                INT,
    provider_game_code     VARCHAR,
    game_type              INT,
    is_live                BOOLEAN,
    game_round_ref         VARCHAR,
    round_created          TIMESTAMPTZ,
    round_ended            TIMESTAMPTZ,
    num_messages           INT,
    is_bonus_locked        BOOLEAN,
    PRIMARY KEY (unique_id)
) ENGINE = iceberg;

CREATE SINK rw_managed_casino_rounds_sink
INTO rw_managed_casino_rounds
FROM mv_casino_rounds_flat
WITH (
    type = 'upsert',
    primary_key = 'unique_id',
    enable_compaction = 'true',
    compaction_interval_sec = '60',
    enable_snapshot_expiration = 'true',
    commit_checkpoint_interval = 5
);

-- --- (company, game) volume rollup -------------------------------------------
DROP SINK IF EXISTS rw_managed_casino_volume_sink;
DROP TABLE IF EXISTS rw_managed_casino_volume;

CREATE TABLE rw_managed_casino_volume (
    company_id          INT,
    game_id             INT,
    game_type           INT,
    rounds              BIGINT,
    transactions        BIGINT,
    total_amount        NUMERIC,
    total_token_amount  NUMERIC,
    avg_fx_to_eur       NUMERIC,
    PRIMARY KEY (company_id, game_id, game_type)
) ENGINE = iceberg;

CREATE SINK rw_managed_casino_volume_sink
INTO rw_managed_casino_volume
FROM mv_casino_volume_by_company_game
WITH (
    type = 'upsert',
    primary_key = 'company_id,game_id,game_type',
    enable_compaction = 'true',
    compaction_interval_sec = '60',
    enable_snapshot_expiration = 'true',
    commit_checkpoint_interval = 5
);

\echo ''
\echo '=== managed iceberg sinks created ==='
SELECT name, connector, sink_type
FROM rw_catalog.rw_sinks
WHERE name IN ('rw_managed_casino_rounds_sink', 'rw_managed_casino_volume_sink');
\echo ''
\echo 'Rows land in iceberg after the first commit (~5s).'
\echo 'Manual checks:'
\echo '   SELECT count(*) FROM rw_managed_casino_rounds;'
\echo '   SELECT count(*) FROM rw_managed_casino_volume;'
