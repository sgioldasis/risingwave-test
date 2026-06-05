{{ config(
    materialized='materialized_view',
    tags=['casino_uc1']
) }}

-- Full-fidelity casino transaction MV for the Databricks raw event log.
-- Keeps the 6 core analytical columns flat; everything else goes into a JSON
-- properties bag so new proto fields never require an ALTER TABLE in Databricks.
-- mv_casino_transactions (used by UC1/UC2 aggregation chain) is kept lean — this
-- MV reads directly from src_casino_prd and does not depend on it.
SELECT
    s."CustomerId"                                           AS customer_id,
    msg."MessageTypeId"                                      AS message_type_id,
    txn."AccountId"                                          AS account_id,
    txn."CurrencyId"                                         AS currency_id,
    TO_TIMESTAMP((txn."Created").seconds)                    AS transaction_created_at,
    ABS(NULLIF(txn."Amount", '')::NUMERIC)                   AS amount_abs,
    jsonb_build_object(
        -- Round level
        'company_id',                   s."CompanyId",
        'casino_provider_id',           s."CasinoProviderId",
        'external_provider_id',         s."ExternalProviderId",
        'game_id',                      (s."GameInfo")."GameId",
        'game_type',                    (s."GameInfo")."GameType",
        'is_live',                      (s."GameInfo")."IsLive",
        'provider_game_code',           (s."GameInfo")."ProviderGameCode",
        'round_ref',                    (s."RoundInfo")."GameRoundRef",
        'round_created_at',             TO_TIMESTAMP(((s."RoundInfo")."RoundCreated").seconds),
        -- Message level
        'message_id',                   msg."MessageId",
        'session_id',                   msg."SessionId",
        'token_type_id',                msg."TokenTypeId",
        'jackpot_win_amount',           NULLIF(msg."JackpotWinAmount", '')::NUMERIC,
        'jackpot_contribution_amount',  NULLIF(msg."JackpotContributionAmount", '')::NUMERIC,
        'is_round_closed',              msg."IsRoundClosed",
        -- Transaction level
        'transaction_id',               txn."TransactionId",
        'currency_rate_to_euro',        NULLIF(txn."CurrencyRateToEuro", '')::NUMERIC,
        'source_id',                    txn."SourceId",
        'bonus_action',                 txn."BonusAction",
        'pandora_journey_id',           txn."PandoraJourneyId",
        'customer_campaign_id',         txn."CustomerCampaignId",
        'campaign_id',                  txn."CampaignId",
        'campaign_type_id',             txn."CampaignTypeId",
        'transaction_type_id',          txn."TransactionTypeId"
    )::TEXT                                                  AS properties
FROM
    {{ ref('src_casino_prd') }}                AS s,
    UNNEST((s."RoundInfo")."Messages")         AS msg,
    UNNEST(msg."Transactions")                 AS txn
