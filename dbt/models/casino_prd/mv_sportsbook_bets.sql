{{ config(
    materialized='materialized_view',
    tags=['casino_uc2']
) }}

SELECT
    "Id"                                                                             AS bet_id,
    ("CustomerInfo")."Id"                                                            AS customer_id,
    ("CustomerInfo")."SegmentId"                                                     AS customer_segment_id,
    "BetTypeId"                                                                      AS bet_type_id,
    "BetStatusId"                                                                    AS bet_status_id,
    ("BetslipInfo")."ChannelId"                                                      AS channel_id,
    ("TotalStake")."Currency"                                                        AS currency_id,
    TO_TIMESTAMP(("PlacedAt").seconds)                                               AS placed_at,
    (("TotalStake")."Euro")."units"::NUMERIC
        + (("TotalStake")."Euro")."nanos"::NUMERIC / 1000000000                     AS stake_euro,
    (("TotalStake")."Local")."units"::NUMERIC
        + (("TotalStake")."Local")."nanos"::NUMERIC / 1000000000                    AS stake_local,
    jsonb_build_object(
        'operator_id',              "OperatorId",
        'bet_type',                 "BetType",
        'in_play',                  "InPlay",
        'total_odds',               ("TotalOdds")."units"::NUMERIC
                                        + ("TotalOdds")."nanos"::NUMERIC / 1000000000,
        'potential_returns_euro',   (("PotentialReturns")."Euro")."units"::NUMERIC
                                        + (("PotentialReturns")."Euro")."nanos"::NUMERIC / 1000000000,
        'lines',                    "Lines",
        'bonus_type',               "BonusType",
        'is_placement',             "IsPlacement",
        'last_updated',             TO_TIMESTAMP(("LastUpdated").seconds)
    )::TEXT                                                  AS properties
FROM {{ ref('src_bets_br') }}
WHERE ("CustomerInfo")."Id" IS NOT NULL
  AND ("PlacedAt").seconds IS NOT NULL
