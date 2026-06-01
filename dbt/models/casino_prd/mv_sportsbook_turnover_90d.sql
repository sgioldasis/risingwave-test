{{ config(
    materialized='materialized_view',
    tags=['casino_uc2']
) }}

SELECT
    ("CustomerInfo")."Id"                                                        AS customer_id,
    TO_TIMESTAMP(("PlacedAt").seconds)                                           AS event_ts,
    SUM((("TotalStake")."Euro")."units"::NUMERIC
        + (("TotalStake")."Euro")."nanos"::NUMERIC / 1000000000) OVER (
        PARTITION BY ("CustomerInfo")."Id"
        ORDER BY TO_TIMESTAMP(("PlacedAt").seconds)
        -- Demo window: 300s (5 min). Shrunk from 7 days so the rolling-window
        -- state evicts within a short demo run and throughput stays bounded.
        -- See BRAZIL_WORKLOAD_TUNING.md §2 / §13.
        RANGE BETWEEN INTERVAL '300 SECONDS' PRECEDING AND CURRENT ROW
    )                                                                            AS rolling_7d_turnover
FROM {{ ref('src_bets_br') }}
WHERE ("CustomerInfo")."Id" IS NOT NULL
  AND ("TotalStake")."Euro" IS NOT NULL
