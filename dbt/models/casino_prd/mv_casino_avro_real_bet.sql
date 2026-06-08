{{ config(
    materialized='materialized_view',
    tags=['casino_avro', 'casino_uc1']
) }}

-- UC1 (Real Bet Amount) sourced from the Avro round-trip topic instead of the
-- production Protobuf topic. Mirrors mv_casino_real_bet logic — same double-UNNEST,
-- same filter — to validate end-to-end Avro encode/decode fidelity.
SELECT
    s."CustomerId"                                   AS customer_id,
    txn."CurrencyId"                                 AS currency_id,
    SUM(ABS(NULLIF(txn."Amount", '')::numeric)) OVER (
        PARTITION BY s."CustomerId", txn."CurrencyId"
        ORDER BY TO_TIMESTAMP((txn."Created").seconds)
        RANGE BETWEEN INTERVAL '300 SECONDS' PRECEDING AND CURRENT ROW
    )                                                AS rolling_5min_real_bet_amount,
    TO_TIMESTAMP((txn."Created").seconds)            AS event_ts
FROM {{ ref('src_casino_avro') }}                AS s,
    UNNEST((s."RoundInfo")."Messages")           AS msg,
    UNNEST(msg."Transactions")                   AS txn
WHERE msg."MessageTypeId" = 1
