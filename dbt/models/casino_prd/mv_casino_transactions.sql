{{ config(
    materialized='materialized_view',
    tags=['casino_uc1']
) }}

-- Transaction-level flat view — shared foundation for UC1 (mv_casino_bets_flat) and UC2
-- (mv_casino_turnover_90d). Double UNNEST: rounds → RoundInfo.Messages[] → Transactions[].
-- Pruned to the 6 columns the downstream MVs actually read (§17 review): dropped 8 unused
-- archive/derived columns (message_created_at, transaction_id, amount_raw, bonus_action,
-- game_id/game_type/is_live, company_id) to cut per-row width, struct-extraction CPU, and
-- state-store I/O across BOTH chains. amount_abs is NULL exactly when Amount was empty/null,
-- so downstream filters use `amount_abs IS NOT NULL` instead of the old amount_raw checks.
SELECT
    s."CustomerId"                                       AS customer_id,
    msg."MessageTypeId"                                  AS message_type_id,
    txn."AccountId"                                      AS account_id,
    txn."CurrencyId"                                     AS currency_id,
    TO_TIMESTAMP((txn."Created").seconds)                AS transaction_created_at,
    ABS(NULLIF(txn."Amount", '')::numeric)               AS amount_abs
FROM
    {{ ref('src_casino_avro') }}               AS s,
    UNNEST((s."RoundInfo")."Messages")         AS msg,
    UNNEST(msg."Transactions")                 AS txn
