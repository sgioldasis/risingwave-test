{{ config(
    materialized='materialized_view',
    tags=['casino_uc1']
) }}

SELECT
    s."CustomerId"                                        AS customer_id,
    msg."MessageTypeId"                                   AS message_type_id,
    TO_TIMESTAMP((msg."Created").seconds)                 AS message_created_at,
    txn."TransactionId"                                   AS transaction_id,
    txn."AccountId"                                       AS account_id,
    txn."CurrencyId"                                      AS currency_id,
    TO_TIMESTAMP((txn."Created").seconds)                 AS transaction_created_at,
    ABS(NULLIF(txn."Amount", '')::numeric)                AS amount_abs,
    txn."Amount"                                          AS amount_raw,
    txn."BonusAction"                                     AS bonus_action,
    (s."GameInfo")."GameId"                               AS game_id,
    (s."GameInfo")."GameType"                             AS game_type,
    (s."GameInfo")."IsLive"                               AS is_live,
    s."CompanyId"                                         AS company_id
FROM
    {{ ref('src_casino_prd') }}                AS s,
    UNNEST((s."RoundInfo")."Messages")             AS msg,
    UNNEST(msg."Transactions")                     AS txn
