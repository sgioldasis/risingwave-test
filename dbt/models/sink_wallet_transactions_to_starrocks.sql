{#
  Model: sink_wallet_transactions_to_starrocks
  Purpose: Upsert-mode sink into a native StarRocks Primary Key table, for
  the StarRocks upsert/point-lookup demo motivated by the internal PAM
  Operational Query Layer proposal. See docs/SR_POC_WALLET_UPSERT_DEMO.md.

  IMPORTANT: requires the StarRocks-side `wallet_transactions` Primary Key
  table (dbt_starrocks/models/wallet_transactions.sql) to already exist --
  RisingWave's starrocks connector targets an existing table by name, it
  does not create one. Declared as an explicit cross-project Dagster dep
  below (asset_key ['wallet_transactions'] -- dagster-dbt keys models as
  [config.schema, name]; the dbt_starrocks project no longer sets +schema,
  so the key has no schema prefix. This dep is what orders the
  starrocks_unified_dbt_assets step BEFORE this sink in
  starrocks_demo_setup_job -- it previously pointed at a phantom
  ['sr_local_db', 'wallet_transactions'] key, so ordering was never
  enforced and a fresh empty database let the sink get created first,
  failing with "Can't find schema for StarRocks table" (2026-11-17).

  Property names (starrocks.mysqlport / starrocks.httpport, one word, no
  underscore) taken from RisingWave's own worked example in its sink-to-
  StarRocks docs; a separate parameter-table summary elsewhere used
  starrocks.query_port / starrocks.http_port instead. Confirmed live
  2026-09-12: mysqlport/httpport is correct -- the sink got past connector
  validation with these names.

  `event_time` is cast to plain TIMESTAMP (no time zone) in the SELECT
  below -- confirmed live that RisingWave's StarRocks sink flatly rejects
  TIMESTAMP WITH TIME ZONE ("Starrocks doesn't store time values with
  timezone information"), matching StarRocks's own `datetime` column type
  which has no timezone concept.

  `commit_checkpoint_interval = 1`: this is a demo of real-time upsert
  visibility, so commit latency matters. Sink decoupling is on by default
  for all RisingWave sinks (confirmed via `SELECT * FROM
  rw_sink_decouple`), which commits every 10 checkpoints by default --
  with this project's `barrier_interval_ms = 2000` /
  `checkpoint_frequency = 2` (risingwave.toml, i.e. a checkpoint every 4s),
  that's up to ~40s of visibility lag on top of the StarRocks stream-load
  round trip itself. Setting this to 1 commits on every checkpoint instead,
  cutting worst-case lag to roughly one checkpoint interval (~4s in this
  project's config).
#}

{{ config(
    materialized='sink',
    schema='public',
    tags=['starrocks', 'wallet', 'upsert-demo'],
    meta={
        'dagster': {
            'deps': [
                {'asset_key': ['wallet_transactions']}
            ]
        }
    }
) }}

CREATE SINK IF NOT EXISTS sink_wallet_transactions_to_starrocks AS
SELECT
    transaction_id,
    account_id,
    type,
    amount,
    status,
    CAST(event_time AS TIMESTAMP) AS event_time
FROM {{ ref('src_wallet_transactions') }}
WITH (
    connector = 'starrocks',
    type = 'upsert',
    primary_key = 'transaction_id',
    starrocks.host = 'starrocks',
    starrocks.mysqlport = '9030',
    starrocks.httpport = '8030',
    starrocks.user = 'root',
    starrocks.password = '',
    starrocks.database = 'sr_local_db',
    starrocks.table = 'wallet_transactions',
    commit_checkpoint_interval = 1
)
