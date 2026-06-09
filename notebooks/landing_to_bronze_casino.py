# Databricks notebook source
# Landing → Bronze Re-processing: Casino Transactions
#
# Reads raw Protobuf bytes from rw_casino_landing and decodes them into a typed
# bronze table that mirrors rw_casino_transactions.
#
# To re-process after a proto schema change:
#   1. Upload the corrected descriptor to /Volumes/de_dev/rw_poc/proto/casinoroundinfodto.pb
#   2. Re-run this notebook

# COMMAND ----------

from pyspark.sql.functions import col, explode, abs as spark_abs, when, to_json, struct, lit
from pyspark.sql.protobuf.functions import from_protobuf

DESCRIPTOR_PATH = "/Volumes/de_dev/rw_poc/proto/casinoroundinfodto.pb"
MESSAGE_NAME    = "Cronus.CasinoService.RoundInfo.Abstractions.CasinoRoundInfoDto"
LANDING_TABLE   = "de_dev.rw_poc.rw_casino_landing"
BRONZE_TABLE    = "de_dev.rw_poc.rw_casino_landing_bronze"

with open(DESCRIPTOR_PATH, "rb") as f:
    descriptor_bytes = f.read()

print(f"Descriptor loaded: {len(descriptor_bytes):,} bytes")

# COMMAND ----------

# Read raw landing bytes and show sample
df_raw = spark.table(LANDING_TABLE)
print(f"Landing rows: {df_raw.count():,}")
display(df_raw.limit(3))

# COMMAND ----------

# Decode Protobuf bytes into a nested struct
# Note: google.protobuf.Timestamp fields (Created, RoundCreated) decode as TimestampType on DBR 13.3+
df_decoded = df_raw.select(
    from_protobuf(
        col("payload"),
        MESSAGE_NAME,
        binaryDescriptorSet=descriptor_bytes
    ).alias("msg")
)

# COMMAND ----------

# Explode Messages[] from RoundInfo
df_messages = df_decoded.select(
    col("msg.CustomerId").alias("customer_id"),
    col("msg.CompanyId").alias("company_id"),
    col("msg.CasinoProviderId").alias("casino_provider_id"),
    col("msg.ExternalProviderId").alias("external_provider_id"),
    col("msg.GameInfo").alias("game_info"),
    col("msg.RoundInfo.GameRoundRef").alias("round_ref"),
    col("msg.RoundInfo.RoundCreated").alias("round_created_at"),
    explode("msg.RoundInfo.Messages").alias("message")
)

# Explode Transactions[] from each Message
df_transactions = df_messages.select(
    col("customer_id"),
    col("company_id"),
    col("casino_provider_id"),
    col("external_provider_id"),
    col("game_info"),
    col("round_ref"),
    col("round_created_at"),
    col("message.MessageTypeId").alias("message_type_id"),
    col("message.MessageId").alias("message_id"),
    col("message.SessionId").alias("session_id"),
    col("message.TokenTypeId").alias("token_type_id"),
    col("message.JackpotWinAmount").alias("jackpot_win_amount_raw"),
    col("message.JackpotContributionAmount").alias("jackpot_contribution_raw"),
    col("message.IsRoundClosed").alias("is_round_closed"),
    explode("message.Transactions").alias("txn")
)

# COMMAND ----------

# Build final bronze columns — mirrors rw_casino_transactions schema
df_bronze = df_transactions.select(
    col("customer_id"),
    col("message_type_id"),
    col("txn.AccountId").alias("account_id"),
    col("txn.CurrencyId").alias("currency_id"),
    col("txn.Created").alias("transaction_created_at"),
    spark_abs(
        when(col("txn.Amount") != "", col("txn.Amount").cast("double"))
        .otherwise(lit(None).cast("double"))
    ).alias("amount_abs"),
    to_json(struct(
        col("company_id"),
        col("casino_provider_id"),
        col("external_provider_id"),
        col("game_info.GameId").alias("game_id"),
        col("game_info.GameType").alias("game_type"),
        col("game_info.IsLive").alias("is_live"),
        col("game_info.ProviderGameCode").alias("provider_game_code"),
        col("round_ref"),
        col("round_created_at"),
        col("message_id"),
        col("session_id"),
        col("token_type_id"),
        when(col("jackpot_win_amount_raw") != "", col("jackpot_win_amount_raw").cast("double"))
            .otherwise(None).alias("jackpot_win_amount"),
        when(col("jackpot_contribution_raw") != "", col("jackpot_contribution_raw").cast("double"))
            .otherwise(None).alias("jackpot_contribution_amount"),
        col("is_round_closed"),
        col("txn.TransactionId").alias("transaction_id"),
        when(col("txn.CurrencyRateToEuro") != "", col("txn.CurrencyRateToEuro").cast("double"))
            .otherwise(None).alias("currency_rate_to_euro"),
        col("txn.SourceId").alias("source_id"),
        col("txn.BonusAction").alias("bonus_action"),
        col("txn.PandoraJourneyId").alias("pandora_journey_id"),
        col("txn.CustomerCampaignId").alias("customer_campaign_id"),
        col("txn.CampaignId").alias("campaign_id"),
        col("txn.CampaignTypeId").alias("campaign_type_id"),
        col("txn.TransactionTypeId").alias("transaction_type_id")
    )).alias("properties")
)

# COMMAND ----------

# Write to bronze table — overwrite = full re-process from landing
df_bronze.write \
    .format("iceberg") \
    .mode("overwrite") \
    .saveAsTable(BRONZE_TABLE)

bronze_count = spark.table(BRONZE_TABLE).count()
print(f"✓ Written {bronze_count:,} rows to {BRONZE_TABLE}")

# COMMAND ----------

dbutils.notebook.exit(str(bronze_count))

# COMMAND ----------

# Verify: compare reprocessed bronze vs live bronze from RisingWave
display(spark.sql(f"""
SELECT
    'landing_bronze (reprocessed)' AS source,
    COUNT(*)                        AS rows,
    COUNT(DISTINCT customer_id)     AS unique_customers,
    MIN(transaction_created_at)     AS earliest,
    MAX(transaction_created_at)     AS latest
FROM {BRONZE_TABLE}
UNION ALL
SELECT
    'casino_transactions (live)'    AS source,
    COUNT(*)                        AS rows,
    COUNT(DISTINCT customer_id)     AS unique_customers,
    MIN(transaction_created_at)     AS earliest,
    MAX(transaction_created_at)     AS latest
FROM de_dev.rw_poc.rw_casino_transactions
"""))
