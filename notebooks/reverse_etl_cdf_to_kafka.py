# Databricks notebook source
# Reverse-ETL CDF -> Kafka (APR-233), PySpark version of the Dagster asset
# reverse_etl_cdf_to_kafka (orchestration/assets/reverse_etl_cdf_setup.py).
#
# Reads the source table's Change Data Feed since the last watermark, turns it into
# Debezium-style before/after/op events, produces them to Kafka as Kafka Connect JSON
# with the schema embedded in every message (the same bytes the Dagster asset sends),
# then advances the watermark.
#
# Differences from the Dagster asset:
#   - It does not add new columns to the RisingWave table (a notebook cannot reach the
#     local RisingWave). After adding a column in Databricks, run
#     ALTER TABLE reverse_etl_<label>_target ADD COLUMN ... in RisingWave *before*
#     running this notebook, or RisingWave leaves that column NULL for the new rows.
#   - The source and state tables must already exist (reverse_etl_<label>_table_setup).
#
# One notebook serves every sync: set the `label` widget (for example cdf or orders) and
# the table, watermark and topic names are derived from it.
#
# Kafka credentials come from a Databricks secret scope, never from the notebook.
# The cluster needs a network path to the Kafka brokers.

# COMMAND ----------

# The names are derived from `label` exactly as ReverseEtlSyncConfig.for_name does in
# orchestration/assets/reverse_etl_config.py (reverse_etl_<label>_<role>). This notebook runs in
# Databricks and cannot import it, so change both together. The four override widgets below
# are normally left empty; fill one in only for a sync that overrides that name in its defs.yaml.
dbutils.widgets.text("label", "cdf")
dbutils.widgets.text("catalog", "de_dev")
dbutils.widgets.text("schema", "sr_poc_external")
dbutils.widgets.text("source_table", "")  # default reverse_etl_<label>_source
dbutils.widgets.text("state_table", "")   # default reverse_etl_<label>_state
dbutils.widgets.text("sync_name", "")     # default reverse_etl_<label>
dbutils.widgets.text("kafka_topic", "")   # default reverse_etl_<label>_topic
dbutils.widgets.text("kafka_bootstrap", "stg-ocp-kfk01-bootstrap.kaizengaming.net:9096")
dbutils.widgets.text("secret_scope", "rw_poc")
dbutils.widgets.text("secret_key_username", "kafka_output_username")
dbutils.widgets.text("secret_key_password", "kafka_output_password")
dbutils.widgets.text("backfill_from_version", "")  # optional first-run override

CATALOG = dbutils.widgets.get("catalog")
SCHEMA = dbutils.widgets.get("schema")
LABEL = dbutils.widgets.get("label").strip()
if not LABEL:
    raise ValueError("The label widget is required (for example cdf or orders)")
BASE = f"reverse_etl_{LABEL}"
SOURCE_TABLE = dbutils.widgets.get("source_table").strip() or f"{BASE}_source"
STATE_TABLE = dbutils.widgets.get("state_table").strip() or f"{BASE}_state"
SYNC_NAME = dbutils.widgets.get("sync_name").strip() or BASE
KAFKA_TOPIC = dbutils.widgets.get("kafka_topic").strip() or f"{BASE}_topic"
KAFKA_BOOTSTRAP = dbutils.widgets.get("kafka_bootstrap")
SECRET_SCOPE = dbutils.widgets.get("secret_scope")
BACKFILL_FROM = dbutils.widgets.get("backfill_from_version").strip()

SOURCE = f"{CATALOG}.{SCHEMA}.{SOURCE_TABLE}"
STATE = f"{CATALOG}.{SCHEMA}.{STATE_TABLE}"
KEY_COLUMNS = ["rid"]  # identity column on the source table; must match key_column in the sync's defs.yaml (default rid)

# UTC so timestamps render as 2026-10-03T03:41:08.345Z, as in the Dagster messages.
spark.conf.set("spark.sql.session.timeZone", "UTC")

# COMMAND ----------

import json
from itertools import zip_longest

from pyspark.sql import functions as F
from pyspark.sql import types as T

CHANGE_TYPE = "_change_type"
COMMIT_VERSION = "_commit_version"
COMMIT_TIMESTAMP = "_commit_timestamp"
CDF_COLUMNS = (CHANGE_TYPE, COMMIT_VERSION, COMMIT_TIMESTAMP)

# Same envelope name convention as the Dagster asset (what the Debezium sink recognises).
ENVELOPE_SCHEMA_NAME = f"{SYNC_NAME}.{SCHEMA}.{SOURCE_TABLE}"

# Databricks SQL type -> Kafka Connect type; everything else travels as a string.
CONNECT_TYPE = {
    "TINYINT": "int8", "SMALLINT": "int16", "INT": "int32", "INTEGER": "int32",
    "BIGINT": "int64", "LONG": "int64", "FLOAT": "float", "DOUBLE": "double", "BOOLEAN": "boolean",
    "TIMESTAMP": "zoned_timestamp",
}

# "zoned_timestamp" is not a Connect type: it marks an ISO-8601 string with a timezone whose schema field
# is named io.debezium.time.ZonedTimestamp, so the Debezium sink creates a timestamptz column.
ZONED_TIMESTAMP = "zoned_timestamp"
ZONED_TIMESTAMP_SCHEMA_NAME = "io.debezium.time.ZonedTimestamp"

# COMMAND ----------

# Live columns of the source table, so a column added in Databricks flows into the
# message schema (and from there into Postgres) with no code change.
columns_df = spark.sql(
    f"SELECT column_name, data_type, is_nullable FROM {CATALOG}.information_schema.columns "
    f"WHERE table_schema = '{SCHEMA}' AND table_name = '{SOURCE_TABLE}' ORDER BY ordinal_position"
)
ROW_FIELDS = [
    (r.column_name, CONNECT_TYPE.get(str(r.data_type).upper(), "string"), str(r.is_nullable).upper() != "NO")
    for r in columns_df.collect()
]
if not ROW_FIELDS:
    raise RuntimeError(f"No columns found for {SOURCE}")
print("Columns:", ROW_FIELDS)

# COMMAND ----------

# Watermark and first-run decision (same logic as the asset).
state_rows = spark.sql(f"SELECT last_commit_version FROM {STATE} WHERE sync_name = '{SYNC_NAME}'").collect()
last_version = int(state_rows[0].last_commit_version) if state_rows else None

history = spark.sql(f"DESCRIBE HISTORY {SOURCE}")
current_version = history.agg(F.max("version")).first()[0]
earliest_version = history.agg(F.min("version")).first()[0]

if last_version is None:
    if BACKFILL_FROM:
        start_version = int(BACKFILL_FROM)
    elif earliest_version == 0:
        start_version = 0  # full history intact: back-fill from the beginning
    else:
        start_version = None  # history aged out: baseline at the current version, sync nothing
else:
    start_version = last_version + 1

print(f"last_version={last_version} current_version={current_version} start_version={start_version}")

# COMMAND ----------

def as_json_friendly(df):
    """Render every column as the Statement Execution API did: timestamps as
    ISO-8601 UTC strings, other non-primitive types as strings; numbers and
    booleans stay typed (they are coerced again below)."""
    exprs = []
    for field in df.schema.fields:
        c = F.col(field.name)
        if isinstance(field.dataType, T.TimestampType):
            c = F.date_format(c, "yyyy-MM-dd'T'HH:mm:ss.SSS'Z'")
        elif not isinstance(field.dataType, (T.NumericType, T.BooleanType, T.StringType)):
            c = c.cast("string")
        exprs.append(c.alias(field.name))
    return df.select(*exprs)


if start_version is not None and start_version <= current_version:
    # ORDER BY _commit_version: commits for one key must go out oldest-to-newest.
    changes = as_json_friendly(
        spark.sql(f"SELECT * FROM table_changes('{SOURCE}', {start_version}) ORDER BY {COMMIT_VERSION}")
    )
    rows = [r.asDict() for r in changes.collect()]
else:
    rows = []  # nothing new (or baselining): table_changes() errors past the latest version
print(f"{len(rows)} CDF row(s)")

# COMMAND ----------

def row_without_cdf_columns(row):
    return {k: v for k, v in row.items() if k not in CDF_COLUMNS}


def to_debezium_events(rows, key_columns):
    """One Debezium-style envelope per logical change. CDF's update_preimage /
    update_postimage pair (same key and commit version) collapses into one event;
    pairing is positional so duplicate-keyed rows still yield one event each."""
    groups, order = {}, []
    for row in rows:
        group_key = (tuple(row.get(c) for c in key_columns), row.get(COMMIT_VERSION))
        if group_key not in groups:
            groups[group_key] = {}
            order.append(group_key)
        groups[group_key].setdefault(row.get(CHANGE_TYPE), []).append(row)

    events = []
    for group_key in order:
        group = groups[group_key]
        any_row = next(iter(next(iter(group.values()))))
        source = {"commit_version": group_key[1], "commit_timestamp": any_row.get(COMMIT_TIMESTAMP)}
        for r in group.get("insert", []):
            events.append({"before": None, "after": row_without_cdf_columns(r), "op": "c", "source": source})
        for r in group.get("delete", []):
            events.append({"before": row_without_cdf_columns(r), "after": None, "op": "d", "source": source})
        for pre, post in zip_longest(group.get("update_preimage", []), group.get("update_postimage", [])):
            events.append({
                "before": row_without_cdf_columns(pre) if pre is not None else None,
                "after": row_without_cdf_columns(post) if post is not None else None,
                "op": "u",
                "source": source,
            })
    return events


def coerce(value, connect_type):
    if value is None:
        return None
    if connect_type.startswith("int"):
        return int(value)
    if connect_type in ("float", "double"):
        return float(value)
    if connect_type == "boolean":
        return value if isinstance(value, bool) else str(value).lower() == "true"
    return value


def coerce_row(row):
    if row is None:
        return None
    return {name: coerce(row.get(name), ctype) for name, ctype, _ in ROW_FIELDS}


def connect_field(name, connect_type, optional):
    if connect_type == ZONED_TIMESTAMP:
        return {"field": name, "type": "string", "name": ZONED_TIMESTAMP_SCHEMA_NAME, "optional": optional}
    return {"field": name, "type": connect_type, "optional": optional}


def row_schema(optional):
    return {
        "type": "struct",
        "name": f"{ENVELOPE_SCHEMA_NAME}.Value",
        "optional": optional,
        "fields": [connect_field(n, t, o) for n, t, o in ROW_FIELDS],
    }


FIELD_TYPES = {n: t for n, t, _ in ROW_FIELDS}


def build_message(event):
    """(key, value) as Kafka Connect JSON with the schema embedded in every message."""
    row_for_key = event.get("after") or event.get("before") or {}
    key = {
        "schema": {
            "type": "struct", "name": f"{ENVELOPE_SCHEMA_NAME}.Key", "optional": False,
            "fields": [{"field": c, "type": FIELD_TYPES[c], "optional": False} for c in KEY_COLUMNS],
        },
        "payload": {c: coerce(row_for_key.get(c), FIELD_TYPES[c]) for c in KEY_COLUMNS},
    }
    value_schema = {
        "type": "struct",
        "name": f"{ENVELOPE_SCHEMA_NAME}.Envelope",
        "optional": False,
        "fields": [
            {"field": "before", **row_schema(True)},
            {"field": "after", **row_schema(True)},
            {"field": "op", "type": "string", "optional": False},
            {
                "field": "source", "type": "struct", "optional": True,
                "fields": [
                    {"field": "commit_version", "type": "int64", "optional": True},
                    {"field": "commit_timestamp", "type": "string", "optional": True},
                ],
            },
        ],
    }
    source = event.get("source") or {}
    payload = {
        "before": coerce_row(event.get("before")),
        "after": coerce_row(event.get("after")),
        "op": event["op"],
        "source": {
            "commit_version": coerce(source.get("commit_version"), "int64"),
            "commit_timestamp": source.get("commit_timestamp"),
        },
    }
    return (
        json.dumps(key, default=str, sort_keys=True),
        json.dumps({"schema": value_schema, "payload": payload}, default=str),
    )


events = to_debezium_events(rows, KEY_COLUMNS)
messages = [build_message(e) for e in events]
op_counts = {}
for e in events:
    op_counts[e["op"]] = op_counts.get(e["op"], 0) + 1
print(f"{len(events)} event(s): {op_counts}")

# COMMAND ----------

# Produce. A failed write raises, so the watermark below is not advanced and the
# next run re-sends (the sink and RisingWave upsert by key, so replays are harmless).
if messages:
    username = dbutils.secrets.get(SECRET_SCOPE, dbutils.widgets.get("secret_key_username"))
    password = dbutils.secrets.get(SECRET_SCOPE, dbutils.widgets.get("secret_key_password"))
    # The Kafka client in Databricks Runtime is shaded: note the kafkashaded. prefix.
    jaas = (
        'kafkashaded.org.apache.kafka.common.security.scram.ScramLoginModule required '
        f'username="{username}" password="{password}";'
    )
    # idx + sortWithinPartitions keeps each key's events in commit order; Kafka's key
    # hashing then keeps one key on one partition.
    out = (
        spark.createDataFrame([(i, k, v) for i, (k, v) in enumerate(messages)], "idx long, key string, value string")
        .repartition("key")
        .sortWithinPartitions("idx")
        .select("key", "value")
    )
    (
        out.write.format("kafka")
        .option("kafka.bootstrap.servers", KAFKA_BOOTSTRAP)
        .option("kafka.security.protocol", "SASL_SSL")
        .option("kafka.sasl.mechanism", "SCRAM-SHA-512")
        .option("kafka.sasl.jaas.config", jaas)
        .option("topic", KAFKA_TOPIC)
        .save()
    )
    print(f"Produced {len(messages)} message(s) to {KAFKA_TOPIC}")

# COMMAND ----------

# Advance the watermark: highest commit version seen, else the current version on a
# first run with nothing to sync, else leave it unchanged.
versions = [int(r[COMMIT_VERSION]) for r in rows if r.get(COMMIT_VERSION) is not None]
if versions:
    new_version = max(versions)
elif last_version is None:
    new_version = current_version
else:
    new_version = last_version

spark.sql(f"""
    MERGE INTO {STATE} AS target
    USING (SELECT '{SYNC_NAME}' AS sync_name, {new_version} AS last_commit_version) AS source
    ON target.sync_name = source.sync_name
    WHEN MATCHED THEN UPDATE SET target.last_commit_version = source.last_commit_version
    WHEN NOT MATCHED THEN INSERT (sync_name, last_commit_version)
        VALUES (source.sync_name, source.last_commit_version)
""")
print(f"Watermark: {last_version} -> {new_version}")

# COMMAND ----------

# Show what was sent: the first 3 messages exactly as produced, schema included.
for key, value in messages[:3]:
    print(json.dumps({"key": json.loads(key), "value": json.loads(value)}, indent=2))
