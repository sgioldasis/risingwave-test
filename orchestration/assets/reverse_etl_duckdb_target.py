"""The demo's DuckDB target: a consumer of the sync's Kafka topic that mirrors the source's current
rows into a local DuckDB file, upserted by the sync's key column (plan section 12, option A — targets
are consumers of the topic; the sync itself writes to Kafka and its state, and nothing else).

The mirror is the topic's latest event per key: Debezium `op` c/u/r upsert the `after` payload, `op`
`d` removes the key. The table is named after the topic (the Jobotron consume asset's precedent), its
columns are the union of the fields the events carried — the current schema's first, then any a drop
left behind — all VARCHAR, since a JSON event carries no types. Like the Jobotron's consumer it is
stateless and re-reads the whole topic per run (demo volumes), draining against the broker's
watermarks before mirroring.

The file is local, so the asset creates it on first run — but DuckDB allows one writer: keep external
readers (DBeaver) read-only or short-lived, one target per file.
"""

import json

from confluent_kafka import TopicPartition
from confluent_kafka.schema_registry.avro import AvroDeserializer
from confluent_kafka.serialization import MessageField, SerializationContext
from dagster import AssetExecutionContext, AssetKey, MetadataValue, asset

from . import reverse_etl_connections as connections
from .reverse_etl_config import DuckdbTargetConfig

_DRAIN_ROUNDS = 10


def build_duckdb_target_asset(cfg: DuckdbTargetConfig):
    """One asset: the topic's mirror in the DuckDB file at cfg.duckdb_target_path."""
    table = cfg.kafka_topic  # the table mirrors the topic

    @asset(
        name=cfg.duckdb_asset,
        group_name=cfg.group_name,
        deps=[AssetKey(dep) for dep in cfg.deps],
        description=(
            f"Mirror {cfg.kafka_topic} into the DuckDB file {cfg.duckdb_target_path}: the topic's "
            f"latest event per {cfg.key_column}, deletes removed. A local consumer of the sync's "
            "topic (plan section 12, option A) — the sync itself does not write this file."
        ),
    )
    def duckdb_target(context: AssetExecutionContext):
        import duckdb

        kafka, registry = connections.kafka(), connections.registry()

        # Read the whole topic from the beginning, no commits: the consumer is stateless and
        # rebuilds the mirror per run. Draining is verified against the broker's watermarks --
        # a cold consumer's first quiet window is a race, not a fact (the Jobotron consume
        # asset learned this live).
        key_deserializer = AvroDeserializer(registry.client(cfg.schema_registry_url)) if cfg.encoding == "avro" else None
        value_deserializer = AvroDeserializer(registry.client(cfg.schema_registry_url)) if cfg.encoding == "avro" else None

        def decode(field: str, message) -> dict:
            if cfg.encoding == "avro":
                deserializer = key_deserializer if field == "key" else value_deserializer
                message_field = MessageField.KEY if field == "key" else MessageField.VALUE
                return deserializer(getattr(message, field)(), SerializationContext(cfg.kafka_topic, message_field))
            return json.loads(getattr(message, field)())["payload"]

        consumer = kafka.consumer(cfg.consumer_group, {"auto.offset.reset": "earliest", "enable.auto.commit": "false"})
        events: dict = {}
        columns: list[str] = []
        read = 0
        try:
            consumer.assign([TopicPartition(cfg.kafka_topic, 0)])
            for _ in range(_DRAIN_ROUNDS):
                while True:
                    batch = consumer.consume(num_messages=500, timeout=5.0)
                    if not batch:
                        break
                    for message in batch:
                        if message.error():
                            continue
                        envelope = decode("value", message)
                        key = decode("key", message)
                        key = key[cfg.key_column] if isinstance(key, dict) else key
                        read += 1
                        if envelope["op"] == "d":
                            events.pop(key, None)  # the key's tombstone: the row is gone
                            continue
                        after = envelope["after"]
                        # Current schema's fields first, then any a drop left behind.
                        columns = list(after) + [name for name in columns if name not in after]
                        events[key] = after
                lo, hi = consumer.get_watermark_offsets(TopicPartition(cfg.kafka_topic, 0), timeout=10.0, cached=False)
                if hi - lo <= read:
                    break
                context.log.info(f"{cfg.kafka_topic} not fully drained yet ({read}/{hi - lo}) -- reading again")
        finally:
            consumer.close()

        # Mirror the events into the file: one VARCHAR column per field the events carried,
        # upsert by key, deletes removed above. The table gains a column when an event
        # carries a field it has never seen (the source's schema evolves between runs).
        connection = duckdb.connect(cfg.duckdb_target_path)
        try:
            connection.execute(
                f"CREATE TABLE IF NOT EXISTS {table} (\"{cfg.key_column}\" VARCHAR PRIMARY KEY)"
            )
            existing = {
                row[0]
                for row in connection.execute("SELECT column_name FROM duckdb_columns() WHERE table_name = ?", [table]).fetchall()
            }
            for name in columns:
                if name not in existing:
                    connection.execute(f'ALTER TABLE {table} ADD COLUMN "{name}" VARCHAR')
            for key, after in events.items():
                connection.execute(f"DELETE FROM {table} WHERE \"{cfg.key_column}\" = ?", [str(key)])
                fields = [name for name in after if name != cfg.key_column]
                names = [cfg.key_column, *fields]
                placeholders = ", ".join("?" for _ in names)
                connection.execute(
                    f"INSERT INTO {table} ({', '.join(f'\"{name}\"' for name in names)}) VALUES ({placeholders})",
                    [str(key), *[str(after[name]) for name in fields]],
                )
        finally:
            connection.close()
        context.log.info(f"Mirrored {len(events)} row(s) from {read} event(s) into {cfg.duckdb_target_path} ({table})")

        context.add_output_metadata(
            {
                "rows": MetadataValue.int(len(events)),
                "events_read": MetadataValue.int(read),
                "duckdb_file": MetadataValue.text(cfg.duckdb_target_path),
                "table": MetadataValue.text(table),
            }
        )

    return duckdb_target
