"""Ensure required output Kafka topics exist on the configured broker."""
import os
from confluent_kafka.admin import AdminClient, NewPartitions, NewTopic
from dagster import AssetExecutionContext, MetadataValue, asset

OUTPUT_TOPICS = [
    "rw_poc_casino_out_avro",
    "rw_poc_casino_out_real_bet",
    "rw_poc_casino_out_turnover_percentage",
    "cronus.casino.out.br.replay",
    "bets-out-br-replay",
]

_DEFAULT_PARTITIONS = 15
_DEFAULT_REPLICATION = 1


def _admin_client() -> AdminClient:
    bootstrap = os.environ.get("KAFKA_OUTPUT_BOOTSTRAP", "")
    if not bootstrap:
        raise ValueError("KAFKA_OUTPUT_BOOTSTRAP must be set in .env")

    conf: dict = {"bootstrap.servers": bootstrap}

    username = os.environ.get("KAFKA_OUTPUT_SASL_USERNAME", "")
    if username:
        conf.update(
            {
                "security.protocol": "SASL_SSL",
                "sasl.mechanism": os.environ.get("KAFKA_OUTPUT_SASL_MECHANISM", "SCRAM-SHA-512"),
                "sasl.username": username,
                "sasl.password": os.environ.get("KAFKA_OUTPUT_SASL_PASSWORD", ""),
            }
        )

    return AdminClient(conf)


@asset(
    group_name="casino_prd_setup",
    description="Create required output Kafka topics on the configured broker if they do not already exist, and increase partition count on existing topics if below the desired count.",
)
def kafka_output_topics_setup(context: AssetExecutionContext):
    admin = _admin_client()

    cluster_metadata = admin.list_topics(timeout=15)
    existing = cluster_metadata.topics
    context.log.info(f"Broker reports {len(existing)} existing topics")

    to_create = [t for t in OUTPUT_TOPICS if t not in existing]
    to_alter = [
        t for t in OUTPUT_TOPICS
        if t in existing and len(existing[t].partitions) < _DEFAULT_PARTITIONS
    ]
    already_ok = [
        t for t in OUTPUT_TOPICS
        if t in existing and len(existing[t].partitions) >= _DEFAULT_PARTITIONS
    ]

    if already_ok:
        context.log.info(f"Already at target partitions: {already_ok}")

    created, altered, failed = [], [], []

    if to_create:
        new_topics = [
            NewTopic(
                topic,
                num_partitions=_DEFAULT_PARTITIONS,
                replication_factor=_DEFAULT_REPLICATION,
            )
            for topic in to_create
        ]
        for topic, future in admin.create_topics(new_topics).items():
            try:
                future.result()
                context.log.info(f"Created topic: {topic}")
                created.append(topic)
            except Exception as e:
                context.log.error(f"Failed to create topic {topic}: {e}")
                failed.append({"topic": topic, "error": str(e)})

    if to_alter:
        new_partitions = [NewPartitions(topic, _DEFAULT_PARTITIONS) for topic in to_alter]
        for topic, future in admin.create_partitions(new_partitions).items():
            try:
                future.result()
                context.log.info(f"Increased partitions to {_DEFAULT_PARTITIONS}: {topic}")
                altered.append(topic)
            except Exception as e:
                context.log.error(f"Failed to alter partitions for {topic}: {e}")
                failed.append({"topic": topic, "error": str(e)})

    if failed:
        raise RuntimeError(f"Failed to create/alter {len(failed)} topic(s): {failed}")

    return {
        "created": MetadataValue.json(created),
        "altered": MetadataValue.json(altered),
        "already_ok": MetadataValue.json(already_ok),
    }
