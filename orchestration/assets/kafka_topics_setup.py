"""Ensure required output Kafka topics exist on the configured broker."""
import os
from confluent_kafka.admin import AdminClient, NewTopic
from dagster import AssetExecutionContext, MetadataValue, asset

OUTPUT_TOPICS = [
    "rw_poc_casino_out_avro",
    "rw_poc_casino_out_real_bet",
    "rw_poc_casino_out_turnover_percentage",
    "cronus.casino.out.br.replay",
    "bets-out-br-replay",
]

_DEFAULT_PARTITIONS = 1
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
    description="Create required output Kafka topics on the configured broker if they do not already exist.",
)
def kafka_output_topics_setup(context: AssetExecutionContext):
    admin = _admin_client()

    existing = set(admin.list_topics(timeout=15).topics.keys())
    context.log.info(f"Broker reports {len(existing)} existing topics")

    to_create = [t for t in OUTPUT_TOPICS if t not in existing]
    already_exist = [t for t in OUTPUT_TOPICS if t in existing]

    if already_exist:
        context.log.info(f"Already exist: {already_exist}")

    if not to_create:
        context.log.info("All output topics already exist — nothing to do")
        return {
            "created": MetadataValue.json([]),
            "already_existed": MetadataValue.json(already_exist),
        }

    new_topics = [
        NewTopic(
            topic,
            num_partitions=_DEFAULT_PARTITIONS,
            replication_factor=_DEFAULT_REPLICATION,
        )
        for topic in to_create
    ]

    futures = admin.create_topics(new_topics)
    created, failed = [], []

    for topic, future in futures.items():
        try:
            future.result()
            context.log.info(f"Created topic: {topic}")
            created.append(topic)
        except Exception as e:
            context.log.error(f"Failed to create topic {topic}: {e}")
            failed.append({"topic": topic, "error": str(e)})

    if failed:
        raise RuntimeError(f"Failed to create {len(failed)} topic(s): {failed}")

    return {
        "created": MetadataValue.json(created),
        "already_existed": MetadataValue.json(already_exist),
    }
