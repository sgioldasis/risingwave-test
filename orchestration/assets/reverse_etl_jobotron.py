"""Jobotron demo plumbing (docs/poc/JOBOTRON_CONTRACT_PARITY.md): the setup, consume and
reset assets around the publish instances. The publish asset itself is the vendored
team component -- one `DeltaToKafkaComponent` defs.yaml instance per market
(orchestration/defs/unverified_customers_{cy,gr,ro}/defs.yaml, MR !166's
kaizen-dagster-delta-to-kafka wheel): it runs the candidate-selection query on the SQL
warehouse and publishes one plain-JSON message per selected customer, keyed by
CustomerId, to the instance's topic. What this module keeps is exactly what a
publish-only component does not do:

- setup: create the instance's Kafka topic (create-if-not-exists, one partition;
  a no-op under dry-run -- not even a Kafka connection).
- consume: the demo worker stand-in -- drain the topic (watermark-verified), dedup
  on the Kafka key (CustomerId), mirror one row per customer into Postgres.
- reset: delete the topic and drop the mirror table, guarded by a name allowlist.

Asset/job names are unchanged from the local-producer days, so the demo UI
(scripts/reverse_etl_demo_server.py) drives the same names; only its publish
action materializes the library asset now.
"""

import json
import time
from dataclasses import dataclass

import psycopg2.extras
from confluent_kafka import TopicPartition
from confluent_kafka.admin import NewTopic
from dagster import (
    AssetExecutionContext,
    AssetKey,
    AssetSelection,
    Config,
    Definitions,
    In,
    MetadataValue,
    Nothing,
    OpExecutionContext,
    asset,
    define_asset_job,
    in_process_executor,
    job,
    op,
)

from .kafka_topics_setup import _DEFAULT_REPLICATION
from . import reverse_etl_connections as connections

BASE = "reverse_etl_jobotron"
MARKETS = ("cy", "gr", "ro")


@dataclass(frozen=True)
class JobotronDemoInstance:
    """The demo plumbing around one publish instance's topic. The topic name is the
    one the publish instance's defs.yaml declares (jobotron_unverified_customers_<market>),
    and every Dagster object here derives its name from the instance: `<instance>_setup`,
    `<instance>_consume`, `<instance>_setup_job`, `<instance>_reset_job`."""

    market: str

    @property
    def instance_name(self) -> str:
        return f"jobotron_unverified_customers_{self.market}"

    @property
    def topic(self) -> str:
        return self.instance_name

    @property
    def group_name(self) -> str:
        return self.instance_name

    @property
    def setup_asset(self) -> str:
        return f"{self.instance_name}_setup"

    @property
    def consume_asset(self) -> str:
        return f"{self.instance_name}_consume"

    @property
    def setup_job(self) -> str:
        return f"{self.instance_name}_setup_job"

    @property
    def reset_job(self) -> str:
        return f"{self.instance_name}_reset_job"


class DemoRunMode(Config):
    """The per-run mode switch for the plumbing assets (the demo UI's checkbox). The
    default is the safe one: a blank launch touches nothing."""

    dry_run: bool = True


# --- setup: the topic ------------------------------------------------------------


def build_setup_asset(inst: JobotronDemoInstance):
    @asset(
        name=inst.setup_asset,
        group_name=inst.group_name,
        description=(
            f"Create the publish instance's Kafka topic {inst.topic} (create-if-not-exists, one "
            "partition). Under dry-run (the run-config default) the asset is a no-op -- zero "
            "side effects, not even a Kafka connection."
        ),
    )
    def setup(context: AssetExecutionContext, config: DemoRunMode):
        if config.dry_run:
            # Zero side effects in dry-run: not even a Kafka connection. The topic
            # matters only for runs that actually publish.
            context.log.info(
                f"Dry run: nothing created -- launch setup with dry_run: false in the run config "
                f"to create the topic {inst.topic}"
            )
            context.add_output_metadata(
                {
                    "topic": MetadataValue.text(inst.topic),
                    "created": MetadataValue.bool(False),
                    "dry_run": MetadataValue.bool(True),
                }
            )
            return

        kafka = connections.kafka()
        admin = kafka.admin_client()
        existing = admin.list_topics(timeout=20).topics
        if inst.topic in existing:
            context.log.info(f"Topic {inst.topic} already exists")
        else:
            admin.create_topics(
                [NewTopic(inst.topic, num_partitions=1, replication_factor=_DEFAULT_REPLICATION)]
            )
            context.log.info(f"Created topic {inst.topic}")

        context.add_output_metadata(
            {
                "topic": MetadataValue.text(inst.topic),
                "created": MetadataValue.bool(True),
                "dry_run": MetadataValue.bool(False),
            }
        )

    return setup


# --- consume: the demo worker stand-in -------------------------------------------


def build_consume_asset(inst: JobotronDemoInstance):
    @asset(
        name=inst.consume_asset,
        group_name=inst.group_name,
        description=(
            f"Demo worker stand-in: read every message on {inst.topic}, decode the JSON, dedup "
            "on the Kafka key (CustomerId), and keep one mirror row per customer in the Postgres "
            f"table {inst.topic}. Stands in for the Jobotron worker, which would act per message "
            "-- the publish run republishes the full eligible set, so the key decides which row "
            "a customer keeps."
        ),
    )
    def consume(context: AssetExecutionContext):
        postgres, kafka = connections.state_postgres(), connections.kafka()
        started = time.monotonic()

        # Read the topic from the beginning. The publish run republishes the full
        # eligible set every time, so the mirror accumulates one row per customer ever
        # seen (INSERT ... ON CONFLICT DO NOTHING). Manual partition assign, no commits:
        # the consumer is stateless and re-reads the whole topic per run (demo volumes
        # are a handful of messages; the RFC sizes ~20k/day).
        consumer = kafka.consumer(
            f"{BASE}-demo-consumer", {"auto.offset.reset": "earliest", "enable.auto.commit": "false"}
        )
        values: list[dict] = []
        try:
            consumer.assign([TopicPartition(inst.topic, 0)])
            # Drain the topic fully, verified against the broker's watermarks. A plain
            # "break on the first quiet window" loses the race on a cold consumer (fresh
            # metadata + fetch handshake) and silently rebuilds the table EMPTY -- verified
            # live. Watermarks make "fully drained" a fact, not a timing guess; the outer
            # cap only guards a pathological loop.
            for _ in range(10):
                while True:
                    batch = consumer.consume(num_messages=500, timeout=5.0)
                    if not batch:
                        break
                    for message in batch:
                        if message.error() or message.topic() != inst.topic:
                            continue
                        try:
                            values.append(json.loads(message.value()))
                        except Exception as e:  # noqa: BLE001 -- surface decode failures per message
                            context.log.warning(f"Could not decode a message on {message.topic()}: {e}")
                lo, hi = consumer.get_watermark_offsets(TopicPartition(inst.topic, 0), timeout=10.0, cached=False)
                if hi - lo <= len(values):
                    break
                context.log.info(f"{inst.topic} not fully drained yet ({len(values)}/{hi - lo}) -- reading again")
        finally:
            consumer.close()

        # Dedup on the Kafka key (CustomerId): the last message wins, matching an
        # upsert-style worker. (The old envelope deduped on ItemId, a content hash --
        # the plain-JSON wire has no hash, and the key is the identity the worker acts on.)
        by_customer: dict[str, dict] = {}
        for value in values:
            customer_id = value.get("CustomerId")
            if customer_id is None:
                context.log.warning(f"A message on {inst.topic} has no CustomerId -- skipped")
                continue
            by_customer[str(customer_id)] = value
        mirror_rows = [
            {
                "customer_id": customer_id,
                "company_id": None if value.get("CompanyId") is None else str(value.get("CompanyId")),
                "run_id": value.get("run_id"),
                "processed_at": value.get("processed_at"),
                "payload": psycopg2.extras.Json(value),
            }
            for customer_id, value in by_customer.items()
        ]

        conn = postgres.connect()
        try:
            with conn.cursor() as cur:
                cur.execute(
                    f"CREATE TABLE IF NOT EXISTS {inst.topic} ("
                    "customer_id TEXT PRIMARY KEY, company_id TEXT, run_id TEXT, "
                    "processed_at TEXT, payload JSONB)"
                )
                cur.executemany(
                    f"INSERT INTO {inst.topic} (customer_id, company_id, run_id, processed_at, payload) "
                    "VALUES (%(customer_id)s, %(company_id)s, %(run_id)s, %(processed_at)s, %(payload)s) "
                    "ON CONFLICT (customer_id) DO UPDATE SET "
                    "company_id = EXCLUDED.company_id, run_id = EXCLUDED.run_id, "
                    "processed_at = EXCLUDED.processed_at, payload = EXCLUDED.payload",
                    mirror_rows,
                )
            conn.commit()
        finally:
            conn.close()
        context.log.info(
            f"{inst.topic}: {len(values)} message(s) read, {len(mirror_rows)} customer(s) mirrored"
        )

        context.add_output_metadata(
            {
                "messages_read": MetadataValue.int(len(values)),
                "items_count": MetadataValue.int(len(mirror_rows)),
                "consume_seconds": MetadataValue.float(round(time.monotonic() - started, 1)),
            }
        )

    return consume


# --- reset -----------------------------------------------------------------------


def _assert_reset_allowed(inst: JobotronDemoInstance) -> None:
    """Refuse to run unless every name this job drops looks like this demo's: the topic
    and the mirror table under jobotron_. Same guard idea as the CDF syncs' reset."""
    if not inst.topic.startswith("jobotron_"):
        raise RuntimeError(
            f"Refusing to reset the Jobotron instance: {inst.topic!r} does not look like a jobotron object"
        )


def build_reset_job(inst: JobotronDemoInstance):
    @op(name=f"{inst.instance_name}_delete_topic", ins={"start": In(Nothing)})
    def delete_topic(context: OpExecutionContext) -> None:
        kafka = connections.kafka()
        _assert_reset_allowed(inst)
        admin = kafka.admin_client()
        if inst.topic in admin.list_topics(timeout=20).topics:
            admin.delete_topics([inst.topic], operation_timeout=30)[inst.topic].result()
            context.log.info(f"Delete requested for topic {inst.topic}")
        else:
            context.log.info(f"Topic {inst.topic} did not exist")
        # Topic deletes are asynchronous; the setup job must not recreate while listed.
        deadline = time.monotonic() + 90
        while inst.topic in admin.list_topics(timeout=20).topics:
            if time.monotonic() > deadline:
                raise RuntimeError(f"Topic {inst.topic} still listed after 90s")
            time.sleep(3)
        context.log.info(f"Topic {inst.topic} is gone")

    @op(name=f"{inst.instance_name}_drop_mirror_table", ins={"start": In(Nothing)})
    def drop_mirror_table(context: OpExecutionContext) -> None:
        postgres = connections.state_postgres()
        _assert_reset_allowed(inst)
        conn = postgres.connect()
        try:
            with conn.cursor() as cur:
                cur.execute(f"DROP TABLE IF EXISTS {inst.topic}")
                context.log.info(f"Dropped Postgres table {inst.topic} (if it existed)")
            conn.commit()
        finally:
            conn.close()

    @job(
        name=inst.reset_job,
        description=(
            f"Tear down the demo plumbing: the Kafka topic {inst.topic} and the demo consumer's "
            f"Postgres mirror table (the publish asset belongs to the library instance and owns "
            "nothing). Then run the setup job and the first publish for a fresh demo."
        ),
        executor_def=in_process_executor,
    )
    def reset_job():
        gone = delete_topic()
        drop_mirror_table(gone)

    return reset_job


# --- defs ------------------------------------------------------------------------


def build_jobotron_demo_defs(inst: JobotronDemoInstance) -> Definitions:
    """The plumbing Definitions for one market: setup + consume assets and the setup
    and reset jobs. The publish asset comes from the market's DeltaToKafkaComponent
    defs.yaml instance, loaded separately via load_defs."""
    setup = build_setup_asset(inst)
    consume = build_consume_asset(inst)
    setup_job = define_asset_job(
        name=inst.setup_job,
        selection=AssetSelection.assets(AssetKey(inst.setup_asset)),
        description="Create the publish instance's topic (create-if-not-exists; a no-op under the dry-run default).",
        executor_def=in_process_executor,
    )
    return Definitions(
        assets=[setup, consume],
        jobs=[setup_job, build_reset_job(inst)],
    )


jobotron_demo_defs = Definitions.merge(
    *[build_jobotron_demo_defs(JobotronDemoInstance(market)) for market in MARKETS]
)
