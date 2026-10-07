"""Health check for a reverse-ETL sync's Debezium JDBC sink, run on a schedule.

Kafka Connect reports a connector RUNNING even when its task has FAILED, and does not restart the task, so
Postgres can fall behind while every Dagster run stays green (seen 2026-10-07: "No resolvable bootstrap urls"
when the consumer was created). The asset check below reads the connector's status from Connect and the sink's
consumer-group lag from Kafka. Task or connector not RUNNING is an error; lag above MAX_LAG_MESSAGES is a
warning only, since a snapshot cannot tell a backlog that is draining right after a sync from a sink that is
stuck. See docs/poc/REVERSE_ETL_DEBEZIUM_JDBC_SINK.md, section 11.
"""

import requests
from confluent_kafka import Consumer, TopicPartition
from dagster import (
    AssetCheckExecutionContext,
    AssetCheckResult,
    AssetCheckSeverity,
    AssetKey,
    AssetSelection,
    DefaultScheduleStatus,
    ScheduleDefinition,
    asset_check,
    define_asset_job,
    in_process_executor,
)

from .reverse_etl_cdf_setup import _kafka_producer_config
from .reverse_etl_config import ReverseEtlSyncConfig
from .reverse_etl_debezium_sink import CONNECT_URL

MAX_LAG_MESSAGES = 10_000
HEALTH_CRON = "*/5 * * * *"


def _connector_problems(cfg: ReverseEtlSyncConfig) -> tuple[list[str], dict[str, str]]:
    """(problems, states) from Connect's status endpoint."""
    try:
        resp = requests.get(f"{CONNECT_URL}/connectors/{cfg.connector_name}/status", timeout=10)
    except requests.RequestException as e:
        return [f"Kafka Connect not reachable at {CONNECT_URL}: {e}"], {}
    if resp.status_code == 404:
        return [f"connector {cfg.connector_name} does not exist"], {}
    if not resp.ok:
        return [f"Connect returned {resp.status_code}: {resp.text[:200]}"], {}

    status = resp.json()
    parts = [("connector", status["connector"])] + [(f"task {t['id']}", t) for t in status.get("tasks", [])]
    problems = [] if status.get("tasks") else ["connector has no tasks"]
    for label, part in parts:
        if part["state"] != "RUNNING":
            cause = next((line for line in (part.get("trace") or "").splitlines() if "Caused by" in line), "")
            problems.append(f"{label} is {part['state']}" + (f" ({cause.strip()[:200]})" if cause else ""))
    return problems, {label: part["state"] for label, part in parts}


def _consumer_lag(cfg: ReverseEtlSyncConfig) -> int:
    """Messages in the topic the sink's consumer group has not committed yet. A partition with no commit counts
    from its first offset, which is where the connector starts (auto.offset.reset=earliest). The consumer uses
    the sink's group id only to read its committed offsets: it never subscribes, so it does not join the group or
    disturb the sink. (The admin client's list_consumer_group_offsets timed out on this cluster.)"""
    consumer = Consumer({**_kafka_producer_config(), "group.id": cfg.consumer_group, "enable.auto.commit": False})
    try:
        topic = consumer.list_topics(cfg.kafka_topic, timeout=15).topics[cfg.kafka_topic]
        partitions = [TopicPartition(cfg.kafka_topic, p) for p in topic.partitions]
        lag = 0
        for tp in consumer.committed(partitions, timeout=20):
            low, high = consumer.get_watermark_offsets(tp, timeout=15)
            lag += max(high - (tp.offset if tp.offset >= 0 else low), 0)
        return lag
    finally:
        consumer.close()


def build_sink_health_check(cfg: ReverseEtlSyncConfig):
    @asset_check(
        asset=AssetKey(cfg.sink_asset),
        name="sink_healthy",
        description=(
            f"The connector {cfg.connector_name} and all its tasks are RUNNING (error otherwise), and the sink's "
            f"consumer group lag on {cfg.kafka_topic} is at most {MAX_LAG_MESSAGES} messages (warning otherwise)."
        ),
    )
    def sink_healthy(context: AssetCheckExecutionContext) -> AssetCheckResult:
        problems, states = _connector_problems(cfg)
        context.log.info(f"Connect status: problems={problems} states={states}")
        warnings: list[str] = []
        lag = None
        try:
            lag = _consumer_lag(cfg)
            context.log.info(f"consumer group lag: {lag}")
            if lag > MAX_LAG_MESSAGES:
                warnings.append(f"consumer group lag is {lag} messages (limit {MAX_LAG_MESSAGES})")
        except Exception as e:  # noqa: BLE001 -- lag is advisory; report why it is missing
            warnings.append(f"could not read consumer group lag: {type(e).__name__} {str(e)[:200]}")

        found = problems + warnings
        return AssetCheckResult(
            passed=not found,
            severity=AssetCheckSeverity.ERROR if problems else AssetCheckSeverity.WARN,
            description="; ".join(found) or f"{cfg.connector_name} is healthy, lag {lag}",
            metadata={"states": states, "consumer_group_lag": lag if lag is not None else "unknown"},
        )

    return sink_healthy


def build_sink_health_job_and_schedule(cfg: ReverseEtlSyncConfig, check):
    """A job that runs only the sink check, and a schedule that runs it every few minutes. Pause the schedule
    in the Dagster UI to stop it."""
    job = define_asset_job(
        name=f"{cfg.sync_name}_sink_health_job",
        selection=AssetSelection.checks(check),
        description=f"Run the sink health check for {cfg.sync_name}.",
        executor_def=in_process_executor,
    )
    schedule = ScheduleDefinition(
        name=f"{cfg.sync_name}_sink_health_schedule",
        job=job,
        cron_schedule=HEALTH_CRON,
        default_status=DefaultScheduleStatus.RUNNING,
    )
    return job, schedule
