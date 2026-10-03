"""Register the Debezium JDBC sink connector: reverse-ETL CDF topic -> host Postgres.

Consumes KAFKA_TOPIC (schema-embedded Debezium envelopes produced by
reverse_etl_cdf_setup.py, also read by the RisingWave table) from the external
SASL_SSL cluster and upserts / deletes into a Postgres table.
See docs/poc/REVERSE_ETL_CDF_POC_PLAN.md.
"""

import os
import time
from typing import Any

import requests
from dagster import AssetExecutionContext, MetadataValue, asset

from .kafka_topics_setup import kafka_output_topics_setup
from .reverse_etl_cdf_setup import KAFKA_TOPIC, reverse_etl_cdf_to_kafka

CONNECT_URL = os.environ.get("KAFKA_CONNECT_URL", "http://kafka-connect:8083")
CONNECTOR_NAME = "reverse_etl_cdf_jdbc_sink"
TARGET_TABLE = "reverse_etl_cdf_poc"

_PLAIN_LOGIN = "org.apache.kafka.common.security.plain.PlainLoginModule"
_SCRAM_LOGIN = "org.apache.kafka.common.security.scram.ScramLoginModule"
_JSON_CONVERTER = "org.apache.kafka.connect.json.JsonConverter"


def _connector_config() -> dict[str, str]:
    bootstrap = os.environ.get("KAFKA_OUTPUT_BOOTSTRAP", "")
    if not bootstrap:
        raise ValueError("KAFKA_OUTPUT_BOOTSTRAP must be set in .env")

    config = {
        "connector.class": "io.debezium.connector.jdbc.JdbcSinkConnector",
        "tasks.max": "1",
        "topics": KAFKA_TOPIC,
        "connection.url": os.environ.get(
            "HOST_POSTGRES_URL", "jdbc:postgresql://host.docker.internal:5432/postgres"
        ),
        "connection.username": os.environ.get("POSTGRES_USER", "postgres"),
        "connection.password": "${env:POSTGRES_PASSWORD}",
        "insert.mode": "upsert",
        "primary.key.mode": "record_key",
        "primary.key.fields": "id",
        "delete.enabled": "true",
        "schema.evolution": "basic",
        "collection.name.format": TARGET_TABLE,
        "key.converter": _JSON_CONVERTER,
        "key.converter.schemas.enable": "true",
        "value.converter": _JSON_CONVERTER,
        "value.converter.schemas.enable": "true",
        "consumer.override.bootstrap.servers": bootstrap,
        "consumer.override.auto.offset.reset": "earliest",
    }

    if os.environ.get("KAFKA_OUTPUT_SASL_USERNAME", ""):
        mechanism = os.environ.get("KAFKA_OUTPUT_SASL_MECHANISM", "SCRAM-SHA-512")
        login_module = _PLAIN_LOGIN if mechanism == "PLAIN" else _SCRAM_LOGIN
        config.update({
            "consumer.override.security.protocol": "SASL_SSL",
            "consumer.override.sasl.mechanism": mechanism,
            "consumer.override.sasl.jaas.config": (
                f'{login_module} required username="${{env:KAFKA_OUTPUT_SASL_USERNAME}}" '
                'password="${env:KAFKA_OUTPUT_SASL_PASSWORD}";'
            ),
        })
    return config


def _wait_for_connect(timeout_s: int = 120) -> None:
    deadline = time.monotonic() + timeout_s
    while True:
        try:
            if requests.get(f"{CONNECT_URL}/connectors", timeout=5).ok:
                return
        except requests.RequestException:
            pass
        if time.monotonic() > deadline:
            raise RuntimeError(f"Kafka Connect not reachable at {CONNECT_URL} after {timeout_s}s")
        time.sleep(3)


def _wait_for_running(context: AssetExecutionContext, timeout_s: int = 90) -> dict[str, Any]:
    deadline = time.monotonic() + timeout_s
    while True:
        resp = requests.get(f"{CONNECT_URL}/connectors/{CONNECTOR_NAME}/status", timeout=10)
        status = resp.json() if resp.ok else {}
        states = [status.get("connector", {}).get("state")] + [t.get("state") for t in status.get("tasks", [])]
        context.log.info(f"{CONNECTOR_NAME} states: {states}")

        failed = [
            x for x in [status.get("connector", {})] + status.get("tasks", []) if x.get("state") == "FAILED"
        ]
        if failed:
            trace = (failed[0].get("trace") or "")[:1500]
            raise RuntimeError(f"{CONNECTOR_NAME} FAILED: {trace}")
        if status.get("tasks") and all(s == "RUNNING" for s in states):
            return status
        if time.monotonic() > deadline:
            raise RuntimeError(f"{CONNECTOR_NAME} not RUNNING after {timeout_s}s: {states}")
        time.sleep(3)


@asset(
    group_name="reverse_etl_poc",
    deps=[kafka_output_topics_setup, reverse_etl_cdf_to_kafka],
    description=(
        f"Create/update the Debezium JDBC sink connector that upserts and deletes rows "
        f"from the {KAFKA_TOPIC} Kafka topic into the host Postgres table "
        f"{TARGET_TABLE}, and wait for it to be RUNNING. "
        "See docs/poc/REVERSE_ETL_CDF_POC_PLAN.md."
    ),
)
def reverse_etl_debezium_jdbc_sink(context: AssetExecutionContext):
    _wait_for_connect()

    resp = requests.put(
        f"{CONNECT_URL}/connectors/{CONNECTOR_NAME}/config", json=_connector_config(), timeout=30
    )
    if not resp.ok:
        raise RuntimeError(f"Connect rejected {CONNECTOR_NAME} config ({resp.status_code}): {resp.text[:1500]}")
    context.log.info(f"Connector {CONNECTOR_NAME} created/updated")

    status = _wait_for_running(context)

    context.add_output_metadata({
        "connector": MetadataValue.text(CONNECTOR_NAME),
        "source_topic": MetadataValue.text(KAFKA_TOPIC),
        "postgres_table": MetadataValue.text(TARGET_TABLE),
        "connector_state": MetadataValue.text(status["connector"]["state"]),
        "task_states": MetadataValue.json([t["state"] for t in status["tasks"]]),
    })
