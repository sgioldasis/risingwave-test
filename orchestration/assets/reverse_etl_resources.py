"""Dagster resources for the systems the reverse-ETL ops and assets reach.

Each one wraps the helper function the code used to call directly, so behaviour is unchanged. The point
is that a test can bind fakes under the same keys (see tests/test_reverse_etl_pipeline.py). The
resources are registered once in orchestration/definitions.py, under these keys: postgres, risingwave,
databricks, kafka, http, registry.

The imports are inside the methods because the modules that use these resources are imported by the
modules that define the helpers.
"""

import requests
from dagster import ConfigurableResource


class PostgresResource(ConfigurableResource):
    def connect(self):
        from .postgres_sink_setup import get_postgres_connection

        return get_postgres_connection()

    def connect_with_timeout(self):
        """The connection the schema-change helpers use (10 s connect timeout, host.docker.internal)."""
        from .reverse_etl_postgres_setup import _get_postgres_connection

        return _get_postgres_connection()


class RisingWaveResource(ConfigurableResource):
    def connect(self):
        from .reverse_etl_risingwave_setup import _get_risingwave_connection

        return _get_risingwave_connection()


class DatabricksResource(ConfigurableResource):
    def require_env(self) -> None:
        from .reverse_etl_cdf_setup import _require_databricks_env

        _require_databricks_env()

    def token(self) -> str:
        from .databricks_optimize import _get_token

        return _get_token()

    def cached_token(self) -> str:
        from .reverse_etl_cdf_setup import _get_token_cached

        return _get_token_cached()

    def reset_meter(self) -> None:
        from .databricks_cost_meter import reset

        reset()

    def run_sql(self, token: str, statement: str) -> dict:
        from .reverse_etl_cdf_setup import _run_sql

        return _run_sql(token, statement)

    def table_location(self, token: str, cfg, fqn: str) -> str:
        from .reverse_etl_cdf_setup import get_table_location

        return get_table_location(token, cfg, fqn)

    def table_location_cached(self, token: str, cfg, fqn: str) -> str:
        from .reverse_etl_cdf_setup import get_table_location_cached

        return get_table_location_cached(token, cfg, fqn)

    def clear_location_cache(self) -> None:
        from .reverse_etl_cdf_setup import clear_control_plane_cache

        clear_control_plane_cache()


class KafkaResource(ConfigurableResource):
    def admin_client(self):
        from .kafka_topics_setup import _admin_client

        return _admin_client()

    def producer(self):
        """A producer whose TLS/SASL handshake has already started."""
        from .reverse_etl_cdf_setup import _kafka_producer

        return _kafka_producer()

    def consumer(self, group_id: str, extra: dict | None = None):
        """A consumer on the output cluster. With no `extra` it only reads committed offsets (it never
        subscribes); `extra` entries override the defaults."""
        from confluent_kafka import Consumer

        from .reverse_etl_cdf_setup import _kafka_producer_config

        return Consumer({**_kafka_producer_config(), "group.id": group_id, "enable.auto.commit": False, **(extra or {})})


class SchemaRegistryResource(ConfigurableResource):
    def client(self, url: str):
        from confluent_kafka.schema_registry import SchemaRegistryClient

        return SchemaRegistryClient({"url": url})


class HttpResource(ConfigurableResource):
    """Kafka Connect, the schema registry's REST API and other plain HTTP calls."""

    def get(self, url: str, timeout: int = 10, **kwargs) -> requests.Response:
        return requests.get(url, timeout=timeout, **kwargs)

    def put(self, url: str, timeout: int = 30, **kwargs) -> requests.Response:
        return requests.put(url, timeout=timeout, **kwargs)

    def post(self, url: str, timeout: int = 30, **kwargs) -> requests.Response:
        return requests.post(url, timeout=timeout, **kwargs)

    def delete(self, url: str, params: dict | None = None, timeout: int = 30) -> requests.Response:
        return requests.delete(url, params=params, timeout=timeout)
