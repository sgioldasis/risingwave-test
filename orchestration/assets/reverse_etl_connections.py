"""The env contract (docs/poc/REVERSE_ETL_RELEASE_DESIGN.md): one env-fed function per connection.

The components declare no Dagster resources — the same defs.yaml deploys to staging and
production, and the environment is the only layer that differs per deployment (the workspace
provisions env per environment: Vault → External Secrets Operator → container_context.<env>.yaml).
Every connection here reads `os.environ` at call time — never import time, so a test (or a
reconfigured process) can change the environment between calls — and the assets call these
functions directly: no `required_resource_keys`, no `context.resources`, nothing for a team to
bind. Names and decisions live in files, values live in the environment.

The functions return the connection's client surface (the methods the assets call); the tests'
fakes (tests/fakes.py) implement the same shapes, and a test patches these functions with them.

## The environment the functions read

| Function | Variables |
|---|---|
| `state_postgres` | `POSTGRES_HOST` (**required** — no default: `host.docker.internal` does not resolve on the macOS host, so one value cannot serve both the containers and the host; the demo sets it per-context via compose on the dagster services), `POSTGRES_PORT` (5432), `POSTGRES_DB` (postgres), `POSTGRES_USER` (postgres), `POSTGRES_PASSWORD` ("") |
| `schema_postgres` | the same variables (the demo's schema-change helpers share the state's credentials; connects to the `postgres` database with a 10 s timeout) |
| `risingwave` | `RISINGWAVE_HOST` (frontend-node-0), `RISINGWAVE_PORT` (4566), `RISINGWAVE_DB` (dev), `RISINGWAVE_USER` (root), `RISINGWAVE_PASSWORD` ("") |
| `databricks` | `DBT_DATABRICKS_HOST`, `DATABRICKS_AZURE_TENANT_ID`, `DATABRICKS_AZURE_CLIENT_ID`, `DATABRICKS_AZURE_CLIENT_SECRET` — the sync's own storage credentials, which delta-rs needs and no resource ever carried; **service principal, never a SAS token** (a SAS token expires and silently broke every MV refresh once) |
| `kafka` | the output cluster's `KAFKA_OUTPUT_BOOTSTRAP` (required — the producer config raises without it) plus the optional `KAFKA_OUTPUT_SASL_USERNAME`/`_MECHANISM`/`_PASSWORD`; the topic-asset admin client reads the demo cluster's own variables (`KAFKA_BOOTSTRAP_SERVERS`/`_SASL_*`, see kafka_topics_setup) |
| `registry` | none — the registry URL is a config field with an org-specific default (everything env-adjacent that is not a credential stays in config) |
| `http` | none — the URL is a config field or an env-read constant at the call site (Kafka Connect's `KAFKA_CONNECT_URL`) |

Everything env-adjacent that is not a credential stays in YAML or config: the registry URL is a
config field, the DuckDB path is typed, topic names are derived. The connector's JDBC
coordinates are assembled by `reverse_etl_debezium_sink` from `HOST_POSTGRES_URL` /
`POSTGRES_USER`, with the password left as the literal `${env:POSTGRES_PASSWORD}` for the
Connect worker to resolve — the password never passes through Dagster.
"""

import os

import psycopg2
import requests


class _Connectable:
    """The one method the Postgres/RisingWave call sites need: .connect()."""

    def __init__(self, open_connection):
        self._open = open_connection

    def connect(self):
        return self._open()


def _require_postgres_host() -> str:
    host = os.environ.get("POSTGRES_HOST", "")
    if not host:
        raise ValueError(
            "POSTGRES_HOST is required (no default: host.docker.internal does not resolve on a "
            "macOS host, so one value cannot serve both the containers and the host). The demo "
            "sets it in docker-compose.yml on the dagster services."
        )
    return host


def state_postgres() -> _Connectable:
    """The sync's watermark state (reverse_etl_state.PostgresSyncState)."""

    def open_connection():
        return psycopg2.connect(
            host=_require_postgres_host(),
            port=int(os.environ.get("POSTGRES_PORT", "5432")),
            dbname=os.environ.get("POSTGRES_DB", "postgres"),
            user=os.environ.get("POSTGRES_USER", "postgres"),
            password=os.environ.get("POSTGRES_PASSWORD", ""),
        )

    return _Connectable(open_connection)


def schema_postgres() -> _Connectable:
    """The demo's schema-change helpers (reverse_etl_postgres_setup): the same credential
    convention as the sink connector, against the `postgres` database, 10 s connect timeout."""

    def open_connection():
        return psycopg2.connect(
            host=_require_postgres_host(),
            port=5432,
            dbname="postgres",
            user=os.environ.get("POSTGRES_USER", "postgres"),
            password=os.environ.get("POSTGRES_PASSWORD", ""),
            connect_timeout=10,
        )

    return _Connectable(open_connection)


def risingwave() -> _Connectable:
    """The RisingWave SQL connection (same convention as risingwave_countries_table.py)."""

    def open_connection():
        return psycopg2.connect(
            host=os.environ.get("RISINGWAVE_HOST", "frontend-node-0"),
            port=int(os.environ.get("RISINGWAVE_PORT", "4566")),
            database=os.environ.get("RISINGWAVE_DB", "dev"),
            user=os.environ.get("RISINGWAVE_USER", "root"),
            password=os.environ.get("RISINGWAVE_PASSWORD", ""),
            # A stalled frontend (TCP accepted, handshake never completes) must fail
            # the sync fast instead of hanging the run forever — seen live when the
            # zero-DBU sync ran from outside the container network (2026-10-14).
            connect_timeout=10,
        )

    return _Connectable(open_connection)


class DatabricksApi:
    """The Databricks REST surface the sync and the demo assets reach. The underlying helpers
    read the service-principal env at call time (their imports are deferred because they live in
    modules that import this one)."""

    def require_env(self) -> None:
        from .reverse_etl_cdf_setup import _require_databricks_env

        _require_databricks_env()

    def token(self) -> str:
        from .databricks_optimize import _get_token

        return _get_token()

    def cached_token(self) -> str:
        from .reverse_etl_cdf_setup import _get_token_cached

        return _get_token_cached()

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


class KafkaClients:
    """The Kafka clients: the demo cluster's admin (topic assets, the reset's group deletes) and
    the output cluster's producer/consumer (the sync, the DuckDB mirror, the health check)."""

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


class SchemaRegistry:
    """The registry client factory. The URL is a config field (an org-specific default), so this
    connection reads no environment."""

    def client(self, url: str):
        from confluent_kafka.schema_registry import SchemaRegistryClient

        return SchemaRegistryClient({"url": url})


class Http:
    """Kafka Connect, the schema registry's REST API and other plain HTTP calls."""

    def get(self, url: str, timeout: int = 10, **kwargs) -> requests.Response:
        return requests.get(url, timeout=timeout, **kwargs)

    def put(self, url: str, timeout: int = 30, **kwargs) -> requests.Response:
        return requests.put(url, timeout=timeout, **kwargs)

    def post(self, url: str, timeout: int = 30, **kwargs) -> requests.Response:
        return requests.post(url, timeout=timeout, **kwargs)

    def delete(self, url: str, params: dict | None = None, timeout: int = 30) -> requests.Response:
        return requests.delete(url, params=params, timeout=timeout)


def databricks() -> DatabricksApi:
    return DatabricksApi()


def kafka() -> KafkaClients:
    return KafkaClients()


def registry() -> SchemaRegistry:
    return SchemaRegistry()


def http() -> Http:
    return Http()
