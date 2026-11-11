"""The Jobotron demo plumbing around the vendored delta-to-kafka publish instances
(docs/poc/JOBOTRON_CONTRACT_PARITY.md): the setup asset (topic), the demo consumer
(drain, dedup on the Kafka key, Postgres mirror), the reset job (allowlist guard)
and the three demo defs. The publish path itself is the team component's
(kaizen-dagster-delta-to-kafka) -- covered by the wheel's own test suite; here it
is pinned only through its import surface and the defs the demo loads.

Kafka, Postgres and the resources are fakes. Run from the repo root:
.venv/bin/python -m pytest
"""

import json
import types
from pathlib import Path

import dagster as dg
import pytest
import yaml

from orchestration.assets import reverse_etl_jobotron as jobotron
from tests.fakes import (
    FakeAdmin,
    FakeKafka,
    bind_connections,
    error_text,
    fake_resources,
)

DEF_ROOT = Path(__file__).resolve().parent.parent / "orchestration" / "defs"
DEFS = {market: DEF_ROOT / f"unverified_customers_{market}" / "defs.yaml" for market in ("cy", "gr", "ro")}


# --- the fakes ---------------------------------------------------------------------------------------------------------


class FakeConsumer:
    """Serves every message once. With `quiet_first` the first read comes back empty although the broker holds
    messages, the cold-consumer race the real code guards against with watermarks."""

    def __init__(self, messages, quiet_first=False):
        self.messages, self.quiet_first, self.delivered = messages, quiet_first, False
        self.assigned: list = []

    def assign(self, partitions):
        self.assigned = partitions

    def consume(self, num_messages=1, timeout=0):
        if self.quiet_first:
            self.quiet_first = False
            return []
        if self.delivered:
            return []
        self.delivered = True
        return [
            types.SimpleNamespace(
                topic=lambda topic=topic: topic,
                value=lambda value=value: value,
                error=lambda: None,
                timestamp=lambda index=index: (1, 1000 + index),
            )
            for index, (topic, value) in enumerate(self.messages)
        ]

    def get_watermark_offsets(self, partition, timeout=0, cached=True):
        return (0, len([1 for topic, _ in self.messages if topic == partition.topic]))

    def close(self):
        pass


class JobotronKafka(FakeKafka):
    """The producer's messages (plus `extra`) are what the consumer reads back. With no
    publish asset locally (it belongs to the vendored component), the tests seed the
    topic through `extra` directly."""

    def __init__(self, calls, topics=(), quiet_first=False, extra=(), **options):
        super().__init__(calls, admin=FakeAdmin(topics), **options)
        self.quiet_first = quiet_first
        self.extra = list(extra)

    def consumer(self, group_id, extra=None):
        messages = [(topic, value) for topic, _, value in self.recording_producer.messages] + self.extra
        return FakeConsumer(messages, self.quiet_first)


class RecordingPostgres:
    """Statements and the rows of each executemany, in order."""

    def __init__(self):
        self.statements: list = []
        self.commits = 0

    def connect(self):
        stub = self

        class Cursor:
            def execute(self, sql, params=()):
                stub.statements.append(("execute", " ".join(sql.split())))

            def executemany(self, sql, rows):
                stub.statements.append(("executemany", " ".join(sql.split()), rows))

            def __enter__(self):
                return self

            def __exit__(self, *exc):
                return False

        class Connection:
            def cursor(self):
                return Cursor()

            def commit(self):
                stub.commits += 1

            def close(self):
                pass

        return Connection()


# --- the environment -----------------------------------------------------------------------------------------------------


class Env:
    """The fake environment the plumbing runs against: Kafka (with a seeded topic) and
    Postgres. There is no Databricks and no publish asset here -- publishing belongs to
    the vendored component."""

    def __init__(self, market, calls, monkeypatch, topic_messages=()):
        self.market = market
        self.inst = jobotron.JobotronDemoInstance(market)
        self.calls = calls
        self.monkeypatch = monkeypatch
        self.topic = f"jobotron_unverified_customers_{market}"
        self.kafka = JobotronKafka(calls, topics=[self.topic], extra=list(topic_messages))
        self.postgres = RecordingPostgres()

    def resources(self):
        res = fake_resources(self.calls)
        res.update(kafka=self.kafka, postgres=self.postgres)
        return res

    def run(self, asset, **run_config):
        config = {"ops": {asset.key.to_user_string(): {"config": run_config}}} if run_config else None
        bind_connections(self.monkeypatch, self.resources())
        return dg.materialize([asset], run_config=config, raise_on_error=False)

    def setup(self, **run_config):
        return self.run(jobotron.build_setup_asset(self.inst), **run_config)

    def consume(self):
        return self.run(jobotron.build_consume_asset(self.inst))


def seeded_env(market, calls, monkeypatch, customers=()):
    """An env whose topic already holds one plain-JSON message per customer (the shape
    the vendored component publishes: the query's columns plus the instance metadata)."""
    messages = [
        (
            f"jobotron_unverified_customers_{market}",
            json.dumps({"CustomerId": customer_id, "CompanyId": 7, "processed_at": "2026-11-11T09:00:00Z", "run_id": "run-1"}).encode(),
        )
        for customer_id in customers
    ]
    return Env(market, calls, monkeypatch, topic_messages=messages)


def metadata(result):
    (event,) = result.get_asset_materialization_events()
    return event.materialization.metadata


@pytest.fixture(autouse=True)
def no_waiting(monkeypatch):
    monkeypatch.setattr(jobotron.time, "sleep", lambda seconds: None)


# --- the vendored wheel's surface ------------------------------------------------------------------------------------------


def test_the_vendored_component_imports_and_builds_one_publish_asset():
    """Import smoke for the vendored wheel plus the publish-asset surface the demo
    defs rely on: one DeltaToKafkaComponent instance builds exactly one asset (the
    publish asset), keyed by the instance's prefix."""
    from kaizen_dagster_delta_to_kafka.components.delta_to_kafka import DeltaToKafkaComponent
    from kaizen_dagster_delta_to_kafka.components.delta_to_kafka.sink.formats.json import JsonFormat

    component = DeltaToKafkaComponent(
        name="unverified_customers_cy",
        asset_key_prefix=["jobotron"],
        query="select 1 as CustomerId",
        workspace="https://adb-1608121643336927.7.azuredatabricks.net",  # the literal-host escape hatch
        warehouse="wh-123",
        kafka_profile="openshift_kafka_connect",
        topic="jobotron_unverified_customers_cy",
        message=JsonFormat(format="json", key_column="CustomerId"),
        metadata=["processed_at", "run_id"],
        dry_run=True,
    )
    built = component.build_defs(None)
    assert sorted(asset.key.to_user_string() for asset in built.assets) == [
        "jobotron/unverified_customers_cy"
    ]
    assert not built.jobs


# --- setup: the topic ----------------------------------------------------------------------------------------------------


class TestSetup:
    def test_a_missing_topic_is_created(self, calls, monkeypatch):
        env = Env("cy", calls, monkeypatch)
        env.kafka.admin.topics = set()
        assert env.setup(dry_run=False).success
        assert env.kafka.admin.created == ["jobotron_unverified_customers_cy"]

    def test_an_existing_topic_is_left_alone(self, calls, monkeypatch):
        env = Env("cy", calls, monkeypatch)
        env.kafka.admin.topics = {"jobotron_unverified_customers_cy"}
        assert env.setup(dry_run=False).success
        assert env.kafka.admin.created == []

    def test_running_setup_twice_changes_nothing(self, calls, monkeypatch):
        env = Env("cy", calls, monkeypatch)
        env.kafka.admin.topics = set()
        assert env.setup(dry_run=False).success
        env.kafka.admin.created = []
        assert env.setup(dry_run=False).success
        assert env.kafka.admin.created == []

    def test_dry_run_setup_is_a_no_op_and_never_connects_to_kafka(self, calls, monkeypatch):
        """Zero side effects under the dry-run default: no topic, not even a Kafka connection."""
        env = Env("cy", calls, monkeypatch)
        result = env.setup()
        assert result.success
        assert env.kafka.admin.created == []
        assert ("kafka", "admin") not in calls
        assert metadata(result)["created"].value is False


# --- consume: the demo worker stand-in -------------------------------------------------------------------------------------


def mirror_rows(postgres, topic):
    (rows,) = [statement[2] for statement in postgres.statements if statement[0] == "executemany" and topic in statement[1]]
    return sorted(row["customer_id"] for row in rows)


class TestConsume:
    def test_every_customer_is_mirrored_keyed_by_customer_id(self, calls, monkeypatch):
        env = seeded_env("cy", calls, monkeypatch, customers=[1001, 1002, 1004])
        result = env.consume()
        assert result.success
        assert mirror_rows(env.postgres, env.topic) == ["1001", "1002", "1004"]
        assert metadata(result)["items_count"].value == 3
        (statement,) = [s for s in env.postgres.statements if s[0] == "execute"]
        assert "CREATE TABLE IF NOT EXISTS jobotron_unverified_customers_cy" in statement[1]
        assert "JSONB" in statement[1]

    def test_a_republished_customer_keeps_one_mirror_row_with_the_latest_message(self, calls, monkeypatch):
        """The publish run republishes the full eligible set; the key decides -- the same
        customer lands once, carrying the message the latest run sent."""
        env = Env(
            "cy",
            calls,
            monkeypatch,
            topic_messages=[
                ("jobotron_unverified_customers_cy", json.dumps({"CustomerId": 1001, "CompanyId": 7, "run_id": "run-1"}).encode()),
                ("jobotron_unverified_customers_cy", json.dumps({"CustomerId": 1001, "CompanyId": 7, "run_id": "run-2"}).encode()),
            ],
        )
        result = env.consume()
        assert metadata(result)["messages_read"].value == 2
        assert metadata(result)["items_count"].value == 1
        (rows,) = [s[2] for s in env.postgres.statements if s[0] == "executemany"]
        assert rows[0]["run_id"] == "run-2"

    def test_a_message_without_a_customer_id_is_skipped(self, calls, monkeypatch):
        env = seeded_env("cy", calls, monkeypatch, customers=[1001])
        env.kafka.extra.append((env.topic, json.dumps({"CompanyId": 7}).encode()))
        result = env.consume()
        assert result.success
        assert mirror_rows(env.postgres, env.topic) == ["1001"]

    def test_a_message_that_cannot_be_decoded_is_skipped(self, calls, monkeypatch):
        env = seeded_env("cy", calls, monkeypatch, customers=[1001])
        env.kafka.extra.append((env.topic, b"not json"))
        result = env.consume()
        assert result.success
        assert metadata(result)["items_count"].value == 1

    def test_nothing_published_yet_leaves_the_table_empty(self, calls, monkeypatch):
        env = Env("cy", calls, monkeypatch)
        result = env.consume()
        assert result.success
        assert metadata(result)["items_count"].value == 0

    def test_a_cold_consumer_that_reads_nothing_at_first_is_read_again(self, calls, monkeypatch):
        env = seeded_env("cy", calls, monkeypatch, customers=[1001, 1002])
        env.kafka.quiet_first = True
        result = env.consume()
        assert metadata(result)["items_count"].value == 2


# --- reset ----------------------------------------------------------------------------------------------------------------


def run_reset(env):
    bind_connections(env.monkeypatch, env.resources())
    defs = dg.Definitions(jobs=[jobotron.build_reset_job(env.inst)])
    return defs.resolve_job_def(env.inst.reset_job).execute_in_process(raise_on_error=False)


class TestReset:
    def test_the_topic_and_the_mirror_table_are_removed(self, calls, monkeypatch):
        env = Env("cy", calls, monkeypatch)
        assert run_reset(env).success
        assert env.kafka.admin.deleted == ["jobotron_unverified_customers_cy"]
        assert [s[1] for s in env.postgres.statements] == ["DROP TABLE IF EXISTS jobotron_unverified_customers_cy"]
        assert env.postgres.commits == 1

    def test_a_topic_that_does_not_exist_is_fine(self, calls, monkeypatch):
        env = Env("cy", calls, monkeypatch)
        env.kafka.admin.topics = set()
        assert run_reset(env).success
        assert env.kafka.admin.deleted == []

    def test_a_topic_that_stays_listed_fails_after_the_deadline(self, calls, monkeypatch):
        env = Env("cy", calls, monkeypatch)
        env.kafka.admin.delete_topics = lambda topics, operation_timeout=0: {t: types.SimpleNamespace(result=lambda: None) for t in topics}
        clock = iter(range(0, 10_000, 40))
        monkeypatch.setattr(jobotron.time, "monotonic", lambda: next(clock))
        result = run_reset(env)
        assert not result.success and "still listed after 90s" in error_text(result)

    def test_anything_that_does_not_look_like_a_jobotron_object_is_never_touched(self, calls, monkeypatch):
        env = Env("cy", calls, monkeypatch)
        # A stand-in instance whose topic escaped the jobotron_ namespace (e.g. a future
        # platform-owned topic): the guard must refuse before anything is touched.
        stand_in = types.SimpleNamespace(
            instance_name="jobotron_unverified_customers_cy",
            topic="production_events",
            reset_job="jobotron_unverified_customers_cy_reset_job",
        )
        bind_connections(env.monkeypatch, env.resources())
        defs = dg.Definitions(jobs=[jobotron.build_reset_job(stand_in)])
        result = defs.resolve_job_def("jobotron_unverified_customers_cy_reset_job").execute_in_process(raise_on_error=False)
        assert not result.success and "Refusing to reset the Jobotron instance" in error_text(result)
        assert env.kafka.admin.deleted == [] and env.postgres.statements == []


# --- the demo defs ----------------------------------------------------------------------------------------------------------


class TestDefs:
    def test_the_three_demo_yamls_point_at_the_vendored_component_with_their_own_topics(self):
        for market in ("cy", "gr", "ro"):
            attrs = yaml.safe_load(DEFS[market].read_text())
            assert (
                attrs["type"]
                == "kaizen_dagster_delta_to_kafka.components.delta_to_kafka.DeltaToKafkaComponent"
            )
            attributes = attrs["attributes"]
            assert attributes["name"] == f"unverified_customers_{market}"
            assert attributes["asset_key_prefix"] == ["jobotron"]
            assert attributes["topic"] == f"jobotron_unverified_customers_{market}"
            assert attributes["message"] == {"format": "json", "key_column": "CustomerId"}
            assert attributes["kafka_profile"] == "openshift_kafka_connect"
            assert attributes["dry_run"] is True

    def test_the_demo_queries_are_the_notebooks_per_market_rules(self):
        assert "ccgGroupID = 1424" in yaml.safe_load(DEFS["cy"].read_text())["attributes"]["query"]
        assert "cusCompanyID = 7" in yaml.safe_load(DEFS["cy"].read_text())["attributes"]["query"]
        assert "cusTypeID not in (2, 5, 6, 10, 11, 15)" in yaml.safe_load(DEFS["gr"].read_text())["attributes"]["query"]
        assert "ccgGroupID = 1425" in yaml.safe_load(DEFS["gr"].read_text())["attributes"]["query"]
        assert "ccgGroupID = 1321" in yaml.safe_load(DEFS["ro"].read_text())["attributes"]["query"]
        assert "cusCompanyID = 4" in yaml.safe_load(DEFS["ro"].read_text())["attributes"]["query"]

    def test_the_demo_plumbing_defs_load_without_name_collisions(self):
        built = [jobotron.build_jobotron_demo_defs(jobotron.JobotronDemoInstance(market)) for market in ("cy", "gr", "ro")]
        merged = dg.Definitions.merge(*built)
        dg.Definitions.validate_loadable(merged)
        asset_names = sorted(asset.key.to_user_string() for asset in merged.assets)
        assert asset_names == sorted(
            f"jobotron_unverified_customers_{market}{role}"
            for market in ("cy", "gr", "ro")
            for role in ("_setup", "_consume")
        )
        job_names = [job.name for job in merged.jobs]
        assert sorted(job_names) == sorted(
            f"jobotron_unverified_customers_{market}_{role}"
            for market in ("cy", "gr", "ro")
            for role in ("setup_job", "reset_job")
        )

    def test_the_publish_asset_comes_from_the_library_not_the_plumbing(self):
        """The plumbing owns setup and consume only -- publishing is the vendored
        component's asset, and nothing here may shadow it."""
        merged = dg.Definitions.merge(
            *[jobotron.build_jobotron_demo_defs(jobotron.JobotronDemoInstance(market)) for market in ("cy", "gr", "ro")]
        )
        names = [asset.key.to_user_string() for asset in merged.assets]
        assert not any(name.endswith("/unverified_customers_cy") or name == "jobotron_unverified_customers_cy" for name in names)
