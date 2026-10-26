"""The reset job runs op by op through Dagster with fake resources. The watermark and the location cache are
cleared BEFORE any Databricks call, so a failed drop cannot leave a stale watermark behind (the recreated source
restarts at version 0).

Run from the repo root:  .venv/bin/python -m pytest
"""

import types

import dagster as dg
import psycopg2.errors
import pytest

from orchestration.assets import reverse_etl_reset as reset_module
from tests.fakes import FakeAdmin, FakeHttp, FakeKafka, bind_connections, error_text, kinds, make_config


@pytest.fixture
def run_reset(resources, monkeypatch):
    def run(cfg, **options):
        job = reset_module.build_reset_job(cfg)
        bind_connections(monkeypatch, resources(**options))
        return job.execute_in_process(raise_on_error=False)

    return run


def watermark_delete_index(calls) -> int:
    return next(i for i, c in enumerate(calls) if c[0] == "postgres" and "DELETE FROM reverse_etl_sync_state" in c[1])


def test_the_ops_run_in_dependency_order(calls, run_reset):
    assert run_reset(make_config()).success
    # connector, RisingWave, Postgres table, Kafka, then the Databricks op (watermark delete first)
    order = [kind for kind in kinds(calls) if kind in ("http", "risingwave", "postgres", "kafka", "databricks")]
    assert order[0] == "http"
    assert order.index("risingwave") < order.index("postgres") < order.index("kafka") < order.index("databricks")


def test_the_registry_subjects_are_not_deleted_for_json(calls, run_reset):
    assert run_reset(make_config()).success
    assert not [c for c in calls if c[0] == "http" and "/subjects/" in c[1]]


def test_the_registry_subjects_are_deleted_for_avro_before_the_databricks_drops(calls, run_reset):
    cfg = make_config(encoding="avro", schema_registry_url="http://registry.invalid")
    assert run_reset(cfg).success
    subject_calls = [i for i, c in enumerate(calls) if c[0] == "http" and "/subjects/" in c[1]]
    assert len(subject_calls) == 4  # key and value, soft then permanent
    assert max(subject_calls) < kinds(calls).index("databricks")


def test_watermark_and_cache_are_cleared_before_the_first_databricks_call(calls, run_reset):
    assert run_reset(make_config()).success
    order = kinds(calls)
    assert watermark_delete_index(calls) < order.index("databricks")
    assert order.index("cache") < order.index("databricks")


def test_both_databricks_tables_are_dropped_on_success(calls, run_reset):
    cfg = make_config()
    run_reset(cfg)
    drops = [sql for kind, sql in calls if kind == "databricks"]
    assert drops == [f"DROP TABLE IF EXISTS {cfg.source_fqn}", f"DROP TABLE IF EXISTS {cfg.state_fqn}"]


def test_a_failed_drop_still_leaves_the_watermark_cleared_and_fails_the_run(calls, run_reset):
    result = run_reset(make_config(), sql_error=RuntimeError("PERMISSION_DENIED: does not have MANAGE"))
    assert not result.success
    assert "MANAGE" in error_text(result)
    watermark_delete_index(calls)  # raises StopIteration if the watermark was not cleared
    assert "cache" in kinds(calls)
    assert kinds(calls).count("databricks") == 1  # stopped at the first failing drop


def test_the_guard_refuses_a_name_outside_the_allowlist(calls, run_reset):
    result = run_reset(make_config(kafka_topic="production_orders"))
    assert not result.success
    assert kinds(calls) == []  # nothing was touched
    assert "Refusing to reset" in error_text(result)


# --- each op's failure and edge paths ------------------------------------------------------------------------------------


class ScriptedHttp(FakeHttp):
    """Answers each DELETE with the status of the first `statuses` key found in its URL (200 when none is)."""

    def __init__(self, calls, statuses=None):
        super().__init__(calls)
        self.statuses, self.deletes = statuses or {}, []

    def delete(self, url, params=None, timeout=30):
        self.deletes.append((url, bool(params)))
        status = next((code for key, code in self.statuses.items() if key in url), 200)
        return types.SimpleNamespace(status_code=status, ok=status < 300, text="registry says no")


class WatermarkPostgres:
    """Postgres whose watermark table may be missing: the DELETE then raises UndefinedTable, as the real server does."""

    def __init__(self, watermark_table_exists=True):
        self.exists, self.commits, self.rollbacks, self.statements = watermark_table_exists, 0, 0, []

    def connect(self):
        database = self

        class Cursor:
            def execute(self, sql, params=()):
                database.statements.append(" ".join(sql.split()))
                if "DELETE FROM reverse_etl_sync_state" in sql and not database.exists:
                    raise psycopg2.errors.UndefinedTable("relation does not exist")

            def __enter__(self):
                return self

            def __exit__(self, *exc):
                return False

        class Connection:
            def cursor(self):
                return Cursor()

            def commit(self):
                database.commits += 1

            def rollback(self):
                database.rollbacks += 1

            def close(self):
                pass

            def __enter__(self):
                return self

            def __exit__(self, *exc):
                return False

        return Connection()


@pytest.fixture
def run_with(resources, monkeypatch):
    """Run the reset job with some of the fake resources replaced."""

    def run(cfg, **overrides):
        bind_connections(monkeypatch, {**resources(), **overrides})
        job = dg.Definitions(jobs=[reset_module.build_reset_job(cfg)]).get_job_def(cfg.reset_job)
        return job.execute_in_process(raise_on_error=False)

    return run


@pytest.fixture(autouse=True)
def no_waiting(monkeypatch):
    monkeypatch.setattr(reset_module.time, "sleep", lambda seconds: None)


class TestConnector:
    def test_a_connector_that_does_not_exist_is_fine(self, calls, run_with):
        cfg = make_config()
        assert run_with(cfg, http=ScriptedHttp(calls, {"/connectors/": 404})).success

    def test_any_other_failure_stops_the_reset_before_anything_else_is_touched(self, calls, run_with):
        cfg = make_config()
        result = run_with(cfg, http=ScriptedHttp(calls, {"/connectors/": 500}))
        assert not result.success and f"Deleting {cfg.connector_name} failed (500)" in error_text(result)
        assert not [kind for kind in kinds(calls) if kind in ("risingwave", "postgres", "databricks")]


class TestConsumerGroup:
    def run(self, run_with, calls, admin):
        return run_with(make_config(), kafka=FakeKafka(calls, admin=admin))

    def test_a_group_that_does_not_exist_is_fine_and_not_retried(self, run_with, calls):
        admin = FakeAdmin(group_failures=[Exception("GROUP_ID_NOT_FOUND: no such group")])
        assert self.run(run_with, calls, admin).success and admin.group_attempts == 1

    def test_a_group_the_connector_has_not_left_yet_is_retried_until_it_goes(self, run_with, calls):
        admin = FakeAdmin(group_failures=[Exception("NON_EMPTY_GROUP"), Exception("NON_EMPTY_GROUP")])
        assert self.run(run_with, calls, admin).success and admin.group_attempts == 3

    def test_a_group_that_never_goes_fails_the_reset_after_six_attempts(self, run_with, calls):
        admin = FakeAdmin(group_failures=[Exception("NON_EMPTY_GROUP")] * 6)
        result = self.run(run_with, calls, admin)
        assert not result.success and "Could not delete consumer group" in error_text(result)
        assert admin.group_attempts == 6
        assert "databricks" not in kinds(calls)


class TestTopic:
    def test_an_existing_topic_is_deleted(self, run_with, calls):
        cfg = make_config()
        admin = FakeAdmin(topics={cfg.kafka_topic, "another_topic"})
        assert run_with(cfg, kafka=FakeKafka(calls, admin=admin)).success
        assert admin.deleted == [cfg.kafka_topic] and admin.topics == {"another_topic"}

    def test_a_topic_that_is_not_there_is_not_deleted(self, run_with, calls):
        admin = FakeAdmin(topics={"another_topic"})
        assert run_with(make_config(), kafka=FakeKafka(calls, admin=admin)).success
        assert admin.deleted == []

    def test_a_topic_that_stays_listed_fails_the_reset_after_the_deadline(self, run_with, calls, monkeypatch):
        cfg = make_config()
        clock = iter(range(0, 10_000, 40))
        monkeypatch.setattr(reset_module.time, "monotonic", lambda: next(clock))
        result = run_with(cfg, kafka=FakeKafka(calls, admin=FakeAdmin(topics={cfg.kafka_topic}, topics_linger=True)))
        assert not result.success and f"Topic {cfg.kafka_topic} still listed after 90s" in error_text(result)
        assert "databricks" not in kinds(calls)


class TestRegistrySubjects:
    AVRO = dict(encoding="avro", schema_registry_url="http://registry.invalid")

    def test_each_subject_is_deleted_soft_then_permanent(self, run_with, calls):
        http = ScriptedHttp(calls)
        assert run_with(make_config(**self.AVRO), http=http).success
        subjects = [(url.rsplit("/", 1)[1].rsplit("-", 1)[-1], permanent) for url, permanent in http.deletes if "/subjects/" in url]
        assert subjects == [("key", False), ("key", True), ("value", False), ("value", True)]

    def test_a_subject_that_does_not_exist_is_fine(self, run_with, calls):
        assert run_with(make_config(**self.AVRO), http=ScriptedHttp(calls, {"/subjects/": 404})).success

    def test_any_other_failure_stops_the_reset_before_the_databricks_drops(self, run_with, calls):
        result = run_with(make_config(**self.AVRO), http=ScriptedHttp(calls, {"/subjects/": 500}))
        assert not result.success and "failed (500): registry says no" in error_text(result)
        assert "databricks" not in kinds(calls)


class TestWatermarkTable:
    def test_a_missing_watermark_table_means_nothing_to_clear(self, run_with):
        database = WatermarkPostgres(watermark_table_exists=False)
        assert run_with(make_config(), postgres=database).success
        assert database.rollbacks == 1

    def test_this_syncs_rows_are_deleted_and_committed(self, run_with):
        cfg = make_config()
        database = WatermarkPostgres()
        assert run_with(cfg, postgres=database).success
        assert any(s.startswith("DELETE FROM reverse_etl_sync_state WHERE sync_name") for s in database.statements)
        assert database.rollbacks == 0 and database.commits == 2  # the watermark delete, then the Postgres table drop

    def test_the_databricks_drops_still_happen_after_a_missing_watermark_table(self, run_with, calls):
        run_with(make_config(), postgres=WatermarkPostgres(watermark_table_exists=False))
        assert kinds(calls).count("databricks") == 2
