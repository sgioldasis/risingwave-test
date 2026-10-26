"""The Postgres side of schema changes: renaming and dropping columns on the sink's target table, and restarting the
sink task afterwards (orchestration/assets/reverse_etl_postgres_setup.py).

Run from the repo root:  .venv/bin/python -m pytest
"""

import types
from unittest import mock

import pytest

from orchestration.assets import reverse_etl_connections as connections
from orchestration.assets import reverse_etl_postgres_setup as setup
from tests.fakes import FakeDatabase, FakeHttp, make_config

CFG = make_config()
TABLE = CFG.postgres_table


class RestartHttp(FakeHttp):
    def __init__(self, calls, status=200):
        super().__init__(calls)
        self.status = status

    def post(self, url, timeout=30, **kwargs):
        self.calls.append(("http", f"POST {url}"))
        return types.SimpleNamespace(status_code=self.status, text="x" * 500)


@pytest.fixture
def connect_kwargs(monkeypatch):
    seen = {}
    monkeypatch.setattr(connections.psycopg2, "connect", lambda **kwargs: seen.update(kwargs) or mock.MagicMock())
    return seen


class TestConnection:
    @pytest.mark.parametrize("connect", ["state_postgres", "schema_postgres"])
    def test_postgres_host_is_required_with_no_default(self, monkeypatch, connect):
        """The env contract: POSTGRES_HOST has no default — host.docker.internal does not resolve
        on the macOS host, so one value cannot serve both the containers and the host."""
        for name in ("POSTGRES_HOST", "POSTGRES_USER", "POSTGRES_PASSWORD"):
            monkeypatch.delenv(name, raising=False)
        with pytest.raises(ValueError, match="POSTGRES_HOST"):
            getattr(connections, connect)().connect()

    def test_defaults_match_the_sink_connectors_convention(self, monkeypatch, connect_kwargs):
        monkeypatch.setenv("POSTGRES_HOST", "host.docker.internal")
        for name in ("POSTGRES_USER", "POSTGRES_PASSWORD"):
            monkeypatch.delenv(name, raising=False)
        connections.schema_postgres().connect()
        assert connect_kwargs == {
            "host": "host.docker.internal",
            "port": 5432,
            "dbname": "postgres",
            "user": "postgres",
            "password": "",
            "connect_timeout": 10,
        }

    def test_the_environment_overrides_host_and_user(self, monkeypatch, connect_kwargs):
        monkeypatch.setenv("POSTGRES_HOST", "db.invalid")
        monkeypatch.setenv("POSTGRES_USER", "someone")
        connections.schema_postgres().connect()
        assert (connect_kwargs["host"], connect_kwargs["user"]) == ("db.invalid", "someone")


def alter_statements(calls):
    return [detail for kind, detail in calls if kind == "postgres" and detail.startswith(("ALTER", "UPDATE"))]


class TestRename:
    def rename(self, calls, existing, renames):
        http = RestartHttp(calls)
        database = FakeDatabase(calls, "postgres", columns=existing)
        return setup.rename_columns_in_postgres(database, http, CFG, renames), http

    def test_a_table_that_does_not_exist_yet_is_left_alone_and_the_sink_is_not_restarted(self, calls):
        ensured, _ = self.rename(calls, [], [("name", "title")])
        assert ensured == [] and not [c for c in calls if c[0] == "http"]

    def test_the_old_column_alone_is_renamed_in_place_and_the_sink_restarted(self, calls):
        ensured, _ = self.rename(calls, ["id", "name"], [("name", "title")])
        assert ensured == ["name -> title"]
        assert alter_statements(calls) == [f'ALTER TABLE {TABLE} RENAME COLUMN "name" TO "title"']
        assert [c for c in calls if c[0] == "http"] == [("http", f"POST http://kafka-connect:8083/connectors/{CFG.connector_name}/tasks/0/restart")]

    def test_both_columns_present_copies_the_values_then_drops_the_old_one(self, calls):
        ensured, _ = self.rename(calls, ["id", "name", "title"], [("name", "title")])
        assert ensured == ["name -> title"]
        assert alter_statements(calls) == [
            f'UPDATE {TABLE} SET "title" = "name"',
            f'ALTER TABLE {TABLE} DROP COLUMN "name"',
        ]

    def test_a_rename_already_applied_changes_nothing_but_still_restarts_the_sink(self, calls):
        ensured, _ = self.rename(calls, ["id", "title"], [("name", "title")])
        assert ensured == ["name -> title"]
        assert alter_statements(calls) == []
        assert [c for c in calls if c[0] == "http"]

    def test_neither_column_present_is_not_a_rename_and_does_not_restart_the_sink(self, calls):
        ensured, _ = self.rename(calls, ["id"], [("name", "title")])
        assert ensured == [] and not [c for c in calls if c[0] == "http"]

    def test_several_renames_are_each_handled_by_their_own_state(self, calls):
        ensured, _ = self.rename(calls, ["id", "a", "b", "c2"], [("a", "a2"), ("b", "b2"), ("c", "c2")])
        assert ensured == ["a -> a2", "b -> b2", "c -> c2"]
        assert len(alter_statements(calls)) == 2

    def test_the_connection_is_committed_and_closed(self, calls):
        database = FakeDatabase(calls, "postgres", columns=["id", "name"])
        connection = database.connect()
        database.connect = lambda: connection
        setup.rename_columns_in_postgres(database, RestartHttp(calls), CFG, [("name", "title")])
        connection.commit.assert_called_once()
        connection.close.assert_called_once()

    def test_a_failed_restart_is_raised_after_the_ddl_is_committed(self, calls):
        database = FakeDatabase(calls, "postgres", columns=["id", "name"])
        connection = database.connect()
        database.connect = lambda: connection
        with pytest.raises(RuntimeError, match="Sink task restart failed: HTTP 500"):
            setup.rename_columns_in_postgres(database, RestartHttp(calls, status=500), CFG, [("name", "title")])
        connection.commit.assert_called_once()


class TestRestartSinkTask:
    @pytest.mark.parametrize("status", [200, 201, 202, 204])
    def test_any_success_status_is_accepted(self, calls, status):
        setup.restart_sink_task(RestartHttp(calls, status), CFG)

    @pytest.mark.parametrize("status", [404, 409, 500])
    def test_anything_else_raises_with_the_first_200_characters_of_the_body(self, calls, status):
        with pytest.raises(RuntimeError) as error:
            setup.restart_sink_task(RestartHttp(calls, status), CFG)
        assert f"HTTP {status}" in str(error.value) and str(error.value).endswith("x" * 200)

    def test_the_connect_url_comes_from_the_environment(self, calls, monkeypatch):
        monkeypatch.setenv("KAFKA_CONNECT_URL", "http://connect.invalid:1")
        setup.restart_sink_task(RestartHttp(calls), CFG)
        assert calls[-1][1].startswith("POST http://connect.invalid:1/connectors/")


class TestDropAbsentColumns:
    def drop(self, calls, existing, source_names):
        database = FakeDatabase(calls, "postgres", columns=existing)
        return setup.drop_absent_columns(database, CFG, source_names)

    def test_a_table_that_does_not_exist_yet_is_a_no_op(self, calls):
        assert self.drop(calls, [], {"description"}) == []

    def test_columns_the_source_no_longer_has_are_dropped_in_name_order(self, calls):
        dropped = self.drop(calls, ["rid", "description", "zeta", "alpha"], {"description"})
        assert dropped == ["alpha", "zeta"]
        assert alter_statements(calls) == [f'ALTER TABLE {TABLE} DROP COLUMN "alpha"', f'ALTER TABLE {TABLE} DROP COLUMN "zeta"']

    def test_the_key_column_is_never_dropped_even_when_the_source_names_omit_it(self, calls):
        assert self.drop(calls, ["rid", "description"], {"description"}) == []

    def test_nothing_is_dropped_when_every_column_is_still_mirrored(self, calls):
        assert self.drop(calls, ["rid", "description"], {"rid", "description"}) == []
        assert alter_statements(calls) == []
