"""The control-plane cache (token and Unity Catalog table locations), using the real functions
from the vendored library (kaizen_dagster_reverse_etl.core.connections.DatabricksApi).

conftest.py points the cache file at a temp directory for every test, so these never touch dagster_storage.
Run from the repo root:  .venv/bin/python -m pytest
"""

import pytest

from kaizen_dagster_reverse_etl.core import connections
from kaizen_dagster_reverse_etl.core.connections import DatabricksApi

FQN = "de_dev.sr_poc_external.reverse_etl_unittest_source"


def api() -> DatabricksApi:
    return DatabricksApi("https://workspace.invalid")


@pytest.fixture
def lookups(monkeypatch):
    """Replace the UC REST lookup with a counter; `lookups.fail` makes it raise like a failed lookup."""

    class Lookups:
        count = 0
        fail = False

    def fake_location(self, token, fqn):
        Lookups.count += 1
        if Lookups.fail:
            raise RuntimeError("UC REST lookup failed (401)")
        return f"abfss://container/{fqn}/{Lookups.count}"

    monkeypatch.setattr(DatabricksApi, "table_location", fake_location)
    return Lookups


def test_the_cache_file_is_redirected_away_from_dagster_storage(tmp_path):
    assert connections._CONTROL_PLANE_CACHE_PATH.startswith(str(tmp_path))


def test_a_second_lookup_inside_the_ttl_is_served_from_the_cache(lookups):
    first = api().table_location_cached("token", FQN)
    second = api().table_location_cached("token", FQN)
    assert first == second
    assert lookups.count == 1


def test_clearing_the_cache_forces_a_fresh_lookup(lookups):
    api().table_location_cached("token", FQN)
    api().clear_location_cache()
    api().table_location_cached("token", FQN)
    assert lookups.count == 2


def test_an_expired_entry_is_fetched_again(lookups, monkeypatch):
    api().table_location_cached("token", FQN)
    monkeypatch.setattr(
        "kaizen_dagster_reverse_etl.core.connections._LOCATION_CACHE_TTL", -1
    )  # the next entry would already be expired
    api().clear_location_cache()
    api().table_location_cached("token", FQN)
    api().table_location_cached("token", FQN)
    assert lookups.count == 3


def test_a_failed_lookup_discards_the_whole_cache(lookups):
    api().table_location_cached("token", FQN)
    lookups.fail = True
    with pytest.raises(RuntimeError, match="401"):
        api().table_location_cached("token", "de_dev.sr_poc_external.other_table")
    lookups.fail = False
    api().table_location_cached("token", FQN)  # the first entry was discarded too
    assert lookups.count == 3


def test_a_cached_token_is_reused_until_it_expires(monkeypatch):
    tokens = iter(["token-1", "token-2"])
    monkeypatch.setattr(DatabricksApi, "token", lambda self: next(tokens))
    assert api().cached_token() == "token-1"
    assert api().cached_token() == "token-1"
    api().clear_location_cache()
    assert api().cached_token() == "token-2"
