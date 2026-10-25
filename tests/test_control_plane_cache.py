"""The control-plane cache (token and Unity Catalog table locations), using the real functions.

conftest.py points the cache file at a temp directory for every test, so these never touch dagster_storage.
Run from the repo root:  .venv/bin/python -m pytest
"""

import pytest

from orchestration.assets import reverse_etl_cdf_setup as cdf_setup
from tests.fakes import make_config

FQN = "de_dev.sr_poc_external.reverse_etl_unittest_source"


@pytest.fixture
def lookups(monkeypatch):
    """Replace the UC REST lookup with a counter; `lookups.fail` makes it raise like a failed lookup."""

    class Lookups:
        count = 0
        fail = False

    def fake_location(token, cfg, fqn):
        Lookups.count += 1
        if Lookups.fail:
            raise RuntimeError("UC REST lookup failed (401)")
        return f"abfss://container/{fqn}/{Lookups.count}"

    monkeypatch.setattr(cdf_setup, "get_table_location", fake_location)
    return Lookups


def test_the_cache_file_is_redirected_away_from_dagster_storage(tmp_path):
    assert cdf_setup._CONTROL_PLANE_CACHE_PATH.startswith(str(tmp_path))


def test_a_second_lookup_inside_the_ttl_is_served_from_the_cache(lookups):
    cfg = make_config()
    first = cdf_setup.get_table_location_cached("token", cfg, FQN)
    second = cdf_setup.get_table_location_cached("token", cfg, FQN)
    assert first == second
    assert lookups.count == 1


def test_clearing_the_cache_forces_a_fresh_lookup(lookups):
    cfg = make_config()
    cdf_setup.get_table_location_cached("token", cfg, FQN)
    cdf_setup.clear_control_plane_cache()
    cdf_setup.get_table_location_cached("token", cfg, FQN)
    assert lookups.count == 2


def test_an_expired_entry_is_fetched_again(lookups, monkeypatch):
    cfg = make_config()
    cdf_setup.get_table_location_cached("token", cfg, FQN)
    monkeypatch.setattr(cdf_setup, "_LOCATION_CACHE_TTL", -1)  # the next entry would already be expired
    cdf_setup.clear_control_plane_cache()
    cdf_setup.get_table_location_cached("token", cfg, FQN)
    cdf_setup.get_table_location_cached("token", cfg, FQN)
    assert lookups.count == 3


def test_a_failed_lookup_discards_the_whole_cache(lookups):
    cfg = make_config()
    cdf_setup.get_table_location_cached("token", cfg, FQN)
    lookups.fail = True
    with pytest.raises(RuntimeError, match="401"):
        cdf_setup.get_table_location_cached("token", cfg, "de_dev.sr_poc_external.other_table")
    lookups.fail = False
    cdf_setup.get_table_location_cached("token", cfg, FQN)  # the first entry was discarded too
    assert lookups.count == 3


def test_a_cached_token_is_reused_until_it_expires(monkeypatch):
    tokens = iter(["token-1", "token-2"])
    monkeypatch.setattr(cdf_setup, "_get_token", lambda: next(tokens))
    assert cdf_setup._get_token_cached() == "token-1"
    assert cdf_setup._get_token_cached() == "token-1"
    cdf_setup.clear_control_plane_cache()
    assert cdf_setup._get_token_cached() == "token-2"
