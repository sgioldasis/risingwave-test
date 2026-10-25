"""Shared fixtures. The fakes live in tests/fakes.py.

`calls` is the shared record of what the fake resources were asked to do; `resources(**options)` builds the fake
resource dict bound to it; `defs_with_fakes(cfg)` is a sync's definitions with those resources attached (its jobs
and assets need them to resolve).
"""

import dagster as dg
import pytest

from orchestration.assets.reverse_etl_defs import build_reverse_etl_defs
from tests.fakes import fake_resources


@pytest.fixture(autouse=True)
def isolated_control_plane_cache(tmp_path, monkeypatch):
    """The sync caches the Databricks token and table locations in a file under dagster_storage. Point it at a temp
    directory so no test reads a developer's real cache or leaves one behind."""
    from orchestration.assets import reverse_etl_cdf_setup as cdf_setup

    monkeypatch.setattr(cdf_setup, "_CONTROL_PLANE_CACHE_PATH", str(tmp_path / "control_plane_cache.json"))


@pytest.fixture
def calls() -> list:
    return []


@pytest.fixture
def resources(calls):
    def build(**options) -> dict:
        return fake_resources(calls, **options)

    return build


@pytest.fixture
def defs_with_fakes(resources):
    def build(cfg, **options) -> dg.Definitions:
        return dg.Definitions.merge(build_reverse_etl_defs(cfg), dg.Definitions(resources=resources(**options)))

    return build
