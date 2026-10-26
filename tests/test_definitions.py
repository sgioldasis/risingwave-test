"""The whole project's definitions: the reverse-ETL assets declare no Dagster resources (the env
contract — connections come from reverse_etl_connections), and the one resource the drt assets
still take is bound.

The per-component tests live in tests/components/. This one imports orchestration.definitions, which needs the dbt
manifests, so it is skipped where they are missing (CI). No services needed. Run from the repo root:
.venv/bin/python -m pytest
"""

import dagster as dg
import pytest

from orchestration.constants import dbt_PROJECT_PATH, dbt_STARROCKS_PROJECT_PATH

DBT_MANIFESTS = [dbt_PROJECT_PATH / "target" / "manifest.json", dbt_STARROCKS_PROJECT_PATH / "target" / "manifest.json"]


@pytest.mark.skipif(
    not all(path.exists() for path in DBT_MANIFESTS),
    reason="importing orchestration.definitions needs the dbt manifests (run `dbt parse` in dbt/ and dbt_starrocks/)",
)
def test_the_reverse_etl_assets_declare_no_resources_and_the_drt_resource_is_bound():
    from orchestration.definitions import defs

    dg.Definitions.validate_loadable(defs)
    assert "drt" in defs.resources
    for asset in defs.assets:
        keys = getattr(asset, "required_resource_keys", None)  # plain AssetSpecs carry none
        if not keys:
            continue
        # a multi-asset (the dbt ones) has no single key: read every spec's
        names = {spec.key.to_user_string() for spec in asset.specs} if hasattr(asset, "specs") else {asset.key.to_user_string()}
        if any(name.startswith("reverse_etl_") for name in names):
            # the env contract: the sync's assets (and the demo's around them) bind nothing —
            # the one exception is the drt comparison sync, whose resource is dagster-embedded's own
            assert set(keys) - {"io_manager", "drt"} == set(), sorted(names)
