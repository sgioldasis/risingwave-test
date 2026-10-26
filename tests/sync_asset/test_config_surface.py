"""The sync asset's config surface: every cfg.<attr> the asset module reads must exist on
CdfSyncConfig. That is the port's axis (release design, step 2): the asset is base-clean, and a
demo-only attribute (propagate_drops, targets, seed_statements, ...) sneaking back into
reverse_etl_cdf_setup.py fails here statically — no services, no run needed.

Run from the repo root:  .venv/bin/python -m pytest
"""

import dataclasses
import re
from pathlib import Path

from orchestration.assets.reverse_etl_config import CdfSyncConfig

ASSET_MODULE = Path(__file__).parents[2] / "orchestration" / "assets" / "reverse_etl_cdf_setup.py"


def test_every_cfg_attribute_the_sync_asset_reads_exists_on_the_base_config():
    read = set(re.findall(r"\bcfg\.([A-Za-z_][A-Za-z0-9_]*)", ASSET_MODULE.read_text()))
    known = {field.name for field in dataclasses.fields(CdfSyncConfig)} | {
        name for name, member in vars(CdfSyncConfig).items() if not name.startswith("_")
    }
    assert read <= known, f"attribute(s) the sync asset reads that CdfSyncConfig does not carry: {sorted(read - known)}"
