"""The compatibility test (release design step 8): a team's minimal dg project — a pyproject
with [tool.dg], a sync component YAML and a target component YAML — loads through
`load_from_defs_folder`, the exact production load path, with no resources configured (the env
contract). The target YAML's `deps` reference the sync component's topic asset, and a variant
with a dangling dep loads too: ordering is the team's declaration, not a load-time constraint.

Run from the repo root:  .venv/bin/python -m pytest
"""

from pathlib import Path

import dagster as dg
import pytest

from dagster.components import load_from_defs_folder

SYNC_YAML = """\
type: orchestration.components.reverse_etl_cdf_sync.ReverseEtlCdfSync

attributes:
  name: orders
  catalog: de_dev
  schema_name: sr_poc_external
  source_table: their_table
  key_column: id
  state_backend: postgres
"""


def write_project(tmp_path: Path, deps: list[str]) -> Path:
    """The smallest project a team could ship: one defs folder per concern, names in YAML,
    values left to the environment. The root module's name is unique per tmp dir — the loader
    imports it, and a second project reusing the name would be handed the first one cached
    in sys.modules."""
    root = "".join(character if character.isalnum() else "_" for character in tmp_path.name)
    (tmp_path / root / "defs" / "orders_sync").mkdir(parents=True)
    (tmp_path / root / "defs" / "company_target").mkdir(parents=True)
    for init in (f"{root}/__init__.py", f"{root}/defs/__init__.py"):
        (tmp_path / init).write_text("")
    (tmp_path / "pyproject.toml").write_text(
        f'[tool.dg]\ndirectory_type = "project"\n\n[tool.dg.project]\nroot_module = "{root}"\n'
    )
    (tmp_path / root / "defs" / "orders_sync" / "defs.yaml").write_text(SYNC_YAML)
    (tmp_path / root / "defs" / "company_target" / "defs.yaml").write_text(
        "type: orchestration.components.reverse_etl_targets.ReverseEtlPostgresTarget\n\n"
        "attributes:\n"
        "  name: company\n"
        "  key_column: Company_ID\n"
        f"  deps: {deps!r}\n".replace("'", '"')
    )
    return tmp_path


def load(project: Path) -> dg.Definitions:
    defs = load_from_defs_folder(path_within_project=project)
    dg.Definitions.validate_loadable(defs)
    return defs


def spec_names(defs: dg.Definitions) -> set:
    return {s.key.to_user_string() for a in defs.assets for s in a.specs}


@pytest.mark.parametrize("deps", [["reverse_etl_orders_topic_setup"], ["not_built_anywhere"]], ids=["known-dep", "dangling-dep"])
def test_a_teams_project_loads_with_no_resources(monkeypatch, tmp_path, deps):
    monkeypatch.syspath_prepend(str(write_project(tmp_path, deps)))
    defs = load(tmp_path)
    assert {"reverse_etl_orders_to_kafka", "reverse_etl_orders_topic_setup", "reverse_etl_company_jdbc_sink"} <= spec_names(defs)
    # the sink declares the team's ordering (known or dangling — both load)
    sink = next(a for a in defs.assets if "reverse_etl_company_jdbc_sink" in {s.key.to_user_string() for s in a.specs})
    (spec,) = sink.specs
    assert {dep.asset_key.to_user_string() for dep in spec.deps} == set(deps)


def test_the_health_side_travels_with_the_target(monkeypatch, tmp_path):
    monkeypatch.syspath_prepend(str(write_project(tmp_path, [])))
    defs = load(tmp_path)
    (check,) = defs.asset_checks
    assert [(spec.name, spec.asset_key.to_user_string()) for spec in check.check_specs] == [
        ("sink_healthy", "reverse_etl_company_jdbc_sink")
    ]
    assert {job.name for job in defs.jobs} >= {"reverse_etl_orders_bootstrap_job", "reverse_etl_company_sink_health_job"}
    # every job resolves (the bootstrap job's topic -> sync edge, the health job's check)
    for job in defs.resolve_all_job_defs():
        assert defs.resolve_job_def(job.name) is not None
