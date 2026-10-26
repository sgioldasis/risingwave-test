"""The import-hygiene scan (release design step 8): the released components' import graph stays
demo-free. A team extracts the sync and its targets by copying the reachable modules — so no
reachable module may import the demo's assets (reverse_etl_demo_*, reverse_etl_change,
reverse_etl_reset) or the dissolved resource layer (reverse_etl_resources). The scanner works
on any package root, and the positive control proves it flags what it must flag — otherwise
the guarantee below is a tautology and the mutation check has nothing to hold it to.

Run from the repo root:  .venv/bin/python -m pytest
"""

import ast
from pathlib import Path
import re

FORBIDDEN = re.compile(r"reverse_etl_(demo_\w+|change|reset|resources)$")

# The released surface: the sync component and the target components (step 5).
RELEASED = [
    "orchestration.components.reverse_etl_cdf_sync",
    "orchestration.components.reverse_etl_targets",
]

PROJECT_ROOT = Path(__file__).parents[3]


def relative_imports(path: Path) -> list[tuple[int, str, list[str]]]:
    """Every relative import as (level, module, names), top-level or deferred (ast, not regex:
    a deferred import inside a function body is still an extractability boundary)."""
    found = []
    for node in ast.walk(ast.parse(path.read_text())):
        if isinstance(node, ast.ImportFrom) and node.level:
            found.append((node.level, node.module or "", [alias.name for alias in node.names]))
    return found


def import_targets(start: str, level: int, module: str, names: list[str]) -> list[str]:
    """The absolute modules a relative import may name: one parent hop per dot. `from x import y`
    can bind the module x.y or the attribute y of x — both forms are candidates."""
    parts = start.split(".")
    base = parts[: len(parts) - level] if len(parts) > level else []
    prefix = ".".join([*base, module]).strip(".")
    targets = [f"{prefix}.{name}".strip(".") for name in names]
    return [*dict.fromkeys(targets), prefix] if prefix else targets


def module_path(module: str, root: Path = PROJECT_ROOT) -> Path:
    return root / Path(*module.split(".")).with_suffix(".py")


def forbidden_imports(module: str, root: Path = PROJECT_ROOT) -> set[str]:
    """The forbidden modules `module` imports, by any of its relative imports."""
    bad = set()
    for level, module_part, names in relative_imports(module_path(module, root)):
        for target in import_targets(module, level, module_part, names):
            if FORBIDDEN.search(target.rsplit(".", 1)[-1]):
                bad.add(target)
    return bad


def reachable_modules(start: str, seen: set[str] | None = None, root: Path = PROJECT_ROOT) -> set[str]:
    """The module set a team would copy: walking relative imports from `start` within the
    package (the components import the assets' builders, the builders import the
    config/state/connection modules, and so on)."""
    seen = seen if seen is not None else set()
    if start in seen:
        return seen
    seen.add(start)
    for level, module_part, names in relative_imports(module_path(start, root)):
        for target in import_targets(start, level, module_part, names):
            if module_path(target, root).exists():
                reachable_modules(target, seen, root)
    return seen


def test_no_reachable_module_imports_a_demo_module():
    """The extractability guarantee: copying the reachable set is enough, so none of it reaches
    into the demo's assets (which need the demo's own tables and credentials)."""
    reached = set()
    for start in RELEASED:
        reached |= reachable_modules(start)
    assert "orchestration.assets.reverse_etl_cdf_setup" in reached  # the walk found the graph
    violations = {module: bad for module in sorted(reached) if (bad := forbidden_imports(module))}
    assert not violations, violations


def test_the_scan_flags_a_demo_import_and_passes_a_clean_one(tmp_path):
    """The positive control: over a synthetic package, a module importing a demo-side one is
    both walked into and flagged; a clean module reaches nothing forbidden."""
    (tmp_path / "team").mkdir()
    for name in ("__init__.py", "reverse_etl_demo_hooks.py", "reverse_etl_state.py"):
        (tmp_path / "team" / name).write_text("")
    (tmp_path / "team" / "clean.py").write_text("from .reverse_etl_state import SyncState\n")
    (tmp_path / "team" / "violator.py").write_text("from .reverse_etl_demo_hooks import DemoSyncHooks\n")

    # `from x import y` may bind the submodule y, so the walk legitimately enters reverse_etl_state
    assert reachable_modules("team.clean", root=tmp_path) == {"team.clean", "team.reverse_etl_state"}
    assert reachable_modules("team.violator", root=tmp_path) == {
        "team.violator", "team.reverse_etl_demo_hooks",
    }
    assert forbidden_imports("team.violator", root=tmp_path) == {"team.reverse_etl_demo_hooks"}
    assert forbidden_imports("team.clean", root=tmp_path) == set()
    # the released graph itself is clean too (the guarantee test pins it over the real tree)
    assert FORBIDDEN.search("reverse_etl_reset") and FORBIDDEN.search("reverse_etl_resources")
    assert not FORBIDDEN.search("reverse_etl_state") and not FORBIDDEN.search("reverse_etl_cdf_setup")
