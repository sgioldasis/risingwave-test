"""Checks against the running local stack, driven through the Dagster webserver container. Skipped by default; run with

    .venv/bin/python -m pytest -m live

They need the stack up (./bin/1_up.sh), the Databricks and staging Kafka / registry credentials in the container, and
`docker` on the host. They WRITE to the DEV Databricks workspace, the staging Kafka topics and the staging registry,
exactly as the demos do, so run them deliberately. The container's code is the bind-mounted orchestration/ directory,
so restart the Dagster containers first if you changed it.
"""

import json
import re
import subprocess

import pytest

pytestmark = [pytest.mark.live, pytest.mark.timeout(1200)]

CONTAINER = "dagster-webserver"
DELTA = "reverse_etl_delta"


def docker_exec(*args: str, env: dict | None = None) -> subprocess.CompletedProcess:
    options = [item for key, value in (env or {}).items() for item in ("-e", f"{key}={value}")]
    return subprocess.run(
        ["docker", "exec", *options, "-w", "/workspace", CONTAINER, *args], capture_output=True, text=True, timeout=900
    )


def materialize(asset: str, config: dict | None = None, env: dict | None = None) -> str:
    """Materialize one asset with the Dagster CLI inside the container; returns the combined log."""
    command = ["dagster", "asset", "materialize", "--select", asset, "-m", "orchestration.definitions"]
    if config:
        command += ["--config-json", json.dumps({"ops": {asset: {"config": config}}})]
    result = docker_exec(*command, env=env)
    return result.stdout + result.stderr


def succeeded(log: str) -> bool:
    return "RUN_SUCCESS" in log and "RUN_FAILURE" not in log


@pytest.fixture(scope="module", autouse=True)
def stack_is_up():
    try:
        running = subprocess.run(
            ["docker", "inspect", "-f", "{{.State.Running}}", CONTAINER], capture_output=True, text=True, timeout=30
        ).stdout.strip()
    except (OSError, subprocess.TimeoutExpired):
        running = ""
    if running != "true":
        pytest.skip(f"the {CONTAINER} container is not running")


def test_the_jobotron_demo_publishes_and_the_consumer_mirrors_the_latest_publish():
    publish = materialize("reverse_etl_jobotron_publish")
    assert succeeded(publish), publish[-2000:]
    selected = int(re.search(r"Candidate query selected (\d+) customer", publish).group(1))
    run_id = re.search(r"__ASSET_JOB - ([0-9a-f-]{36}) -", publish).group(1)

    consume = materialize("reverse_etl_jobotron_consume")
    assert succeeded(consume), consume[-2000:]
    held = [int(n) for n in re.findall(rf"latest publish {run_id} holds (\d+) customer", consume)]
    assert held and sum(held) == selected  # the consumer found this very publish, and all of it


def target_values() -> str:
    """The delta target's values of the demo column, read through the env-fed state connection."""
    code = (
        "from orchestration.assets.reverse_etl_connections import state_postgres\n"
        "cur = state_postgres().connect().cursor()\n"
        "cur.execute('SELECT renamed_col_1 FROM reverse_etl_delta_target WHERE renamed_col_1 IS NOT NULL')\n"
        "print('VALUES', [row[0] for row in cur.fetchall()])\n"
    )
    return docker_exec("python", "-c", code).stdout


def make_change(change: str) -> str:
    asset = f"{DELTA}_make_change"
    return materialize(asset, {"change": change})


def drop_demo_columns() -> None:
    """Drop every demo column the source has and sync the drops to the targets, so each run starts from the demo's
    original shape and leaves it that way."""
    for _ in range(6):
        if "skipped" in make_change("drop_column"):
            break
    assert succeeded(materialize(f"{DELTA}_to_kafka"))


def test_a_value_written_before_a_rename_keeps_its_value_at_the_default_chunk_size():
    """Add a column and fill it on the latest row, rename it, then sync both commits in one backlog with 10 versions per
    chunk. The sync must split the chunks at the rename, so the value arrives under the new name instead of as null."""
    drop_demo_columns()
    try:
        assert succeeded(make_change("add_column"))
        assert succeeded(make_change("rename_column"))
        sync = materialize(f"{DELTA}_to_kafka", env={"REVERSE_ETL_CDF_CHUNK_VERSIONS": "10"})
        assert succeeded(sync), sync[-2000:]
        assert "new_col_1 -> renamed_col_1" in sync
        values = target_values()
        assert "'added " in values, values
    finally:
        drop_demo_columns()
