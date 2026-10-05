"""Run drt (https://github.com/drt-hub/drt) against the demo project in orchestration/drt_demo/.

drt is a third-party reverse-ETL tool, installed in its own virtualenv inside the Dagster
container (DRT_VENV, outside the Dagster environment). It reads a Databricks table and
writes it to Postgres. This module only starts it:

- the Databricks access token is minted here with the same service-principal flow the other
  assets use, and handed to drt in an environment variable, never written to a file;
- drt is started with a minimal environment (PATH, HOME and that token), so it never sees the
  service principal's client secret or the Kafka credentials in this container's environment;
- the profile file holds only the workspace host and SQL warehouse path.

For a comparison with the CDF pipeline; see docs/poc/REVERSE_ETL_DEBEZIUM_JDBC_SINK.md.
"""

import os
import shutil
import subprocess
from pathlib import Path

from dagster import OpExecutionContext, in_process_executor, job, op

from .databricks_optimize import DATABRICKS_HOST, WAREHOUSE_ID, _get_token
from .reverse_etl_cdf_setup import _require_databricks_env

DRT_VENV = Path(os.environ.get("DRT_VENV", "/home/dagster/drt-venv"))
PROJECT_SOURCE = Path(__file__).resolve().parent.parent / "drt_demo"
# drt writes state next to the project, so run it from a writable copy that persists.
WORKDIR = Path(os.environ.get("DRT_WORKDIR", "/home/dagster/drt-demo"))
PROFILE_NAME = "reverse_etl_drt"
TOKEN_ENV = "DRT_DATABRICKS_TOKEN"


def _write_profile(home: Path) -> None:
    profile_dir = home / ".drt"
    profile_dir.mkdir(parents=True, exist_ok=True)
    hostname = DATABRICKS_HOST.removeprefix("https://").removeprefix("http://").rstrip("/")
    (profile_dir / "profiles.yml").write_text(
        f"{PROFILE_NAME}:\n"
        "  type: databricks\n"
        f"  server_hostname: {hostname}\n"
        f"  http_path: /sql/1.0/warehouses/{WAREHOUSE_ID}\n"
        f"  access_token_env: {TOKEN_ENV}\n"
        "  catalog: de_dev\n"
        "  schema: sr_poc_external\n"
    )


def run_drt(args: list[str], timeout: int = 900, extra_env: dict[str, str] | None = None) -> subprocess.CompletedProcess:
    """Run `drt <args>` in the demo project and return the finished process (output captured)."""
    _require_databricks_env()
    if not (DRT_VENV / "bin" / "drt").exists():
        raise RuntimeError(f"drt is not installed at {DRT_VENV} (uv pip install 'drt-core[databricks,postgres]')")

    WORKDIR.mkdir(parents=True, exist_ok=True)
    shutil.copytree(PROJECT_SOURCE, WORKDIR, dirs_exist_ok=True, ignore=shutil.ignore_patterns("__pycache__"))
    home = Path(os.environ.get("HOME", "/home/dagster"))
    _write_profile(home)

    env = {"PATH": f"{DRT_VENV / 'bin'}:/usr/bin:/bin", "HOME": str(home), TOKEN_ENV: _get_token(), **(extra_env or {})}
    return subprocess.run(
        [str(DRT_VENV / "bin" / "drt"), *args],
        cwd=WORKDIR,
        env=env,
        capture_output=True,
        text=True,
        timeout=timeout,
    )


# Names follow reverse_etl_<label>_<role>; the label is cdf_drt (target table reverse_etl_cdf_drt_target).
DRT_SYNC_NAME = "cdf_to_postgres"


@op(name="reverse_etl_cdf_drt_sync")
def run_drt_sync(context: OpExecutionContext) -> None:
    # PGTZ=UTC: drt writes timestamps without a timezone, and Postgres would read them in its own zone.
    result = run_drt(["run", "--select", DRT_SYNC_NAME, "--verbose"], extra_env={"PGTZ": "UTC"})
    output = "\n".join(
        line for line in (result.stdout + result.stderr).splitlines() if "pyarrow" not in line and line.strip()
    )
    context.log.info(output)
    if result.returncode != 0:
        raise RuntimeError(f"drt sync {DRT_SYNC_NAME} failed (exit {result.returncode}):\n{output[-1500:]}")


@job(
    name="reverse_etl_cdf_drt_sync_job",
    description=(
        "Comparison demo: sync the Databricks table reverse_etl_cdf_source straight into the Postgres table "
        "reverse_etl_cdf_drt_target with drt (mirror, no Kafka). Does not alter the target table, so a new "
        "source column makes it fail until the table is altered by hand."
    ),
    executor_def=in_process_executor,
)
def reverse_etl_cdf_drt_sync_job():
    run_drt_sync()
