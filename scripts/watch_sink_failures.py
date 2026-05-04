#!/usr/bin/env python3
"""Lightweight watchdog that recreates only failed sinks on SINK_FAIL events.

This script polls RisingWave event logs and reacts to new SINK_FAIL entries by
running a targeted dbt model for the affected sink.
"""

from __future__ import annotations

import argparse
import json
import logging
import os
import signal
import subprocess
import sys
import time
from dataclasses import dataclass
from pathlib import Path
from typing import Dict, Iterable, List, Optional


LOGGER = logging.getLogger("sink_watchdog")

# Map RisingWave sink object name -> dbt model name.
SINK_TO_DBT_MODEL: Dict[str, str] = {
    "iceberg_funnel_sink": "sink_funnel_to_iceberg",
    "rw_managed_funnel_sink": "sink_funnel_to_rw_iceberg",
    "sink_hermes_features_to_iceberg": "sink_hermes_features_to_iceberg",
    "funnel_postgres_sink": "sink_funnel_to_postgres",
    "funnel_kafka_sink": "sink_funnel_to_kafka",
}


@dataclass
class SinkFailEvent:
    unique_id: str
    sink_name: str
    sink_id: Optional[int]
    error: str
    timestamp_text: str


class GracefulShutdown:
    """Signal-aware shutdown flag."""

    def __init__(self) -> None:
        self.stop = False
        signal.signal(signal.SIGINT, self._handle)
        signal.signal(signal.SIGTERM, self._handle)

    def _handle(self, signum: int, _frame) -> None:
        LOGGER.info("Received signal %s, shutting down.", signum)
        self.stop = True


def run_cmd(command: List[str], cwd: Optional[Path] = None) -> subprocess.CompletedProcess:
    """Run a command and return completed process with captured output."""
    return subprocess.run(
        command,
        cwd=str(cwd) if cwd else None,
        text=True,
        capture_output=True,
        check=False,
    )


def load_state(state_file: Path) -> Dict[str, object]:
    if not state_file.exists():
        return {"seen_event_ids": []}
    try:
        return json.loads(state_file.read_text())
    except Exception as exc:  # pylint: disable=broad-except
        LOGGER.warning("Could not parse state file %s (%s). Starting fresh.", state_file, exc)
        return {"seen_event_ids": []}


def save_state(state_file: Path, state: Dict[str, object]) -> None:
    state_file.parent.mkdir(parents=True, exist_ok=True)
    temp = state_file.with_suffix(".tmp")
    temp.write_text(json.dumps(state, indent=2, sort_keys=True))
    temp.replace(state_file)


def fetch_sink_fail_events(
    rw_host: str,
    rw_port: int,
    rw_user: str,
    rw_db: str,
    lookback_minutes: int,
) -> List[SinkFailEvent]:
    """Fetch recent SINK_FAIL events from rw_event_logs."""
    sql = f"""
SELECT unique_id,
       timestamp::text,
       info::text
FROM rw_catalog.rw_event_logs
WHERE event_type = 'SINK_FAIL'
  AND timestamp > NOW() - INTERVAL '{lookback_minutes} minutes'
ORDER BY timestamp ASC, unique_id ASC;
""".strip()

    cmd = [
        "psql",
        "-h",
        rw_host,
        "-p",
        str(rw_port),
        "-U",
        rw_user,
        "-d",
        rw_db,
        "-At",
        "-F",
        "\t",
        "-c",
        sql,
    ]
    proc = run_cmd(cmd)
    if proc.returncode != 0:
        raise RuntimeError(f"psql query failed: {proc.stderr.strip()}")

    events: List[SinkFailEvent] = []
    for line in proc.stdout.splitlines():
        parts = line.split("\t", 2)
        if len(parts) != 3:
            continue
        unique_id, ts_text, info_text = parts
        try:
            info_obj = json.loads(info_text)
            sink_fail = info_obj.get("sinkFail", {})
            sink_name = sink_fail.get("sinkName", "")
            sink_id_raw = sink_fail.get("sinkId")
            sink_id = int(sink_id_raw) if sink_id_raw is not None else None
            error = sink_fail.get("error", "")
        except Exception:  # pylint: disable=broad-except
            continue
        if not sink_name:
            continue
        events.append(
            SinkFailEvent(
                unique_id=unique_id,
                sink_name=sink_name,
                sink_id=sink_id,
                error=error,
                timestamp_text=ts_text,
            )
        )
    return events


def run_targeted_dbt(
    model_name: str,
    project_root: Path,
    use_container: bool,
    container_name: str,
) -> subprocess.CompletedProcess:
    """Run dbt for a single model either locally or inside the dagster container."""
    if use_container:
        cmd = [
            "docker",
            "exec",
            container_name,
            "bash",
            "-lc",
            f"cd /workspace/dbt && uv run dbt run --profiles-dir . --select {model_name}",
        ]
        return run_cmd(cmd)

    cmd = ["uv", "run", "dbt", "run", "--profiles-dir", ".", "--select", model_name]
    return run_cmd(cmd, cwd=project_root / "dbt")


def maybe_recreate_sink(
    event: SinkFailEvent,
    project_root: Path,
    use_container: bool,
    container_name: str,
    dry_run: bool,
) -> bool:
    model = SINK_TO_DBT_MODEL.get(event.sink_name)
    if not model:
        LOGGER.warning(
            "No dbt mapping for sink '%s' (sink_id=%s). Skipping.",
            event.sink_name,
            event.sink_id,
        )
        return False

    LOGGER.warning(
        "Recreating sink '%s' via dbt model '%s' due to SINK_FAIL %s (error=%s)",
        event.sink_name,
        model,
        event.unique_id,
        event.error,
    )
    if dry_run:
        LOGGER.info("Dry-run enabled; skipping dbt execution for '%s'.", event.sink_name)
        return True

    proc = run_targeted_dbt(
        model_name=model,
        project_root=project_root,
        use_container=use_container,
        container_name=container_name,
    )
    if proc.returncode == 0:
        LOGGER.info("Successfully recreated sink '%s'.", event.sink_name)
        return True

    LOGGER.error(
        "Failed to recreate sink '%s' (model=%s). exit=%s\nstdout:\n%s\nstderr:\n%s",
        event.sink_name,
        model,
        proc.returncode,
        proc.stdout.strip(),
        proc.stderr.strip(),
    )
    return False


def cap_seen_ids(ids: Iterable[str], max_len: int = 2000) -> List[str]:
    seen = list(ids)
    if len(seen) <= max_len:
        return seen
    return seen[-max_len:]


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Watch SINK_FAIL events and auto-recreate affected sink")
    parser.add_argument("--poll-interval-sec", type=float, default=10.0)
    parser.add_argument("--lookback-minutes", type=int, default=10)
    parser.add_argument("--state-file", default="/tmp/rw_sink_watchdog_state.json")
    parser.add_argument("--project-root", default=".")
    parser.add_argument("--rw-host", default="localhost")
    parser.add_argument("--rw-port", type=int, default=4566)
    parser.add_argument("--rw-user", default="root")
    parser.add_argument("--rw-db", default="dev")
    parser.add_argument("--container-name", default="dagster-webserver")
    parser.add_argument(
        "--run-dbt-in-container",
        action="store_true",
        default=True,
        help="Run dbt in docker container (default true).",
    )
    parser.add_argument(
        "--no-run-dbt-in-container",
        dest="run_dbt_in_container",
        action="store_false",
        help="Run dbt from local environment instead of container.",
    )
    parser.add_argument("--once", action="store_true", help="Process one polling cycle and exit.")
    parser.add_argument("--dry-run", action="store_true", help="Log actions without running dbt.")
    parser.add_argument("--verbose", action="store_true")
    return parser.parse_args()


def main() -> int:
    args = parse_args()
    logging.basicConfig(
        level=logging.DEBUG if args.verbose else logging.INFO,
        format="%(asctime)s %(levelname)s %(name)s - %(message)s",
    )

    project_root = Path(args.project_root).resolve()
    state_file = Path(args.state_file)
    state_exists = state_file.exists()
    state = load_state(state_file)
    seen_ids = set(state.get("seen_event_ids", []))

    shutdown = GracefulShutdown()
    LOGGER.info(
        "Starting sink watchdog: poll_interval=%ss lookback=%sm container=%s",
        args.poll_interval_sec,
        args.lookback_minutes,
        args.container_name,
    )

    # On first run, avoid replaying historical failures by seeding seen IDs.
    if not state_exists:
        try:
            bootstrap_events = fetch_sink_fail_events(
                rw_host=args.rw_host,
                rw_port=args.rw_port,
                rw_user=args.rw_user,
                rw_db=args.rw_db,
                lookback_minutes=args.lookback_minutes,
            )
            if bootstrap_events:
                seen_ids.update(event.unique_id for event in bootstrap_events)
                state["seen_event_ids"] = cap_seen_ids(sorted(seen_ids))
                save_state(state_file, state)
                LOGGER.info(
                    "Initialized state with %d existing SINK_FAIL event(s); only new failures will trigger actions.",
                    len(bootstrap_events),
                )
        except Exception as exc:  # pylint: disable=broad-except
            LOGGER.warning("Bootstrap scan failed: %s", exc)

    while not shutdown.stop:
        try:
            events = fetch_sink_fail_events(
                rw_host=args.rw_host,
                rw_port=args.rw_port,
                rw_user=args.rw_user,
                rw_db=args.rw_db,
                lookback_minutes=args.lookback_minutes,
            )

            new_events = [e for e in events if e.unique_id not in seen_ids]
            if new_events:
                LOGGER.info("Detected %d new SINK_FAIL event(s).", len(new_events))
            for event in new_events:
                maybe_recreate_sink(
                    event=event,
                    project_root=project_root,
                    use_container=args.run_dbt_in_container,
                    container_name=args.container_name,
                    dry_run=args.dry_run,
                )
                seen_ids.add(event.unique_id)

            state["seen_event_ids"] = cap_seen_ids(sorted(seen_ids))
            save_state(state_file, state)

        except Exception as exc:  # pylint: disable=broad-except
            LOGGER.error("Watchdog loop error: %s", exc)

        if args.once:
            break

        time.sleep(args.poll_interval_sec)

    LOGGER.info("Sink watchdog stopped.")
    return 0


if __name__ == "__main__":
    sys.exit(main())
