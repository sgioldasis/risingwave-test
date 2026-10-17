"""Postgres-side schema reconciliation for the reverse-ETL demo targets.

The Debezium JDBC sink only ever ADDs columns (it mirrors each message's
fields); it never removes them, and it re-adds a column if a replayed old
message still carries it. This module complements the sink for drop
propagation: the sync asset calls drop_absent_columns() after producing a
batch, guarded by cfg.propagate_drops and the evidence rule documented there.
"""

import os

import psycopg2
import requests

from .reverse_etl_config import ReverseEtlSyncConfig


def _get_postgres_connection():
    """Same credential convention as the sink connector (POSTGRES_USER /
    POSTGRES_PASSWORD against the HOST_POSTGRES_URL database) and the demo
    helper's check_postgres. Reached from the Dagster container through
    host.docker.internal, like the sink's default JDBC URL."""
    return psycopg2.connect(
        host=os.environ.get("POSTGRES_HOST", "host.docker.internal"),
        port=5432,
        dbname="postgres",
        user=os.environ.get("POSTGRES_USER", "postgres"),
        password=os.environ.get("POSTGRES_PASSWORD", ""),
        connect_timeout=10,
    )


def rename_columns_in_postgres(cfg: ReverseEtlSyncConfig, renames: list[tuple[str, str]]) -> list[str]:
    """Rename columns on the Postgres target with native ALTER TABLE ... RENAME
    COLUMN, preserving their data; returns the renames ensured, as "old -> new".

    After renaming it restarts the sink connector's task: the JDBC sink caches
    the table schema in memory, and its stale cache makes the next message —
    which carries the new name — fail the task unrecoverably (the same failure
    mode as a manual column drop, documented in the sink doc). Restarted from
    its committed offsets, the task rebuilds the cache from the real table.
    Idempotent: a rename already applied is detected and not re-attempted, but
    the restart still runs so a retried run cannot leave the stale cache
    behind. When BOTH columns exist — the new one created empty by the sink's
    schema evolution while the rename was pending — the data is copied across
    and the old column dropped, mirroring the RisingWave dance."""
    conn = _get_postgres_connection()
    ensured: list[str] = []
    try:
        with conn.cursor() as cur:
            cur.execute(
                "SELECT column_name FROM information_schema.columns "
                "WHERE table_schema = 'public' AND table_name = %s",
                (cfg.postgres_table,),
            )
            existing = {row[0] for row in cur.fetchall()}
            if not existing:
                return []
            for old, new in renames:
                if old in existing and new in existing:
                    cur.execute(f'UPDATE {cfg.postgres_table} SET "{new}" = "{old}"')
                    cur.execute(f'ALTER TABLE {cfg.postgres_table} DROP COLUMN "{old}"')
                    ensured.append(f"{old} -> {new}")
                elif old in existing and new not in existing:
                    cur.execute(f'ALTER TABLE {cfg.postgres_table} RENAME COLUMN "{old}" TO "{new}"')
                    ensured.append(f"{old} -> {new}")
                elif old not in existing and new in existing:
                    ensured.append(f"{old} -> {new}")
        conn.commit()
    finally:
        conn.close()
    if ensured:
        restart_sink_task(cfg)
    return ensured


def restart_sink_task(cfg: ReverseEtlSyncConfig) -> None:
    """POST /connectors/<name>/tasks/0/restart — the recovery the sink doc
    prescribes after manual Postgres schema surgery; here it is part of the
    rename itself, because the sink's in-memory schema cache outlives the
    DDL it did not perform."""
    url = os.environ.get("KAFKA_CONNECT_URL", "http://kafka-connect:8083")
    response = requests.post(f"{url}/connectors/{cfg.connector_name}/tasks/0/restart", timeout=15)
    if response.status_code not in (200, 201, 202, 204):
        raise RuntimeError(
            f"Sink task restart failed: HTTP {response.status_code} — {response.text[:200]}"
        )


def drop_absent_columns(cfg: ReverseEtlSyncConfig, source_names: set[str]) -> list[str]:
    """Drop every column of the Postgres target the source no longer mirrors
    (the key column excepted); returns the names actually dropped. A no-op
    (returns []) when the table doesn't exist yet.

    The broader-than-evidence rule is safe here precisely because the sink
    creates the table purely from message fields — every column it holds is a
    former source column, unlike RisingWave's designed <col>_ts companions —
    and it self-heals a sink replay re-adding a dropped column from an old
    message."""
    conn = _get_postgres_connection()
    try:
        with conn.cursor() as cur:
            cur.execute(
                "SELECT column_name FROM information_schema.columns "
                "WHERE table_schema = 'public' AND table_name = %s",
                (cfg.postgres_table,),
            )
            existing = {row[0] for row in cur.fetchall()}
            if not existing:
                return []
            extra = sorted(existing - set(source_names) - {cfg.key_column})
            for name in extra:
                cur.execute(f'ALTER TABLE {cfg.postgres_table} DROP COLUMN "{name}"')
        conn.commit()
        return extra
    finally:
        conn.close()
