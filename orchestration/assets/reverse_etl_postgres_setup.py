"""Postgres-side schema reconciliation for the reverse-ETL demo targets.

The Debezium JDBC sink only ever ADDs columns (it mirrors each message's
fields); it never removes them, and it re-adds a column if a replayed old
message still carries it. This module complements the sink for drop
propagation: the sync asset calls drop_absent_columns() after producing a
batch, guarded by cfg.propagate_drops and the evidence rule documented there.
"""

import os

import psycopg2

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
