"""The sync's watermark state: where the last synced version and the last-seen schema live.

Two backends behind one small interface (plan section 12):

- `PostgresSyncState` — the default: the shared Postgres table `reverse_etl_sync_state`, keyed by
  sync_name, that the team creates (D1). The precondition fails with the DDL when it is absent.
- `DuckDBSyncState` — a local file, single writer, created on first run (D15): a local file is not
  shared infrastructure, so the sync may bring it into being itself.

Both keep the same columns and the same semantics — append-only writes, freshest-row-wins reads —
so a sync's replay behaviour does not depend on the backend. No imports from the other asset
modules, so any of them can import this one.
"""

from typing import Protocol

import psycopg2

STATE_TABLE = "reverse_etl_sync_state"


def state_table_ddl(table: str = STATE_TABLE) -> str:
    """The state table's DDL — what the demo's table_setup runs and what the Postgres
    precondition's error carries, so a team without the demo can create the table from the
    failure message alone."""
    return f"""CREATE TABLE IF NOT EXISTS {table} (
    sync_name TEXT NOT NULL,
    last_commit_version BIGINT NOT NULL,
    last_schema TEXT,
    updated_at TIMESTAMPTZ NOT NULL DEFAULT now()
)"""


# Back-compat name for the default table's DDL (tests and docs refer to it).
STATE_TABLE_DDL = state_table_ddl()


class SyncState(Protocol):
    """The sync's watermark store. A failed run leaves the watermark unmoved, so the next run
    replays the same versions and re-runs whatever failed — every implementation must keep that
    true (targets upsert by key, which makes the replay safe)."""

    def require(self) -> None:
        """The run's precondition. Postgres: fail with the DDL when the table is absent. DuckDB:
        create the file and table on first run."""
        ...

    def read(self, sync_name: str) -> list[tuple[int | None, str | None]]:
        """[(last_commit_version, last_schema), ...] freshest first. An empty list is the
        first-run state, not an error."""
        ...

    def write(self, sync_name: str, version: int, last_schema: str) -> None: ...

    def create(self) -> None:
        """Create the store if absent (the demo's table_setup)."""
        ...

    def clear(self, sync_name: str) -> str:
        """Delete one sync's rows (the store may be shared — never drop it); returns the line to
        log."""
        ...


class PostgresSyncState:
    """The watermark in the shared Postgres table (keyed by sync_name, so a reset clears only its
    own rows)."""

    def __init__(self, postgres, table: str = STATE_TABLE):
        self.postgres, self.table = postgres, table

    def ddl(self) -> str:
        return state_table_ddl(self.table)

    def require(self) -> None:
        conn = self.postgres.connect()
        try:
            with conn.cursor() as cur:
                cur.execute(f"SELECT to_regclass('{self.table}')")
                present = cur.fetchall()[0][0] is not None
        finally:
            conn.close()
        if not present:
            raise RuntimeError(
                f"The watermark state table {self.table} does not exist -- create it before running the sync:\n"
                f"{self.ddl()}"
            )

    def read(self, sync_name: str) -> list[tuple[int | None, str | None]]:
        conn = self.postgres.connect()
        try:
            with conn.cursor() as cur:
                try:
                    cur.execute(
                        f"SELECT last_commit_version, last_schema FROM {self.table} "
                        "WHERE sync_name = %s ORDER BY last_commit_version DESC",
                        (sync_name,),
                    )
                except psycopg2.errors.UndefinedTable:
                    conn.rollback()
                    return []
                return [(int(v) if v is not None else None, s) for v, s in cur.fetchall()]
        finally:
            conn.close()

    def write(self, sync_name: str, version: int, last_schema: str) -> None:
        # The precondition at the top of the run checked the table, so an INSERT here never
        # creates it — a table dropped mid-run fails the run with the watermark unmoved, and the
        # next run replays the same versions.
        conn = self.postgres.connect()
        try:
            with conn.cursor() as cur:
                cur.execute(
                    f"INSERT INTO {self.table} (sync_name, last_commit_version, last_schema) VALUES (%s, %s, %s)",
                    (sync_name, version, last_schema),
                )
            conn.commit()
        finally:
            conn.close()

    def create(self) -> None:
        conn = self.postgres.connect()
        try:
            with conn.cursor() as cur:
                cur.execute(self.ddl())
            conn.commit()
        finally:
            conn.close()

    def clear(self, sync_name: str) -> str:
        conn = self.postgres.connect()
        try:
            with conn.cursor() as cur:
                try:
                    cur.execute(f"DELETE FROM {self.table} WHERE sync_name = %s", (sync_name,))
                except psycopg2.errors.UndefinedTable:
                    conn.rollback()
                    return f"{self.table} does not exist — nothing to clear"
                else:
                    conn.commit()
                    return (
                        f"Cleared {sync_name}'s watermark rows from {self.table} "
                        "(other syncs' watermarks untouched)"
                    )
        finally:
            conn.close()


class DuckDBSyncState:
    """The watermark in a local DuckDB file: one sync per file (DuckDB allows one writer), same
    table and columns as the Postgres backend. Created on first run (D15) — a local file is not
    shared infrastructure, so require() brings it into being instead of failing."""

    def __init__(self, path: str):
        self.path = path

    def _connect(self):
        # Imported here so a Postgres-state sync never pays the duckdb import.
        import duckdb

        return duckdb.connect(self.path)

    def ddl(self) -> str:
        return state_table_ddl(STATE_TABLE)

    def require(self) -> None:
        self.create()

    def create(self) -> None:
        with self._connect() as con:
            con.execute(self.ddl())

    def read(self, sync_name: str) -> list[tuple[int | None, str | None]]:
        with self._connect() as con:
            rows = con.execute(
                f"SELECT last_commit_version, last_schema FROM {STATE_TABLE} "
                "WHERE sync_name = ? ORDER BY last_commit_version DESC",
                [sync_name],
            ).fetchall()
        return [(int(v) if v is not None else None, s) for v, s in rows]

    def write(self, sync_name: str, version: int, last_schema: str) -> None:
        with self._connect() as con:
            con.execute(
                f"INSERT INTO {STATE_TABLE} (sync_name, last_commit_version, last_schema) VALUES (?, ?, ?)",
                [sync_name, version, last_schema],
            )

    def clear(self, sync_name: str) -> str:
        with self._connect() as con:
            con.execute(f"DELETE FROM {STATE_TABLE} WHERE sync_name = ?", [sync_name])
        return f"Cleared {sync_name}'s watermark rows from {self.path} (other syncs' watermarks untouched)"


def build_sync_state(cfg, postgres=None) -> SyncState:
    """The state backend the config chose. Postgres needs the postgres resource; DuckDB needs
    only its file path — a duckdb-state sync never requires the postgres resource."""
    if cfg.state_backend == "duckdb":
        return DuckDBSyncState(cfg.state_path)
    if postgres is None:
        raise ValueError("the postgres state backend needs the postgres resource")
    return PostgresSyncState(postgres, cfg.state_table)
