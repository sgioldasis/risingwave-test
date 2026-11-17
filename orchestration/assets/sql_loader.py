"""Loader for the extracted StarRocks SQL files under sql/starrocks/.

The Dagster assets' StarRocks DDL (Routine Loads, AGGREGATE/UNIQUE KEY
tables, the synchronous rollup MV, the serving-feature demo objects)
lives as plain .sql files in sql/starrocks/ -- one file per object,
audience-readable -- instead of inline Python string constants (moved
there 2026-11-17). Placeholders use {name} and are filled with
str.format; the files must contain no literal braces. The reverse-ETL
target (reverse_etl_starrocks_target.py) deliberately stays inline: its
DDL is generated from the sync's row fields, not static SQL.
"""

from pathlib import Path

_SQL_DIR = Path(__file__).resolve().parents[2] / "sql" / "starrocks"


def load_sql(name: str, **params: str) -> str:
    """Read sql/starrocks/<name>.sql and fill its {placeholders}."""
    return (_SQL_DIR / f"{name}.sql").read_text().format(**params)
