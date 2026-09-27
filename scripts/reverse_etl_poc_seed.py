#!/usr/bin/env python3
"""Seed a handful of INSERT/UPDATE/DELETE statements against the reverse-ETL
CDF POC source table (de_dev.sr_poc_external.reverse_etl_cdf_poc_source), so
the reverse_etl_cdf_to_kafka Dagster asset has new/changed/deleted rows to
sync. See docs/poc/REVERSE_ETL_CDF_POC_PLAN.md.

Not a configurable load generator like scripts/wallet_producer.py -- just
enough traffic to exercise all three CDF change types in one run.

Uses MERGE INTO (upsert), not a blind INSERT, for the "new rows" step --
Databricks Unity Catalog's PRIMARY KEY constraint is informational only and
does not reject duplicate ids (confirmed against current Databricks docs:
"Databricks does not enforce uniqueness during writes"), so a plain INSERT
run twice silently creates duplicate physical rows sharing the same id. This
was hit live: re-running this script produced two id=1/id=2 rows apiece,
which then made Databricks and RisingWave's PK-enforced mirror table
diverge in row count. Databricks' own recommendation for real uniqueness is
exactly this -- enforce it in the write path via MERGE INTO.

Usage:
    uv run python scripts/reverse_etl_poc_seed.py
"""

import sys
from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(REPO_ROOT))

from orchestration.assets.reverse_etl_cdf_setup import (  # noqa: E402
    CATALOG,
    SCHEMA,
    SOURCE_TABLE,
    _get_token,
    _require_databricks_env,
    _run_sql,
)


def main() -> None:
    _require_databricks_env()
    token = _get_token()
    table = f"{CATALOG}.{SCHEMA}.{SOURCE_TABLE}"

    print(f"Seeding {table} ...")

    # New rows (insert) -- MERGE, not blind INSERT, so re-running this script
    # never creates duplicate ids (see module docstring).
    _run_sql(
        token,
        f"""
        MERGE INTO {table} AS target
        USING (
            SELECT 1 AS id, 'first' AS value, current_timestamp() AS updated_at
            UNION ALL SELECT 2, 'second', current_timestamp()
            UNION ALL SELECT 3, 'third', current_timestamp()
        ) AS source
        ON target.id = source.id
        WHEN MATCHED THEN UPDATE SET target.value = source.value, target.updated_at = source.updated_at
        WHEN NOT MATCHED THEN INSERT (id, value, updated_at)
            VALUES (source.id, source.value, source.updated_at)
        """,
    )
    print("Upserted ids 1, 2, 3")

    # Changed row (update) -- CDF emits update_preimage + update_postimage
    _run_sql(
        token,
        f"UPDATE {table} SET value = 'second-updated', updated_at = current_timestamp() WHERE id = 2",
    )
    print("Updated id 2")

    # Deleted row (delete)
    _run_sql(token, f"DELETE FROM {table} WHERE id = 3")
    print("Deleted id 3")

    print("Done.")


if __name__ == "__main__":
    main()
