#!/usr/bin/env python3
"""Seed a handful of INSERT/UPDATE/DELETE statements against the reverse-ETL
CDF POC source table (de_dev.sr_poc_external.reverse_etl_cdf_poc_source), so
the reverse_etl_cdf_to_kafka Dagster asset has new/changed/deleted rows to
sync. See docs/poc/REVERSE_ETL_CDF_POC_PLAN.md.

Not a configurable load generator like scripts/wallet_producer.py -- just
enough traffic to exercise all three CDF change types in one run.

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

    # New rows (insert)
    _run_sql(
        token,
        f"""
        INSERT INTO {table} (id, value, updated_at) VALUES
            (1, 'first', current_timestamp()),
            (2, 'second', current_timestamp()),
            (3, 'third', current_timestamp())
        """,
    )
    print("Inserted ids 1, 2, 3")

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
