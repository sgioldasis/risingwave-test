"""Convert the orders/avro watermark (state) tables to plain Delta.

The cdf state table was already recreated as plain Delta (Option A, verified in
option_a_plain_state_verification.py). The orders/avro state tables still carry
the old UniForm TBLPROPERTIES from the original setup DDL, and OSS Delta cannot
commit to UniForm tables ([DELTA_MISSING_ICEBERG_CLASS]) -- so the zero-DBU Spark
sync would fail at the watermark MERGE. The setup asset's CREATE TABLE IF NOT
EXISTS does not touch existing tables, so this script converts them in place:
drop, recreate plain, and re-insert the preserved watermark rows.

Same auth/warehouse machinery as the setup assets (Statement Execution API).
See docs/poc/REVERSE_ETL_DBU_COST_NOTES.md section 5.
"""

import sys

from orchestration.assets.databricks_optimize import DATABRICKS_HOST, _get_token
from orchestration.assets.reverse_etl_cdf_setup import _run_sql

CATALOG = "de_dev"
SCHEMA = "sr_poc_external"
TABLES = [f"{CATALOG}.{SCHEMA}.reverse_etl_orders_state", f"{CATALOG}.{SCHEMA}.reverse_etl_avro_state"]


def select_rows(data: dict) -> list[tuple[str, int]]:
    """Rows of `SELECT sync_name, last_commit_version ...` as (name, version) tuples."""
    return [(row[0], row[1]) for row in (data.get("result") or {}).get("data_array", [])]


def show_tblproperties(data: dict) -> list[list]:
    return [row for row in (data.get("result") or {}).get("data_array", [])]


def has_uniform(props: list[list]) -> bool:
    return any("universalFormat" in str(cell) for row in props for cell in row)


def convert(token: str, table: str) -> bool:
    print(f"\n=== {table} ===")

    before_props = show_tblproperties(_run_sql(token, f"SHOW TBLPROPERTIES {table}"))
    print(f"before: universalFormat={'PRESENT' if has_uniform(before_props) else 'absent'}")

    existing = select_rows(_run_sql(token, f"SELECT sync_name, last_commit_version FROM {table} ORDER BY sync_name"))
    print(f"before rows (preserved): {existing}")

    _run_sql(token, f"DROP TABLE IF EXISTS {table}")
    _run_sql(token, f"CREATE TABLE {table} (sync_name STRING, last_commit_version BIGINT) USING DELTA")
    for name, version in existing:
        _run_sql(token, f"INSERT INTO {table} VALUES ('{name}', {int(version)})")

    after_props = show_tblproperties(_run_sql(token, f"SHOW TBLPROPERTIES {table}"))
    after_rows = select_rows(_run_sql(token, f"SELECT sync_name, last_commit_version FROM {table} ORDER BY sync_name"))
    uniform_after = has_uniform(after_props)
    print(f"after:  universalFormat={'PRESENT' if uniform_after else 'absent'} (want absent)")
    print(f"after rows: {after_rows}")

    ok = not uniform_after and after_rows == existing
    print("PASS" if ok else "FAIL")
    return ok


def main() -> int:
    token = _get_token()
    failures = [table for table in TABLES if not convert(token, table)]
    if failures:
        print(f"\nFAILED: {failures}")
        return 1
    print(f"\nOK: state tables are plain Delta, watermark rows preserved. ({DATABRICKS_HOST})")
    return 0


if __name__ == "__main__":
    sys.exit(main())
