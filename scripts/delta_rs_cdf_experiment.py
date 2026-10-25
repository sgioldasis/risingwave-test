"""Zero-DBU CDF experiment, take 2 — deltalake 1.x API (arro3-based)."""
import sys

REPO = "/Users/s.gioldasis-si/Projects/risingwave-test"
LOCATION = ("abfss://sr-poc-cont1@stkznneusrpoccdddevstd.dfs.core.windows.net/"
            "sr_poc_external/__unitystorage/schemas/6fa9db04-0d77-41dd-bc2d-2e8a8aeced7f/"
            "tables/747277c1-fdbb-4757-8055-78d6c2a0b1ed")
ACCOUNT = "stkznneusrpoccdddevstd"


def load_env(path):
    env = {}
    for line in open(path):
        line = line.strip()
        if not line or line.startswith("#") or "=" not in line:
            continue
        k, v = line.split("=", 1)
        env[k.strip()] = v.strip().strip('"').strip("'")
    return env


env = load_env(f"{REPO}/.env")

import deltalake
from deltalake import DeltaTable
import pyarrow as pa  # used to normalize arro3 objects

print(f"deltalake version: {deltalake.__version__}")

opts = {
    "account_name": ACCOUNT,
    "client_id": env["ADLS_CLIENT_ID"],
    "tenant_id": env["ADLS_TENANT_ID"],
    "client_secret": env["ADLS_CLIENT_SECRET"],
}
print(f"credentials: ADLS_* principal ({opts['client_id'][:4]}…, tenant {opts['tenant_id'][:8]}…)")

dt = DeltaTable(LOCATION, storage_options=opts)
print(f"opened table directly from abfss:// — version={dt.version()}")


def as_pa(obj):
    """Normalize a deltalake/arro3 result to a pyarrow Table."""
    if isinstance(obj, pa.Table):
        return obj
    if hasattr(obj, "read_all"):
        obj = obj.read_all()
    return pa.table(obj)


# --- current state ----------------------------------------------------------
print("\n=== current state (read via delta-rs, no Databricks) ===")
state_tbl = as_pa(dt.to_pyarrow_dataset().to_table())
print("columns:", state_tbl.column_names)
for row in state_tbl.to_pylist():
    print(" ", row)

# --- the change feed, from version 0 ----------------------------------------
import inspect
print(f"\n=== load_cdf{inspect.signature(dt.load_cdf)} from version 0 ===")
cdf_raw = dt.load_cdf(0)
cdf = as_pa(cdf_raw)
print(f"{cdf.num_rows} CDF row(s), columns: {cdf.column_names}")
for row in cdf.to_pylist():
    slim = {k: v for k, v in row.items()}
    print(" ", slim)

# --- replay check: does the CDF reconstruct the current state? --------------
print("\n=== replay check ===")
final = {}
for row in sorted(cdf.to_pylist(), key=lambda r: r.get("_commit_version") or 0):
    ct = row["_change_type"]
    if ct == "insert" or ct == "update_postimage":
        final[row["rid"]] = row["value"]
    elif ct == "delete":
        final.pop(row["rid"], None)
print("replayed:", final)
print("actual:  ", {r["rid"]: r["value"] for r in state_tbl.to_pylist()})
