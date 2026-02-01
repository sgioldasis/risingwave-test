"""Constants for Dagster orchestration."""
from pathlib import Path

# Base paths
PROJECT_ROOT = Path(__file__).parent.parent
dbt_PROJECT_PATH = PROJECT_ROOT / "dbt"
dbt_MANIFEST_PATH = dbt_PROJECT_PATH / "target" / "manifest.json"

# dbt profile configuration
dbt_PROFILE_NAME = "funnel_profile"
dbt_TARGET_NAME = "dev"
