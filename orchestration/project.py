"""Dbt project configuration for Dagster."""
from dagster_dbt import DbtProject
from .constants import dbt_PROJECT_PATH, dbt_MANIFEST_PATH

dbt_project = DbtProject(
    project_dir=dbt_PROJECT_PATH,
    target_path=dbt_PROJECT_PATH / "target",
)
