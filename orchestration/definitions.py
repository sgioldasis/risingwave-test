"""Dagster definitions for the realtime funnel dbt project."""
from dagster import Definitions, define_asset_job, ScheduleDefinition, AssetExecutionContext
from dagster_dbt import DbtCliResource, DbtProject, dbt_assets, DagsterDbtTranslator

from .constants import dbt_PROJECT_PATH


# Custom translator to add compute kind and group based on dbt tags
class CustomDagsterDbtTranslator(DagsterDbtTranslator):
    def get_asset_spec(self, manifest, unique_id, project):
        # Get base spec from parent
        spec = super().get_asset_spec(manifest, unique_id, project)
        
        # Get the dbt resource properties
        dbt_resource_props = manifest.get("nodes", {}).get(unique_id, {})
        tags = dbt_resource_props.get("tags", [])
        
        # If the model has 'iceberg' tag, add it as a kind and group
        if "iceberg" in tags:
            # Get existing kinds or start with empty set
            kinds = set(spec.kinds) if spec.kinds else set()
            kinds.add("iceberg")
            # Return a new spec with the updated kinds and datalake group
            return spec.replace_attributes(
                kinds=frozenset(kinds),
                group_name="datalake"
            )
        else:
            # Set group to "risingwave" for non-iceberg models
            return spec.replace_attributes(group_name="risingwave")


# Define the dbt project
dbt_project = DbtProject(
    project_dir=dbt_PROJECT_PATH,
)

# Create translator instance
custom_translator = CustomDagsterDbtTranslator()


@dbt_assets(manifest=dbt_project.manifest_path, dagster_dbt_translator=custom_translator)
def realtime_funnel_dbt_assets(context: AssetExecutionContext, dbt: DbtCliResource):
    """All dbt assets for the realtime funnel project."""
    context.log.info(f"Starting dbt build for project: {dbt_PROJECT_PATH}")
    
    # Run dbt build and stream events
    yield from dbt.cli(["build"], context=context).stream()


# Define jobs
dbt_build_job = define_asset_job(
    name="dbt_build_job",
    selection=[realtime_funnel_dbt_assets],
    description="Build all dbt models for the realtime funnel project",
)

# Define schedules - run every 5 minutes
dbt_build_schedule = ScheduleDefinition(
    job=dbt_build_job,
    cron_schedule="*/5 * * * *",
    name="dbt_build_schedule",
    description="Run dbt build every 5 minutes",
)

# Dagster definitions
defs = Definitions(
    assets=[realtime_funnel_dbt_assets],
    jobs=[dbt_build_job],
    schedules=[dbt_build_schedule],
    resources={
        "dbt": DbtCliResource(project_dir=str(dbt_PROJECT_PATH)),
    },
)
