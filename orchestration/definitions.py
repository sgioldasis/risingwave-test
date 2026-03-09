"""Dagster definitions for the realtime funnel dbt project."""
import os
import subprocess
import json
from datetime import datetime, timezone
from pathlib import Path
from dagster import (
    Definitions,
    define_asset_job,
    ScheduleDefinition,
    AssetExecutionContext,
    asset,
    sensor,
    RunRequest,
    DefaultSensorStatus,
    AssetKey,
)
from dagster_dbt import DbtCliResource, DbtProject, dbt_assets, DagsterDbtTranslator

from .constants import dbt_PROJECT_PATH
from .assets.iceberg_countries import iceberg_countries

# Note: RisingWave native Iceberg source is working via Trino-written tables
# See ICEBERG_RISINGWAVE_INTEGRATION.md for details


def _ensure_valid_manifest():
    """Ensure manifest exists with all required fields for Dagster."""
    manifest_path = Path(dbt_PROJECT_PATH) / "target" / "manifest.json"
    manifest_path.parent.mkdir(parents=True, exist_ok=True)
    
    manifest = None
    needs_write = False
    
    if manifest_path.exists():
        try:
            with open(manifest_path) as f:
                manifest = json.load(f)
        except Exception:
            manifest = None
    
    if manifest is None:
        manifest = {}
        needs_write = True
    
    # Ensure all required fields exist
    required_fields = {
        "nodes": {},
        "sources": {},
        "metrics": {},
        "exposures": {},
        "macros": {},
        "docs": {},
        "child_map": {},
        "parent_map": {},
        "groups": {},
        "selectors": {},
        "disabled": {},
    }
    
    for field, default_value in required_fields.items():
        if field not in manifest:
            manifest[field] = default_value
            needs_write = True
    
    # Ensure metadata exists
    if "metadata" not in manifest:
        manifest["metadata"] = {
            "dbt_schema_version": "https://schemas.getdbt.com/dbt/manifest/v12.json",
            "dbt_version": "1.10.0",
            "generated_at": datetime.now(timezone.utc).isoformat(),
            "invocation_id": "init",
            "env": {}
        }
        needs_write = True
    
    if needs_write:
        with open(manifest_path, 'w') as f:
            json.dump(manifest, f)
        print(f"Validated/fixed manifest at {manifest_path}")


# Run validation immediately on module load
_ensure_valid_manifest()


# Custom translator to add compute kind and group based on dbt tags
class CustomDagsterDbtTranslator(DagsterDbtTranslator):
    def get_asset_spec(self, manifest, unique_id, project):
        # Get base spec from parent
        spec = super().get_asset_spec(manifest, unique_id, project)
        
        # Get the dbt resource properties
        dbt_resource_props = manifest.get("nodes", {}).get(unique_id, {})
        tags = dbt_resource_props.get("tags", [])
        config = dbt_resource_props.get("config", {})
        meta = config.get("meta", {})
        dagster_meta = meta.get("dagster", {})
        deps = dagster_meta.get("deps", [])
        
        # Build deps list from meta config
        asset_deps = []
        for dep in deps:
            if isinstance(dep, str):
                asset_deps.append(AssetKey(dep))
            elif isinstance(dep, dict):
                asset_key = dep.get("asset_key")
                if asset_key:
                    if isinstance(asset_key, str):
                        asset_deps.append(AssetKey(asset_key))
                    elif isinstance(asset_key, list):
                        asset_deps.append(AssetKey(asset_key))
        
        # If the model has 'iceberg' tag, add it as a kind and group
        if "iceberg" in tags:
            kinds = set(spec.kinds) if spec.kinds else set()
            kinds.add("iceberg")
            new_spec = spec.replace_attributes(
                kinds=frozenset(kinds),
                group_name="datalake"
            )
        else:
            # Set group to "risingwave" for non-iceberg models
            new_spec = spec.replace_attributes(group_name="risingwave")
        
        # Add explicit deps if specified in meta
        if asset_deps:
            from dagster import AssetDep
            existing_deps = list(new_spec.deps) if new_spec.deps else []
            existing_keys = {dep.asset_key for dep in existing_deps}
            for new_dep_key in asset_deps:
                if new_dep_key not in existing_keys:
                    existing_deps.append(AssetDep(asset=new_dep_key))
            new_spec = new_spec.replace_attributes(deps=existing_deps)
        
        return new_spec


# Ensure manifest.json exists and is valid before defining the project
_manifest_path = Path(dbt_PROJECT_PATH) / "target" / "manifest.json"
_needs_compile = True

if _manifest_path.exists():
    try:
        with open(_manifest_path) as f:
            _existing_manifest = json.load(f)
        _has_child_map = "child_map" in _existing_manifest
        _has_nodes = "nodes" in _existing_manifest
        _nodes = _existing_manifest.get("nodes", {})
        _has_real_models = len(_nodes) > 0
        
        if _has_child_map and _has_nodes and _has_real_models:
            _needs_compile = False
            print(f"Valid manifest.json found with {len(_nodes)} models, skipping compile")
    except Exception as e:
        print(f"Error reading existing manifest: {e}, will recreate")

if _needs_compile:
    print("manifest.json not found or invalid, running dbt compile...")
    (_manifest_path.parent).mkdir(parents=True, exist_ok=True)
    
    _dbt_env = os.environ.copy()
    if "DBT_HOST" not in _dbt_env:
        _dbt_env["DBT_HOST"] = "risingwave-frontend"
    if "DBT_PROFILES_DIR" not in _dbt_env:
        _dbt_env["DBT_PROFILES_DIR"] = str(dbt_PROJECT_PATH)
    if "DBT_PASSWORD" not in _dbt_env:
        _dbt_env["DBT_PASSWORD"] = "root"
    
    result = subprocess.run(
        ["dbt", "compile", "--project-dir", str(dbt_PROJECT_PATH)],
        capture_output=True,
        text=True,
        cwd=str(dbt_PROJECT_PATH.parent),
        env=_dbt_env
    )
    if result.returncode != 0:
        print(f"Warning: dbt compile failed: {result.stderr}")
    else:
        print("dbt compile completed successfully")

dbt_project = DbtProject(
    project_dir=str(dbt_PROJECT_PATH),
)

# Create translator instance
custom_translator = CustomDagsterDbtTranslator()


@dbt_assets(manifest=dbt_project.manifest_path, dagster_dbt_translator=custom_translator)
def realtime_funnel_dbt_assets(context: AssetExecutionContext, dbt: DbtCliResource):
    """All dbt assets for the realtime funnel project."""
    context.log.info(f"Starting dbt build for project: {dbt_PROJECT_PATH}")
    
    # Check if we have actual models in the manifest
    manifest_path = Path(dbt_PROJECT_PATH) / "target" / "manifest.json"
    has_models = False
    try:
        with open(manifest_path) as f:
            manifest = json.load(f)
            has_models = len(manifest.get("nodes", {})) > 0
    except Exception:
        pass
    
    if not has_models:
        context.log.info("No dbt models found in manifest. Running dbt compile first...")
        _dbt_env = os.environ.copy()
        if "DBT_HOST" not in _dbt_env:
            _dbt_env["DBT_HOST"] = "risingwave-frontend"
        if "DBT_PROFILES_DIR" not in _dbt_env:
            _dbt_env["DBT_PROFILES_DIR"] = str(dbt_PROJECT_PATH)
        if "DBT_PASSWORD" not in _dbt_env:
            _dbt_env["DBT_PASSWORD"] = "root"
        
        result = subprocess.run(
            ["dbt", "compile", "--project-dir", str(dbt_PROJECT_PATH)],
            capture_output=True,
            text=True,
            cwd=str(dbt_PROJECT_PATH.parent),
            env=_dbt_env
        )
        if result.returncode != 0:
            context.log.error(f"dbt compile failed: {result.stderr}")
            raise Exception(f"dbt compile failed: {result.stderr}")
        context.log.info("dbt compile completed. Models will appear on next materialization.")
        return {"status": "initial_compile", "message": "Run again to materialize models"}
    
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

# ML Training Asset - depends on funnel_training dbt model
@asset(
    group_name="ml",
    description="Train ML models using last minute of funnel data and save to MinIO",
    deps=[AssetKey(["public", "funnel_training"])],
)
def ml_trained_models(context: AssetExecutionContext):
    """Asset that trains ML models and saves them to MinIO model registry."""
    import sys
    sys.path.insert(0, str(Path(__file__).parent.parent))
    
    from ml.training.trainer import ModelTrainer
    
    context.log.info("Starting ML model training...")
    
    try:
        trainer = ModelTrainer()
        results = trainer.train_all_metrics(minutes_back=4)
        
        if results and results.successful_models > 0:
            context.log.info(f"Successfully trained {results.successful_models} models")
            for metric, result in results.models.items():
                context.log.info(
                    f"  {metric}: {result.model_type}, "
                    f"r²={result.r2:.4f}, mae={result.mae:.4f}, "
                    f"version={result.version}"
                )
            return {
                "trained_models": results.successful_models,
                "total_models": results.total_models,
                "metrics": list(results.models.keys())
            }
        else:
            context.log.warning("No models were trained - insufficient data")
            return {"trained_models": 0, "metrics": []}
            
    except Exception as e:
        context.log.error(f"ML training failed: {e}")
        raise


# Define ML training job
ml_training_job = define_asset_job(
    name="ml_training_job",
    selection=[ml_trained_models],
    description="Train ML models on latest funnel data",
)

# Define Iceberg countries job
iceberg_countries_job = define_asset_job(
    name="iceberg_countries_job",
    selection=["iceberg_countries"],
    description="Create and populate Iceberg countries reference table",
)

# ML Training Schedule (5 minutes - Production)
ml_training_schedule = ScheduleDefinition(
    job=ml_training_job,
    cron_schedule="*/5 * * * *",
    name="ml_training_schedule",
    description="Train ML models every 5 minutes",
)

# Realtime Demo Schedule (20 seconds - Demo)
@sensor(
    job=ml_training_job,
    name="ml_training_sensor_realtime",
    description="Train ML models every 20 seconds for realtime demo",
    default_status=DefaultSensorStatus.STOPPED,
    minimum_interval_seconds=20,
)
def ml_training_sensor_realtime(context):
    """Sensor that triggers ML training every 20 seconds for demos."""
    timestamp = datetime.now(timezone.utc).strftime("%Y%m%d_%H%M%S")
    return RunRequest(run_key=f"ml_training_{timestamp}")


# Dagster definitions
defs = Definitions(
    assets=[
        # Yield iceberg_countries first (dependency of dbt assets)
        iceberg_countries,
        realtime_funnel_dbt_assets,
        ml_trained_models,
    ],
    jobs=[
        dbt_build_job,
        ml_training_job,
        iceberg_countries_job,
    ],
    schedules=[dbt_build_schedule, ml_training_schedule],
    sensors=[ml_training_sensor_realtime],
    resources={
        "dbt": DbtCliResource(project_dir=str(dbt_PROJECT_PATH)),
    },
)
