"""Dagster definitions for the realtime funnel dbt project."""
import logging
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
    AssetSelection,
)
from dagster_dbt import DbtCliResource, DbtProject, dbt_assets, DagsterDbtTranslator

from .constants import dbt_PROJECT_PATH
from .assets.iceberg_countries import iceberg_countries
from .assets.risingwave_udfs import risingwave_python_udfs
from .assets.postgres_sink_setup import postgres_funnel_table
from .assets.iceberg_compaction import iceberg_compaction_job, spark_session_resource
from .assets.casino_prd_setup import (
    casino_prd_proto_fetch,
    casino_prd_proto_compile,
    casino_prd_proto_upload,
    casino_trino_views,
)

# Set up logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(name)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

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
        logger.info(f"Validated/fixed manifest at {manifest_path}")


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
        
        # Check materialization type
        materialized = config.get("materialized", "")
        
        # If the model has 'iceberg' tag, add it as a kind
        if "iceberg" in tags:
            kinds = set(spec.kinds) if spec.kinds else set()
            kinds.add("iceberg")
            spec = spec.replace_attributes(kinds=frozenset(kinds))
        
        # Casino UC models get their own groups; shared sources go to casino_uc1
        if "casino_uc2" in tags and "casino_uc1" not in tags:
            new_spec = spec.replace_attributes(group_name="casino_uc2")
        elif "casino_uc1" in tags:
            new_spec = spec.replace_attributes(group_name="casino_uc1")
        # Assign group based on where the model runs (not its target)
        # Models materialized as sinks, materialized_views, or tables in RisingWave
        # go to the "risingwave" group, even if they target Iceberg
        elif materialized in ["sink", "materialized_view", "view", "table"]:
            new_spec = spec.replace_attributes(group_name="risingwave")
        elif "iceberg" in tags:
            # True Iceberg tables (not RisingWave objects targeting Iceberg)
            new_spec = spec.replace_attributes(group_name="datalake")
        else:
            # Default to risingwave group
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

        # Casino Kafka source tables depend on proto descriptors being in MinIO.
        # Declaring this here (on the AssetSpec) is required before the function
        # parameter on @dbt_assets can reference casino_prd_proto_upload.
        if materialized == "kafka_table" and (
            "casino_uc1" in tags or "casino_uc2" in tags
        ):
            from dagster import AssetDep
            existing_deps = list(new_spec.deps) if new_spec.deps else []
            proto_key = AssetKey(["casino_prd_proto_upload"])
            if proto_key not in {d.asset_key for d in existing_deps}:
                existing_deps.append(AssetDep(asset=proto_key))
            new_spec = new_spec.replace_attributes(deps=existing_deps)

        return new_spec


# Always regenerate manifest.json on code load via `dbt parse`, so model
# enable/disable, tag, and config changes are picked up without a manual
# recompile (this is what was missing — a stale manifest kept showing
# disabled models in Dagster).
#
# `dbt parse` rebuilds the manifest from the model files. In this project the
# on-run-start hooks (create_udfs / iceberg connection) may also fire, which
# touch RisingWave — they are idempotent, but if RisingWave/network is down at
# code-load time parse may fail. In that case we fall back to the existing
# manifest so the code location still loads.
_manifest_path = Path(dbt_PROJECT_PATH) / "target" / "manifest.json"
_manifest_path.parent.mkdir(parents=True, exist_ok=True)

_dbt_env = os.environ.copy()
_dbt_env.setdefault("DBT_HOST", "risingwave-frontend")
_dbt_env.setdefault("DBT_PROFILES_DIR", str(dbt_PROJECT_PATH))
_dbt_env.setdefault("DBT_PASSWORD", "root")

if not _manifest_path.exists():
    # No manifest at all — must parse on first load.
    logger.info("No manifest found — running `dbt parse` to generate it...")
    _parse = subprocess.run(
        ["dbt", "parse", "--no-partial-parse", "--project-dir", str(dbt_PROJECT_PATH)],
        capture_output=True, text=True,
        cwd=str(dbt_PROJECT_PATH.parent), env=_dbt_env,
    )
    if _parse.returncode != 0:
        logger.warning(f"`dbt parse` failed: {_parse.stderr.strip()[-300:]}")
    else:
        logger.info("`dbt parse` completed — manifest generated")
else:
    logger.info("Using existing manifest.json (run `dbt parse` manually to refresh after model changes)")

# Initialize DbtProject with explicit manifest path
dbt_project = DbtProject(
    project_dir=str(dbt_PROJECT_PATH),
)

# Verify manifest was loaded correctly
logger.info(f"DbtProject initialized with manifest: {dbt_project.manifest_path}")
if dbt_project.manifest_path.exists():
    try:
        with open(dbt_project.manifest_path) as f:
            manifest_check = json.load(f)
        nodes_count = len(manifest_check.get("nodes", {}))
        logger.info(f"Manifest loaded successfully with {nodes_count} nodes")
        if nodes_count == 0:
            logger.warning("WARNING: Manifest has 0 nodes, assets will not appear in Dagster UI")
    except Exception as e:
        logger.error(f"ERROR reading manifest: {e}")
else:
    logger.error(f"ERROR: Manifest not found at {dbt_project.manifest_path}")

# Create translator instance
custom_translator = CustomDagsterDbtTranslator()


@dbt_assets(manifest=dbt_project.manifest_path, dagster_dbt_translator=custom_translator, exclude="casino_prd")
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

    # Keep Dagster behavior consistent with bin/3_run_dbt.sh by dropping sinks
    # before build, so CREATE SINK IF NOT EXISTS definitions are refreshed.
    context.log.info("Dropping pre-build sinks via dbt run-operation...")
    _dbt_env = os.environ.copy()
    if "DBT_HOST" not in _dbt_env:
        _dbt_env["DBT_HOST"] = "risingwave-frontend"
    if "DBT_PROFILES_DIR" not in _dbt_env:
        _dbt_env["DBT_PROFILES_DIR"] = str(dbt_PROJECT_PATH)
    if "DBT_PASSWORD" not in _dbt_env:
        _dbt_env["DBT_PASSWORD"] = "root"

    drop_result = subprocess.run(
        [
            "dbt",
            "run-operation",
            "drop_prebuild_sinks",
            "--project-dir",
            str(dbt_PROJECT_PATH),
            "--profiles-dir",
            str(dbt_PROJECT_PATH),
        ],
        capture_output=True,
        text=True,
        cwd=str(dbt_PROJECT_PATH.parent),
        env=_dbt_env,
    )
    if drop_result.returncode != 0:
        context.log.error(f"dbt run-operation drop_prebuild_sinks failed: {drop_result.stderr}")
        raise Exception(f"dbt run-operation drop_prebuild_sinks failed: {drop_result.stderr}")
    context.log.info("Pre-build sinks dropped successfully")
    
    # Run dbt build and stream events
    yield from dbt.cli(["build"], context=context).stream()


@dbt_assets(
    manifest=dbt_project.manifest_path,
    dagster_dbt_translator=custom_translator,
    select="casino_prd",
    name="casino_prd_dbt_assets",
)
def casino_prd_dbt_assets(
    context: AssetExecutionContext,
    dbt: DbtCliResource,
    casino_prd_proto_upload,
):
    """dbt assets for Casino production — UC1 (Real Bet Amount) + UC2 (Turnover Percentage) in one step."""
    drop_result = subprocess.run(
        ["dbt", "run-operation", "drop_prebuild_sinks",
         "--project-dir", str(dbt_PROJECT_PATH),
         "--profiles-dir", str(dbt_PROJECT_PATH)],
        capture_output=True, text=True,
        cwd=str(dbt_PROJECT_PATH.parent), env=_dbt_env,
    )
    if drop_result.returncode != 0:
        context.log.warning(f"drop_prebuild_sinks failed (safe on fresh start): {drop_result.stderr[-200:]}")
    else:
        context.log.info("Pre-build casino sinks dropped")
    yield from dbt.cli(["build"], context=context).stream()
    # Ramp source rate limit from 1 (build-time) to 200 (steady-state).
    # Sources are created with rate_limit=1 so no data accumulates during MV
    # creation, keeping backfill time near zero. We ramp up after all MVs exist.
    import psycopg2
    try:
        rw_host = os.environ.get("DBT_HOST", "frontend-node-0")
        conn = psycopg2.connect(host=rw_host, port=4566, user="root", database="dev")
        conn.autocommit = True
        cur = conn.cursor()
        cur.execute("ALTER TABLE src_casino_prd SET source_rate_limit = 200")
        cur.execute("ALTER TABLE src_bets_br SET source_rate_limit = 200")
        conn.close()
        context.log.info("Source rate limits ramped to 200 rows/s")
    except Exception as e:
        context.log.warning(f"Failed to ramp source rate limits: {e}")


# Define jobs
dbt_build_job = define_asset_job(
    name="dbt_build_job",
    selection=[realtime_funnel_dbt_assets],
    description="Build all dbt models for the realtime funnel project",
)

# Job to setup PostgreSQL sink table and create the sink
# First creates the postgres table, then runs dbt (which creates the sink)
postgres_sink_job = define_asset_job(
    name="postgres_sink_job",
    selection=AssetSelection.assets(postgres_funnel_table) | AssetSelection.assets(realtime_funnel_dbt_assets),
    description="Create PostgreSQL table and RisingWave sink for funnel data",
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

casino_prd_full_job = define_asset_job(
    name="casino_prd_full_job",
    selection=(
        AssetSelection.assets(
            casino_prd_proto_fetch,
            casino_prd_proto_compile,
            casino_prd_proto_upload,
        )
        | AssetSelection.assets(casino_prd_dbt_assets)
        | AssetSelection.assets(casino_trino_views)
    ),
    description="End-to-end casino demo: proto setup → UC1 + UC2 → Trino views",
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
        # Create Python UDFs before dbt models run
        risingwave_python_udfs,
        # Create PostgreSQL table for RisingWave sink
        postgres_funnel_table,
        realtime_funnel_dbt_assets,
        ml_trained_models,
        # Casino production prerequisites
        casino_prd_proto_fetch,
        casino_prd_proto_compile,
        casino_prd_proto_upload,
        # Casino dbt assets (UC1 + UC2 in one step)
        casino_prd_dbt_assets,
        # Trino metadata views for Grafana (snapshot count, live data files)
        casino_trino_views,
    ],
    jobs=[
        dbt_build_job,
        ml_training_job,
        iceberg_countries_job,
        iceberg_compaction_job,
        postgres_sink_job,
        casino_prd_full_job,
    ],
    schedules=[
        dbt_build_schedule,
        ml_training_schedule,
    ],
    sensors=[ml_training_sensor_realtime],
    resources={
        "dbt": DbtCliResource(project_dir=str(dbt_PROJECT_PATH)),
        "spark": spark_session_resource,
    },
)
