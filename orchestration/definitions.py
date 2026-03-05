"""Dagster definitions for the realtime funnel dbt project."""
import os
import subprocess
import psycopg2
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


# CRITICAL: Validate/fix manifest BEFORE any DbtProject is instantiated
# This prevents KeyError when @dbt_assets decorator reads the manifest
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
    
    # Add a dummy model so @dbt_assets has something to work with initially
    # This gets replaced when dbt compile runs
    if not manifest.get("nodes"):
        manifest["nodes"]["model.funnel.dummy_init"] = {
            "resource_type": "model",
            "depends_on": {"macros": [], "nodes": []},
            "config": {"materialized": "view"},
            "name": "dummy_init",
            "package_name": "funnel",
            "path": "dummy_init.sql",
            "original_file_path": "models/dummy_init.sql",
            "unique_id": "model.funnel.dummy_init",
            "fqn": ["funnel", "dummy_init"],
            "alias": "dummy_init",
            "checksum": {"name": "sha256", "checksum": ""},
            "tags": [],
            "refs": [],
            "sources": [],
            "metrics": [],
            "description": "",
            "columns": {},
            "meta": {},
            "docs": {"show": True, "node_color": None},
            "patch_path": None,
            "compiled_path": None,
            "build_path": None,
            "deferred": False,
            "unrendered_config": {},
            "created_at": 1
        }
        manifest["child_map"]["model.funnel.dummy_init"] = []
        manifest["parent_map"]["model.funnel.dummy_init"] = []
        needs_write = True
    
    if needs_write:
        with open(manifest_path, 'w') as f:
            json.dump(manifest, f)
        print(f"Validated/fixed manifest at {manifest_path}")


# Run validation immediately on module load
_ensure_valid_manifest()


def run_sql_command(sql: str) -> tuple[bool, str]:
    """Run a SQL command against RisingWave using psycopg2."""
    conn = None
    try:
        conn = psycopg2.connect(
            host="risingwave-frontend",
            port=4566,
            database="dev",
            user="root"
        )
        conn.autocommit = True
        with conn.cursor() as cur:
            cur.execute(sql)
            # Fetch results if any
            if cur.description:
                result = cur.fetchall()
                return True, str(result)
            return True, "OK"
    except Exception as e:
        return False, str(e)
    finally:
        if conn:
            conn.close()


def check_schema_needs_cleanup() -> bool:
    """Check if sources exist with old schema (numeric instead of DOUBLE for amount)."""
    conn = None
    try:
        conn = psycopg2.connect(
            host="risingwave-frontend",
            port=4566,
            database="dev",
            user="root"
        )
        with conn.cursor() as cur:
            # Check if src_purchase exists and has numeric amount column
            cur.execute("""
                SELECT data_type 
                FROM information_schema.columns 
                WHERE table_name = 'src_purchase' AND column_name = 'amount'
            """)
            result = cur.fetchone()
            if result and result[0].lower() in ('numeric', 'decimal'):
                return True  # Old schema detected, need cleanup
        return False  # Schema is correct or source doesn't exist yet
    except Exception:
        return False  # Error means source probably doesn't exist
    finally:
        if conn:
            conn.close()


def recreate_sources():
    """Drop and recreate sources with correct schema (only if needed)."""
    # Sinks already dropped by caller
    
    # Drop existing sources (ignore errors if they don't exist)
    run_sql_command("DROP SOURCE IF EXISTS src_cart CASCADE;")
    run_sql_command("DROP SOURCE IF EXISTS src_purchase CASCADE;")
    run_sql_command("DROP SOURCE IF EXISTS src_page CASCADE;")
    
    # Recreate sources with correct schema
    run_sql_command("""
        CREATE SOURCE IF NOT EXISTS src_cart (
            user_id int,
            item_id varchar,
            event_time timestamp
        ) WITH (
            connector = 'kafka',
            topic = 'cart_events',
            properties.bootstrap.server = 'redpanda:9092',
            scan.startup.mode = 'earliest'
        ) FORMAT PLAIN ENCODE JSON;
    """)
    
    run_sql_command("""
        CREATE SOURCE IF NOT EXISTS src_purchase (
            user_id int,
            amount DOUBLE,
            event_time timestamp
        ) WITH (
            connector = 'kafka',
            topic = 'purchases',
            properties.bootstrap.server = 'redpanda:9092',
            scan.startup.mode = 'earliest'
        ) FORMAT PLAIN ENCODE JSON;
    """)
    
    run_sql_command("""
        CREATE SOURCE IF NOT EXISTS src_page (
            user_id int,
            page_id varchar,
            event_time timestamp
        ) WITH (
            connector = 'kafka',
            topic = 'page_views',
            properties.bootstrap.server = 'redpanda:9092',
            scan.startup.mode = 'earliest'
        ) FORMAT PLAIN ENCODE JSON;
    """)


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


# Ensure manifest.json exists and is valid before defining the project
_manifest_path = Path(dbt_PROJECT_PATH) / "target" / "manifest.json"
_needs_compile = True

if _manifest_path.exists():
    # Check if existing manifest is valid (has required fields and actual models)
    try:
        with open(_manifest_path) as f:
            _existing_manifest = json.load(f)
        _has_child_map = "child_map" in _existing_manifest
        _has_nodes = "nodes" in _existing_manifest
        _nodes = _existing_manifest.get("nodes", {})
        _has_real_models = len(_nodes) > 0
        # Check if we have models OTHER than just the dummy_init model
        _has_only_dummy = len(_nodes) == 1 and "model.funnel.dummy_init" in _nodes
        
        if _has_child_map and _has_nodes and _has_real_models and not _has_only_dummy:
            _needs_compile = False
            print(f"Valid manifest.json found with {len(_nodes)} models, skipping compile")
        elif _has_only_dummy:
            # Has only the dummy model - need to run dbt compile
            print("Manifest has only dummy model, will compile")
        elif _has_child_map and _has_nodes:
            # Has structure but no real models - need to compile
            print("Manifest exists but has no models, will compile")
        else:
            print("Existing manifest missing required fields, will recreate")
    except Exception as e:
        print(f"Error reading existing manifest: {e}, will recreate")

if _needs_compile:
    print("manifest.json not found or invalid, running dbt compile...")
    # Create target directory if it doesn't exist
    (_manifest_path.parent).mkdir(parents=True, exist_ok=True)
    
    # Set environment for dbt compile (use host from env or default to docker service)
    _dbt_env = os.environ.copy()
    if "DBT_HOST" not in _dbt_env:
        _dbt_env["DBT_HOST"] = "risingwave-frontend"
    # Ensure dbt can find profiles.yml and has password
    if "DBT_PROFILES_DIR" not in _dbt_env:
        _dbt_env["DBT_PROFILES_DIR"] = str(dbt_PROJECT_PATH)
    if "DBT_PASSWORD" not in _dbt_env:
        _dbt_env["DBT_PASSWORD"] = "root"
    
    result = subprocess.run(
        ["dbt", "compile", "--project-dir", str(dbt_PROJECT_PATH)],
        capture_output=True,
        text=True,
        cwd=str(dbt_PROJECT_PATH.parent),  # Run from project root
        env=_dbt_env
    )
    if result.returncode != 0:
        print(f"Warning: dbt compile failed: {result.stderr}")
        print(f"stdout: {result.stdout}")
        # Only create dummy manifest if there's NO existing manifest at all
        # Don't replace a valid manifest with a dummy one
        if not _manifest_path.exists():
            # Create a minimal manifest with a dummy model to satisfy @dbt_assets
            _manifest_path.write_text(json.dumps({
                "nodes": {
                    "model.funnel.dummy_init": {
                        "resource_type": "model",
                        "package_name": "funnel",
                        "path": "dummy_init.sql",
                        "original_file_path": "models/dummy_init.sql",
                        "unique_id": "model.funnel.dummy_init",
                        "fqn": ["funnel", "dummy_init"],
                        "alias": "dummy_init",
                        "checksum": {"name": "sha256", "checksum": ""},
                        "config": {
                            "enabled": True,
                            "alias": None,
                            "schema": None,
                            "database": None,
                            "tags": [],
                            "meta": {},
                            "group": None,
                            "materialized": "view",
                            "incremental_strategy": None,
                            "persist_docs": {},
                            "quoting": {},
                            "column_types": {},
                            "full_refresh": None,
                            "unique_key": None,
                            "on_schema_change": "ignore",
                            "grants": {},
                            "packages": [],
                            "docs": {"show": True, "node_color": None},
                            "contract": {"enforced": False},
                            "access": "protected"
                        },
                        "tags": [],
                        "description": "",
                        "columns": {},
                        "meta": {},
                        "group": None,
                        "docs": {"show": True, "node_color": None},
                        "patch_path": None,
                        "build_path": None,
                        "deferred": False,
                        "unrendered_config": {},
                        "created_at": 1704067200,
                        "compiled_code": "select 1 as id",
                        "extra_ctes_injected": True,
                        "extra_ctes": [],
                        "relation_name": "dummy_init"
                    }
                },
                "sources": {},
                "metrics": {},
                "exposures": {},
                "macros": {},
                "docs": {},
                "child_map": {"model.funnel.dummy_init": []},
                "parent_map": {"model.funnel.dummy_init": []},
                "groups": {},
                "selectors": {},
                "disabled": {},
                "metadata": {
                    "dbt_schema_version": "https://schemas.getdbt.com/dbt/manifest/v12.json",
                    "dbt_version": "1.10.0",
                    "generated_at": datetime.now(timezone.utc).isoformat(),
                    "invocation_id": "init",
                    "env": {}
                }
            }))
            print("Created minimal manifest with dummy model to allow Dagster to start")
        else:
            print("Keeping existing manifest (dbt compile failed but manifest exists)")
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
        # Set environment for dbt compile
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
        # Return early - models will be available on next run after reload
        return {"status": "initial_compile", "message": "Run again to materialize models"}
    
    # Always drop sinks first (they depend on sources and prevent source recreation)
    context.log.info("Dropping existing sinks...")
    run_sql_command("DROP SINK IF EXISTS iceberg_cart_events_sink CASCADE;")
    run_sql_command("DROP SINK IF EXISTS iceberg_purchases_sink CASCADE;")
    run_sql_command("DROP SINK IF EXISTS iceberg_page_views_sink CASCADE;")
    context.log.info("Sinks dropped")
    
    # Check if we need to recreate sources due to schema changes
    if check_schema_needs_cleanup():
        context.log.info("Schema mismatch detected (old 'numeric' type found). Recreating sources...")
        recreate_sources()
        context.log.info("Sources recreated with correct schema")
    else:
        context.log.info("No schema changes detected, skipping source recreation")
    
    # Run dbt clean first to clear cached compiled files
    context.log.info("Running dbt clean to clear target directory...")
    result = subprocess.run(
        ["dbt", "clean", "--project-dir", str(dbt_PROJECT_PATH)],
        capture_output=True,
        text=True
    )
    if result.returncode != 0:
        context.log.warning(f"dbt clean output: {result.stdout}")
        context.log.warning(f"dbt clean errors: {result.stderr}")
    else:
        context.log.info("dbt clean completed successfully")
    
    # Regenerate manifest.json after clean (needed for Dagster to load definitions)
    context.log.info("Running dbt compile to regenerate manifest...")
    result = subprocess.run(
        ["dbt", "compile", "--project-dir", str(dbt_PROJECT_PATH)],
        capture_output=True,
        text=True
    )
    if result.returncode != 0:
        context.log.warning(f"dbt compile output: {result.stdout}")
        context.log.warning(f"dbt compile errors: {result.stderr}")
    else:
        context.log.info("dbt compile completed successfully")
    
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
# The asset key for dbt models is [schema, model_name] = ["public", "funnel_training"]
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
        results = trainer.train_all_metrics(minutes_back=1)
        
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

# Option 1: Standard Schedule (5 minutes - Production)
ml_training_schedule_standard = ScheduleDefinition(
    job=ml_training_job,
    cron_schedule="*/5 * * * *",
    name="ml_training_schedule_standard",
    description="Train ML models every 5 minutes",
)

# Option 2: Realtime Demo Schedule (20 seconds - Demo)
# Note: Dagster's minimum cron interval is 1 minute, so we use a sensor for sub-minute intervals
@sensor(
    job=ml_training_job,
    name="ml_training_sensor_realtime",
    description="Train ML models every 20 seconds for realtime demo",
    default_status=DefaultSensorStatus.STOPPED,  # Disabled by default
    minimum_interval_seconds=20,
)
def ml_training_sensor_realtime(context):
    """Sensor that triggers ML training every 20 seconds for demos.
    
    Enable this sensor in the Dagster UI for realtime demos.
    Disable it and use the standard schedule for normal operation.
    """
    # Use timestamp-based run_key for realtime training
    # This ensures a new run is triggered every tick
    from datetime import datetime, timezone
    timestamp = datetime.now(timezone.utc).strftime("%Y%m%d_%H%M%S")
    return RunRequest(run_key=f"ml_training_{timestamp}")


# Always include both schedule and sensor in definitions
# Sensor is stopped by default, can be enabled in UI
ml_schedules = [ml_training_schedule_standard]
ml_sensors = [ml_training_sensor_realtime]


# Dagster definitions
defs = Definitions(
    assets=[realtime_funnel_dbt_assets, ml_trained_models],
    jobs=[dbt_build_job, ml_training_job],
    schedules=[dbt_build_schedule] + ml_schedules,
    sensors=ml_sensors,
    resources={
        "dbt": DbtCliResource(project_dir=str(dbt_PROJECT_PATH)),
    },
)
