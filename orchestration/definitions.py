"""Dagster definitions for the realtime funnel dbt project."""
import subprocess
import psycopg2
from pathlib import Path
from dagster import Definitions, define_asset_job, ScheduleDefinition, AssetExecutionContext
from dagster_dbt import DbtCliResource, DbtProject, dbt_assets, DagsterDbtTranslator

from .constants import dbt_PROJECT_PATH


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


# Ensure manifest.json exists before defining the project
_manifest_path = Path(dbt_PROJECT_PATH) / "target" / "manifest.json"
if not _manifest_path.exists():
    print("manifest.json not found, running dbt compile...")
    result = subprocess.run(
        ["dbt", "compile", "--project-dir", str(dbt_PROJECT_PATH)],
        capture_output=True,
        text=True
    )
    if result.returncode != 0:
        print(f"Warning: dbt compile failed: {result.stderr}")
    else:
        print("dbt compile completed successfully")

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

# Dagster definitions
defs = Definitions(
    assets=[realtime_funnel_dbt_assets],
    jobs=[dbt_build_job],
    schedules=[dbt_build_schedule],
    resources={
        "dbt": DbtCliResource(project_dir=str(dbt_PROJECT_PATH)),
    },
)
